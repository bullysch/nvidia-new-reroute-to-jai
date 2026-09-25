import express from "express";

const app = express();

const PORT = Number(process.env.PORT || 10000);

const NVIDIA_API_KEY = process.env.NVIDIA_API_KEY;
const PROXY_API_KEY = process.env.PROXY_API_KEY || "";

const NVIDIA_BASE_URL =
  process.env.NVIDIA_BASE_URL ||
  "https://integrate.api.nvidia.com/v1";

const DEFAULT_MODEL =
  process.env.DEFAULT_MODEL ||
  "openai/gpt-oss-20b";

if (!NVIDIA_API_KEY) {
  console.error("ERROR: NVIDIA_API_KEY is not configured.");
  process.exit(1);
}

app.use(express.json({ limit: "10mb" }));

/*
 * CORS
 */
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Authorization, Content-Type"
  );
  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, OPTIONS"
  );

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

/*
 * Proxy authentication
 */
function authenticate(req, res, next) {
  if (!PROXY_API_KEY) {
    return next();
  }

  const auth = req.headers.authorization || "";

  if (auth !== `Bearer ${PROXY_API_KEY}`) {
    return res.status(401).json({
      error: {
        message: "Invalid proxy API key",
        type: "invalid_request_error",
        code: "invalid_api_key"
      }
    });
  }

  next();
}

/*
 * Health checks
 */
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    service: "janitor-nvidia-proxy"
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok"
  });
});

/*
 * OpenAI-compatible model list
 */
app.get("/v1/models", authenticate, (req, res) => {
  res.json({
    object: "list",
    data: [
      {
        id: DEFAULT_MODEL,
        object: "model",
        created: Math.floor(Date.now() / 1000),
        owned_by: "nvidia"
      }
    ]
  });
});

/*
 * Convert NVIDIA's streaming response into a clean
 * OpenAI-compatible SSE stream.
 *
 * IMPORTANT:
 * NVIDIA may include reasoning-related information.
 * We intentionally forward ONLY:
 *
 *   choices[].delta.content
 *
 * and discard reasoning_content.
 */
async function proxyStream(upstreamResponse, res) {
  res.status(upstreamResponse.status);

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");

  if (!upstreamResponse.body) {
    res.end();
    return;
  }

  const reader = upstreamResponse.body.getReader();
  const decoder = new TextDecoder();

  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      /*
       * NVIDIA sends SSE events separated by blank lines.
       */
      const events = buffer.split(/\r?\n\r?\n/);

      /*
       * Keep the final incomplete event for the next chunk.
       */
      buffer = events.pop() || "";

      for (const event of events) {
        const lines = event.split(/\r?\n/);

        for (const line of lines) {
          if (!line.startsWith("data:")) {
            continue;
          }

          const data = line.slice(5).trim();

          if (!data) {
            continue;
          }

          /*
           * NVIDIA terminates the stream with [DONE].
           */
          if (data === "[DONE]") {
            res.write("data: [DONE]\\n\\n");
            continue;
          }

          let json;

          try {
            json = JSON.parse(data);
          } catch {
            /*
             * Ignore malformed/incomplete SSE data.
             */
            continue;
          }

          /*
           * Extract ONLY normal assistant content.
           */
          const choices = json.choices || [];

          for (const choice of choices) {
            const delta = choice.delta || {};

            /*
             * Deliberately ignore:
             *
             * delta.reasoning_content
             *
             * reasoning_content
             *
             * other NVIDIA-specific fields.
             */
            if (typeof delta.content === "string" && delta.content.length > 0) {
              const cleanChunk = {
                id: json.id || `chatcmpl-${Date.now()}`,
                object: "chat.completion.chunk",
                created: json.created || Math.floor(Date.now() / 1000),
                model: json.model || DEFAULT_MODEL,
                choices: [
                  {
                    index: choice.index ?? 0,
                    delta: {
                      content: delta.content
                    },
                    finish_reason: choice.finish_reason ?? null
                  }
                ]
              };

              res.write(
                `data: ${JSON.stringify(cleanChunk)}\n\n`
              );
            }

            /*
             * Forward the finish signal even if there is no content.
             */
            if (choice.finish_reason) {
              const finishChunk = {
                id: json.id || `chatcmpl-${Date.now()}`,
                object: "chat.completion.chunk",
                created: json.created || Math.floor(Date.now() / 1000),
                model: json.model || DEFAULT_MODEL,
                choices: [
                  {
                    index: choice.index ?? 0,
                    delta: {},
                    finish_reason: choice.finish_reason
                  }
                ]
              };

              res.write(
                `data: ${JSON.stringify(finishChunk)}\n\n`
              );
            }
          }
        }
      }
    }

    /*
     * Process anything left in the buffer.
     */
    if (buffer.trim()) {
      const lines = buffer.split(/\r?\n/);

      for (const line of lines) {
        if (!line.startsWith("data:")) {
          continue;
        }

        const data = line.slice(5).trim();

        if (!data || data === "[DONE]") {
          continue;
        }

        try {
          const json = JSON.parse(data);

          for (const choice of json.choices || []) {
            const delta = choice.delta || {};

            if (
              typeof delta.content === "string" &&
              delta.content.length > 0
            ) {
              const cleanChunk = {
                id: json.id || `chatcmpl-${Date.now()}`,
                object: "chat.completion.chunk",
                created:
                  json.created || Math.floor(Date.now() / 1000),
                model: json.model || DEFAULT_MODEL,
                choices: [
                  {
                    index: choice.index ?? 0,
                    delta: {
                      content: delta.content
                    },
                    finish_reason: choice.finish_reason ?? null
                  }
                ]
              };

              res.write(
                `data: ${JSON.stringify(cleanChunk)}\n\n`
              );
            }
          }
        } catch {
          // Ignore incomplete trailing data.
        }
      }
    }

    /*
     * Always terminate the OpenAI-compatible stream.
     */
    res.write("data: [DONE]\n\n");

  } catch (error) {
    console.error("Streaming error:", error);

  } finally {
    res.end();
  }
}

/*
 * Main chat completions endpoint
 */
app.post("/v1/chat/completions", authenticate, async (req, res) => {
  try {
    const body = req.body || {};

    if (!Array.isArray(body.messages)) {
      return res.status(400).json({
        error: {
          message: "messages must be an array",
          type: "invalid_request_error",
          code: "invalid_messages"
        }
      });
    }

    const model = body.model || DEFAULT_MODEL;

    /*
     * Copy the request.
     */
    const upstreamBody = {
      ...body,
      model
    };

    /*
     * GPT-OSS supports low/medium/high reasoning effort.
     *
     * If JanitorAI doesn't provide one, use LOW rather than
     * NVIDIA's default MEDIUM. This reduces unnecessary
     * reasoning tokens and latency.
     */
    if (
      model === "openai/gpt-oss-20b" &&
      !upstreamBody.reasoning_effort
    ) {
      upstreamBody.reasoning_effort = "low";
    }

    const upstreamResponse = await fetch(
      `${NVIDIA_BASE_URL}/chat/completions`,
      {
        method: "POST",

        headers: {
          "Authorization": `Bearer ${NVIDIA_API_KEY}`,
          "Content-Type": "application/json",
          "Accept": body.stream
            ? "text/event-stream"
            : "application/json"
        },

        body: JSON.stringify(upstreamBody)
      }
    );

    /*
     * If NVIDIA returned an error, pass it through unchanged.
     */
    if (!upstreamResponse.ok) {
      const errorText = await upstreamResponse.text();

      console.error(
        `NVIDIA returned HTTP ${upstreamResponse.status}:`,
        errorText
      );

      res.status(upstreamResponse.status);

      res.setHeader(
        "Content-Type",
        upstreamResponse.headers.get("content-type") ||
          "application/json"
      );

      return res.send(errorText);
    }

    /*
     * Streaming
     */
    if (body.stream) {
      return await proxyStream(upstreamResponse, res);
    }

    /*
     * Normal non-streaming request.
     *
     * For non-streaming responses, return NVIDIA's JSON.
     * The response's normal message.content is what JanitorAI
     * needs.
     */
    const contentType =
      upstreamResponse.headers.get("content-type") || "";

    const text = await upstreamResponse.text();

    res.status(upstreamResponse.status);

    if (contentType.includes("application/json")) {
      res.setHeader("Content-Type", "application/json");
      return res.send(text);
    }

    return res.send(text);

  } catch (error) {
    console.error("Proxy error:", error);

    return res.status(502).json({
      error: {
        message: "Unable to reach NVIDIA API",
        type: "proxy_error",
        code: "upstream_unavailable"
      }
    });
  }
});

/*
 * 404
 */
app.use((req, res) => {
  res.status(404).json({
    error: {
      message: "Not found",
      type: "invalid_request_error"
    }
  });
});

/*
 * Start server
 */
app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Janitor NVIDIA proxy listening on port ${PORT}`
  );
});

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
 *
 * JanitorAI sends PROXY_API_KEY.
 * The proxy then uses NVIDIA_API_KEY to contact NVIDIA.
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
 * Health check
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
 * Clean NVIDIA SSE stream.
 *
 * NVIDIA can send reasoning information such as:
 *
 *   reasoning_content
 *
 * JanitorAI should receive the actual assistant response,
 * not the reasoning stream.
 *
 * Therefore this function forwards ONLY:
 *
 *   choices[].delta.content
 *
 * Everything else is ignored.
 */
async function proxyStream(upstreamResponse, res) {
  res.status(upstreamResponse.status);

  res.setHeader(
    "Content-Type",
    "text/event-stream; charset=utf-8"
  );

  res.setHeader(
    "Cache-Control",
    "no-cache, no-transform"
  );

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

      buffer += decoder.decode(value, {
        stream: true
      });

      /*
       * SSE events are separated by a blank line.
       */
      const events = buffer.split(/\r?\n\r?\n/);

      /*
       * Keep the final incomplete event for the
       * next network chunk.
       */
      buffer = events.pop() || "";

      for (const event of events) {
        /*
         * Extract only data: lines.
         */
        const dataLines = event
          .split(/\r?\n/)
          .filter(line => line.startsWith("data:"));

        if (dataLines.length === 0) {
          continue;
        }

        /*
         * SSE allows multiple data: lines.
         */
        const data = dataLines
          .map(line => line.slice(5).trimStart())
          .join("\n")
          .trim();

        if (!data) {
          continue;
        }

        /*
         * NVIDIA's normal stream termination.
         */
        if (data === "[DONE]") {
          res.write("data: [DONE]\n\n");
          continue;
        }

        let json;

        try {
          json = JSON.parse(data);
        } catch (error) {
          /*
           * Some upstream SSE events may not contain JSON.
           * Ignore them rather than breaking the entire stream.
           */
          console.warn(
            "Ignoring non-JSON NVIDIA SSE event:",
            JSON.stringify(data)
          );

          continue;
        }

        /*
         * Process each choice.
         */
        for (const choice of json.choices || []) {
          const delta = choice.delta || {};

          /*
           * IMPORTANT:
           *
           * We intentionally DO NOT forward:
           *
           *   delta.reasoning_content
           *
           * Only normal assistant content is sent to JanitorAI.
           */
          if (
            typeof delta.content === "string" &&
            delta.content.length > 0
          ) {
            const cleanChunk = {
              id:
                json.id ||
                `chatcmpl-${Date.now()}`,

              object: "chat.completion.chunk",

              created:
                json.created ||
                Math.floor(Date.now() / 1000),

              model:
                json.model ||
                DEFAULT_MODEL,

              choices: [
                {
                  index: choice.index ?? 0,

                  delta: {
                    content: delta.content
                  },

                  finish_reason: null
                }
              ]
            };

            res.write(
              `data: ${JSON.stringify(cleanChunk)}\n\n`
            );
          }

          /*
           * Forward the final finish reason.
           */
          if (choice.finish_reason) {
            const finishChunk = {
              id:
                json.id ||
                `chatcmpl-${Date.now()}`,

              object: "chat.completion.chunk",

              created:
                json.created ||
                Math.floor(Date.now() / 1000),

              model:
                json.model ||
                DEFAULT_MODEL,

              choices: [
                {
                  index: choice.index ?? 0,

                  delta: {},

                  finish_reason:
                    choice.finish_reason
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

    /*
     * Flush any remaining decoder data.
     */
    buffer += decoder.decode();

  } catch (error) {
    console.error(
      "Streaming error:",
      error
    );

  } finally {
    /*
     * Always terminate the OpenAI-compatible stream.
     */
    res.write(
      "data: [DONE]\n\n"
    );

    res.end();
  }
}

/*
 * Main chat-completions endpoint
 */
app.post(
  "/v1/chat/completions",
  authenticate,
  async (req, res) => {
    try {
      const body = req.body || {};

      /*
       * Validate messages.
       */
      if (!Array.isArray(body.messages)) {
        return res.status(400).json({
          error: {
            message:
              "messages must be an array",

            type:
              "invalid_request_error",

            code:
              "invalid_messages"
          }
        });
      }

      /*
       * Use the model supplied by JanitorAI.
       * Fall back to DEFAULT_MODEL if none was supplied.
       */
      const model =
        body.model ||
        DEFAULT_MODEL;

      /*
       * Forward the request to NVIDIA.
       *
       * We intentionally don't force a reasoning_effort
       * value here. JanitorAI's request is preserved.
       */
      const upstreamBody = {
        ...body,
        model
      };

      /*
       * Contact NVIDIA.
       */
      const upstreamResponse =
        await fetch(
          `${NVIDIA_BASE_URL}/chat/completions`,
          {
            method: "POST",

            headers: {
              "Authorization":
                `Bearer ${NVIDIA_API_KEY}`,

              "Content-Type":
                "application/json",

              "Accept":
                body.stream
                  ? "text/event-stream"
                  : "application/json"
            },

            body:
              JSON.stringify(
                upstreamBody
              )
          }
        );

      /*
       * NVIDIA returned an error.
       *
       * Pass the error back to JanitorAI so that
       * it can display the actual NVIDIA error.
       */
      if (!upstreamResponse.ok) {
        const errorText =
          await upstreamResponse.text();

        console.error(
          `NVIDIA returned HTTP ${upstreamResponse.status}:`,
          errorText
        );

        res.status(
          upstreamResponse.status
        );

        res.setHeader(
          "Content-Type",
          upstreamResponse.headers.get(
            "content-type"
          ) ||
            "application/json"
        );

        return res.send(
          errorText
        );
      }

      /*
       * STREAMING REQUEST
       */
      if (body.stream) {
        return await proxyStream(
          upstreamResponse,
          res
        );
      }

      /*
       * NON-STREAMING REQUEST
       */
      const contentType =
        upstreamResponse.headers.get(
          "content-type"
        ) || "";

      const text =
        await upstreamResponse.text();

      res.status(
        upstreamResponse.status
      );

      if (
        contentType.includes(
          "application/json"
        )
      ) {
        res.setHeader(
          "Content-Type",
          "application/json"
        );

        return res.send(
          text
        );
      }

      return res.send(
        text
      );

    } catch (error) {
      console.error(
        "Proxy error:",
        error
      );

      return res.status(502).json({
        error: {
          message:
            "Unable to reach NVIDIA API",

          type:
            "proxy_error",

          code:
            "upstream_unavailable"
        }
      });
    }
  }
);

/*
 * Unknown endpoint
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
app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `Janitor NVIDIA proxy listening on port ${PORT}`
    );
  }
);

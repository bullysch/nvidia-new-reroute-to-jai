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
 *
 * JanitorAI may make browser-side requests depending on its
 * current API/proxy implementation.
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
 * Simple proxy authentication.
 *
 * If PROXY_API_KEY is empty, authentication is disabled.
 * For a public Render deployment, KEEP THIS SET.
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
 * OpenAI-compatible model listing.
 *
 * JanitorAI can use this to discover the configured model.
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
 * Main chat-completions endpoint.
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

    /*
     * Use the model JanitorAI sends if present.
     * Otherwise use DEFAULT_MODEL.
     */
    const model = body.model || DEFAULT_MODEL;

    /*
     * Forward essentially the complete OpenAI-style request.
     *
     * This means things such as:
     * messages
     * temperature
     * top_p
     * max_tokens
     * stop
     * stream
     * frequency_penalty
     * presence_penalty
     * seed
     * etc.
     *
     * are preserved.
     */
    const upstreamBody = {
      ...body,
      model
    };

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
     * Streaming response
     */
    if (body.stream) {
      res.status(upstreamResponse.status);

      res.setHeader(
        "Content-Type",
        upstreamResponse.headers.get("content-type") ||
          "text/event-stream"
      );

      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");
      res.setHeader("X-Accel-Buffering", "no");

      if (!upstreamResponse.body) {
        return res.end();
      }

      const reader = upstreamResponse.body.getReader();

      try {
        while (true) {
          const { done, value } = await reader.read();

          if (done) break;

          res.write(Buffer.from(value));
        }
      } catch (error) {
        console.error("Streaming error:", error);
      } finally {
        res.end();
      }

      return;
    }

    /*
     * Normal non-streaming response.
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
 * Prevent accidental exposure of arbitrary NVIDIA endpoints.
 */
app.use((req, res) => {
  res.status(404).json({
    error: {
      message: "Not found",
      type: "invalid_request_error"
    }
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Janitor NVIDIA proxy listening on port ${PORT}`);
});

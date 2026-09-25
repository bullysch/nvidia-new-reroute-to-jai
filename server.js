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
  "z-ai/glm-5.3";

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
 * Streaming proxy
 *
 * We preserve NVIDIA's SSE data rather than rebuilding it.
 *
 * This is important because JanitorAI expects a normal
 * OpenAI-compatible stream.
 */
async function proxyStream(upstreamResponse, res) {
  res.status(upstreamResponse.status);

  res.setHeader(
    "Content-Type",
    upstreamResponse.headers.get("content-type") ||
      "text/event-stream"
  );

  res.setHeader(
    "Cache-Control",
    "no-cache, no-transform"
  );

  res.setHeader(
    "Connection",
    "keep-alive"
  );

  res.setHeader(
    "X-Accel-Buffering",
    "no"
  );

  if (!upstreamResponse.body) {
    console.error("NVIDIA returned no response body.");

    res.end();
    return;
  }

  const reader =
    upstreamResponse.body.getReader();

  try {
    while (true) {
      const {
        done,
        value
      } = await reader.read();

      if (done) {
        break;
      }

      if (value) {
        res.write(
          Buffer.from(value)
        );
      }
    }

  } catch (error) {
    /*
     * A client disconnecting is not necessarily an error.
     */
    console.error(
      "Streaming error:",
      error
    );

  } finally {
    res.end();
  }
}

/*
 * Chat completions
 */
app.post(
  "/v1/chat/completions",
  authenticate,
  async (req, res) => {

    try {
      const body =
        req.body || {};

      if (
        !Array.isArray(
          body.messages
        )
      ) {
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
       * Use JanitorAI's requested model
       * or DEFAULT_MODEL.
       */
      const model =
        body.model ||
        DEFAULT_MODEL;

      /*
       * Preserve the entire JanitorAI request.
       */
      const upstreamBody = {
        ...body,
        model
      };

      console.log(
        "Request:",
        JSON.stringify({
          model,
          stream:
            Boolean(body.stream),
          messageCount:
            body.messages.length
        })
      );

      /*
       * Send request to NVIDIA.
       */
      const upstreamResponse =
        await fetch(
          `${NVIDIA_BASE_URL}/chat/completions`,
          {
            method: "POST",

            headers: {
              Authorization:
                `Bearer ${NVIDIA_API_KEY}`,

              "Content-Type":
                "application/json",

              Accept:
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

      console.log(
        "NVIDIA response:",
        upstreamResponse.status,
        upstreamResponse.statusText
      );

      /*
       * Handle NVIDIA errors.
       */
      if (!upstreamResponse.ok) {
        const errorText =
          await upstreamResponse.text();

        console.error(
          "NVIDIA API error:",
          errorText
        );

        res.status(
          upstreamResponse.status
        );

        res.setHeader(
          "Content-Type",
          "application/json"
        );

        return res.send(
          errorText
        );
      }

      /*
       * Streaming request.
       */
      if (body.stream) {
        return await proxyStream(
          upstreamResponse,
          res
        );
      }

      /*
       * Non-streaming request.
       */
      const contentType =
        upstreamResponse.headers.get(
          "content-type"
        ) ||
        "application/json";

      const responseText =
        await upstreamResponse.text();

      res.status(
        upstreamResponse.status
      );

      res.setHeader(
        "Content-Type",
        contentType
      );

      return res.send(
        responseText
      );

    } catch (error) {

      console.error(
        "Proxy error:",
        error
      );

      if (!res.headersSent) {
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

      res.end();
    }
  }
);

/*
 * 404
 */
app.use(
  (req, res) => {
    res.status(404).json({
      error: {
        message:
          "Not found",

        type:
          "invalid_request_error"
      }
    });
  }
);

/*
 * Graceful Render shutdown
 */
function shutdown(signal) {
  console.log(
    `${signal} received. Shutting down...`
  );

  server.close(() => {
    console.log(
      "HTTP server closed."
    );

    process.exit(0);
  });

  /*
   * Don't wait forever for open
   * connections.
   */
  setTimeout(() => {
    process.exit(0);
  }, 5000).unref();
}

const server =
  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `Janitor NVIDIA proxy listening on port ${PORT}`
      );
    }
  );

process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);

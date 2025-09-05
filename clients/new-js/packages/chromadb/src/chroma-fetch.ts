import {
  ChromaClientError,
  ChromaConnectionError,
  ChromaForbiddenError,
  ChromaNotFoundError,
  ChromaQuotaExceededError,
  ChromaRateLimitError,
  ChromaUnauthorizedError,
  ChromaUniqueError,
} from "./errors";
import { defaultRetryConfig, RetryConfig } from "./retry";

const RETRYABLE_STATUS = new Set([502, 503, 504]);

const isRetryableError = (error: unknown): boolean => {
  if (!error || typeof error !== "object") {
    return false;
  }

  const name = (error as { name?: unknown }).name;
  return name === "TypeError" || name === "FetchError";
};

const CONNECTION_ERROR_MESSAGE =
  "Failed to connect to chromadb. Make sure your server is running and try again. If you are running from a browser, make sure that your chromadb instance is configured to allow requests from the current origin using the CHROMA_SERVER_CORS_ALLOW_ORIGINS environment variable.";

const shouldRetryResponse = (status: number): boolean =>
  RETRYABLE_STATUS.has(status);

const shouldRetryError = (error: unknown): boolean => isRetryableError(error);

const computeDelaySeconds = (config: RetryConfig, attempt: number): number => {
  const exponent = Math.max(attempt - 1, 0);
  const exponentialDelay = config.minDelay * Math.pow(config.factor, exponent);
  const capped = Math.min(config.maxDelay, Math.max(config.minDelay, exponentialDelay));
  if (!config.jitter) {
    return capped;
  }
  return Math.random() * capped;
};

const sleep = async (seconds: number): Promise<void> => {
  if (seconds <= 0) {
    return;
  }
  await new Promise((resolve) => setTimeout(resolve, seconds * 1000));
};

const buildConnectionError = (error: unknown): ChromaConnectionError =>
  new ChromaConnectionError(CONNECTION_ERROR_MESSAGE, error);

const throwForResponse = async (
  response: Response,
  input: RequestInfo | URL,
): Promise<never> => {
  switch (response.status) {
    case 400: {
      let status = "Bad Request";
      try {
        const responseBody = await response.json();
        status = responseBody.message || status;
      } catch {}
      throw new ChromaClientError(
        `Bad request to ${(input as Request).url || "Chroma"
        } with status: ${status}`,
      );
    }
    case 401:
      throw new ChromaUnauthorizedError(`Unauthorized`);
    case 403:
      throw new ChromaForbiddenError(
        `You do not have permission to access the requested resource.`,
      );
    case 404:
      throw new ChromaNotFoundError(
        `The requested resource could not be found`,
      );
    case 409:
      throw new ChromaUniqueError("The resource already exists");
    case 422: {
      try {
        const body = await response.json();
        if (
          body &&
          body.message &&
          (body.message.startsWith("Quota exceeded") ||
            body.message.startsWith("Billing limit exceeded"))
        ) {
          throw new ChromaQuotaExceededError(body?.message);
        }
      } catch {}
      break;
    }
    case 429:
      throw new ChromaRateLimitError("Rate limit exceeded");
  }

  throw new ChromaConnectionError(
    `Unable to connect to the chromadb server (status: ${response.status}). Please try again later.`,
  );
};

export const createChromaFetch = (options?: {
  retryConfig?: RetryConfig | null;
}): typeof fetch => {
  const userConfig = options?.retryConfig;
  const retriesEnabled = userConfig !== null;
  const config = userConfig ?? defaultRetryConfig;
  const maxAttempts = retriesEnabled
    ? Math.max(config.maxAttempts, 1)
    : 1;

  return async (input, init) => {
    let attempt = 0;
    let lastError: unknown;

    while (attempt < maxAttempts) {
      attempt += 1;
      try {
        const response = await fetch(input, init);
        if (response.ok) {
          return response;
        }

        if (retriesEnabled && shouldRetryResponse(response.status)) {
          if (attempt < maxAttempts) {
            try {
              response.body?.cancel();
            } catch {}
            await sleep(computeDelaySeconds(config, attempt));
            continue;
          }
        }

        await throwForResponse(response, input);
      } catch (err) {
        lastError = err;
        if (retriesEnabled && shouldRetryError(err) && attempt < maxAttempts) {
          await sleep(computeDelaySeconds(config, attempt));
          continue;
        }

        break;
      }
    }

    if (!lastError) {
      throw new ChromaConnectionError(CONNECTION_ERROR_MESSAGE);
    }

    throw buildConnectionError(lastError);
  };
};

export const chromaFetch = createChromaFetch();

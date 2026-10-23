import {
  ChromaBackoffError,
  ChromaClientError,
  ChromaConnectionError,
  ChromaConditionalWriteConflictError,
  ChromaError,
  ChromaForbiddenError,
  ChromaNotFoundError,
  ChromaQuotaExceededError,
  ChromaRateLimitError,
  ChromaServerError,
  ChromaStaleReadError,
  ChromaUnauthorizedError,
  ChromaUniqueError,
} from "./errors";

type ErrorBody = {
  error?: string;
  message?: string;
};

const offlineError = (error: any): boolean => {
  return Boolean(
    (error?.name === "TypeError" || error?.name === "FetchError") &&
      (error.message?.includes("fetch failed") ||
        error.message?.includes("Failed to fetch") ||
        error.message?.includes("ENOTFOUND")),
  );
};

const getErrorBody = async (response: Response): Promise<ErrorBody> => {
  try {
    return await response.clone().json();
  } catch {
    return {};
  }
};

const getErrorMessage = async (response: Response): Promise<string> => {
  const body = await getErrorBody(response);
  return (
    body.message || body.error || `${response.status}: ${response.statusText}`
  );
};

export const chromaFetch: typeof fetch = async (input, init) => {
  let response: Response;
  try {
    response = await fetch(input, init);
  } catch (err) {
    if (offlineError(err)) {
      throw new ChromaConnectionError(
        "Failed to connect to chromadb. Make sure your server is running and try again. If you are running from a browser, make sure that your chromadb instance is configured to allow requests from the current origin using the CHROMA_SERVER_CORS_ALLOW_ORIGINS environment variable.",
      );
    }
    throw new ChromaConnectionError("Failed to connect to Chroma");
  }

  if (response.ok) {
    return response;
  }

  switch (response.status) {
    case 400:
      let status = "Bad Request";
      try {
        const responseBody = await response.json();
        status = responseBody.message || status;
      } catch {}
      throw new ChromaClientError(
        `Bad request to ${
          (input as Request).url || "Chroma"
        } with status: ${status}`,
      );
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
      const conflictBody = await getErrorBody(response);
      if (
        conflictBody.error === "ConditionalWriteConflictError" ||
        conflictBody.message === "conditional write conflict"
      ) {
        throw new ChromaConditionalWriteConflictError(
          conflictBody.message || "conditional write conflict",
        );
      }
      throw new ChromaUniqueError(
        conflictBody.message || "The resource already exists",
      );
    case 412:
      const preconditionBody = await getErrorBody(response);
      if (preconditionBody.error === "StaleReadError") {
        throw new ChromaStaleReadError(
          preconditionBody.message || "stale read",
        );
      }
      throw new ChromaClientError(
        preconditionBody.message || "Precondition Failed",
      );
    case 422:
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
        throw new ChromaClientError(body?.message || "Unprocessable Entity");
      } catch (error) {
        if (
          error instanceof ChromaQuotaExceededError ||
          error instanceof ChromaClientError
        ) {
          throw error;
        }
        throw new ChromaClientError(
          `Unprocessable Entity: ${response.statusText}`,
        );
      }
    case 429:
      const rateLimitBody = await getErrorBody(response);
      if (rateLimitBody.error === "Backoff") {
        throw new ChromaBackoffError(
          rateLimitBody.message || "Backoff and retry",
        );
      }
      throw new ChromaRateLimitError("Rate limit exceeded");
  }

  const errorMessage = await getErrorMessage(response);
  throw new ChromaServerError(errorMessage);
};

import {
  ChromaClientError,
  ChromaConnectionError,
  ChromaForbiddenError,
  ChromaNotFoundError,
  ChromaQuotaExceededError,
  ChromaUnauthorizedError,
  ChromaUniqueError,
} from "./errors";

const offlineError = (error: any): boolean => {
  return Boolean(
    (error?.name === "TypeError" || error?.name === "FetchError") &&
      (error.message?.includes("fetch failed") ||
        error.message?.includes("Failed to fetch") ||
        error.message?.includes("ENOTFOUND")),
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
      throw new ChromaUniqueError("The resource already exists");
    case 422:
      const body = await response.json();
      if (body && body?.message.startsWith("Quota exceeded")) {
        throw new ChromaQuotaExceededError(body?.message);
      }
      break;
  }

  throw new ChromaConnectionError(
    `Unable to connect to the chromadb server. Please try again later.`,
  );
};

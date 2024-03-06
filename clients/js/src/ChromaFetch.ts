import {
  ChromaUnauthorizedError,
  ChromaClientError,
  ChromaConnectionError,
  ChromaForbiddenError,
  ChromaNotFoundError,
  ChromaServerError,
  ChromaValueError,
  ChromaError,
} from "./Errors";
import { FetchAPI } from "./generated";

function isOfflineError(error: any): boolean {
  return Boolean(
    error &&
      error.name === "TypeError" &&
      error.message?.includes("fetch failed")
  );
}

function parseServerError(error: string | undefined): Error | undefined {
  if (!error) {
    return undefined;
  }

  const regex = /(\w+)\('(.+)'\)/;
  const match = error.match(regex);
  if (match) {
    const [, name, message] = match;
    switch (name) {
      case "ValueError":
        return new ChromaValueError(message);
      default:
        return new ChromaError(name, message);
    }
  }
  return undefined;
}

/** This utility allows a single entrypoint for custom error handling logic
 *  that works across all ChromaClient methods.
 *
 *  It is intended to be passed to the ApiApi constructor.
 */
export const chromaFetch: FetchAPI = async (
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<Response> => {
  try {
    const resp = await fetch(input, init);

    const clonedResp = resp.clone();
    const respBody = await clonedResp.json();
    if (!clonedResp.ok) {
      switch (resp.status) {
        case 400:
          throw new ChromaClientError(
            `Bad request to ${input} with status: ${resp.statusText}`
          );
        case 401:
          throw new ChromaUnauthorizedError(`Unauthorized`);
        case 403:
          throw new ChromaForbiddenError(
            `You do not have permission to access the requested resource.`
          );
        case 404:
          throw new ChromaNotFoundError(
            `The requested resource could not be found: ${input}`
          );
        case 500:
          throw parseServerError() new ChromaServerError(
            `The server encountered an error while handling the request: ${input}`
          );
        case 502:
        case 503:
        case 504:
          throw new ChromaConnectionError(
            `Unable to connect to the chromadb server. Please try again later.`
          );
      }
      throw new Error(
        `Failed to fetch ${input} with status ${resp.status}: ${resp.statusText}`
      );
    }

    if (respBody.error) {
      throw new ChromaServerError(respBody.error);
    }

    return resp;
  } catch (error) {
    if (isOfflineError(error)) {
      throw new ChromaConnectionError(
        "Failed to connect to chromadb. Make sure your server is running and try again.",
        error
      );
    }
    throw error;
  }
};

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
      throw new Error(
        "Failed to connect to chromadb. Make sure your server is running and try again. If you are running from a browser, make sure that your chromadb instance is configured to allow requests from the current origin using the CHROMA_SERVER_CORS_ALLOW_ORIGINS environment variable.",
      );
    }
    throw new Error("Failed to connect to Chroma");
  }

  if (response.ok) {
    return response;
  }

  switch (response.status) {
    case 400:
      throw new Error(
        `Bad request to ${input} with status: ${response.statusText}`,
      );
    case 401:
      throw new Error(`Unauthorized`);
    case 403:
      throw new Error(
        `You do not have permission to access the requested resource.`,
      );
    case 404:
      throw new Error(`The requested resource could not be found`);
    case 409:
      throw new Error("The resource already exists");
    default:
      throw new Error(
        `Unable to connect to the chromadb server. Please try again later.`,
      );
  }
};

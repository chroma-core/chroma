export const DEFAULT_TENANT = "default_tenant";
export const DEFAULT_DATABASE = "default_database";

export type HttpMethod =
  | "GET"
  | "POST"
  | "PUT"
  | "DELETE"
  | "HEAD"
  | "CONNECT"
  | "OPTIONS"
  | "PATCH"
  | "TRACE"
  | undefined;

export const normalizeMethod = (method?: string): HttpMethod => {
  if (method) {
    switch (method.toUpperCase()) {
      case "GET":
        return "GET";
      case "POST":
        return "POST";
      case "PUT":
        return "PUT";
      case "DELETE":
        return "DELETE";
      case "HEAD":
        return "HEAD";
      case "CONNECT":
        return "CONNECT";
      case "OPTIONS":
        return "OPTIONS";
      case "PATCH":
        return "PATCH";
      case "TRACE":
        return "TRACE";
      default:
        return undefined;
    }
  }
  return undefined;
};

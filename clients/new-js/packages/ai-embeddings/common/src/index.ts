export * from "./schema-utils";

/**
 * Decode a base64-encoded int8 embedding to a number array.
 * Used by embedding providers that return base64-encoded embeddings (e.g., Perplexity).
 */
export function decodeBase64Embedding(b64String: string): number[] {
  const buffer = Buffer.from(b64String, "base64");
  const int8Array = new Int8Array(
    buffer.buffer,
    buffer.byteOffset,
    buffer.byteLength,
  );
  return Array.from(int8Array);
}

const camelToSnake = (str: string): string => {
  return str.replace(/([A-Z])/g, "_$1").toLowerCase();
};

export const snakeCase = (input: any): any => {
  if (Array.isArray(input)) {
    return input.map(snakeCase);
  }

  if (input !== null && typeof input === "object") {
    return Object.fromEntries(
      Object.entries(input).map(([key, value]) => [
        camelToSnake(key),
        snakeCase(value),
      ]),
    );
  }

  return input;
};

export const isBrowser = (): boolean => {
  return (
    typeof window !== "undefined" && typeof window.document !== "undefined"
  );
};

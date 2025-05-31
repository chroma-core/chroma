export * from "./schema-utils";

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

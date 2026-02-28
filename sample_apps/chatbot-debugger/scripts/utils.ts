export const CLOUD_HOST = "https://api.trychroma.com:8000";

export const timeout = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

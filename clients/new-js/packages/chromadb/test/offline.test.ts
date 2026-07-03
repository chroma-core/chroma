import { expect, test } from "@jest/globals";
import { ChromaClient } from "../src";
import { parseConnectionPath } from "../src/utils";

test("it fails with a nice error when offline", async () => {
  const chroma = new ChromaClient({ host: "example.invalid" });
  try {
    await chroma.createCollection({ name: "test" });
    throw new Error("Should have thrown an error.");
  } catch (e) {
    expect((e as Error).message).toMatchInlineSnapshot(
      `"Failed to connect to chromadb. Make sure your server is running and try again. If you are running from a browser, make sure that your chromadb instance is configured to allow requests from the current origin using the CHROMA_SERVER_CORS_ALLOW_ORIGINS environment variable."`,
    );
  }
});

test("deprecated path parsing preserves the default client port when no port is provided", () => {
  expect(parseConnectionPath("http://localhost")).toEqual({
    ssl: false,
    host: "localhost",
    port: undefined,
  });

  expect(parseConnectionPath("https://api.trychroma.com")).toEqual({
    ssl: true,
    host: "api.trychroma.com",
    port: undefined,
  });
});

test("deprecated path parsing keeps explicit ports", () => {
  expect(parseConnectionPath("http://localhost:80")).toEqual({
    ssl: false,
    host: "localhost",
    port: 80,
  });

  expect(parseConnectionPath("https://api.trychroma.com:443")).toEqual({
    ssl: true,
    host: "api.trychroma.com",
    port: 443,
  });

  expect(parseConnectionPath("http://localhost:8001")).toEqual({
    ssl: false,
    host: "localhost",
    port: 8001,
  });
});

test("deprecated path client keeps the default port when the path omits a port", () => {
  const chroma = new ChromaClient({ path: "http://localhost" });

  expect((chroma as any).apiClient.getConfig().baseUrl).toBe(
    "http://localhost:8000",
  );
});

test("deprecated path client keeps explicit default ports", () => {
  const httpClient = new ChromaClient({ path: "http://localhost:80" });
  const httpsClient = new ChromaClient({
    path: "https://api.trychroma.com:443",
  });

  expect((httpClient as any).apiClient.getConfig().baseUrl).toBe(
    "http://localhost:80",
  );
  expect((httpsClient as any).apiClient.getConfig().baseUrl).toBe(
    "https://api.trychroma.com:443",
  );
});

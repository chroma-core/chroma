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

test("deprecated path parsing keeps explicit custom ports", () => {
  expect(parseConnectionPath("http://localhost:8001")).toEqual({
    ssl: false,
    host: "localhost",
    port: 8001,
  });
});

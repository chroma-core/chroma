import { expect, test, jest } from "@jest/globals";
import { ChromaClient } from "../src";
import { DefaultService as Api } from "../src/api";

test("preflight", async () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });
  const preflight = await client.getPreflightChecks();
  expect(preflight).toBeDefined();
  expect(preflight.supports_base64_encoding).toBe(true);
  expect(preflight.max_batch_size).not.toBeUndefined();
});

test("legacy preflight", async () => {
  jest.spyOn(Api, "preFlightChecks").mockResolvedValue({
    data: {
      max_batch_size: 100,
    },
  } as any);

  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });
  const preflight = await client.getPreflightChecks();
  expect(preflight).toBeDefined();
  expect(preflight.supports_base64_encoding).toBeUndefined();
  expect(preflight.max_batch_size).not.toBeUndefined();

  expect(await client.supportsBase64Encoding()).toBe(false);
});

test("preflight with no values", async () => {
  jest.spyOn(Api, "preFlightChecks").mockResolvedValue({
    data: {},
  } as any);

  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });
  const preflight = await client.getPreflightChecks();
  expect(preflight).toBeDefined();
  expect(preflight.supports_base64_encoding).toBeUndefined();
  expect(preflight.max_batch_size).toBeUndefined();

  expect(await client.supportsBase64Encoding()).toBe(false);
  expect(await client.getMaxBatchSize()).toBe(-1);
});

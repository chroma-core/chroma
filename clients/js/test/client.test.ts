import { expect, test } from "@jest/globals";
import { ChromaClient } from "../src/ChromaClient";
import chroma from "./initClient";

test("it should create the client connection", async () => {
  expect(chroma).toBeDefined();
  expect(chroma).toBeInstanceOf(ChromaClient);
});

test("it should get the version", async () => {
  const version = await chroma.version();
  expect(version).toBeDefined();
  expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/);
});

test("it should get the heartbeat", async () => {
  const heartbeat = await chroma.heartbeat();
  expect(heartbeat).toBeDefined();
  expect(heartbeat).toBeGreaterThan(0);
});

test("it should reset the database", async () => {
  await chroma.reset();
  const collections = await chroma.listCollections();
  expect(collections).toBeDefined();
  expect(Array.isArray(collections)).toBe(true);
  expect(collections.length).toBe(0);

  await chroma.createCollection({ name: "test" });
  const collections2 = await chroma.listCollections();
  expect(collections2).toBeDefined();
  expect(Array.isArray(collections2)).toBe(true);
  expect(collections2.length).toBe(1);

  await chroma.reset();
  const collections3 = await chroma.listCollections();
  expect(collections3).toBeDefined();
  expect(Array.isArray(collections3)).toBe(true);
  expect(collections3.length).toBe(0);
});

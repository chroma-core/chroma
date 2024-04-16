import { expect, test } from "@jest/globals";
import { chromaBasic } from "./initClientWithAuth";
import chromaNoAuth from "./initClient";

test("it should get the version without auth needed", async () => {
  const version = await chromaNoAuth.version();
  expect(version).toBeDefined();
  expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/);
});

test("it should get the heartbeat without auth needed", async () => {
  const heartbeat = await chromaNoAuth.heartbeat();
  expect(heartbeat).toBeDefined();
  expect(heartbeat).toBeGreaterThan(0);
});

test("it should raise error when non authenticated", async () => {
  await expect(chromaNoAuth.listCollections()).rejects.toMatchObject({
    status: 403,
  });
});

test("it should list collections", async () => {
  await chromaBasic.reset();
  let collections = await chromaBasic.listCollections();
  expect(collections).toBeDefined();
  expect(collections).toBeInstanceOf(Array);
  expect(collections.length).toBe(0);
  await chromaBasic.createCollection({ name: "test" });
  collections = await chromaBasic.listCollections();
  expect(collections.length).toBe(1);
});

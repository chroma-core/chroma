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

test("it should throw error when non authenticated", async () => {
  try {
    await chromaNoAuth.listCollections();
  } catch (e) {
    expect(e).toBeInstanceOf(Error);
  }
});

test("it should list collections", async () => {
  const client = chromaBasic();

  await client.reset();
  let collections = await client.listCollections();
  expect(collections).toBeDefined();
  expect(collections).toBeInstanceOf(Array);
  expect(collections.length).toBe(0);
  await client.createCollection({ name: "test" });
  collections = await client.listCollections();
  expect(collections.length).toBe(1);
});

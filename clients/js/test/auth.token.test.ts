import { expect, test } from "@jest/globals";
import {
  chromaTokenDefault,
  chromaTokenBearer,
  chromaTokenXToken,
  cloudClient,
} from "./initClientWithAuth";
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
  await expect(chromaNoAuth.listCollections()).rejects.toBeInstanceOf(Error);
});

if (process.env.XTOKEN_TEST) {
  test.each([
    ["x-token", chromaTokenXToken],
    ["cloud client", cloudClient],
  ])(`it should list collections with %s`, async (_, clientBuilder) => {
    const client = clientBuilder();
    await client.reset();
    let collections = await client.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    await client.createCollection({
      name: "test",
    });
    collections = await client.listCollections();
    expect(collections.length).toBe(1);
  });
} else {
  test.each([
    ["default token", chromaTokenDefault],
    ["bearer token", chromaTokenBearer],
  ])(`it should list collections with %s`, async (_, clientBuilder) => {
    const client = clientBuilder();
    await client.reset();
    let collections = await client.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    await client.createCollection({
      name: "test",
    });
    collections = await client.listCollections();
    expect(collections.length).toBe(1);
  });
}

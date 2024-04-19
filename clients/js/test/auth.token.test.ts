import { expect, test } from "@jest/globals";
import {
  chromaTokenDefault,
  chromaTokenBearer,
  chromaTokenXToken,
  cloudClient,
} from "./initClientWithAuth";
import chromaNoAuth from "./initClient";
import { ChromaForbiddenError } from "../src/Errors";

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
  await expect(chromaNoAuth.listCollections()).rejects.toBeInstanceOf(
    ChromaForbiddenError
  );
});

if (!process.env.XTOKEN_TEST) {
  test("it should list collections with default token config", async () => {
    await chromaTokenDefault.reset();
    let collections = await chromaTokenDefault.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    const collection = await chromaTokenDefault.createCollection({
      name: "test",
    });
    collections = await chromaTokenDefault.listCollections();
    expect(collections.length).toBe(1);
  });

  test("it should list collections with explicit bearer token config", async () => {
    await chromaTokenBearer.reset();
    let collections = await chromaTokenBearer.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    const collection = await chromaTokenBearer.createCollection({
      name: "test",
    });
    collections = await chromaTokenBearer.listCollections();
    expect(collections.length).toBe(1);
  });
} else {
  test("it should list collections with explicit x-token token config", async () => {
    await chromaTokenXToken.reset();
    let collections = await chromaTokenXToken.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    const collection = await chromaTokenXToken.createCollection({
      name: "test",
    });
    collections = await chromaTokenXToken.listCollections();
    expect(collections.length).toBe(1);
  });

  test("it should list collections with explicit x-token token config in CloudClient", async () => {
    await cloudClient.reset();
    let collections = await cloudClient.listCollections();
    expect(collections).toBeDefined();
    expect(collections).toBeInstanceOf(Array);
    expect(collections.length).toBe(0);
    const collection = await cloudClient.createCollection({ name: "test" });
    collections = await cloudClient.listCollections();
    expect(collections.length).toBe(1);
  });
}

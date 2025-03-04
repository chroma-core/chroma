import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { chromaTokenDefault, chromaTokenBearer } from "./initClientWithAuth";
import { ChromaClient } from "../src/ChromaClient";
import { StartedTestContainer } from "testcontainers";
import { startChromaContainer } from "./startChromaContainer";

describe("token auth", () => {
  let chromaUrl: string;
  let noAuthClient: ChromaClient;
  let container: StartedTestContainer;

  beforeAll(async () => {
    const { url, container: chromaContainer } = await startChromaContainer({
      authType: "token",
    });
    chromaUrl = url;
    noAuthClient = new ChromaClient({ path: url });
    container = chromaContainer;
  }, 120_000);

  afterAll(async () => {
    await container.stop();
  });

  test("it should get the version without auth needed", async () => {
    const version = await noAuthClient.version();
    expect(version).toBeDefined();
    expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/);
  });

  test("it should get the heartbeat without auth needed", async () => {
    const heartbeat = await noAuthClient.heartbeat();
    expect(heartbeat).toBeDefined();
    expect(heartbeat).toBeGreaterThan(0);
  });

  test("it should raise error when non authenticated", async () => {
    await expect(noAuthClient.listCollections()).rejects.toBeInstanceOf(Error);
  });

  test.each([
    ["default token", chromaTokenDefault],
    ["bearer token", chromaTokenBearer],
  ])(`it should list collections with %s`, async (_, clientBuilder) => {
    const client = clientBuilder(chromaUrl);
    await client.reset();
    let collections = await client.listCollections();
    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections).toHaveLength(0);
    await client.createCollection({
      name: "test",
    });
    collections = await client.listCollections();
    expect(collections).toHaveLength(1);
  });
});

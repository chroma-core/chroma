import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { chromaBasic } from "./initClientWithAuth";
import { startChromaContainer } from "./startChromaContainer";
import { ChromaClient } from "../src/ChromaClient";
import { StartedTestContainer } from "testcontainers";

describe("auth basic", () => {
  let basicAuthClient: ChromaClient;
  let noAuthClient: ChromaClient;
  let container: StartedTestContainer;

  beforeAll(async () => {
    const { url, container: chromaContainer } = await startChromaContainer({
      authType: "basic",
    });
    basicAuthClient = chromaBasic(url);
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

  test("it should throw error when non authenticated", async () => {
    try {
      await noAuthClient.listCollections();
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
    }
  });

  test("it should list collections", async () => {
    await basicAuthClient.reset();
    let collections = await basicAuthClient.listCollections();
    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections).toHaveLength(0);
    await basicAuthClient.createCollection({ name: "test" });
    collections = await basicAuthClient.listCollections();
    expect(collections.length).toBe(1);
  });
});

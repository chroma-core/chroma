import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { ChromaClient } from "../src/ChromaClient";
import { StartedTestContainer } from "testcontainers";
import { startChromaContainer } from "./startChromaContainer";

describe("client test", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  test("it should create the client connection", async () => {
    expect(client).toBeDefined();
    expect(client).toBeInstanceOf(ChromaClient);
  });

  test("it should get the version", async () => {
    const version = await client.version();
    expect(version).toBeDefined();
    expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/);
  });

  test("it should get the heartbeat", async () => {
    const heartbeat = await client.heartbeat();
    expect(heartbeat).toBeDefined();
    expect(heartbeat).toBeGreaterThan(0);
  });

  test("it should reset the database", async () => {
    await client.reset();
    const collections = await client.listCollections();
    expect(collections).toBeDefined();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections.length).toBe(0);

    await client.createCollection({ name: "test" });
    const collections2 = await client.listCollections();
    expect(collections2).toBeDefined();
    expect(Array.isArray(collections2)).toBe(true);
    expect(collections2.length).toBe(1);

    await client.reset();
    const collections3 = await client.listCollections();
    expect(collections3).toBeDefined();
    expect(Array.isArray(collections3)).toBe(true);
    expect(collections3.length).toBe(0);
  });
});

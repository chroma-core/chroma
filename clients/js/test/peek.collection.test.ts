import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { IDS, EMBEDDINGS } from "./data";
import { InvalidCollectionError } from "../src/Errors";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

describe("peek records", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should peek a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
    const results = await collection.peek({ limit: 2 });
    expect(results).toBeDefined();
    expect(typeof results).toBe("object");
    expect(results.ids.length).toBe(2);
    expect(["test1", "test2"]).toEqual(expect.arrayContaining(results.ids));
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    expect(async () => {
      await collection.peek({});
    }).rejects.toThrow(InvalidCollectionError);
  });
});

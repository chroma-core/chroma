import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { InvalidCollectionError } from "../src/Errors";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

describe("upsert records", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should upsert embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = ["test1", "test2"];
    const embeddings = [
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
    ];
    await client.addRecords(collection, { ids, embeddings });
    const count = await client.countRecords(collection);
    expect(count).toBe(2);

    const ids2 = ["test2", "test3"];
    const embeddings2 = [
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 15],
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    ];

    await client.upsertRecords(collection, {
      ids: ids2,
      embeddings: embeddings2,
    });

    const count2 = await client.countRecords(collection);
    expect(count2).toBe(3);
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    expect(async () => {
      await client.upsertRecords(collection, {
        ids: ["test1"],
        embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
        metadatas: [{ test: "meta1" }],
        documents: ["doc1"],
      });
    }).rejects.toThrow(InvalidCollectionError);
  });
});

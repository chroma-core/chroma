import { beforeEach, describe, expect, test } from "@jest/globals";
import { ChromaClient } from "../src/ChromaClient";

describe("bulk upsert records", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should bulk upsert embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = ["test1", "test2"];
    const embeddings = [
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
    ];

    const processedIds: string[] = [];

    await client.bulkUpsertRecords(collection, {
      ids,
      embeddings,
      maxBatchSize: 1,
      onBatchProcessed: (ids) => {
        processedIds.push(...ids);
      },
    });
    expect(processedIds).toEqual(ids);

    const count = await client.countRecords(collection);
    expect(count).toBe(2);
  });
});

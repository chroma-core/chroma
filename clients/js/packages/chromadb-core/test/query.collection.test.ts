import { beforeEach, describe, expect, test } from "@jest/globals";
import { IncludeEnum } from "../src/types";
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from "./data";

import { IEmbeddingFunction } from "../src/embeddings/IEmbeddingFunction";
import { ChromaNotFoundError } from "../src/Errors";
import { ChromaClient } from "../src/ChromaClient";

class TestEmbeddingFunction implements IEmbeddingFunction {
  constructor() {}

  public async generate(texts: string[]): Promise<number[][]> {
    let embeddings: number[][] = [];
    for (let i = 0; i < texts.length; i += 1) {
      embeddings.push([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
    return embeddings;
  }
}

describe("query records", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should query a collection, singular", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
    const results = await collection.query({
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 2,
    });
    expect(results.ids[0]).toHaveLength(2);
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test1", "test2"]));
    expect(results.ids[0]).not.toContain("test3");
    expect(results.included).toEqual(
      expect.arrayContaining(["metadatas", "documents", "distances"]),
    );
  });

  test("it should query a collection, array", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
    const results = await collection.query({
      queryEmbeddings: [
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      ],
      nResults: 2,
    });
    expect(results.ids[0]).toHaveLength(2);
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test1", "test2"]));
    expect(results.ids[0]).not.toContain("test3");
    expect(results.ids[1]).toHaveLength(2);
    expect(results.ids[1]).toEqual(expect.arrayContaining(["test1", "test2"]));
    expect(results.ids[1]).not.toContain("test3");
    expect(results.included).toEqual(
      expect.arrayContaining(["metadatas", "documents", "distances"]),
    );
  });

  // test where_document
  test("it should get embedding with matching documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 3,
      whereDocument: { $contains: "This is a test" },
    });

    // it should only return doc1
    expect(results?.ids[0]).toHaveLength(1);
    expect(results.ids[0]).toContain("test1");
    expect(results.ids[0]).not.toContain("test2");
    expect(results.documents[0]).toContain("This is a test");

    const results2 = await collection.query({
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 3,
      whereDocument: { $contains: "This is a test" },
      include: [IncludeEnum.Embeddings],
    });

    expect(results2.embeddings?.[0]?.length).toBe(1);
    expect(results2.embeddings?.[0][0]).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    ]);
    expect(results2.included).toEqual(expect.arrayContaining(["embeddings"]));
  });

  test("it should exclude documents matching - not_contains", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 3,
      whereDocument: { $not_contains: "This is a test" },
    });

    // it should only return doc1
    expect(results?.ids[0]).toHaveLength(2);
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test2", "test3"]));
  });

  // test queryTexts
  test("it should query a collection with text", async () => {
    let embeddingFunction = new TestEmbeddingFunction();
    const collection = await client.createCollection({
      name: "test",
      embeddingFunction: embeddingFunction,
    });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryTexts: "test",
      nResults: 3,
      whereDocument: { $contains: "This is a test" },
    });

    expect(results?.ids[0]).toHaveLength(1);
    expect(results.ids[0]).toContain("test1");
    expect(results.ids[0]).not.toContain("test2");
    expect(results.documents[0]).toContain("This is a test");
  });

  test("it should query a collection with text and where", async () => {
    let embeddingFunction = new TestEmbeddingFunction();
    const collection = await client.createCollection({
      name: "test",
      embeddingFunction: embeddingFunction,
    });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryTexts: "test",
      nResults: 3,
      where: { float_value: 2 },
    });

    expect(results?.ids[0]).toHaveLength(1);
    expect(results.ids[0]).toContain("test3");
    expect(results.ids[0]).not.toContain("test2");
    expect(results.documents[0]).toContain("This is a third test");
  });

  test("it should query a collection with text and where in", async () => {
    let embeddingFunction = new TestEmbeddingFunction();
    const collection = await client.createCollection({
      name: "test",
      embeddingFunction: embeddingFunction,
    });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryTexts: "test",
      nResults: 3,
      where: { float_value: { $in: [2, 5, 10] } },
    });

    expect(results.ids[0]).toHaveLength(1);
    expect(results.ids[0]).toContain("test3");
    expect(results.ids[0]).not.toContain("test2");
    expect(results.documents[0]).toContain("This is a third test");
  });

  test("it should query a collection with text and where nin", async () => {
    const collection = await client.createCollection({
      name: "test",
      embeddingFunction: new TestEmbeddingFunction(),
    });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryTexts: "test",
      nResults: 3,
      where: { float_value: { $nin: [-2, 0] } },
    });

    expect(results).toBeDefined();
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test3"]));
    expect(results.ids[0]).not.toEqual(expect.arrayContaining(["test2"]));
    expect(results.documents[0]).toEqual(
      expect.arrayContaining(["This is a third test"]),
    );
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    await expect(async () => {
      await collection.query({ queryEmbeddings: [1, 2, 3] });
    }).rejects.toThrow(ChromaNotFoundError);
  });

  test("it should query a collection with specific IDs", async () => {
    const collection = await client.createCollection({
      name: "test",
      embeddingFunction: new TestEmbeddingFunction(),
    });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.query({
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 3,
      ids: ["test1", "test3"],
    });

    expect(results).toBeDefined();
    expect(results.ids[0]).toHaveLength(2);
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test1", "test3"]));
    expect(results.ids[0]).not.toContain("test2");

    expect(results.documents[0]).toEqual(
      expect.arrayContaining(["This is a test", "This is a third test"]),
    );
    expect(results.metadatas[0]).toEqual(
      expect.arrayContaining([
        { test: "test1", float_value: -2 },
        { test: "test3", float_value: 2 },
      ]),
    );
  });
});

describe("id filtering", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should filter by IDs in a small dataset", async () => {
    const collection = await client.createCollection({
      name: "test_id_filtering_small",
    });

    const numVectors = 100;
    const dim = 10;
    const smallRecords: number[][] = [];
    const ids: string[] = [];

    for (let i = 0; i < numVectors; i++) {
      const embedding = Array.from({ length: dim }, () => Math.random());
      smallRecords.push(embedding);
      ids.push(`id_${i}`);
    }

    await collection.add({
      ids: ids,
      embeddings: smallRecords,
    });

    const queryIds = ids.filter((_, i) => i % 10 === 0);

    const queryEmbedding = Array.from({ length: dim }, () => Math.random());
    const results = await collection.query({
      queryEmbeddings: queryEmbedding,
      ids: queryIds,
      nResults: numVectors,
    });

    const allReturnedIds = results.ids[0];
    allReturnedIds.forEach((id) => {
      expect(queryIds).toContain(id);
    });
  });

  test("it should filter by IDs in a medium dataset", async () => {
    const collection = await client.createCollection({
      name: "test_id_filtering_medium",
    });

    const numVectors = 1000;
    const dim = 10;
    const mediumRecords: number[][] = [];
    const ids: string[] = [];

    for (let i = 0; i < numVectors; i++) {
      const embedding = Array.from({ length: dim }, () => Math.random());
      mediumRecords.push(embedding);
      ids.push(`id_${i}`);
    }

    await collection.add({
      ids: ids,
      embeddings: mediumRecords,
    });

    const queryIds = ids.filter((_, i) => i % 10 === 0);

    const queryEmbedding = Array.from({ length: dim }, () => Math.random());
    const results = await collection.query({
      queryEmbeddings: queryEmbedding,
      ids: queryIds,
      nResults: numVectors,
    });

    const allReturnedIds = results.ids[0];
    allReturnedIds.forEach((id) => {
      expect(queryIds).toContain(id);
    });

    const multiQueryEmbeddings = [
      Array.from({ length: dim }, () => Math.random()),
      Array.from({ length: dim }, () => Math.random()),
      Array.from({ length: dim }, () => Math.random()),
    ];

    const multiResults = await collection.query({
      queryEmbeddings: multiQueryEmbeddings,
      ids: queryIds,
      nResults: 10,
    });

    expect(multiResults.ids.length).toBe(multiQueryEmbeddings.length);
    multiResults.ids.forEach((idSet) => {
      idSet.forEach((id) => {
        expect(queryIds).toContain(id);
      });
    });
  });

  test("it should handle ID filtering with deleted and upserted IDs", async () => {
    const collection = await client.createCollection({
      name: "test_id_filtering_e2e",
    });

    const dim = 10;
    const numVectors = 100;
    const embeddings: number[][] = [];
    const ids: string[] = [];
    const metadatas: Record<string, any>[] = [];

    for (let i = 0; i < numVectors; i++) {
      const embedding = Array.from({ length: dim }, () => Math.random());
      embeddings.push(embedding);
      ids.push(`id_${i}`);
      metadatas.push({ index: i });
    }

    await collection.add({
      embeddings: embeddings,
      ids: ids,
      metadatas: metadatas,
    });

    const idsToDelete = ids.slice(10, 30);
    await collection.delete({ ids: idsToDelete });

    const idsToUpsertExisting = ids.slice(30, 50);
    const idsToUpsertNew = Array.from(
      { length: 20 },
      (_, i) => `id_${numVectors + i}`,
    );

    const upsertEmbeddings: number[][] = [];
    const upsertMetadatas: Record<string, any>[] = [];

    for (
      let i = 0;
      i < idsToUpsertExisting.length + idsToUpsertNew.length;
      i++
    ) {
      const embedding = Array.from({ length: dim }, () => Math.random());
      upsertEmbeddings.push(embedding);
      upsertMetadatas.push({ index: i, upserted: true });
    }

    await collection.upsert({
      embeddings: upsertEmbeddings,
      ids: [...idsToUpsertExisting, ...idsToUpsertNew],
      metadatas: upsertMetadatas,
    });

    const validQueryIds = [
      ...ids.slice(5, 10),
      ...ids.slice(35, 45),
      ...idsToUpsertNew.slice(5, 15),
    ];

    const queryEmbedding = Array.from({ length: dim }, () => Math.random());
    const results = await collection.query({
      queryEmbeddings: queryEmbedding,
      ids: validQueryIds,
      nResults: validQueryIds.length,
      include: [IncludeEnum.Metadatas],
    });

    const allReturnedIds = results.ids[0];

    allReturnedIds.forEach((id) => {
      expect(validQueryIds).toContain(id);
    });

    // Verify upserted IDs have updated metadata
    results.ids[0].forEach((id, idx) => {
      if (idsToUpsertExisting.includes(id) || idsToUpsertNew.includes(id)) {
        const metadata = results.metadatas?.[0]?.[idx];
        if (metadata) {
          expect(metadata.upserted).toBe(true);
        }
      }
    });

    // Test querying a specific upserted ID
    const upsertedId = idsToUpsertExisting[0];
    const upsertResults = await collection.query({
      queryEmbeddings: queryEmbedding,
      ids: upsertedId,
      nResults: 1,
      include: [IncludeEnum.Metadatas],
    });

    const firstMetadata = upsertResults.metadatas?.[0]?.[0];
    expect(firstMetadata).toBeTruthy();
    if (firstMetadata) {
      expect(firstMetadata.upserted).toBe(true);
    }

    const deletedId = idsToDelete[0];
    await expect(async () => {
      await collection.query({
        queryEmbeddings: queryEmbedding,
        ids: deletedId,
        nResults: 1,
        include: [IncludeEnum.Metadatas],
      });
    }).rejects.toThrow();
  });
});

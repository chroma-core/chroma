import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { IncludeEnum } from "../src/types";
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from "./data";

import { IEmbeddingFunction } from "../src/embeddings/IEmbeddingFunction";
import { InvalidCollectionError } from "../src/Errors";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

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
    await client.addRecords(collection, { ids: IDS, embeddings: EMBEDDINGS });
    const results = await client.queryRecords(collection, {
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 2,
    });
    expect(results.ids[0]).toHaveLength(2);
    expect(results.ids[0]).toEqual(expect.arrayContaining(["test1", "test2"]));
    expect(results.ids[0]).not.toContain("test3");
    expect(results.included).toEqual(
      expect.arrayContaining(["metadatas", "documents"]),
    );
  });

  test("it should query a collection, array", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.addRecords(collection, { ids: IDS, embeddings: EMBEDDINGS });
    const results = await client.queryRecords(collection, {
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
      expect.arrayContaining(["metadatas", "documents"]),
    );
  });

  // test where_document
  test("it should get embedding with matching documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
      queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      nResults: 3,
      whereDocument: { $contains: "This is a test" },
    });

    // it should only return doc1
    expect(results?.ids[0]).toHaveLength(1);
    expect(results.ids[0]).toContain("test1");
    expect(results.ids[0]).not.toContain("test2");
    expect(results.documents[0]).toContain("This is a test");

    const results2 = await client.queryRecords(collection, {
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
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
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
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
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
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
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
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
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
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await client.queryRecords(collection, {
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
    expect(async () => {
      await client.queryRecords(collection, { queryEmbeddings: [1, 2, 3] });
    }).rejects.toThrow(InvalidCollectionError);
  });
});

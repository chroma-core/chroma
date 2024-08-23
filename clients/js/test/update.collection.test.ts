import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { IncludeEnum } from "../src/types";
import { IDS, DOCUMENTS, EMBEDDINGS, METADATAS } from "./data";
import { InvalidCollectionError } from "../src/Errors";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

describe("update records", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should get embedding with matching documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });

    const results = await collection.get({
      ids: "test1",
      include: [
        IncludeEnum.Embeddings,
        IncludeEnum.Metadatas,
        IncludeEnum.Documents,
      ],
    });

    expect(results?.embeddings?.[0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    await collection.update({
      ids: ["test1"],
      embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
      metadatas: [{ test: "test1new" }],
      documents: ["doc1new"],
    });

    const results2 = await collection.get({
      ids: "test1",
      include: [
        IncludeEnum.Embeddings,
        IncludeEnum.Metadatas,
        IncludeEnum.Documents,
      ],
    });
    expect(results2?.embeddings?.[0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 11]);
    expect(results2.metadatas?.[0]).toEqual({
      test: "test1new",
      float_value: -2,
    });
    expect(results2.documents?.[0]).toEqual("doc1new");
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    expect(async () => {
      await collection.update({
        ids: ["test1"],
        embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
        metadatas: [{ test: "meta1" }],
        documents: ["doc1"],
      });
    }).rejects.toThrow(InvalidCollectionError);
  });

  test("should support updating records without a document or an embedding", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: "test1",
      documents: "doc1",
      embeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      metadatas: { test: "test1", float_value: 0.1 },
    });
    await collection.update({
      ids: ["test1"],
      metadatas: [{ test: "meta1" }],
    });

    const results = await collection.get({
      ids: "test1",
      include: [IncludeEnum.Metadatas],
    });

    expect(results.metadatas[0]?.test).toEqual("meta1");
  });

  // this currently fails
  // test("it should update metadata or documents to array of Nones", async () => {
  //
  //   const collection = await client.createCollection({ name: "test" });
  //   await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS, documents: DOCUMENTS });

  //   await collection.update({
  //     ids: ["test1"],
  //     metadatas: [undefined],
  //   });

  //   const results3 = await collection.get({
  //     ids: ["test1"],
  //     include: [
  //       IncludeEnum.Embeddings,
  //       IncludeEnum.Metadatas,
  //       IncludeEnum.Documents,
  //     ]
  //   });
  //   expect(results3).toBeDefined();
  //   expect(results3).toBeInstanceOf(Object);
  //   expect(results3.metadatas[0]).toEqual({});
  // });
});

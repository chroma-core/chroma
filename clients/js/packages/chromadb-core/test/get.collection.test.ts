import { beforeEach, describe, expect, test } from "@jest/globals";
import { DOCUMENTS, EMBEDDINGS, IDS, METADATAS } from "./data";
import { ChromaNotFoundError, InvalidArgumentError } from "../src/Errors";
import { DefaultEmbeddingFunction } from "../src/embeddings/DefaultEmbeddingFunction";
import { ChromaClient } from "../src/ChromaClient";

describe("get collections", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should get documents from a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    const results = await collection.get({ ids: ["test1"] });
    expect(results.ids).toHaveLength(1);
    expect(results.ids).toEqual(expect.arrayContaining(["test1"]));
    expect(results.ids).not.toContain("test2");
    expect(results.included).toEqual(
      expect.arrayContaining(["metadatas", "documents"]),
    );

    const results2 = await collection.get({
      where: { test: "test1" },
    });
    expect(results2.ids).toHaveLength(1);
    expect(results2.ids).toEqual(expect.arrayContaining(["test1"]));
  });

  test("it should throw an error for invalid where clause", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    await expect(
      collection.get({
        where: {
          //@ts-ignore supposed to fail
          test: { $contains: "hello" },
        },
      }),
    ).rejects.toThrow(InvalidArgumentError);
  });

  test("it should get embedding with matching documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });
    const results2 = await collection.get({
      whereDocument: { $contains: "This is a test" },
    });
    expect(results2.ids).toHaveLength(1);
    expect(results2.ids).toEqual(expect.arrayContaining(["test1"]));
  });

  test("it should get records not matching", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
      documents: DOCUMENTS,
    });
    const results2 = await collection.get({
      whereDocument: { $not_contains: "This is another" },
    });
    expect(results2.ids).toHaveLength(2);
    expect(results2.ids).toEqual(expect.arrayContaining(["test1", "test3"]));
  });

  test("it should filter documents using comparison operators", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    const items = await collection.get({
      where: { float_value: { $gt: -1.4 } },
    });
    expect(items.ids).toHaveLength(2);
    expect(items.ids).toEqual(expect.arrayContaining(["test2", "test3"]));
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    await expect(async () => {
      await collection.get({ ids: IDS });
    }).rejects.toThrow(ChromaNotFoundError);
  });

  test("it should throw an error if the collection does not exist", async () => {
    await expect(
      async () =>
        await client.getCollection({
          name: "test",
          embeddingFunction: new DefaultEmbeddingFunction(),
        }),
    ).rejects.toThrow(Error);
  });
});

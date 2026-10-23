import { expect, test, beforeEach, describe } from "@jest/globals";
import { ChromaClient, ChromaValueError, Search } from "../src";
import { EMBEDDINGS, IDS, METADATAS } from "./utils/data";

describe("collection handle", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  let collectionId: string;

  beforeEach(async () => {
    await client.reset();
    const collection = await client.createCollection({ name: "test" });
    collectionId = collection.id;
  });

  test("it should return a collection synchronously", () => {
    const handle = client.collection(collectionId);
    expect(handle).toBeDefined();
    expect(handle.id).toBe(collectionId);
  });

  test("it should add records with pre-computed embeddings", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    const count = await handle.count();
    expect(count).toBe(3);
  });

  test("it should get records", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    const res = await handle.get({ ids: ["test1"] });
    expect(res.ids).toEqual(["test1"]);
  });

  test("it should delete records", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    await handle.delete({ ids: ["test1"] });
    const count = await handle.count();
    expect(count).toBe(2);
  });

  test("it should count records", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    const count = await handle.count();
    expect(count).toBe(3);
  });

  test("it should peek records", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    const res = await handle.peek({ limit: 2 });
    expect(res.ids).toHaveLength(2);
  });

  test("it should query with pre-computed embeddings", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    const results = await handle.query({
      queryEmbeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
      nResults: 2,
    });
    expect(results.ids[0]).toHaveLength(2);
  });

  // Search is not implemented in the local test server
  test.skip("it should search with pre-computed embeddings", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    const results = await handle.search(
      new Search()
        .rank({ $knn: { query: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] } })
        .limit(2),
    );
    expect(results).toBeDefined();
  });

  test("it should update records with pre-computed embeddings", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    await handle.update({
      ids: ["test1"],
      embeddings: [[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]],
    });
    const res = await handle.get({
      ids: ["test1"],
      include: ["embeddings"],
    });
    expect(res.embeddings?.[0]).toEqual([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
  });

  test("it should upsert records with pre-computed embeddings", async () => {
    const handle = client.collection(collectionId);
    await handle.upsert({
      ids: ["new1"],
      embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
    });
    const count = await handle.count();
    expect(count).toBe(1);
  });

  test("it should error on add without embeddings", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.add({
        ids: ["test1"],
        documents: ["This is a test"],
      }),
    ).rejects.toThrow(ChromaValueError);
    await expect(
      handle.add({
        ids: ["test1"],
        documents: ["This is a test"],
      }),
    ).rejects.toThrow(/client\.getCollection\(\)/);
  });

  test("it should error on upsert without embeddings", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.upsert({
        ids: ["test1"],
        documents: ["This is a test"],
      }),
    ).rejects.toThrow(ChromaValueError);
  });

  test("it should error on query without queryEmbeddings", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.query({
        queryTexts: ["test"],
      }),
    ).rejects.toThrow(ChromaValueError);
    await expect(
      handle.query({
        queryTexts: ["test"],
      }),
    ).rejects.toThrow(/client\.getCollection\(\)/);
  });

  test("it should error on search with string knn query", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.search(
        new Search()
          .rank({ $knn: { query: "a text query" } })
          .limit(2),
      ),
    ).rejects.toThrow(ChromaValueError);
    await expect(
      handle.search(
        new Search()
          .rank({ $knn: { query: "a text query" } })
          .limit(2),
      ),
    ).rejects.toThrow(/client\.getCollection\(\)/);
  });

  test("it should error on modify", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.modify({ name: "new_name" }),
    ).rejects.toThrow(ChromaValueError);
    await expect(
      handle.modify({ name: "new_name" }),
    ).rejects.toThrow(/modify\(\)/);
  });

  test("it should error on fork", async () => {
    const handle = client.collection(collectionId);
    await expect(
      handle.fork({ name: "forked" }),
    ).rejects.toThrow(ChromaValueError);
    await expect(
      handle.fork({ name: "forked" }),
    ).rejects.toThrow(/fork\(\)/);
  });

  test("it should allow update with only metadata (no documents or embeddings)", async () => {
    const handle = client.collection(collectionId);
    await handle.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
    });
    await handle.update({
      ids: ["test1"],
      metadatas: [{ updated: true }],
    });
    const res = await handle.get({ ids: ["test1"] });
    expect(res.metadatas?.[0]).toEqual({ updated: true });
  });
});

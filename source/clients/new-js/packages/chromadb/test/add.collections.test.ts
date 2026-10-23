import { expect, test, describe, beforeEach } from "@jest/globals";
import { DOCUMENTS, EMBEDDINGS, IDS, METADATAS } from "./utils/data";
import { ChromaClient } from "../src";

describe("add collections", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should add single embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    const id = "test1";
    const embedding = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const metadata = { test: "test" };
    await collection.add({
      ids: [id],
      embeddings: [embedding],
      metadatas: [metadata],
    });
    const count = await collection.count();
    expect(count).toBe(1);
    const res = await collection.get({
      ids: [id],
      include: ["embeddings"],
    });
    expect(res.embeddings?.[0]).toEqual(embedding);
  });

  test("it should add batch embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      documents: DOCUMENTS,
    });
    const count = await collection.count();
    expect(count).toBe(3);
    var res = await collection.get({
      include: ["embeddings"],
    });
    expect(res.embeddings).toEqual(EMBEDDINGS);
  });

  test("add documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      documents: DOCUMENTS,
    });
    const results = await collection.get({ ids: ["test1"] });
    expect(results.documents[0] || "").toBe("This is a test");
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    await expect(async () => {
      await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
    }).rejects.toThrow();
  });

  test("It should return an error when inserting duplicate IDs in the same batch", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = IDS.concat(["test1"]);
    const embeddings = EMBEDDINGS.concat([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
    const metadatas = METADATAS.concat([{ test: "test1", float_value: 0.1 }]);
    try {
      await collection.add({ ids, embeddings, metadatas });
    } catch (e: any) {
      expect(e.message).toMatch("duplicates");
    }
  });

  test("should error on empty embedding", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = ["id1"];
    const embeddings = [[]];
    const metadatas = [{ test: "test1", float_value: 0.1 }];
    try {
      await collection.add({ ids, embeddings, metadatas });
    } catch (e: any) {
      expect(e.message).toMatch(
        "Expected each embedding to be a non-empty array of numbers",
      );
    }
  });
});

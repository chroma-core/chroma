import { beforeEach, describe, expect, test } from "@jest/globals";
import { ChromaClient } from "../src";
import { EMBEDDINGS, IDS, METADATAS } from "./utils/data";

describe("delete collection", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should delete documents from a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    let count = await collection.count();
    expect(count).toBe(3);
    const result = await collection.delete({
      where: { test: "test1" },
    });
    expect(result).toHaveProperty("deleted");
    expect(result.deleted).toBe(1);
    count = await collection.count();
    expect(count).toBe(2);

    const remainingEmbeddings = await collection.get();
    expect(remainingEmbeddings?.ids).toEqual(
      expect.arrayContaining(["test2", "test3"]),
    );
  });

  test("it should delete with limit", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    let count = await collection.count();
    expect(count).toBe(3);

    // All 3 records match, but limit to 2
    const result = await collection.delete({
      where: { float_value: { $gte: -3 } },
      limit: 2,
    });
    expect(result.deleted).toBe(2);
    count = await collection.count();
    expect(count).toBe(1);
  });

  test("it should delete with limit zero as no-op", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    const result = await collection.delete({
      where: { test: "test1" },
      limit: 0,
    });
    expect(result.deleted).toBe(0);
    const count = await collection.count();
    expect(count).toBe(3);
  });

  test("it should error when limit is used without where", async () => {
    const collection = await client.createCollection({ name: "test" });
    await expect(async () => {
      await collection.delete({ ids: ["test1"], limit: 5 });
    }).rejects.toThrow(
      "limit can only be specified when a where or whereDocument clause is provided",
    );
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    await expect(async () => {
      await collection.delete({ where: { test: "test1" } });
    }).rejects.toThrow();
  });
});

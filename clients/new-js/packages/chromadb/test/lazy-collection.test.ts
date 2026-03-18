import { expect, test, beforeEach, describe } from "@jest/globals";
import { ChromaClient } from "../src";
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from "./utils/data";

describe("thin collection via client.collection(id)", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("client.collection(id) returns synchronously", async () => {
    const created = await client.createCollection({ name: "test" });
    const thin = client.collection(created.id);
    expect(thin).toBeDefined();
    expect(thin.id).toBe(created.id);
  });

  test("accessing name on thin collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    expect(() => thin.name).toThrow("thin collection");
  });

  test("accessing metadata on thin collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    expect(() => thin.metadata).toThrow("thin collection");
  });

  test("accessing configuration on thin collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    expect(() => thin.configuration).toThrow("thin collection");
  });

  test("count works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    const count = await thin.count();
    expect(count).toBe(3);
  });

  test("add with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    await thin.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });

    const count = await thin.count();
    expect(count).toBe(3);
  });

  test("get works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      documents: DOCUMENTS,
    });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    const result = await thin.get({ ids: ["test1"] });
    expect(result.ids).toHaveLength(1);
    expect(result.ids[0]).toBe("test1");
    expect(result.documents[0]).toBe("This is a test");
  });

  test("delete works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    await thin.delete({ ids: ["test1"] });

    const count = await thin.count();
    expect(count).toBe(2);
  });

  test("query with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    const result = await thin.query({
      queryEmbeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
      nResults: 2,
    });
    expect(result.ids[0]).toHaveLength(2);
  });

  test("upsert with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    await thin.upsert({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });

    const count = await thin.count();
    expect(count).toBe(3);
  });

  test("update with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);
    await thin.update({
      ids: ["test1"],
      embeddings: [[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]],
    });

    const result = await thin.get({ ids: ["test1"], include: ["embeddings"] });
    expect(result.embeddings[0]).toEqual([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
  });

  test("cache returns same instance on repeated calls", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin1 = freshClient.collection(created.id);
    const thin2 = freshClient.collection(created.id);
    expect(thin1).toBe(thin2);
  });

  test("getCollection populates cache so collection(id) returns hydrated instance", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });

    const fetched = await freshClient.getCollection({ name: "test" });

    const fromCache = freshClient.collection(fetched.id);
    expect(fromCache).toBe(fetched);
    expect(fromCache.name).toBe("test");
  });

  test("createCollection populates cache", async () => {
    const created = await client.createCollection({ name: "test" });
    const fromCache = client.collection(created.id);
    expect(fromCache).toBe(created);
    expect(fromCache.name).toBe("test");
  });

  test("getCollection hydrates existing thin instance in place", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });

    // Create a thin collection first
    const thin = freshClient.collection(created.id);
    expect(() => thin.name).toThrow("thin collection");

    // Now fetch the same collection by name — should hydrate the thin instance
    const fetched = await freshClient.getCollection({ name: "test" });

    // The fetched reference should be the same object as the thin reference
    expect(fetched).toBe(thin);
    // And properties should now be accessible
    expect(thin.name).toBe("test");
  });

  test("deleteCollection removes thin collection from cache", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });

    // Create a thin collection in cache
    const thin = freshClient.collection(created.id);
    expect(thin.id).toBe(created.id);

    // Delete the collection
    await freshClient.deleteCollection({ name: "test" });

    // Subsequent collection(id) call should return a new instance, not the stale one
    const afterDelete = freshClient.collection(created.id);
    expect(afterDelete).not.toBe(thin);
  });

  test("listCollections does not populate cache", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });

    // List collections
    const listed = await freshClient.listCollections();
    expect(listed).toHaveLength(1);

    // collection(id) should return a new thin instance, not the listed one
    const fromCollection = freshClient.collection(created.id);
    expect(fromCollection).not.toBe(listed[0]);
    // It should be thin (unhydrated)
    expect(() => fromCollection.name).toThrow("thin collection");
  });
});

import { expect, test, beforeEach, describe } from "@jest/globals";
import { ChromaClient } from "../src";
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from "./utils/data";

describe("lazy collection via client.collection(id)", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("client.collection(id) returns synchronously", async () => {
    const created = await client.createCollection({ name: "test" });
    const lazy = client.collection(created.id);
    expect(lazy).toBeDefined();
    expect(lazy.id).toBe(created.id);
  });

  test("accessing name on unhydrated collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    // Reset to get a fresh client with no cache
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    expect(() => lazy.name).toThrow("unhydrated");
  });

  test("accessing metadata on unhydrated collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    expect(() => lazy.metadata).toThrow("unhydrated");
  });

  test("accessing configuration on unhydrated collection throws", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    expect(() => lazy.configuration).toThrow("unhydrated");
  });

  test("count works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    const count = await lazy.count();
    expect(count).toBe(3);
  });

  test("add with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    await lazy.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });

    const count = await lazy.count();
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
    const lazy = freshClient.collection(created.id);
    const result = await lazy.get({ ids: ["test1"] });
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
    const lazy = freshClient.collection(created.id);
    await lazy.delete({ ids: ["test1"] });

    const count = await lazy.count();
    expect(count).toBe(2);
  });

  test("query with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    const result = await lazy.query({
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
    const lazy = freshClient.collection(created.id);
    await lazy.upsert({
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });

    const count = await lazy.count();
    expect(count).toBe(3);
  });

  test("update with pre-computed embeddings works without hydration", async () => {
    const created = await client.createCollection({ name: "test" });
    await created.add({ ids: IDS, embeddings: EMBEDDINGS });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy = freshClient.collection(created.id);
    await lazy.update({
      ids: ["test1"],
      embeddings: [[10, 9, 8, 7, 6, 5, 4, 3, 2, 1]],
    });

    const result = await lazy.get({ ids: ["test1"], include: ["embeddings"] });
    expect(result.embeddings[0]).toEqual([10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
  });

  test("cache returns same instance on repeated calls", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const lazy1 = freshClient.collection(created.id);
    const lazy2 = freshClient.collection(created.id);
    expect(lazy1).toBe(lazy2);
  });

  test("getCollection populates cache so collection(id) returns hydrated instance", async () => {
    const created = await client.createCollection({ name: "test" });
    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });

    // First getCollection to populate cache
    const fetched = await freshClient.getCollection({ name: "test" });

    // Now collection(id) should return the cached hydrated instance
    const fromCache = freshClient.collection(fetched.id);
    expect(fromCache).toBe(fetched);
    expect(fromCache.name).toBe("test");
  });

  test("createCollection populates cache", async () => {
    const created = await client.createCollection({ name: "test" });
    // Same client used for creation should have it cached
    const fromCache = client.collection(created.id);
    expect(fromCache).toBe(created);
    expect(fromCache.name).toBe("test");
  });
});

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

  test("add with documents triggers hydration and makes properties accessible", async () => {
    const created = await client.createCollection({ name: "test" });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);

    // Before hydration, properties throw
    expect(() => thin.name).toThrow("thin collection");

    // add with documents triggers hydration (needs embedding function)
    await thin.add({ ids: IDS, embeddings: EMBEDDINGS, documents: DOCUMENTS });

    // After hydration, properties are accessible
    expect(thin.name).toBe("test");
    expect(thin.configuration).toBeDefined();

    const count = await thin.count();
    expect(count).toBe(3);
  });

  test("modify triggers hydration and makes properties accessible", async () => {
    const created = await client.createCollection({
      name: "test",
      metadata: { key: "value" },
    });

    const freshClient = new ChromaClient({
      path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
    });
    const thin = freshClient.collection(created.id);

    expect(() => thin.name).toThrow("thin collection");

    await thin.modify({ metadata: { key: "updated" } });

    expect(thin.name).toBe("test");
    expect(thin.metadata).toEqual({ key: "updated" });
  });
});

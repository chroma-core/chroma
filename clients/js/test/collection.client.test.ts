import { expect, test, beforeEach } from "@jest/globals";
import chroma from "./initClient";
import { DefaultEmbeddingFunction } from "../src/embeddings/DefaultEmbeddingFunction";

beforeEach(async () => {
  await chroma.reset();
});

test("it should list collections", async () => {
  let collections = await chroma.listCollections();
  expect(Array.isArray(collections)).toBe(true);
  expect(collections).toHaveLength(0);
  await chroma.createCollection({ name: "test" });
  collections = await chroma.listCollections();
  expect(collections).toHaveLength(1);
});

test("it should create a collection", async () => {
  const collection = await chroma.createCollection({ name: "test" });
  expect(collection).toBeDefined();
  expect(collection).toHaveProperty("name");
  expect(collection).toHaveProperty("id");
  expect(collection.name).toBe("test");
  let collections = await chroma.listCollections();

  expect(collections).toMatchInlineSnapshot(`
    [
      {
        "database": "default_database",
        "dimension": null,
        "id": "${collection.id}",
        "metadata": null,
        "name": "test",
        "tenant": "default_tenant",
        "version": 0,
      },
    ]
  `);

  expect([{ name: "test2", metadata: null }]).not.toEqual(
    expect.arrayContaining(collections),
  );

  await chroma.reset();
  const collection2 = await chroma.createCollection({
    name: "test2",
    metadata: { test: "test" },
  });
  expect(collection2).toBeDefined();
  expect(collection2).toHaveProperty("name");
  expect(collection2).toHaveProperty("id");
  expect(collection2.name).toBe("test2");
  expect(collection2).toHaveProperty("metadata");
  expect(collection2.metadata).toHaveProperty("test");
  expect(collection2.metadata).toEqual({ test: "test" });
  let collections2 = await chroma.listCollections();
  expect(collections2).toMatchInlineSnapshot(`
    [
      {
        "database": "default_database",
        "dimension": null,
        "id": "${collection2.id}",
        "metadata": {
          "test": "test",
        },
        "name": "test2",
        "tenant": "default_tenant",
        "version": 0,
      },
    ]
  `);
});

test("it should get a collection", async () => {
  const collection = await chroma.createCollection({ name: "test" });
  const collection2 = await chroma.getCollection({
    name: "test",
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection).toBeDefined();
  expect(collection2).toBeDefined();
  expect(collection).toHaveProperty("name");
  expect(collection2).toHaveProperty("name");
  expect(collection.name).toBe(collection2.name);
});

// test("it should get or create a collection", async () => {
//   await chroma.createCollection("test");

//   const collection2 = await chroma.getOrCreateCollection("test");
//   expect(collection2).toBeDefined();
//   expect(collection2).toHaveProperty("name");
//   expect(collection2.name).toBe("test");

//   const collection3 = await chroma.getOrCreateCollection("test3");
//   expect(collection3).toBeDefined();
//   expect(collection3).toHaveProperty("name");
//   expect(collection3.name).toBe("test3");
// });

test("it should delete a collection", async () => {
  const collection = await chroma.createCollection({ name: "test" });
  let collections = await chroma.listCollections();
  expect(collections.length).toBe(1);
  await chroma.deleteCollection({ name: "test" });
  collections = await chroma.listCollections();
  expect(collections.length).toBe(0);
});

// TODO: I want to test this, but I am not sure how to
// test('custom index params', async () => {
//     throw new Error('not implemented')
//     await chroma.reset()
//     const collection = await chroma.createCollection('test', {"hnsw:space": "cosine"})
// })

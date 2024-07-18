import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { DefaultEmbeddingFunction } from "../src/embeddings/DefaultEmbeddingFunction";

test("it should modify collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  expect(collection.name).toBe("test");
  expect(collection.metadata).toBeNull();

  collection.name = "test2";
  await chroma.updateCollection(collection);
  expect(collection.name).toBe("test2");
  expect(collection.metadata).toBeNull();

  const collection2 = await chroma.getCollection({
    name: "test2",
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection2.name).toBe("test2");
  expect(collection2.metadata).toBeNull();

  // test changing name and metadata independently
  // and verify there are no side effects
  const original_name = "test3";
  const new_name = "test4";
  const original_metadata = { test: "test" };
  const new_metadata = { test: "test2" };

  const collection3 = await chroma.createCollection({
    name: original_name,
    metadata: original_metadata,
  });
  expect(collection3.name).toBe(original_name);
  expect(collection3.metadata).toEqual(original_metadata);

  collection3.name = new_name;
  await chroma.updateCollection(collection3);
  expect(collection3.name).toBe(new_name);
  expect(collection3.metadata).toEqual(original_metadata);

  const collection4 = await chroma.getCollection({
    name: new_name,
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection4.name).toBe(new_name);
  expect(collection4.metadata).toEqual(original_metadata);

  collection3.metadata = new_metadata;
  await chroma.updateCollection(collection3);
  expect(collection3.name).toBe(new_name);
  expect(collection3.metadata).toEqual(new_metadata);

  const collection5 = await chroma.getCollection({
    name: new_name,
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection5.name).toBe(new_name);
  expect(collection5.metadata).toEqual(new_metadata);
});

test("it fails with a nice error when calling the legacy functions", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  // @ts-ignore
  expect(collection.peek()).rejects.toThrowErrorMatchingInlineSnapshot(
    `"Collection's methods have been moved to ChromaClient. Please use ChromaClient.peekRecords() instead."`,
  );
});

test("it should store metadata", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({
    name: "test",
    metadata: { test: "test" },
  });
  expect(collection.metadata).toEqual({ test: "test" });

  // get the collection
  const collection2 = await chroma.getCollection({
    name: "test",
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection2.metadata).toEqual({ test: "test" });

  // get or create the collection
  const collection3 = await chroma.getOrCreateCollection({ name: "test" });
  expect(collection3.metadata).toEqual({ test: "test" });

  // modify
  collection3.metadata = { test: "test2" };
  await chroma.updateCollection(collection3);
  expect(collection3.metadata).toEqual({ test: "test2" });

  // get it again
  const collection4 = await chroma.getCollection({
    name: "test",
    embeddingFunction: new DefaultEmbeddingFunction(),
  });
  expect(collection4.metadata).toEqual({ test: "test2" });
});

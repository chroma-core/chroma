import { expect, test, beforeEach, describe } from "@jest/globals";
import { DefaultEmbeddingFunction, ChromaClient } from "../src";
import {
  CreateCollectionConfiguration,
  UpdateCollectionConfiguration,
} from "../src/CollectionConfiguration";

describe("collection operations", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should list collections", async () => {
    let collections = await client.listCollections();
    expect(Array.isArray(collections)).toBe(true);
    expect(collections).toHaveLength(0);
    await client.createCollection({ name: "test" });
    collections = await client.listCollections();
    expect(collections).toHaveLength(1);
  });

  test("it should list collections with metadata", async () => {
    await client.createCollection({ name: "test", metadata: { test: "test" } });
    const collections = await client.listCollectionsAndMetadata();
    expect(collections).toHaveLength(1);
    const [collection] = collections;
    expect(collection).toHaveProperty("metadata");
    expect(collection.metadata).toHaveProperty("test");
    expect(collection.metadata).toEqual({ test: "test" });
  });

  test("it should create a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    expect(collection).toBeDefined();
    expect(collection).toHaveProperty("name");
    expect(collection).toHaveProperty("id");
    expect(collection.name).toBe("test");
    let collections = await client.listCollections();
    expect(collections).toHaveLength(1);

    const [returnedCollection] = collections;

    expect(returnedCollection).toEqual("test");

    expect([{ name: "test2", metadata: null }]).not.toEqual(
      expect.arrayContaining(collections),
    );

    await client.reset();
    const collection2 = await client.createCollection({
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
    const collections2 = await client.listCollections();
    expect(collections2).toHaveLength(1);
    const [returnedCollection2] = collections2;
    expect(returnedCollection2).toEqual("test2");
  });

  test("it should get a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    const collection2 = await client.getCollection({
      name: "test",
      embeddingFunction: new DefaultEmbeddingFunction(),
    });
    expect(collection).toBeDefined();
    expect(collection2).toBeDefined();
    expect(collection).toHaveProperty("name");
    expect(collection2).toHaveProperty("name");
    expect(collection.name).toBe(collection2.name);
  });

  test("it should get or create a collection", async () => {
    await client.createCollection({ name: "test" });
    const collection2 = await client.getOrCreateCollection({ name: "test" });
    expect(collection2).toBeDefined();
    expect(collection2).toHaveProperty("name");
    expect(collection2.name).toBe("test");
    const collection3 = await client.getOrCreateCollection({ name: "test3" });
    expect(collection3).toBeDefined();
    expect(collection3).toHaveProperty("name");
    expect(collection3.name).toBe("test3");
  });

  test("it should delete a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    let collections = await client.listCollections();
    expect(collections.length).toBe(1);
    await client.deleteCollection({ name: "test" });
    collections = await client.listCollections();
    expect(collections.length).toBe(0);
  });

  test("it should create a collection with configuration", async () => {
    const config: CreateCollectionConfiguration = {
      hnsw: {
        space: "cosine",
        ef_construction: 100,
        max_neighbors: 10,
        ef_search: 20,
        num_threads: 2,
      },
    };
    const collection = await client.createCollection({
      name: "test_config_create",
      configuration: config,
    });
    expect(collection).toBeDefined();
    expect(collection.name).toBe("test_config_create");
    expect(collection.configuration).toBeDefined();
    expect(collection.configuration).toHaveProperty("hnsw");
    expect(collection.configuration?.hnsw?.space).toBe("cosine");
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100);
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(10);
    expect(collection.configuration?.hnsw?.ef_search).toBe(20);
    expect(collection.configuration?.hnsw?.num_threads).toBe(2);
  });

  test("it should get a collection with configuration", async () => {
    const config: CreateCollectionConfiguration = {
      hnsw: {
        space: "l2",
        ef_construction: 150,
        max_neighbors: 15,
      },
    };
    await client.createCollection({
      name: "test_config_get",
      configuration: config,
    });

    const collection = await client.getCollection({
      name: "test_config_get",
      embeddingFunction: new DefaultEmbeddingFunction(),
    });
    console.log("Configuration after getCollection:", collection.configuration);
    expect(collection).toBeDefined();
    expect(collection.name).toBe("test_config_get");
    expect(collection.configuration).toBeDefined();
    expect(collection.configuration).toHaveProperty("hnsw");
    expect(collection.configuration?.hnsw?.space).toBe("l2");
    expect(collection.configuration?.hnsw?.ef_construction).toBe(150);
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(15);
    // ef_search and num_threads should have default values if not specified
    expect(collection.configuration?.hnsw?.ef_search).toBeDefined();
    expect(collection.configuration?.hnsw?.num_threads).toBeDefined();
  });

  test("it should update a collection configuration", async () => {
    const initialConfig: CreateCollectionConfiguration = {
      hnsw: {
        space: "cosine",
        ef_search: 10,
        num_threads: 1,
      },
    };
    const collection = await client.createCollection({
      name: "test_config_update",
      configuration: initialConfig,
    });

    expect(collection.configuration?.hnsw?.ef_search).toBe(10);
    expect(collection.configuration?.hnsw?.num_threads).toBe(1);

    // Update configuration
    const updateConfig: UpdateCollectionConfiguration = {
      hnsw: {
        ef_search: 20,
        num_threads: 2,
      },
    };
    await collection.modify({ configuration: updateConfig });

    // Get the collection again to verify the update
    const updatedCollection = await client.getCollection({
      name: "test_config_update",
      embeddingFunction: new DefaultEmbeddingFunction(),
    });
    console.log(
      "Configuration after modify and getCollection:",
      updatedCollection.configuration,
    );
    expect(updatedCollection).toBeDefined();
    expect(updatedCollection.configuration).toBeDefined();
    expect(updatedCollection.configuration).toHaveProperty("hnsw");
    // Check updated values
    expect(updatedCollection.configuration?.hnsw?.ef_search).toBe(20);
    expect(updatedCollection.configuration?.hnsw?.num_threads).toBe(2);
    // Check that unspecified values remain (space, ef_construction, max_neighbors)
    expect(updatedCollection.configuration?.hnsw?.space).toBe("cosine");
    // Remove checks for defaults as API doesn't return them currently
    expect(
      updatedCollection.configuration?.hnsw?.ef_construction,
    ).toBeDefined(); // Default value
    expect(updatedCollection.configuration?.hnsw?.max_neighbors).toBeDefined(); // Default value
  });

  // TODO: I want to test this, but I am not sure how to
  // test('custom index params', async () => {
  //     throw new Error('not implemented')
  //     await client.reset()
  //     const collection = await client.createCollection('test', {"hnsw:space": "cosine"})
  // })
});

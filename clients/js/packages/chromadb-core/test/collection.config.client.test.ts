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
    expect(collection.configuration?.hnsw?.ef_search).toBe(100);
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
    expect(updatedCollection.configuration?.hnsw?.ef_search).toBe(20);
    expect(updatedCollection.configuration?.hnsw?.space).toBe("cosine");
    expect(updatedCollection.configuration?.hnsw?.ef_construction).toBe(100);
    expect(updatedCollection.configuration?.hnsw?.max_neighbors).toBe(16);
  });

  test("it should reject invalid configurations", async () => {
    // Test invalid HNSW parameters
    const invalidHnswConfig: CreateCollectionConfiguration = {
      hnsw: {
        ef_construction: -1, // Invalid value
        space: "cosine",
      },
    };

    await expect(
      client.createCollection({
        name: "test_invalid_hnsw",
        configuration: invalidHnswConfig,
      }),
      // Expecting an error related to validation, the exact message might vary
    ).rejects.toThrow();

    // TODO: Add test for invalid space for embedding function if supported
    // This might require a custom embedding function in JS or mocking.
  });

  test("it should apply default configurations when none are provided", async () => {
    const collection = await client.createCollection({
      name: "test_defaults",
    });
    expect(collection.configuration).toBeDefined();
    expect(collection.configuration).toHaveProperty("hnsw");
    expect(collection.configuration?.hnsw?.space).toBe("l2"); // Default
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100); // Default
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(16); // Default
    expect(collection.configuration?.hnsw?.ef_search).toBe(100); // Default
  });

  test("it should apply defaults for unspecified hnsw params (space)", async () => {
    const partialConfig: CreateCollectionConfiguration = {
      hnsw: { space: "cosine" },
    };
    const collection = await client.createCollection({
      name: "test_partial_space",
      configuration: partialConfig,
    });
    expect(collection.configuration?.hnsw?.space).toBe("cosine"); // Specified
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100); // Default
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(16); // Default
    expect(collection.configuration?.hnsw?.ef_search).toBe(100); // Default
  });

  test("it should apply defaults for unspecified hnsw params (ef_construction)", async () => {
    const partialConfig: CreateCollectionConfiguration = {
      hnsw: { ef_construction: 200, space: "l2" }, // space is required
    };
    const collection = await client.createCollection({
      name: "test_partial_ef_construction",
      configuration: partialConfig,
    });
    expect(collection.configuration?.hnsw?.space).toBe("l2"); // Specified
    expect(collection.configuration?.hnsw?.ef_construction).toBe(200); // Specified
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(16); // Default
    expect(collection.configuration?.hnsw?.ef_search).toBe(100); // Default
  });

  test("it should apply defaults for unspecified hnsw params (max_neighbors)", async () => {
    const partialConfig: CreateCollectionConfiguration = {
      hnsw: { max_neighbors: 32, space: "l2" }, // space is required
    };
    const collection = await client.createCollection({
      name: "test_partial_max_neighbors",
      configuration: partialConfig,
    });
    expect(collection.configuration?.hnsw?.space).toBe("l2"); // Specified
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100); // Default
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(32); // Specified
    expect(collection.configuration?.hnsw?.ef_search).toBe(100); // Default
  });

  test("it should apply defaults for unspecified hnsw params (ef_search)", async () => {
    const partialConfig: CreateCollectionConfiguration = {
      hnsw: { ef_search: 50, space: "l2" }, // space is required
    };
    const collection = await client.createCollection({
      name: "test_partial_ef_search",
      configuration: partialConfig,
    });
    expect(collection.configuration?.hnsw?.space).toBe("l2"); // Specified
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100); // Default
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(16); // Default
    expect(collection.configuration?.hnsw?.ef_search).toBe(50); // Specified
  });

  test("it should apply defaults for unspecified hnsw params (num_threads)", async () => {
    const partialConfig: CreateCollectionConfiguration = {
      hnsw: { num_threads: 4, space: "l2" }, // space is required
    };
    const collection = await client.createCollection({
      name: "test_partial_num_threads",
      configuration: partialConfig,
    });
    expect(collection.configuration?.hnsw?.space).toBe("l2"); // Specified
    expect(collection.configuration?.hnsw?.ef_construction).toBe(100); // Default
    expect(collection.configuration?.hnsw?.max_neighbors).toBe(16); // Default
    expect(collection.configuration?.hnsw?.ef_search).toBe(100); // Default
  });
});

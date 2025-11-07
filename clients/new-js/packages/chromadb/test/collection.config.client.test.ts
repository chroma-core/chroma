import { expect, test, beforeEach, describe } from "@jest/globals";
import {
  ChromaClient,
  CreateCollectionConfiguration,
  UpdateCollectionConfiguration,
} from "../src";
import { DefaultEmbeddingFunction } from "@chroma-core/default-embed";
import { EmbeddingFunction, EmbeddingFunctionSpace } from "../src/embedding-function";
import { CollectionMetadata } from "../src/types";
import { Schema, VectorIndexConfig } from "../src/schema";

class DefaultSpaceCustomEmbeddingFunction implements EmbeddingFunction {
  private _dim: number;
  private _modelName: string;

  constructor(modelName: string, dim: number = 3) {
    this._dim = dim;
    this._modelName = modelName;
  }

  async generate(texts: string[]): Promise<number[][]> {
    return texts.map(() => Array(this._dim).fill(1.0));
  }

  name = "default_space_custom_ef";

  getConfig(): Record<string, any> {
    return { model_name: this._modelName, dim: this._dim };
  }

  buildFromConfig(config: Record<string, any>): EmbeddingFunction {
    return new DefaultSpaceCustomEmbeddingFunction(
      config.model_name,
      config.dim
    );
  }

  defaultSpace(): EmbeddingFunctionSpace {
    if (this._modelName === "i_want_cosine") {
      return "cosine";
    } else if (this._modelName === "i_want_l2") {
      return "l2";
    } else if (this._modelName === "i_want_ip") {
      return "ip";
    } else {
      return "cosine";
    }
  }

  supportedSpaces(): EmbeddingFunctionSpace[] {
    if (this._modelName === "i_want_cosine") {
      return ["cosine"];
    } else if (this._modelName === "i_want_l2") {
      return ["l2"];
    } else if (this._modelName === "i_want_ip") {
      return ["ip"];
    } else if (this._modelName === "i_want_anything") {
      return ["cosine", "l2", "ip"];
    } else {
      return ["cosine", "l2", "ip"];
    }
  }

  static buildFromConfig(config: Record<string, any>): EmbeddingFunction {
    return new DefaultSpaceCustomEmbeddingFunction(
      config.model_name,
      config.dim
    );
  }
}

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
        // num_threads: 1,
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
        // num_threads: 2,
      },
    };
    await collection.modify({ configuration: updateConfig });

    // Get the collection again to verify the update
    const updatedCollection = await client.getCollection({
      name: "test_config_update",
      embeddingFunction: new DefaultEmbeddingFunction(),
    });
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
      hnsw: {
        // num_threads: 4,
        space: "l2",
      }, // space is required
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

describe("default space functionality", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should create collection with custom embedding function and default space (cosine)", async () => {
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_cosine",
        3
      ),
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_cosine", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("cosine");
  });

  test("it should create collection with custom embedding function and default space (l2)", async () => {
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_l2",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_l2",
        3
      ),
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_l2", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("l2");
  });

  test("it should create collection with custom embedding function and default space (ip)", async () => {
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_ip",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_ip",
        3
      ),
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_ip", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("ip");
  });

  test("it should create collection with custom embedding function and default space (anything)", async () => {
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_anything",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_anything",
        3
      ),
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_anything", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("cosine");
  });

  test("it should create collection with custom embedding function and valid explicit configuration", async () => {
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_with_valid_config",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_anything",
        3
      ),
      configuration: { hnsw: { space: "l2" } },
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_anything", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("l2");
  });

  test("it should warn but still create collection with invalid space configuration", async () => {
    // Now warns instead of raising error for invalid space configurations
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_with_invalid_config",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_cosine",
        3
      ),
      configuration: { hnsw: { space: "l2" } },
    });

    // Collection should still be created despite the warning
    expect(collection).toBeDefined();
    expect(collection.configuration?.hnsw?.space).toBe("l2");

    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_cosine", dim: 3 });
    }
  });

  test("it should create collection with custom embedding function and metadata space", async () => {
    const metadata: CollectionMetadata = { "hnsw:space": "ip" };
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_with_metadata",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_anything",
        3
      ),
      metadata,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_anything", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("ip");
  });

  test("it should warn but still create collection with invalid metadata space", async () => {
    const metadata: CollectionMetadata = { "hnsw:space": "l2" };

    // Now warns instead of raising error for invalid space configurations
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_with_invalid_metadata",
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_cosine",
        3
      ),
      metadata,
    });

    // Collection should still be created despite the warning
    expect(collection).toBeDefined();
    expect(collection.configuration?.hnsw?.space).toBe("l2");

    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_cosine", dim: 3 });
    }
  });

  test("it should prioritize configuration over metadata when both are provided", async () => {
    const metadata: CollectionMetadata = { "hnsw:space": "l2" };
    const collection = await client.createCollection({
      name: "test_default_space_custom_embedding_function_with_metadata_and_config",
      configuration: { hnsw: { space: "ip" } },
      embeddingFunction: new DefaultSpaceCustomEmbeddingFunction(
        "i_want_anything",
        3
      ),
      metadata,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default_space_custom_ef");
      expect(ef.config).toEqual({ model_name: "i_want_anything", dim: 3 });
    }

    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("ip");
  });

  test("it should not set default space when embedding function has no supportedSpaces method", async () => {
    const partialEF: EmbeddingFunction = {
      generate: async (texts: string[]) => texts.map(() => [1.0, 1.0, 1.0]),
      defaultSpace: () => "cosine",
    };

    const collection = await client.createCollection({
      name: "test_partial_embedding_function",
      embeddingFunction: partialEF,
    });

    expect(collection).toBeDefined();
    const hnswConfig = collection.configuration?.hnsw;
    expect(hnswConfig).toBeDefined();
    expect(hnswConfig?.space).toBe("l2");
  });
});

describe("embedding function null vs undefined handling", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should use default embedding function when embeddingFunction is undefined", async () => {
    const collection = await client.createCollection({
      name: "test_undefined_embedding_function",
      embeddingFunction: undefined,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    if (ef?.type === "known") {
      expect(ef.name).toBe("default");
    }
  });

  test("it should NOT use default embedding function when embeddingFunction is explicitly null", async () => {
    const collection = await client.createCollection({
      name: "test_null_embedding_function",
      embeddingFunction: null,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeNull();
  });

  test("it should NOT use default embedding function when schema has embedding function", async () => {
    const schema = new Schema();
    const mockEf = new DefaultSpaceCustomEmbeddingFunction("i_want_cosine", 3);
    schema.createIndex(new VectorIndexConfig({ embeddingFunction: mockEf }));

    const collection = await client.createCollection({
      name: "test_schema_embedding_function",
      schema,
      embeddingFunction: undefined,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    // Should use schema embedding function, not default
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    expect(ef?.name).toBe("default_space_custom_ef");
  });

  test("it should NOT use default embedding function when schema has null embedding function", async () => {
    const schema = new Schema();
    schema.createIndex(new VectorIndexConfig({ embeddingFunction: null }));

    const collection = await client.createCollection({
      name: "test_schema_null_embedding_function",
      schema,
      embeddingFunction: undefined,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    // Schema has null, so should NOT use default (null means explicitly no embedding function)
    expect(ef?.type).toBe("legacy");
  });

  test("it should NOT use default when embeddingFunction is null", async () => {
    const schema = new Schema();
    schema.createIndex(new VectorIndexConfig());

    const collection = await client.createCollection({
      name: "test_null_with_schema_undefined",
      schema,
      embeddingFunction: null,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef?.type).toBe("legacy");
  });

  test("it should error if both schema and embedding function are provided", async () => {
    const schema = new Schema();
    const providedEf = new DefaultSpaceCustomEmbeddingFunction("i_want_l2", 5);

    try {
      const collection = await client.createCollection({
        name: "test_provided_over_schema",
        schema,
        embeddingFunction: providedEf,
      });
    } catch (error) {
      expect(error).toBeDefined();
    }
  });
  test("it should use provided embedding function if no schema", async () => {
    const providedEf = new DefaultSpaceCustomEmbeddingFunction("i_want_l2", 5);

    const collection = await client.createCollection({
      name: "test_provided_over_schema",
      embeddingFunction: providedEf,
    });

    expect(collection).toBeDefined();
    const ef = (collection.configuration as any)?.embedding_function;
    expect(ef).toBeDefined();
    expect(ef?.type).toBe("known");
    expect(ef?.name).toBe("default_space_custom_ef");
    expect(ef?.config).toEqual({ model_name: "i_want_l2", dim: 5 });
  });
});

import { CollectionImpl } from "../src/collection";
import type { CollectionConfiguration } from "../src/collection-configuration";
import type { CollectionMetadata } from "../src/types";
import {
  registerEmbeddingFunction,
  registerSparseEmbeddingFunction,
  EmbeddingFunction,
  SparseEmbeddingFunction,
} from "../src/embedding-function";
import {
  DOCUMENT_KEY,
  EMBEDDING_KEY,
  Schema,
  FtsIndexConfig,
  StringInvertedIndexConfig,
  IntInvertedIndexConfig,
  FloatInvertedIndexConfig,
  BoolInvertedIndexConfig,
  SparseVectorIndexConfig,
  VectorIndexConfig,
} from "../src/schema";
import type { ChromaClient } from "../src/chroma-client";

class MockEmbedding implements EmbeddingFunction {
  public readonly name = "mock_embedding";

  constructor(private readonly modelName = "mock_model") { }

  async generate(texts: string[]): Promise<number[][]> {
    return texts.map(() => [1, 2, 3]);
  }

  getConfig(): Record<string, any> {
    return { modelName: this.modelName };
  }

  defaultSpace(): "cosine" {
    return "cosine";
  }

  supportedSpaces(): ("cosine" | "l2" | "ip")[] {
    return ["cosine", "l2", "ip"];
  }

  static buildFromConfig(config: Record<string, any>): MockEmbedding {
    return new MockEmbedding(config.modelName);
  }
}

class MockSparseEmbedding implements SparseEmbeddingFunction {
  public readonly name = "mock_sparse";

  constructor(private readonly identifier = "mock_sparse") { }

  async generate(texts: string[]) {
    return texts.map(() => ({ indices: [0, 1], values: [1, 1] }));
  }

  getConfig(): Record<string, any> {
    return { identifier: this.identifier };
  }

  static buildFromConfig(config: Record<string, any>): MockSparseEmbedding {
    return new MockSparseEmbedding(config.identifier);
  }
}

class DeterministicSparseEmbedding implements SparseEmbeddingFunction {
  public readonly name = "deterministic_sparse";

  constructor(private readonly label = "det") { }

  async generate(texts: string[]) {
    return texts.map((text, index) => {
      const indices: number[] = [];
      const values: number[] = [];

      for (let i = 0; i < text.length; i++) {
        indices.push(index * 1000 + i);
        values.push(text.charCodeAt(i) / 100.0);
      }

      return { indices, values };
    });
  }

  getConfig(): Record<string, any> {
    return { label: this.label };
  }

  static buildFromConfig(
    config: Record<string, any>,
  ): DeterministicSparseEmbedding {
    return new DeterministicSparseEmbedding(config.label);
  }
}

beforeAll(() => {
  try {
    registerEmbeddingFunction("mock_embedding", MockEmbedding as any);
  } catch (_err) {
    // ignore double registration in watch mode
  }
  try {
    registerSparseEmbeddingFunction("mock_sparse", MockSparseEmbedding as any);
  } catch (_err) {
    // ignore double registration in watch mode
  }
});

describe("Schema", () => {
  it("default schema initialization", () => {
    const schema = new Schema();

    expect(schema.defaults).toBeDefined();

    expect(schema.defaults.string).not.toBeNull();
    expect(schema.defaults.string?.ftsIndex?.enabled).toBe(false);
    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(true);

    expect(schema.defaults.floatList).not.toBeNull();
    expect(schema.defaults.floatList?.vectorIndex?.enabled).toBe(false);

    expect(schema.defaults.sparseVector).not.toBeNull();
    expect(schema.defaults.sparseVector?.sparseVectorIndex?.enabled).toBe(
      false,
    );

    expect(schema.defaults.intValue).not.toBeNull();
    expect(schema.defaults.intValue?.intInvertedIndex?.enabled).toBe(true);

    expect(schema.defaults.floatValue).not.toBeNull();
    expect(schema.defaults.floatValue?.floatInvertedIndex?.enabled).toBe(true);

    expect(schema.defaults.boolean).not.toBeNull();
    expect(schema.defaults.boolean?.boolInvertedIndex?.enabled).toBe(true);

    const overrideKeys = Object.keys(schema.keys);
    expect(overrideKeys).toEqual(
      expect.arrayContaining([DOCUMENT_KEY, EMBEDDING_KEY]),
    );
    expect(overrideKeys).toHaveLength(2);

    const documentOverride = schema.keys[DOCUMENT_KEY];
    expect(documentOverride.string?.ftsIndex?.enabled).toBe(true);
    expect(documentOverride.string?.stringInvertedIndex?.enabled).toBe(false);

    const embeddingOverride = schema.keys[EMBEDDING_KEY];
    expect(embeddingOverride.floatList?.vectorIndex?.enabled).toBe(true);
    expect(embeddingOverride.floatList?.vectorIndex?.config.sourceKey).toBe(
      DOCUMENT_KEY,
    );
  });

  it("create sparse vector index on key", () => {
    const schema = new Schema();
    const config = new SparseVectorIndexConfig();

    const result = schema.createIndex(config, "custom_sparse_key");
    expect(result).toBe(schema);

    const override = schema.keys["custom_sparse_key"];
    expect(override.sparseVector?.sparseVectorIndex?.enabled).toBe(true);
    expect(override.sparseVector?.sparseVectorIndex?.config).toBe(config);
    expect(override.string).toBeNull();
    expect(override.floatList).toBeNull();
    expect(override.intValue).toBeNull();
    expect(override.floatValue).toBeNull();
    expect(override.boolean).toBeNull();

    expect(schema.defaults.sparseVector?.sparseVectorIndex?.enabled).toBe(
      false,
    );
  });

  it("create sparse vector index with custom config", () => {
    const schema = new Schema();
    const embeddingFunc = new MockSparseEmbedding("custom_sparse_ef");
    const config = new SparseVectorIndexConfig({
      embeddingFunction: embeddingFunc,
      sourceKey: "custom_document_field",
    });

    const result = schema.createIndex(config, "sparse_embeddings");
    expect(result).toBe(schema);

    const override = schema.keys["sparse_embeddings"];
    const sparseIndex = override.sparseVector?.sparseVectorIndex;
    expect(sparseIndex?.enabled).toBe(true);
    expect(sparseIndex?.config).toBe(config);
    expect(sparseIndex?.config.embeddingFunction).toBe(embeddingFunc);
    expect(sparseIndex?.config.sourceKey).toBe("custom_document_field");

    expect(schema.defaults.sparseVector?.sparseVectorIndex?.enabled).toBe(
      false,
    );
    expect(
      schema.defaults.sparseVector?.sparseVectorIndex?.config.embeddingFunction,
    ).toBeUndefined();
  });

  it("delete string inverted index on key", () => {
    const schema = new Schema();
    const config = new StringInvertedIndexConfig();

    const result = schema.deleteIndex(config, "custom_text_key");
    expect(result).toBe(schema);

    const override = schema.keys["custom_text_key"];
    expect(override.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(override.string?.stringInvertedIndex?.config).toBe(config);

    expect(schema.keys[DOCUMENT_KEY].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
    expect(schema.keys[EMBEDDING_KEY].string).toBeNull();
    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(true);
  });

  it("chained create and delete operations", () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("chained_test");
    const sparseConfig = new SparseVectorIndexConfig({
      sourceKey: "raw_text",
      embeddingFunction: sparseEf,
    });
    const stringConfig = new StringInvertedIndexConfig();

    const result = schema
      .createIndex(sparseConfig, "embeddings_key")
      .deleteIndex(stringConfig, "text_key_1")
      .deleteIndex(stringConfig, "text_key_2");

    expect(result).toBe(schema);

    const embeddingsOverride = schema.keys["embeddings_key"];
    expect(embeddingsOverride.sparseVector?.sparseVectorIndex?.enabled).toBe(
      true,
    );
    expect(
      embeddingsOverride.sparseVector?.sparseVectorIndex?.config.sourceKey,
    ).toBe("raw_text");
    expect(embeddingsOverride.string).toBeNull();
    expect(embeddingsOverride.floatList).toBeNull();

    const textKey1 = schema.keys["text_key_1"];
    expect(textKey1.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(textKey1.sparseVector).toBeNull();

    const textKey2 = schema.keys["text_key_2"];
    expect(textKey2.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(textKey2.sparseVector).toBeNull();

    expect(schema.defaults.sparseVector?.sparseVectorIndex?.enabled).toBe(
      false,
    );
    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(true);
  });

  it("vector index config and restrictions", () => {
    const schema = new Schema();
    const vectorConfig = new VectorIndexConfig({
      space: "cosine",
      sourceKey: "custom_source",
    });

    const result = schema.createIndex(vectorConfig);
    expect(result).toBe(schema);

    const defaultsVector = schema.defaults.floatList?.vectorIndex;
    expect(defaultsVector?.enabled).toBe(false);
    expect(defaultsVector?.config.space).toBe("cosine");
    expect(defaultsVector?.config.sourceKey).toBe("custom_source");

    const embeddingVector = schema.keys[EMBEDDING_KEY].floatList?.vectorIndex;
    expect(embeddingVector?.enabled).toBe(true);
    expect(embeddingVector?.config.space).toBe("cosine");
    expect(embeddingVector?.config.sourceKey).toBe(DOCUMENT_KEY);

    expect(() =>
      schema.createIndex(new VectorIndexConfig({ space: "l2" }), "my_vectors"),
    ).toThrow(/Vector index cannot be enabled on specific keys/);
    expect(() =>
      schema.createIndex(new VectorIndexConfig({ space: "l2" }), DOCUMENT_KEY),
    ).toThrow(/Cannot create index on special key '#document'/);
    expect(() =>
      schema.createIndex(new VectorIndexConfig({ space: "ip" }), EMBEDDING_KEY),
    ).toThrow(/Cannot create index on special key '#embedding'/);
  });

  it("vector index with embedding function and hnsw", () => {
    const schema = new Schema();
    const mockEf = new MockEmbedding("custom_model_v2");
    const vectorConfig = new VectorIndexConfig({
      embeddingFunction: mockEf,
      space: "l2",
      hnsw: { ef_construction: 200, max_neighbors: 32, ef_search: 100 },
      sourceKey: "custom_document_field",
    });

    const result = schema.createIndex(vectorConfig);
    expect(result).toBe(schema);

    const defaultsVector = schema.defaults.floatList?.vectorIndex;
    expect(defaultsVector?.enabled).toBe(false);
    expect(defaultsVector?.config.embeddingFunction).toBe(mockEf);
    expect(defaultsVector?.config.space).toBe("l2");
    expect(defaultsVector?.config.hnsw).toEqual({
      ef_construction: 200,
      max_neighbors: 32,
      ef_search: 100,
    });
    expect(defaultsVector?.config.sourceKey).toBe("custom_document_field");

    const embeddingVector = schema.keys[EMBEDDING_KEY].floatList?.vectorIndex;
    expect(embeddingVector?.enabled).toBe(true);
    expect(embeddingVector?.config.embeddingFunction).toBe(mockEf);
    expect(embeddingVector?.config.space).toBe("l2");
    expect(embeddingVector?.config.hnsw).toEqual({
      ef_construction: 200,
      max_neighbors: 32,
      ef_search: 100,
    });
    expect(embeddingVector?.config.sourceKey).toBe(DOCUMENT_KEY);
  });

  it("fts index config and restrictions", () => {
    const schema = new Schema();
    const ftsConfig = new FtsIndexConfig();

    const result = schema.createIndex(ftsConfig);
    expect(result).toBe(schema);

    const defaultsString = schema.defaults.string;
    expect(defaultsString?.ftsIndex?.enabled).toBe(false);
    expect(defaultsString?.ftsIndex?.config).toBe(ftsConfig);

    const documentOverride = schema.keys[DOCUMENT_KEY];
    expect(documentOverride.string?.ftsIndex?.enabled).toBe(true);
    expect(documentOverride.string?.ftsIndex?.config).toBe(ftsConfig);

    expect(() =>
      schema.createIndex(new FtsIndexConfig(), "custom_text_field"),
    ).toThrow(/FTS index cannot be enabled on specific keys/);
    expect(() =>
      schema.createIndex(new FtsIndexConfig(), DOCUMENT_KEY),
    ).toThrow(/Cannot create index on special key '#document'/);
    expect(() =>
      schema.createIndex(new FtsIndexConfig(), EMBEDDING_KEY),
    ).toThrow(/Cannot create index on special key '#embedding'/);
  });

  it("special keys blocked for all index types", () => {
    const schema = new Schema();

    expect(() =>
      schema.createIndex(new StringInvertedIndexConfig(), DOCUMENT_KEY),
    ).toThrow(/Cannot create index on special key '#document'/);
    expect(() =>
      schema.createIndex(new StringInvertedIndexConfig(), EMBEDDING_KEY),
    ).toThrow(/Cannot create index on special key '#embedding'/);
    expect(() =>
      schema.createIndex(new SparseVectorIndexConfig(), DOCUMENT_KEY),
    ).toThrow(/Cannot create index on special key '#document'/);
    expect(() =>
      schema.createIndex(new SparseVectorIndexConfig(), EMBEDDING_KEY),
    ).toThrow(/Cannot create index on special key '#embedding'/);
  });

  it("cannot enable or disable all indexes for custom key", () => {
    const schema = new Schema();

    // TODO: Consider removing this check in the future to allow enabling all indexes for a key
    expect(() => schema.createIndex(undefined, "my_key")).toThrow(
      /Cannot enable all index types for key 'my_key'/,
    );

    // TODO: Consider removing this check in the future to allow disabling all indexes for a key
    expect(() => schema.deleteIndex(undefined, "my_key")).toThrow(
      /Cannot disable all index types for key 'my_key'/,
    );
  });

  it("cannot delete vector or fts index", () => {
    const schema = new Schema();

    expect(() => schema.deleteIndex(new VectorIndexConfig())).toThrow(
      "Deleting vector index is not currently supported.",
    );
    expect(() =>
      schema.deleteIndex(new VectorIndexConfig(), "my_vectors"),
    ).toThrow("Deleting vector index is not currently supported.");
    expect(() => schema.deleteIndex(new FtsIndexConfig())).toThrow(
      "Deleting FTS index is not currently supported.",
    );
    expect(() =>
      schema.deleteIndex(new FtsIndexConfig(), "my_text_field"),
    ).toThrow("Deleting FTS index is not currently supported.");
  });

  it("disable string inverted index globally", () => {
    const schema = new Schema();
    const config = new StringInvertedIndexConfig();

    const result = schema.deleteIndex(config);
    expect(result).toBe(schema);

    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(schema.defaults.string?.stringInvertedIndex?.config).toBe(config);

    expect(schema.keys[DOCUMENT_KEY].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
    expect(schema.keys[EMBEDDING_KEY].floatList?.vectorIndex?.enabled).toBe(
      true,
    );
  });

  it("disable string inverted index on key", () => {
    const schema = new Schema();
    const config = new StringInvertedIndexConfig();

    const result = schema.deleteIndex(config, "my_text_field");
    expect(result).toBe(schema);

    const override = schema.keys["my_text_field"];
    expect(override.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(override.string?.stringInvertedIndex?.config).toBe(config);
    expect(override.floatList).toBeNull();
    expect(override.sparseVector).toBeNull();
    expect(override.intValue).toBeNull();

    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(true);
    expect(schema.keys[DOCUMENT_KEY].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
  });

  it("disable int inverted index", () => {
    const schema = new Schema();
    const configGlobal = new IntInvertedIndexConfig();

    expect(schema.defaults.intValue?.intInvertedIndex?.enabled).toBe(true);

    schema.deleteIndex(configGlobal);
    expect(schema.defaults.intValue?.intInvertedIndex?.enabled).toBe(false);
    expect(schema.defaults.intValue?.intInvertedIndex?.config).toBe(
      configGlobal,
    );

    const configKey = new IntInvertedIndexConfig();
    schema.deleteIndex(configKey, "age_field");

    const override = schema.keys["age_field"];
    expect(override.intValue?.intInvertedIndex?.enabled).toBe(false);
    expect(override.intValue?.intInvertedIndex?.config).toBe(configKey);
    expect(override.string).toBeNull();
    expect(override.floatList).toBeNull();
    expect(override.sparseVector).toBeNull();
    expect(override.floatValue).toBeNull();
    expect(override.boolean).toBeNull();
  });

  // Additional tests will be appended below.

  it("serialize and deserialize default schema", async () => {
    const schema = new Schema();
    const json = schema.serializeToJSON();

    expect(json).toHaveProperty("defaults");
    expect(json).toHaveProperty("keys");

    const defaults = json.defaults;
    expect(defaults["string"]!["fts_index"]!.enabled).toBe(false);
    expect(defaults["string"]!["fts_index"]!.config).toEqual({});
    expect(defaults["string"]!["string_inverted_index"]!.enabled).toBe(true);
    expect(defaults["string"]!["string_inverted_index"]!.config).toEqual({});

    const vectorJson = defaults["float_list"]!["vector_index"]!;
    expect(vectorJson.enabled).toBe(false);
    expect(vectorJson.config!.embedding_function).toEqual({ type: "legacy" });
    expect(vectorJson.config!.space).toBeUndefined();

    const sparseJson = defaults["sparse_vector"]!["sparse_vector_index"]!;
    expect(sparseJson.enabled).toBe(false);
    expect(sparseJson.config!.embedding_function).toEqual({ type: "legacy" });

    expect(defaults["int"]!["int_inverted_index"]!.enabled).toBe(true);
    expect(defaults["float"]!["float_inverted_index"]!.enabled).toBe(true);
    expect(defaults["bool"]!["bool_inverted_index"]!.enabled).toBe(true);

    const overrides = json.keys;
    expect(overrides).toHaveProperty(DOCUMENT_KEY);
    expect(overrides).toHaveProperty(EMBEDDING_KEY);

    const documentJson = overrides[DOCUMENT_KEY]!["string"]!;
    expect(documentJson["fts_index"]!.enabled).toBe(true);
    expect(documentJson["fts_index"]!.config).toEqual({});
    expect(documentJson["string_inverted_index"]!.enabled).toBe(false);

    const embeddingJson =
      overrides[EMBEDDING_KEY]!["float_list"]!["vector_index"]!;
    expect(embeddingJson.enabled).toBe(true);
    expect(embeddingJson.config!.embedding_function).toEqual({
      type: "legacy",
    });
    expect(embeddingJson.config!.source_key).toBe(DOCUMENT_KEY);

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized).toBeDefined();
    expect(deserialized!.defaults.string?.ftsIndex?.enabled).toBe(false);
    expect(deserialized!.defaults.string?.stringInvertedIndex?.enabled).toBe(
      true,
    );
    expect(deserialized!.defaults.floatList?.vectorIndex?.enabled).toBe(false);
    expect(
      deserialized!.defaults.sparseVector?.sparseVectorIndex?.enabled,
    ).toBe(false);
    expect(deserialized!.defaults.intValue?.intInvertedIndex?.enabled).toBe(
      true,
    );
    expect(deserialized!.defaults.floatValue?.floatInvertedIndex?.enabled).toBe(
      true,
    );
    expect(deserialized!.defaults.boolean?.boolInvertedIndex?.enabled).toBe(
      true,
    );
    expect(deserialized!.keys[DOCUMENT_KEY].string?.ftsIndex?.enabled).toBe(
      true,
    );
    expect(
      deserialized!.keys[EMBEDDING_KEY].floatList?.vectorIndex?.enabled,
    ).toBe(true);
  });

  it("serialize and deserialize with vector config and no embedding function", async () => {
    const schema = new Schema();
    const vectorConfig = new VectorIndexConfig({
      space: "cosine",
      embeddingFunction: null,
    });

    schema.createIndex(vectorConfig);

    const json = schema.serializeToJSON();
    const defaultsVector = json.defaults["float_list"]!["vector_index"]!;
    expect(defaultsVector.enabled).toBe(false);
    expect(defaultsVector.config!.space).toBe("cosine");
    expect(defaultsVector.config!.embedding_function!.type).toBe("legacy");

    const embeddingVector =
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!;
    expect(embeddingVector.enabled).toBe(true);
    expect(embeddingVector.config!.space).toBe("cosine");
    expect(embeddingVector.config!.embedding_function!.type).toBe("legacy");
    expect(embeddingVector.config!.source_key).toBe(DOCUMENT_KEY);

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.floatList?.vectorIndex?.config.space).toBe(
      "cosine",
    );
    expect(
      deserialized?.defaults.floatList?.vectorIndex?.config.embeddingFunction,
    ).toBeUndefined();
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config.space,
    ).toBe("cosine");
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config
        .embeddingFunction,
    ).toBeUndefined();
  });

  it("serialize and deserialize with custom embedding function", async () => {
    const schema = new Schema();
    const mockEf = new MockEmbedding("custom_model_v3");
    const vectorConfig = new VectorIndexConfig({
      embeddingFunction: mockEf,
      space: "ip",
      hnsw: { ef_construction: 256, max_neighbors: 48, ef_search: 128 },
    });

    schema.createIndex(vectorConfig);

    const json = schema.serializeToJSON();
    const defaultsVector = json.defaults["float_list"]!["vector_index"]!;
    expect(defaultsVector.config!.space).toBe("ip");
    expect(defaultsVector.config!.embedding_function).toEqual({
      type: "known",
      name: "mock_embedding",
      config: { modelName: "custom_model_v3" },
    });
    expect(defaultsVector.config!.hnsw).toEqual({
      ef_construction: 256,
      max_neighbors: 48,
      ef_search: 128,
    });

    const embeddingVector =
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!;
    expect(embeddingVector.config!.embedding_function).toEqual({
      type: "known",
      name: "mock_embedding",
      config: { modelName: "custom_model_v3" },
    });
    expect(embeddingVector.config!.space).toBe("ip");
    expect(embeddingVector.config!.hnsw).toEqual({
      ef_construction: 256,
      max_neighbors: 48,
      ef_search: 128,
    });

    const deserialized = await Schema.deserializeFromJSON(json);
    const desDefaultsVector = deserialized?.defaults.floatList?.vectorIndex;
    expect(desDefaultsVector?.config.embeddingFunction).toBeDefined();
    expect(desDefaultsVector?.config.embeddingFunction?.getConfig?.()).toEqual({
      modelName: "custom_model_v3",
    });
    expect(desDefaultsVector?.config.space).toBe("ip");
    expect(desDefaultsVector?.config.hnsw).toEqual({
      ef_construction: 256,
      max_neighbors: 48,
      ef_search: 128,
    });
  });

  it("serialize and deserialize with SPANN config", async () => {
    const schema = new Schema();
    const mockEf = new MockEmbedding("spann_model");
    const spannConfig = {
      search_nprobe: 100,
      write_nprobe: 50,
      ef_construction: 200,
      ef_search: 150,
    };
    const vectorConfig = new VectorIndexConfig({
      embeddingFunction: mockEf,
      space: "cosine",
      spann: spannConfig,
    });

    schema.createIndex(vectorConfig);

    const json = schema.serializeToJSON();
    const defaultsVector = json.defaults["float_list"]!["vector_index"]!;
    expect(defaultsVector.config!.space).toBe("cosine");
    expect(defaultsVector.config!.embedding_function).toEqual({
      type: "known",
      name: "mock_embedding",
      config: { modelName: "spann_model" },
    });
    expect(defaultsVector.config!.spann).toEqual(spannConfig);
    expect(defaultsVector.config!.hnsw).toBeUndefined();

    const embeddingVector =
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!;
    expect(embeddingVector.config!.spann).toEqual(spannConfig);
    expect(embeddingVector.config!.hnsw).toBeUndefined();

    const deserialized = await Schema.deserializeFromJSON(json);
    const desDefaultsVector = deserialized?.defaults.floatList?.vectorIndex;
    expect(desDefaultsVector?.config.spann).toEqual(spannConfig);
    expect(desDefaultsVector?.config.hnsw).toBeNull();
    expect(desDefaultsVector?.config.embeddingFunction?.getConfig?.()).toEqual({
      modelName: "spann_model",
    });
    const desEmbeddingVector =
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex;
    expect(desEmbeddingVector?.config.spann).toEqual(spannConfig);
    expect(desEmbeddingVector?.config.hnsw).toBeNull();
  });

  it("serialize and deserialize complex mixed modifications", async () => {
    const schema = new Schema();

    const vectorConfig = new VectorIndexConfig({
      embeddingFunction: new MockEmbedding("mixed_test_model"),
      space: "ip",
      hnsw: { ef_construction: 300, max_neighbors: 64 },
    });
    schema.createIndex(vectorConfig);

    const sparseConfig = new SparseVectorIndexConfig({
      embeddingFunction: new MockSparseEmbedding("sparse_model"),
      sourceKey: "text_field",
    });
    schema.createIndex(sparseConfig, "embeddings_field");

    schema.deleteIndex(new StringInvertedIndexConfig(), "tags");
    schema.deleteIndex(new IntInvertedIndexConfig(), "count");
    schema.deleteIndex(new FloatInvertedIndexConfig(), "price");

    const json = schema.serializeToJSON();
    const defaultsVector = json.defaults["float_list"]!["vector_index"]!;
    expect(defaultsVector.config!.space).toBe("ip");
    expect(defaultsVector.config!.hnsw).toEqual({
      ef_construction: 300,
      max_neighbors: 64,
    });

    const overrides = json.keys;
    expect(overrides).toHaveProperty("embeddings_field");
    expect(overrides).toHaveProperty("tags");
    expect(overrides).toHaveProperty("count");
    expect(overrides).toHaveProperty("price");
    expect(overrides).toHaveProperty(DOCUMENT_KEY);
    expect(overrides).toHaveProperty(EMBEDDING_KEY);

    const embeddingsFieldJson = overrides["embeddings_field"]!;
    expect(
      embeddingsFieldJson["sparse_vector"]!["sparse_vector_index"]!.enabled,
    ).toBe(true);
    expect(
      embeddingsFieldJson["sparse_vector"]!["sparse_vector_index"]!.config!
        .source_key,
    ).toBe("text_field");
    expect(
      embeddingsFieldJson["sparse_vector"]!["sparse_vector_index"]!.config!
        .embedding_function,
    ).toEqual({
      type: "known",
      name: "mock_sparse",
      config: { identifier: "sparse_model" },
    });
    expect(Object.keys(embeddingsFieldJson)).toEqual(["sparse_vector"]);

    const tagsJson = overrides["tags"]!;
    expect(tagsJson["string"]!["string_inverted_index"]!.enabled).toBe(false);
    expect(tagsJson["string"]!["string_inverted_index"]!.config).toEqual({});

    const countJson = overrides["count"]!;
    expect(countJson["int"]!["int_inverted_index"]!.enabled).toBe(false);
    expect(countJson["int"]!["int_inverted_index"]!.config).toEqual({});

    const priceJson = overrides["price"]!;
    expect(priceJson["float"]!["float_inverted_index"]!.enabled).toBe(false);
    expect(priceJson["float"]!["float_inverted_index"]!.config).toEqual({});

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(
      deserialized?.keys["embeddings_field"].sparseVector?.sparseVectorIndex
        ?.enabled,
    ).toBe(true);
    expect(
      deserialized?.keys["embeddings_field"].sparseVector?.sparseVectorIndex
        ?.config.sourceKey,
    ).toBe("text_field");
    expect(
      deserialized?.keys["tags"].string?.stringInvertedIndex?.enabled,
    ).toBe(false);
    expect(
      deserialized?.keys["count"].intValue?.intInvertedIndex?.enabled,
    ).toBe(false);
    expect(
      deserialized?.keys["price"].floatValue?.floatInvertedIndex?.enabled,
    ).toBe(false);
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config.space,
    ).toBe("ip");
    expect(deserialized?.defaults.string?.stringInvertedIndex?.enabled).toBe(
      true,
    );
    expect(
      deserialized?.defaults.sparseVector?.sparseVectorIndex?.enabled,
    ).toBe(false);
  });

  it("multiple index types on same key", async () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("multi_test");

    schema.createIndex(
      new SparseVectorIndexConfig({
        sourceKey: "source",
        embeddingFunction: sparseEf,
      }),
      "multi_field",
    );
    schema.createIndex(new StringInvertedIndexConfig(), "multi_field");

    const override = schema.keys["multi_field"];
    expect(override.sparseVector?.sparseVectorIndex?.enabled).toBe(true);
    expect(override.string?.stringInvertedIndex?.enabled).toBe(true);
    expect(override.floatList).toBeNull();
    expect(override.intValue).toBeNull();
    expect(override.floatValue).toBeNull();
    expect(override.boolean).toBeNull();

    const json = schema.serializeToJSON();
    const multiFieldJson = json.keys["multi_field"]!;
    expect(
      multiFieldJson["sparse_vector"]!["sparse_vector_index"]!.enabled,
    ).toBe(true);
    expect(multiFieldJson["string"]!["string_inverted_index"]!.enabled).toBe(
      true,
    );

    const deserialized = await Schema.deserializeFromJSON(json);
    const desOverride = deserialized?.keys["multi_field"];
    expect(desOverride?.sparseVector?.sparseVectorIndex?.enabled).toBe(true);
    expect(desOverride?.string?.stringInvertedIndex?.enabled).toBe(true);
  });

  it("override then revert to default", async () => {
    const schema = new Schema();
    const stringConfig = new StringInvertedIndexConfig();

    schema.createIndex(stringConfig, "temp_field");
    expect(schema.keys["temp_field"].string?.stringInvertedIndex?.enabled).toBe(
      true,
    );

    schema.deleteIndex(stringConfig, "temp_field");
    expect(schema.keys["temp_field"].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );

    const json = schema.serializeToJSON();
    expect(
      json.keys["temp_field"]!["string"]!["string_inverted_index"]!.enabled,
    ).toBe(false);

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(
      deserialized?.keys["temp_field"].string?.stringInvertedIndex?.enabled,
    ).toBe(false);
  });

  it("error handling invalid operations", () => {
    const schema = new Schema();

    expect(() =>
      schema.createIndex(new VectorIndexConfig(), EMBEDDING_KEY),
    ).toThrow(/Cannot create index on special key '#embedding'/);
    expect(() =>
      schema.createIndex(new FtsIndexConfig(), DOCUMENT_KEY),
    ).toThrow(/Cannot create index on special key '#document'/);
    expect(() => schema.createIndex()).toThrow(
      /Cannot enable all index types globally/,
    );
    // TODO: Consider removing this check in the future to allow enabling all indexes for a key
    expect(() => schema.createIndex(undefined, "mykey")).toThrow(
      /Cannot enable all index types for key 'mykey'/,
    );
    // TODO: Consider removing this check in the future to allow disabling all indexes for a key
    expect(() => schema.deleteIndex(undefined, "mykey")).toThrow(
      /Cannot disable all index types for key 'mykey'/,
    );
    expect(() => schema.deleteIndex(new VectorIndexConfig())).toThrow(
      /Deleting vector index is not currently supported/,
    );
    expect(() => schema.deleteIndex(new FtsIndexConfig())).toThrow(
      /Deleting FTS index is not currently supported/,
    );
    expect(() =>
      schema.createIndex(new VectorIndexConfig(), "custom_field"),
    ).toThrow(/Vector index cannot be enabled on specific keys/);
    expect(() =>
      schema.createIndex(new FtsIndexConfig(), "custom_field"),
    ).toThrow(/FTS index cannot be enabled on specific keys/);
  });

  it("empty schema serialization", async () => {
    const schema = new Schema();
    const json = schema.serializeToJSON();

    expect(Object.keys(json.defaults)).toEqual(
      expect.arrayContaining([
        "string",
        "float_list",
        "sparse_vector",
        "int",
        "float",
        "bool",
      ]),
    );
    expect(Object.keys(json.keys)).toEqual(
      expect.arrayContaining([DOCUMENT_KEY, EMBEDDING_KEY]),
    );

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.string?.ftsIndex?.enabled).toBe(false);
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.enabled,
    ).toBe(true);
  });

  it("multiple serialize deserialize roundtrips", async () => {
    const schema = new Schema();
    const json1 = schema.serializeToJSON();
    const schema2 = await Schema.deserializeFromJSON(json1);
    const json2 = schema2?.serializeToJSON();
    const schema3 = json2 ? await Schema.deserializeFromJSON(json2) : undefined;
    const json3 = schema3?.serializeToJSON();

    expect(json1).toBeDefined();
    expect(json2).toBeDefined();
    expect(json3).toBeDefined();
    expect(schema3?.defaults.string?.stringInvertedIndex?.enabled).toBe(true);
    expect(schema3?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.enabled).toBe(
      true,
    );
  });

  it("many key overrides stress", async () => {
    const schema = new Schema();

    const sparseEf = new MockSparseEmbedding("stress_test");
    for (let i = 0; i < 50; i += 1) {
      const key = `field_${i}`;
      if (i === 0) {
        schema.createIndex(
          new SparseVectorIndexConfig({
            sourceKey: `source_${i}`,
            embeddingFunction: sparseEf,
          }),
          key,
        );
      } else if (i % 2 === 1) {
        schema.deleteIndex(new StringInvertedIndexConfig(), key);
      } else {
        schema.deleteIndex(new IntInvertedIndexConfig(), key);
      }
    }

    expect(Object.keys(schema.keys)).toHaveLength(52);
    expect(
      schema.keys["field_0"].sparseVector?.sparseVectorIndex?.enabled,
    ).toBe(true);
    expect(schema.keys["field_1"].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
    expect(schema.keys["field_2"].intValue?.intInvertedIndex?.enabled).toBe(
      false,
    );

    const json = schema.serializeToJSON();
    expect(Object.keys(json.keys)).toHaveLength(52);

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(Object.keys(deserialized!.keys)).toHaveLength(52);
    expect(
      deserialized!.keys["field_0"].sparseVector?.sparseVectorIndex?.config
        .sourceKey,
    ).toBe("source_0");
    expect(
      deserialized!.keys["field_49"].string?.stringInvertedIndex?.enabled,
    ).toBe(false);
    expect(
      deserialized!.keys["field_48"].intValue?.intInvertedIndex?.enabled,
    ).toBe(false);
  });

  it("chained operations maintain consistency", () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("chained_consistency");

    const result = schema
      .createIndex(
        new SparseVectorIndexConfig({
          sourceKey: "text",
          embeddingFunction: sparseEf,
        }),
        "field1",
      )
      .deleteIndex(new StringInvertedIndexConfig(), "field2")
      .deleteIndex(new StringInvertedIndexConfig(), "field3")
      .deleteIndex(new IntInvertedIndexConfig(), "field4");

    expect(result).toBe(schema);
    expect(schema.keys["field1"].sparseVector?.sparseVectorIndex?.enabled).toBe(
      true,
    );
    expect(schema.keys["field2"].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
    expect(schema.keys["field3"].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );
    expect(schema.keys["field4"].intValue?.intInvertedIndex?.enabled).toBe(
      false,
    );
  });

  it("float and bool inverted indexes", async () => {
    const schema = new Schema();
    expect(schema.defaults.floatValue?.floatInvertedIndex?.enabled).toBe(true);
    expect(schema.defaults.boolean?.boolInvertedIndex?.enabled).toBe(true);

    schema.deleteIndex(new FloatInvertedIndexConfig());
    expect(schema.defaults.floatValue?.floatInvertedIndex?.enabled).toBe(false);

    schema.deleteIndex(new BoolInvertedIndexConfig());
    expect(schema.defaults.boolean?.boolInvertedIndex?.enabled).toBe(false);

    schema.createIndex(new FloatInvertedIndexConfig(), "price");
    expect(schema.keys["price"].floatValue?.floatInvertedIndex?.enabled).toBe(
      true,
    );

    schema.deleteIndex(new BoolInvertedIndexConfig(), "is_active");
    expect(schema.keys["is_active"].boolean?.boolInvertedIndex?.enabled).toBe(
      false,
    );

    const json = schema.serializeToJSON();
    expect(json.defaults["float"]!["float_inverted_index"]!.enabled).toBe(
      false,
    );
    expect(json.defaults["bool"]!["bool_inverted_index"]!.enabled).toBe(false);
    expect(json.keys["price"]!["float"]!["float_inverted_index"]!.enabled).toBe(
      true,
    );
    expect(
      json.keys["is_active"]!["bool"]!["bool_inverted_index"]!.enabled,
    ).toBe(false);

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.floatValue?.floatInvertedIndex?.enabled).toBe(
      false,
    );
    expect(deserialized?.defaults.boolean?.boolInvertedIndex?.enabled).toBe(
      false,
    );
    expect(
      deserialized?.keys["price"].floatValue?.floatInvertedIndex?.enabled,
    ).toBe(true);
    expect(
      deserialized?.keys["is_active"].boolean?.boolInvertedIndex?.enabled,
    ).toBe(false);
  });

  it("space inference from embedding function", async () => {
    const schema = new Schema();
    schema.createIndex(
      new VectorIndexConfig({
        embeddingFunction: new MockEmbedding("space_inference"),
      }),
    );

    const json = schema.serializeToJSON();
    expect(json.defaults["float_list"]!["vector_index"]!.config!.space).toBe(
      "cosine",
    );
    expect(
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!.config!.space,
    ).toBe("cosine");

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.floatList?.vectorIndex?.config.space).toBe(
      "cosine",
    );
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config.space,
    ).toBe("cosine");
  });

  it("explicit space overrides embedding function default", async () => {
    const schema = new Schema();
    schema.createIndex(
      new VectorIndexConfig({
        embeddingFunction: new MockEmbedding("override_space"),
        space: "l2",
      }),
    );

    const json = schema.serializeToJSON();
    expect(json.defaults["float_list"]!["vector_index"]!.config!.space).toBe(
      "l2",
    );
    expect(
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!.config!.space,
    ).toBe("l2");

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.floatList?.vectorIndex?.config.space).toBe(
      "l2",
    );
    expect(
      deserialized?.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config.space,
    ).toBe("l2");
  });

  it("space inference with no embedding function", async () => {
    const schema = new Schema();
    schema.createIndex(
      new VectorIndexConfig({ embeddingFunction: null, space: "ip" }),
    );

    const json = schema.serializeToJSON();
    expect(json.defaults["float_list"]!["vector_index"]!.config!.space).toBe(
      "ip",
    );
    expect(
      json.defaults["float_list"]!["vector_index"]!.config!.embedding_function!
        .type,
    ).toBe("legacy");

    const embeddingVector =
      json.keys[EMBEDDING_KEY]!["float_list"]!["vector_index"]!;
    expect(embeddingVector.config!.space).toBe("ip");
    expect(embeddingVector.config!.embedding_function!.type).toBe("legacy");

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.defaults.floatList?.vectorIndex?.config.space).toBe(
      "ip",
    );
    expect(
      deserialized?.defaults.floatList?.vectorIndex?.config.embeddingFunction,
    ).toBeUndefined();
  });

  it("space inference remains stable across roundtrips", async () => {
    const schema = new Schema();
    schema.createIndex(
      new VectorIndexConfig({
        embeddingFunction: new MockEmbedding("roundtrip_space"),
      }),
    );

    const json1 = schema.serializeToJSON();
    expect(
      json1["defaults"]["float_list"]!["vector_index"]!.config!.space,
    ).toBe("cosine");
    const schema2 = await Schema.deserializeFromJSON(json1);

    const json2 = schema2?.serializeToJSON();
    expect(
      json2?.["defaults"]["float_list"]!["vector_index"]!.config!.space,
    ).toBe("cosine");
    const schema3 = json2 ? await Schema.deserializeFromJSON(json2) : undefined;

    const json3 = schema3?.serializeToJSON();
    expect(
      json3?.["defaults"]["float_list"]!["vector_index"]!.config!.space,
    ).toBe("cosine");
    expect(schema3?.defaults.floatList?.vectorIndex?.config.space).toBe(
      "cosine",
    );
  });

  it("key overrides have independent configs", async () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("independent_test");

    schema.createIndex(
      new SparseVectorIndexConfig({
        sourceKey: "default_source",
        embeddingFunction: sparseEf,
      }),
      "field1",
    );
    schema.createIndex(new StringInvertedIndexConfig(), "field2");

    expect(
      schema.keys["field1"].sparseVector?.sparseVectorIndex?.config.sourceKey,
    ).toBe("default_source");
    expect(schema.keys["field2"].string?.stringInvertedIndex?.enabled).toBe(
      true,
    );

    const json = schema.serializeToJSON();
    const deserialized = await Schema.deserializeFromJSON(json);
    expect(
      deserialized?.keys["field1"].sparseVector?.sparseVectorIndex?.config
        .sourceKey,
    ).toBe("default_source");
    expect(
      deserialized?.keys["field2"].string?.stringInvertedIndex?.enabled,
    ).toBe(true);
  });

  it("global default changes do not affect existing overrides", () => {
    const schema = new Schema();

    const initialEf = new MockEmbedding("initial_model");
    schema.createIndex(
      new VectorIndexConfig({
        embeddingFunction: initialEf,
        space: "cosine",
        hnsw: { ef_construction: 100, max_neighbors: 16 },
      }),
    );

    const initialOverride =
      schema.keys[EMBEDDING_KEY].floatList?.vectorIndex?.config.hnsw;
    expect(initialOverride).toEqual({
      ef_construction: 100,
      max_neighbors: 16,
    });

    const updatedEf = new MockEmbedding("updated_model");
    schema.createIndex(
      new VectorIndexConfig({
        embeddingFunction: updatedEf,
        space: "l2",
        hnsw: { ef_construction: 200, max_neighbors: 32 },
      }),
    );

    const defaultsVector = schema.defaults.floatList?.vectorIndex;
    expect(defaultsVector?.config.space).toBe("l2");
    expect(defaultsVector?.config.hnsw).toEqual({
      ef_construction: 200,
      max_neighbors: 32,
    });

    const embeddingVector = schema.keys[EMBEDDING_KEY].floatList?.vectorIndex;
    expect(embeddingVector?.config.space).toBe("l2");
    expect(embeddingVector?.config.hnsw).toEqual({
      ef_construction: 200,
      max_neighbors: 32,
    });
  });

  it("key specific overrides remain independent", async () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("key_specific_test");

    schema.createIndex(
      new SparseVectorIndexConfig({
        sourceKey: "source_a",
        embeddingFunction: sparseEf,
      }),
      "key_a",
    );
    schema.createIndex(new StringInvertedIndexConfig(), "key_b");
    schema.createIndex(new StringInvertedIndexConfig(), "key_c");

    expect(
      schema.keys["key_a"].sparseVector?.sparseVectorIndex?.config.sourceKey,
    ).toBe("source_a");
    expect(schema.keys["key_b"].string?.stringInvertedIndex?.enabled).toBe(
      true,
    );
    expect(schema.keys["key_c"].string?.stringInvertedIndex?.enabled).toBe(
      true,
    );

    schema.deleteIndex(new StringInvertedIndexConfig(), "key_b");
    expect(schema.keys["key_b"].string?.stringInvertedIndex?.enabled).toBe(
      false,
    );

    const json = schema.serializeToJSON();
    const deserialized = await Schema.deserializeFromJSON(json);
    expect(
      deserialized?.keys["key_a"].sparseVector?.sparseVectorIndex?.config
        .sourceKey,
    ).toBe("source_a");
    expect(
      deserialized?.keys["key_b"].string?.stringInvertedIndex?.enabled,
    ).toBe(false);
    expect(
      deserialized?.keys["key_c"].string?.stringInvertedIndex?.enabled,
    ).toBe(true);
  });

  it("global default disable then key enable", () => {
    const schema = new Schema();
    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(true);

    schema.deleteIndex(new StringInvertedIndexConfig());
    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(false);

    schema.createIndex(new StringInvertedIndexConfig(), "important_field");
    schema.createIndex(new StringInvertedIndexConfig(), "searchable_field");

    expect(schema.defaults.string?.stringInvertedIndex?.enabled).toBe(false);
    expect(
      schema.keys["important_field"].string?.stringInvertedIndex?.enabled,
    ).toBe(true);
    expect(
      schema.keys["searchable_field"].string?.stringInvertedIndex?.enabled,
    ).toBe(true);

    const json = schema.serializeToJSON();
    expect(json.keys).toHaveProperty("important_field");
    expect(json.keys).toHaveProperty("searchable_field");
    expect(json.keys).toHaveProperty(DOCUMENT_KEY);
    expect(json.keys).toHaveProperty(EMBEDDING_KEY);
    expect(json.keys).not.toHaveProperty("other_field");
  });

  it("partial override fills from defaults", async () => {
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("partial_test");
    schema.createIndex(
      new SparseVectorIndexConfig({
        sourceKey: "my_source",
        embeddingFunction: sparseEf,
      }),
      "multi_index_field",
    );

    const override = schema.keys["multi_index_field"];
    expect(override.sparseVector?.sparseVectorIndex?.enabled).toBe(true);
    expect(override.string).toBeNull();
    expect(override.intValue).toBeNull();
    expect(override.floatValue).toBeNull();
    expect(override.boolean).toBeNull();
    expect(override.floatList).toBeNull();

    const json = schema.serializeToJSON();
    const fieldJson = json.keys["multi_index_field"];
    expect(fieldJson["sparse_vector"]).toBeDefined();
    expect(fieldJson["string"]).toBeUndefined();
    expect(fieldJson["int"]).toBeUndefined();
    expect(fieldJson["float"]).toBeUndefined();
    expect(fieldJson["bool"]).toBeUndefined();
    expect(fieldJson["float_list"]).toBeUndefined();

    const deserialized = await Schema.deserializeFromJSON(json);
    const desOverride = deserialized?.keys["multi_index_field"];
    expect(desOverride?.sparseVector?.sparseVectorIndex?.enabled).toBe(true);
    expect(desOverride?.string).toBeNull();
    expect(desOverride?.intValue).toBeNull();
  });

  it("sparse vector cannot be created globally", () => {
    const schema = new Schema();
    expect(() => schema.createIndex(new SparseVectorIndexConfig())).toThrow(
      /Sparse vector index must be created on a specific key/,
    );
  });

  it("sparse vector cannot be deleted", () => {
    const schema = new Schema();
    const config = new SparseVectorIndexConfig();
    schema.createIndex(config, "my_key");
    expect(() => schema.deleteIndex(config, "my_key")).toThrow(
      /Deleting sparse vector index is not currently supported/,
    );
  });

  it("uses schema embedding function fallback when collection embedding is missing", async () => {
    const schema = new Schema();
    const embedding = new MockEmbedding("schema_model");
    schema.createIndex(new VectorIndexConfig({ embeddingFunction: embedding }));

    const collection = new CollectionImpl({
      chromaClient: null as unknown as ChromaClient,
      apiClient: {} as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    const embedFn = (
      collection as unknown as {
        getSchemaEmbeddingFunction: () => EmbeddingFunction | undefined;
      }
    ).getSchemaEmbeddingFunction();
    expect(embedFn).toBeDefined();
    const result = await embedFn!.generate(["hello"]);
    expect(result).toEqual([[1, 2, 3]]);
  });

  it("sparse auto-embedding with #document source", async () => {
    const sparseEf = new DeterministicSparseEmbedding("doc_sparse");
    const schema = new Schema();
    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: DOCUMENT_KEY,
      }),
      "doc_sparse",
    );

    let capturedRecords: any = null;
    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options) => {
        capturedRecords = options.body;
        return { data: {} };
      }),
    };

    const mockChromaClient = {
      getMaxBatchSize: jest.fn().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn().mockResolvedValue(false),
      _path: jest
        .fn()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    await collection.add({
      ids: ["1", "2"],
      documents: ["Hello, world!", "Test document"],
      embeddings: [
        [1, 2, 3],
        [4, 5, 6],
      ], // Provide dummy embeddings to skip auto-generation
    });

    expect(capturedRecords).not.toBeNull();
    expect(capturedRecords.metadatas).toHaveLength(2);

    // Expected from batch call
    const expectedBatch = await sparseEf.generate([
      "Hello, world!",
      "Test document",
    ]);

    expect(capturedRecords.metadatas[0]).toHaveProperty("doc_sparse");
    expect(capturedRecords.metadatas[0].doc_sparse).toEqual({
      "#type": "sparse_vector",
      ...expectedBatch[0],
    });

    expect(capturedRecords.metadatas[1]).toHaveProperty("doc_sparse");
    expect(capturedRecords.metadatas[1].doc_sparse).toEqual({
      "#type": "sparse_vector",
      ...expectedBatch[1],
    });
  });

  it("sparse auto-embedding with metadata field source", async () => {
    const sparseEf = new DeterministicSparseEmbedding("content_sparse");
    const schema = new Schema();
    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: "content",
      }),
      "content_sparse",
    );

    let capturedRecords: any = null;
    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options) => {
        capturedRecords = options.body;
        return { data: {} };
      }),
    };

    const mockChromaClient = {
      getMaxBatchSize: jest.fn().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn().mockResolvedValue(false),
      _path: jest
        .fn()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    await collection.add({
      ids: ["s1", "s2", "s3"],
      documents: ["ignored1", "ignored2", "ignored3"],
      embeddings: [
        [1, 2],
        [3, 4],
        [5, 6],
      ], // Provide dummy embeddings to skip auto-generation
      metadatas: [
        { content: "sparse content one" },
        { content: "sparse content two" },
        { content: "sparse content three" },
      ],
    });

    expect(capturedRecords).not.toBeNull();
    expect(capturedRecords.metadatas).toHaveLength(3);

    // Expected from batch call
    const expectedBatch = await sparseEf.generate([
      "sparse content one",
      "sparse content two",
      "sparse content three",
    ]);

    for (let i = 0; i < 3; i++) {
      expect(capturedRecords.metadatas[i]).toHaveProperty("content_sparse");
      expect(capturedRecords.metadatas[i]).toHaveProperty("content");
      expect(capturedRecords.metadatas[i].content_sparse).toEqual({
        "#type": "sparse_vector",
        ...expectedBatch[i],
      });
    }
  });

  it("sparse auto-embedding with mixed metadata null and filled", async () => {
    const sparseEf = new DeterministicSparseEmbedding("mixed_sparse");
    const schema = new Schema();
    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: DOCUMENT_KEY,
      }),
      "mixed_sparse",
    );

    let capturedRecords: any = null;
    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options) => {
        capturedRecords = options.body;
        return { data: {} };
      }),
    };

    const mockChromaClient = {
      getMaxBatchSize: jest.fn().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn().mockResolvedValue(false),
      _path: jest
        .fn()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    await collection.add({
      ids: ["n1", "n2", "n3", "n4"],
      documents: ["doc one", "doc two", "doc three", "doc four"],
      embeddings: [
        [1, 2],
        [3, 4],
        [5, 6],
        [7, 8],
      ], // Provide dummy embeddings to skip auto-generation
      metadatas: [null as any, null as any, { existing: "data" }, null as any],
    });

    expect(capturedRecords).not.toBeNull();
    expect(capturedRecords.metadatas).toHaveLength(4);

    // Expected from batch call
    const expectedBatch = await sparseEf.generate([
      "doc one",
      "doc two",
      "doc three",
      "doc four",
    ]);

    // All should have sparse embeddings added
    for (let i = 0; i < 4; i++) {
      expect(capturedRecords.metadatas[i]).toHaveProperty("mixed_sparse");
      expect(capturedRecords.metadatas[i].mixed_sparse).toEqual({
        "#type": "sparse_vector",
        ...expectedBatch[i],
      });
    }

    // Third one should still have existing data
    expect(capturedRecords.metadatas[2].existing).toBe("data");
  });

  it("sparse auto-embedding skips existing values", async () => {
    const sparseEf = new DeterministicSparseEmbedding("preserve");
    const schema = new Schema();
    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: DOCUMENT_KEY,
      }),
      "preserve_sparse",
    );

    let capturedRecords: any = null;
    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options) => {
        capturedRecords = options.body;
        return { data: {} };
      }),
    };

    const mockChromaClient = {
      getMaxBatchSize: jest.fn().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn().mockResolvedValue(false),
      _path: jest
        .fn()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    const existingSparse = { indices: [999], values: [123.456] };

    await collection.add({
      ids: ["preserve1", "preserve2"],
      documents: ["auto document", "manual document"],
      embeddings: [
        [1, 2],
        [3, 4],
      ], // Provide dummy embeddings to skip auto-generation
      metadatas: [null as any, { preserve_sparse: existingSparse }],
    });

    expect(capturedRecords).not.toBeNull();
    expect(capturedRecords.metadatas).toHaveLength(2);

    // First should have auto-generated embedding (single item batch)
    const expectedAuto = await sparseEf.generate(["auto document"]);
    expect(capturedRecords.metadatas[0]).toHaveProperty("preserve_sparse");
    expect(capturedRecords.metadatas[0].preserve_sparse).toEqual({
      "#type": "sparse_vector",
      ...expectedAuto[0],
    });

    // Second should preserve the manually provided one (already serialized in input)
    expect(capturedRecords.metadatas[1].preserve_sparse).toEqual({
      "#type": "sparse_vector",
      ...existingSparse,
    });
  });

  it("sparse auto-embedding with missing source field", async () => {
    const sparseEf = new DeterministicSparseEmbedding("missing_field");
    const schema = new Schema();
    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: "text_field",
      }),
      "field_sparse",
    );

    let capturedRecords: any = null;
    const mockApiClient = {
      post: jest.fn().mockImplementation(async (options) => {
        capturedRecords = options.body;
        return { data: {} };
      }),
    };

    const mockChromaClient = {
      getMaxBatchSize: jest.fn().mockResolvedValue(1000),
      supportsBase64Encoding: jest.fn().mockResolvedValue(false),
      _path: jest
        .fn()
        .mockResolvedValue({
          path: "/api/v1",
          tenant: "default_tenant",
          database: "default_database",
        }),
    };

    const collection = new CollectionImpl({
      chromaClient: mockChromaClient as unknown as ChromaClient,
      apiClient: mockApiClient as any,
      id: "test-id",
      name: "test",
      configuration: {} as CollectionConfiguration,
      metadata: undefined as CollectionMetadata | undefined,
      embeddingFunction: undefined,
      schema,
    });

    await collection.add({
      ids: ["f1", "f2", "f3", "f4"],
      documents: ["doc1", "doc2", "doc3", "doc4"],
      embeddings: [
        [1, 2],
        [3, 4],
        [5, 6],
        [7, 8],
      ], // Provide dummy embeddings to skip auto-generation
      metadatas: [
        { text_field: "valid text" },
        { text_field: 123 },
        { other_field: "value" },
        null as any,
      ],
    });

    expect(capturedRecords).not.toBeNull();
    expect(capturedRecords.metadatas).toHaveLength(4);

    // Only first one should have sparse embedding (single item batch)
    const expected = await sparseEf.generate(["valid text"]);
    expect(capturedRecords.metadatas[0]).toHaveProperty("field_sparse");
    expect(capturedRecords.metadatas[0].field_sparse).toEqual({
      "#type": "sparse_vector",
      ...expected[0],
    });

    // Others should NOT have sparse embedding
    expect(capturedRecords.metadatas[1]).not.toHaveProperty("field_sparse");
    expect(capturedRecords.metadatas[2]).not.toHaveProperty("field_sparse");
    expect(capturedRecords.metadatas[3]).toBeNull();
  });

  it("accepts Key instance for VectorIndexConfig sourceKey", () => {
    const { K } = require("../src/execution");
    const schema = new Schema();
    const vectorConfig = new VectorIndexConfig({
      sourceKey: K.DOCUMENT,
    });

    expect(vectorConfig.sourceKey).toBe("#document");

    // Also test with custom key
    const customKey = K("myfield");
    const vectorConfig2 = new VectorIndexConfig({
      sourceKey: customKey,
    });

    expect(vectorConfig2.sourceKey).toBe("myfield");
  });

  it("accepts Key instance for SparseVectorIndexConfig sourceKey", () => {
    const { K } = require("../src/execution");
    const schema = new Schema();
    const sparseConfig = new SparseVectorIndexConfig({
      sourceKey: K.DOCUMENT,
    });

    expect(sparseConfig.sourceKey).toBe("#document");

    // Also test with custom key
    const customKey = K("myfield");
    const sparseConfig2 = new SparseVectorIndexConfig({
      sourceKey: customKey,
    });

    expect(sparseConfig2.sourceKey).toBe("myfield");
  });

  it("serializes Key sourceKey correctly", async () => {
    const { K } = require("../src/execution");
    const schema = new Schema();
    const sparseEf = new MockSparseEmbedding("key_test");

    schema.createIndex(
      new SparseVectorIndexConfig({
        embeddingFunction: sparseEf,
        sourceKey: K.DOCUMENT,
      }),
      "sparse_field"
    );

    const json = schema.serializeToJSON();
    expect(json.keys["sparse_field"]?.["sparse_vector"]?.["sparse_vector_index"]?.config?.source_key).toBe("#document");

    const deserialized = await Schema.deserializeFromJSON(json);
    expect(deserialized?.keys["sparse_field"].sparseVector?.sparseVectorIndex?.config.sourceKey).toBe("#document");
  });
});

import { test, describe, afterEach, expect } from "@jest/globals";
import {
  CloudClient,
  K,
  Schema,
  SparseVectorIndexConfig,
  VectorIndexConfig,
} from "chromadb";
import {
  ChromaCloudQwenEmbeddingFunction,
  ChromaCloudQwenEmbeddingModel,
} from "@chroma-core/chroma-cloud-qwen";
import { ChromaCloudSpladeEmbeddingFunction } from "@chroma-core/chroma-cloud-splade";

describe("Integration Test", () => {
  const credentials = {
    apiKey: process.env.CHROMA_API_KEY,
    database: process.env.CHROMA_DATABASE,
    tenant: process.env.CHROMA_TENANT,
  };

  const missing = Object.values(credentials).filter((ev) => !ev);

  if (missing.length > 0) {
    return;
  }

  const collectionName = "test-cloud-efs";

  const client = new CloudClient();

  afterEach(async () => {
    try {
      await client.deleteCollection({ name: collectionName });
    } catch {}
  });

  test("it should hydrate API keys from client in EFs", async () => {
    const denseEf = new ChromaCloudQwenEmbeddingFunction({
      model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
      task: null,
    });

    const sparseEF = new ChromaCloudSpladeEmbeddingFunction();

    const schema = new Schema();

    schema.createIndex(new VectorIndexConfig({ embeddingFunction: denseEf }));

    schema.createIndex(
      new SparseVectorIndexConfig({
        sourceKey: K.DOCUMENT,
        embeddingFunction: sparseEF,
      }),
      "sparse_embedding",
    );

    await client.createCollection({
      name: collectionName,
      schema,
    });

    process.env.CHROMA_API_KEY = "";

    const collection = await client.getCollection({ name: collectionName });

    const collectionDenseEF =
      collection.schema?.keys["#embedding"]?.floatList?.vectorIndex?.config
        .embeddingFunction;

    const collectionSparseEF =
      collection.schema?.keys["sparse_embedding"]?.sparseVector
        ?.sparseVectorIndex?.config.embeddingFunction;

    expect(collectionSparseEF).toBeDefined();
    expect(collectionSparseEF).toBeInstanceOf(
      ChromaCloudSpladeEmbeddingFunction,
    );

    expect(collectionDenseEF).toBeDefined();
    expect(collectionDenseEF).toBeInstanceOf(ChromaCloudQwenEmbeddingFunction);
  });
});

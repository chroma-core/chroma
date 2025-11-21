import { afterEach, describe } from "@jest/globals";
import {
  CloudClient,
  K,
  Schema,
  SparseVectorIndexConfig,
  VectorIndexConfig,
} from "../src";
import {
  ChromaCloudQwenEmbeddingFunction,
  ChromaCloudQwenEmbeddingModel,
} from "@chroma-core/chroma-cloud-qwen";
import { ChromaCloudSpladeEmbeddingFunction } from "@chroma-core/chroma-cloud-splade";

process.env.CHROMA_API_KEY = "ck-71t3AP6wTK9fSsntKFjW3PUxFG91QsqpXAuCwSXNtJsx";
process.env.CHROMA_TENANT = "53530937-993c-4af8-94c8-4eaf101a6578";
process.env.CHROMA_DATABASE = "testbloop";

describe("client test", () => {
  const collectionName = "test-cloud-efs";

  const envVariables = {
    apiKey: process.env.CHROMA_API_KEY,
    database: process.env.CHROMA_DATABASE,
    tenant: process.env.CHROMA_TENANT,
  };

  afterEach(async () => {
    const client = new CloudClient(envVariables);
    try {
      await client.deleteCollection({ name: collectionName });
    } catch {}
  });

  const missingVars = Object.values(envVariables).filter((ev) => !ev);

  if (missingVars.length > 0) {
    it.skip("Skipping API key hydration test", () => {});
  } else {
    it("should test API key hydration for native Chroma EFs", async () => {
      const client = new CloudClient();

      const denseEf = new ChromaCloudQwenEmbeddingFunction({
        model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
        task: null,
      });

      const sparseEf = new ChromaCloudSpladeEmbeddingFunction();

      const schema = new Schema();

      schema.createIndex(
        new SparseVectorIndexConfig({
          sourceKey: K.DOCUMENT,
          embeddingFunction: sparseEf,
        }),
        "sparse_embedding",
      );

      schema.createIndex(
        new VectorIndexConfig({
          embeddingFunction: denseEf,
        }),
      );

      const collectionCreate = await client.createCollection({
        name: "test-cloud-efs",
        schema,
      });

      process.env.CHROMA_API_KEY = "";

      const collectionGet = await client.getCollection({
        name: "test-cloud-efs",
      });

      const x = 3;
    });
  }
});

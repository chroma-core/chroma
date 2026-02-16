import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import {
  ChromaCloudSpladeArgs,
  ChromaCloudSpladeEmbeddingFunction,
  ChromaCloudSpladeEmbeddingModel,
  maxPoolSparseVectors,
  chunkText,
} from "./index";
import { CloudClient } from "chromadb";

describe("ChromaCloudSpladeEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.CHROMA_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new ChromaCloudSpladeEmbeddingFunction({
        model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
      });
      expect(embedder.name).toBe("chroma-cloud-splade");

      const config = embedder.getConfig();
      expect(config.model).toBe("prithivida/Splade_PP_en_v1");
      expect(config.api_key_env_var).toBe("CHROMA_API_KEY");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.CHROMA_API_KEY;
    delete process.env.CHROMA_API_KEY;

    try {
      expect(() => {
        new ChromaCloudSpladeEmbeddingFunction({
          model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        });
      }).toThrow("Chroma Embedding API key is required");
    } finally {
      if (originalEnv) {
        process.env.CHROMA_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_CHROMA_API_KEY = "test-api-key";

    try {
      const embedder = new ChromaCloudSpladeEmbeddingFunction({
        model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        apiKeyEnvVar: "CUSTOM_CHROMA_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_CHROMA_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_CHROMA_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.CHROMA_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "CHROMA_API_KEY",
        model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
      };

      const embedder =
        ChromaCloudSpladeEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate sparse embeddings";
    if (!process.env.CHROMA_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new ChromaCloudSpladeEmbeddingFunction({
          model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        });
        const texts = ["Hello world", "Test text"];
        const embeddings = await embedder.generate(texts);

        expect(embeddings.length).toBe(texts.length);

        embeddings.forEach((embedding) => {
          expect(embedding.indices).toBeDefined();
          expect(embedding.values).toBeDefined();
          expect(embedding.indices.length).toBe(embedding.values.length);
          expect(embedding.indices.length).toBeGreaterThan(0);

          // Check that indices are sorted
          for (let i = 1; i < embedding.indices.length; i++) {
            expect(embedding.indices[i]).toBeGreaterThan(
              embedding.indices[i - 1],
            );
          }
        });

        // Verify embeddings are different
        expect(embeddings[0].indices).not.toEqual(embeddings[1].indices);
      });
    }

    const generateQueryEmbeddingsTest = "should generate query embeddings";
    if (!process.env.CHROMA_API_KEY) {
      it.skip(generateQueryEmbeddingsTest, () => {});
    } else {
      it(generateQueryEmbeddingsTest, async () => {
        const embedder = new ChromaCloudSpladeEmbeddingFunction({
          model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        });
        const texts = ["search query"];
        const embeddings = await embedder.generateForQueries!(texts);

        expect(embeddings).toBeDefined();
        expect(embeddings.length).toBe(1);
        expect(embeddings[0].indices).toBeDefined();
        expect(embeddings[0].values).toBeDefined();
      });
    }
  }

  const validateConfigUpdateTest = "should throw error when updating model";
  if (!process.env.CHROMA_API_KEY) {
    it.skip(validateConfigUpdateTest, () => {});
  } else {
    it(validateConfigUpdateTest, () => {
      const embedder = new ChromaCloudSpladeEmbeddingFunction({
        model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
      });

      expect(() => {
        embedder.validateConfigUpdate({ model: "new-model" });
      }).toThrow("Model cannot be updated");
    });
  }

  it("should test API key hydration from Chroma Client", async () => {
    process.env.CHROMA_API_KEY = "test";

    const client = new CloudClient({
      tenant: "test-tenant",
      database: "test-database",
    });

    process.env.CHROMA_API_KEY = "";

    const config: ChromaCloudSpladeArgs = {
      model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
      client,
    };

    const ef = new ChromaCloudSpladeEmbeddingFunction(config);

    expect(ef).toBeDefined();
    expect(ef).toBeInstanceOf(ChromaCloudSpladeEmbeddingFunction);
  });

  it("should test API key hydration from client when building from config", async () => {
    const client = new CloudClient({
      apiKey: "test",
      tenant: "test-tenant",
      database: "test-database",
    });

    const config = {
      api_key_env_var: "CHROMA_API_KEY",
      model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
    };

    const embedder = ChromaCloudSpladeEmbeddingFunction.buildFromConfig(
      config,
      client,
    );

    expect(embedder.getConfig()).toEqual(config);
  });
});

describe("maxPoolSparseVectors", () => {
  it("should return single vector as-is", () => {
    const vec = { indices: [1, 3, 5], values: [0.1, 0.2, 0.3] };
    const result = maxPoolSparseVectors([vec]);
    expect(result).toBe(vec);
  });

  it("should max pool two vectors with disjoint indices", () => {
    const v1 = { indices: [1, 3], values: [0.5, 0.8] };
    const v2 = { indices: [2, 4], values: [0.6, 0.9] };
    const result = maxPoolSparseVectors([v1, v2]);
    expect(result.indices).toEqual([1, 2, 3, 4]);
    expect(result.values).toEqual([0.5, 0.6, 0.8, 0.9]);
  });

  it("should take max value for overlapping indices", () => {
    const v1 = { indices: [1, 2, 3], values: [0.5, 0.8, 0.1] };
    const v2 = { indices: [1, 2, 3], values: [0.3, 0.9, 0.4] };
    const result = maxPoolSparseVectors([v1, v2]);
    expect(result.indices).toEqual([1, 2, 3]);
    expect(result.values).toEqual([0.5, 0.9, 0.4]);
  });

  it("should handle partial overlap", () => {
    const v1 = { indices: [1, 3, 5], values: [0.5, 0.8, 0.1] };
    const v2 = { indices: [2, 3, 6], values: [0.6, 0.2, 0.9] };
    const result = maxPoolSparseVectors([v1, v2]);
    expect(result.indices).toEqual([1, 2, 3, 5, 6]);
    expect(result.values).toEqual([0.5, 0.6, 0.8, 0.1, 0.9]);
  });

  it("should handle three vectors", () => {
    const v1 = { indices: [1, 2], values: [0.1, 0.5] };
    const v2 = { indices: [1, 3], values: [0.3, 0.6] };
    const v3 = { indices: [2, 3], values: [0.9, 0.2] };
    const result = maxPoolSparseVectors([v1, v2, v3]);
    expect(result.indices).toEqual([1, 2, 3]);
    expect(result.values).toEqual([0.3, 0.9, 0.6]);
  });

  it("should return sorted indices", () => {
    const v1 = { indices: [100, 200], values: [0.1, 0.2] };
    const v2 = { indices: [50, 150], values: [0.3, 0.4] };
    const result = maxPoolSparseVectors([v1, v2]);
    expect(result.indices).toEqual([50, 100, 150, 200]);
  });
});

describe("chunkText", () => {
  it("should not chunk short text", () => {
    const text = "This is a short document.";
    expect(chunkText(text)).toEqual([text]);
  });

  it("should chunk text exceeding the character limit", () => {
    // Generate text well over 2000 characters
    const text = "word ".repeat(600).trim(); // ~3000 chars
    const chunks = chunkText(text);
    expect(chunks.length).toBeGreaterThan(1);
  });

  it("should split on word boundaries", () => {
    const text = "word ".repeat(600).trim();
    const chunks = chunkText(text);
    for (const chunk of chunks) {
      // No chunk should start or end with a partial word
      expect(chunk).not.toMatch(/^\s/);
      expect(chunk).not.toMatch(/\s$/);
    }
  });

  it("should cover all content in chunks", () => {
    const words = Array.from({ length: 600 }, (_, i) => `word${i}`);
    const text = words.join(" ");
    const chunks = chunkText(text);
    const allChunkText = chunks.join(" ");
    for (const word of words) {
      expect(allChunkText).toContain(word);
    }
  });

  it("should handle empty string", () => {
    expect(chunkText("")).toEqual([""]);
  });
});

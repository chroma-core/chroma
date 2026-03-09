import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { PerplexityEmbeddingFunction } from "./index";

describe("PerplexityEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "pplx-embed-v1-0.6b";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.PERPLEXITY_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new PerplexityEmbeddingFunction({});
      expect(embedder.name).toBe("perplexity");

      const config = embedder.getConfig();
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("PERPLEXITY_API_KEY");
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.PERPLEXITY_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new PerplexityEmbeddingFunction({
        modelName: "pplx-embed-v1-4b",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("pplx-embed-v1-4b");
    });
  }

  it("should throw error when API key is missing", () => {
    const originalEnv = process.env.PERPLEXITY_API_KEY;
    delete process.env.PERPLEXITY_API_KEY;

    try {
      expect(() => {
        new PerplexityEmbeddingFunction({});
      }).toThrow("Perplexity API key is required");
    } finally {
      if (originalEnv) {
        process.env.PERPLEXITY_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_PERPLEXITY_API_KEY = "test-api-key";

    try {
      const embedder = new PerplexityEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_PERPLEXITY_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_PERPLEXITY_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_PERPLEXITY_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.PERPLEXITY_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "PERPLEXITY_API_KEY",
        model_name: "pplx-embed-v1-4b",
      };

      const embedder = PerplexityEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig().model_name).toBe(config.model_name);
      expect(embedder.getConfig().api_key_env_var).toBe(config.api_key_env_var);
    });
  }

  const generateEmbeddingsTest = "should generate embeddings";
  if (!process.env.PERPLEXITY_API_KEY) {
    it.skip(generateEmbeddingsTest, () => {});
  } else {
    it(generateEmbeddingsTest, async () => {
      const embedder = new PerplexityEmbeddingFunction({});
      const texts = ["Hello world", "Test text"];
      const embeddings = await embedder.generate(texts);

      expect(embeddings.length).toBe(texts.length);

      embeddings.forEach((embedding) => {
        expect(embedding.length).toBeGreaterThan(0);
      });

      expect(embeddings[0]).not.toEqual(embeddings[1]);
    });
  }
});
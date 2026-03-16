import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { VoyageAIEmbeddingFunction } from "./index";

describe("VoyageAIEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "voyage-3.5-lite";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.VOYAGE_API_KEY) {
    it.skip(defaultParametersTest, () => { });
  } else {
    it(defaultParametersTest, () => {
      const embedder = new VoyageAIEmbeddingFunction({ modelName: MODEL });
      expect(embedder.name).toBe("voyageai");

      const config = embedder.getConfig();
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("VOYAGE_API_KEY");
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.VOYAGE_API_KEY) {
    it.skip(customParametersTest, () => { });
  } else {
    it(customParametersTest, () => {
      const embedder = new VoyageAIEmbeddingFunction({
        modelName: "custom-model",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.VOYAGE_API_KEY;
    delete process.env.VOYAGE_API_KEY;

    try {
      expect(() => {
        new VoyageAIEmbeddingFunction({ modelName: MODEL });
      }).toThrow("Voyage API key is required");
    } finally {
      if (originalEnv) {
        process.env.VOYAGE_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_VOYAGE_API_KEY = "test-api-key";

    try {
      const embedder = new VoyageAIEmbeddingFunction({
        modelName: MODEL,
        apiKeyEnvVar: "CUSTOM_VOYAGE_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_VOYAGE_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_VOYAGE_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.VOYAGE_API_KEY) {
    it.skip(buildFromConfigTest, () => { });
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "VOYAGE_API_KEY",
        model_name: "config-model",
        truncation: true,
      };

      const embedder = VoyageAIEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.VOYAGE_API_KEY) {
      it.skip(generateEmbeddingsTest, () => { });
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new VoyageAIEmbeddingFunction({ modelName: MODEL });
        const texts = ["Hello world", "Test text"];
        const embeddings = await embedder.generate(texts);

        expect(embeddings.length).toBe(texts.length);

        embeddings.forEach((embedding) => {
          expect(embedding.length).toBeGreaterThan(0);
        });

        expect(embeddings[0]).not.toEqual(embeddings[1]);
      });
    }
  }
});

import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { MistralEmbeddingFunction } from "./index";

describe("MistralEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.MISTRAL_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new MistralEmbeddingFunction();
      expect(embedder.name).toBe("mistral");

      const config = embedder.getConfig();
      expect(config.model).toBe("mistral-embed");
      expect(config.api_key_env_var).toBe("MISTRAL_API_KEY");
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.MISTRAL_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new MistralEmbeddingFunction({
        model: "custom-model",
      });

      const config = embedder.getConfig();
      expect(config.model).toBe("custom-model");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.MISTRAL_API_KEY;
    delete process.env.MISTRAL_API_KEY;

    try {
      expect(() => {
        new MistralEmbeddingFunction();
      }).toThrow("Mistral API key is required");
    } finally {
      if (originalEnv) {
        process.env.MISTRAL_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_MISTRAL_API_KEY = "test-api-key";

    try {
      const embedder = new MistralEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_MISTRAL_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_MISTRAL_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_MISTRAL_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.MISTRAL_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "MISTRAL_API_KEY",
        model: "config-model",
      };

      const embedder = MistralEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.MISTRAL_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new MistralEmbeddingFunction();
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

import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { MorphEmbeddingFunction } from "./index";

describe("MorphEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "morph-embedding-v2";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.MORPH_API_KEY) {
    it.skip(defaultParametersTest, () => { });
  } else {
    it(defaultParametersTest, () => {
      const embedder = new MorphEmbeddingFunction();
      expect(embedder.name).toBe("morph");

      const config = embedder.getConfig();
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("MORPH_API_KEY");
      expect(config.api_base).toBe("https://api.morphllm.com/v1");
      expect(config.encoding_format).toBe("float");
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.MORPH_API_KEY) {
    it.skip(customParametersTest, () => { });
  } else {
    it(customParametersTest, () => {
      const embedder = new MorphEmbeddingFunction({
        model_name: "custom-model",
        api_base: "https://custom-api.com/v1",
        encoding_format: "base64",
        api_key_env_var: "MORPH_API_KEY",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.api_base).toBe("https://custom-api.com/v1");
      expect(config.encoding_format).toBe("base64");
      expect(config.api_key_env_var).toBe("MORPH_API_KEY");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.MORPH_API_KEY;
    delete process.env.MORPH_API_KEY;

    try {
      expect(() => {
        new MorphEmbeddingFunction();
      }).toThrow("API key not found");
    } finally {
      if (originalEnv) {
        process.env.MORPH_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_MORPH_API_KEY = "test-api-key";

    try {
      const embedder = new MorphEmbeddingFunction({
        api_key_env_var: "CUSTOM_MORPH_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_MORPH_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_MORPH_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.MORPH_API_KEY) {
    it.skip(buildFromConfigTest, () => { });
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "MORPH_API_KEY",
        model_name: "config-model",
        api_base: "https://config-api.com/v1",
        encoding_format: "float" as const,
      };

      const embedder = MorphEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });
  }

  const generateEmbeddingsTest = "should generate embeddings";
  if (!process.env.MORPH_API_KEY) {
    it.skip(generateEmbeddingsTest, () => { });
  } else {
    it(generateEmbeddingsTest, async () => {
      const embedder = new MorphEmbeddingFunction();
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
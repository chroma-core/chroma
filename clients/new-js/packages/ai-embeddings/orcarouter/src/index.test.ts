import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { OrcaRouterEmbeddingFunction } from "./index";

describe("OrcaRouterEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "openai/text-embedding-3-small";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.ORCAROUTER_API_KEY) {
    it.skip(defaultParametersTest, () => { });
  } else {
    it(defaultParametersTest, () => {
      const embedder = new OrcaRouterEmbeddingFunction();
      expect(embedder.name).toBe("orcarouter");

      const config = embedder.getConfig();
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("ORCAROUTER_API_KEY");
      expect(config.api_base).toBe("https://api.orcarouter.ai/v1");
      expect(config.encoding_format).toBe("float");
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.ORCAROUTER_API_KEY) {
    it.skip(customParametersTest, () => { });
  } else {
    it(customParametersTest, () => {
      const embedder = new OrcaRouterEmbeddingFunction({
        model_name: "custom-model",
        api_base: "https://custom-api.com/v1",
        encoding_format: "base64",
        api_key_env_var: "ORCAROUTER_API_KEY",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.api_base).toBe("https://custom-api.com/v1");
      expect(config.encoding_format).toBe("base64");
      expect(config.api_key_env_var).toBe("ORCAROUTER_API_KEY");
    });
  }

  it("should throw custom error for missing API key", () => {
    const originalEnv = process.env.ORCAROUTER_API_KEY;
    delete process.env.ORCAROUTER_API_KEY;

    try {
      expect(() => {
        new OrcaRouterEmbeddingFunction();
      }).toThrow("API key not found");
    } finally {
      if (originalEnv) {
        process.env.ORCAROUTER_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_ORCAROUTER_API_KEY = "test-api-key";

    try {
      const embedder = new OrcaRouterEmbeddingFunction({
        api_key_env_var: "CUSTOM_ORCAROUTER_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_ORCAROUTER_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_ORCAROUTER_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.ORCAROUTER_API_KEY) {
    it.skip(buildFromConfigTest, () => { });
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "ORCAROUTER_API_KEY",
        model_name: "config-model",
        api_base: "https://config-api.com/v1",
        encoding_format: "float" as const,
      };

      const embedder = OrcaRouterEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });
  }

  const generateEmbeddingsTest = "should generate embeddings";
  if (!process.env.ORCAROUTER_API_KEY) {
    it.skip(generateEmbeddingsTest, () => { });
  } else {
    it(generateEmbeddingsTest, async () => {
      const embedder = new OrcaRouterEmbeddingFunction();
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

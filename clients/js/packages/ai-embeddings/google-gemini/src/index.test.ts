import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { GoogleGeminiEmbeddingFunction } from "./index";

describe("GoogleGeminiEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.GEMINI_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new GoogleGeminiEmbeddingFunction();
      expect(embedder.name).toBe("google-generative-ai");

      const config = embedder.getConfig();
      expect(config.model_name).toBe("text-embedding-004");
      expect(config.api_key_env_var).toBe("GEMINI_API_KEY");
      expect(config.task_type).toBeUndefined();
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.GEMINI_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new GoogleGeminiEmbeddingFunction({
        modelName: "custom-model",
        taskType: "custom-task",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.task_type).toBe("custom-task");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.GEMINI_API_KEY;
    delete process.env.GEMINI_API_KEY;

    try {
      expect(() => {
        new GoogleGeminiEmbeddingFunction();
      }).toThrow("Gemini API key is required");
    } finally {
      if (originalEnv) {
        process.env.GEMINI_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_GEMINI_API_KEY = "test-api-key";

    try {
      const embedder = new GoogleGeminiEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_GEMINI_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_GEMINI_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_GEMINI_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.GEMINI_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "GEMINI_API_KEY",
        model_name: "config-model",
        task_type: "config-task",
      };

      const embedder = GoogleGeminiEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.GEMINI_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new GoogleGeminiEmbeddingFunction();
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

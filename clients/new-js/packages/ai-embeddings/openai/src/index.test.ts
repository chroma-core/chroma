import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { OpenAIEmbeddingFunction } from "./index";

describe("OpenAIEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "text-embedding-3-small";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.OPENAI_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new OpenAIEmbeddingFunction({ modelName: MODEL });
      expect(embedder.name).toBe("openai");

      const config = embedder.getConfig();
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("OPENAI_API_KEY");
      expect(config.dimensions).toBeUndefined();
      expect(config.organization_id).toBeUndefined();
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.OPENAI_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new OpenAIEmbeddingFunction({
        modelName: "custom-model",
        dimensions: 2000,
        apiKeyEnvVar: "OPENAI_API_KEY",
        organizationId: "custom-organization-id",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.organization_id).toBe("custom-organization-id");
      expect(config.dimensions).toBe(2000);
      expect(config.api_key_env_var).toBe("OPENAI_API_KEY");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.OPENAI_API_KEY;
    delete process.env.OPENAI_API_KEY;

    try {
      expect(() => {
        new OpenAIEmbeddingFunction({ modelName: MODEL });
      }).toThrow("OpenAI API key is required");
    } finally {
      if (originalEnv) {
        process.env.OPENAI_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_OPENAI_API_KEY = "test-api-key";

    try {
      const embedder = new OpenAIEmbeddingFunction({
        modelName: MODEL,
        apiKeyEnvVar: "CUSTOM_OPENAI_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_OPENAI_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_OPENAI_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.OPENAI_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "OPENAI_API_KEY",
        model_name: "config-model",
        dimensions: 2000,
        organization_id: "custom-organization-id",
      };

      const embedder = OpenAIEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.OPENAI_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new OpenAIEmbeddingFunction({ modelName: MODEL });
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

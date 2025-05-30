import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { CohereConfig, CohereEmbeddingFunction } from "./index";

describe("CohereEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.COHERE_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new CohereEmbeddingFunction();
      expect(embedder.name).toBe("cohere");

      const config = embedder.getConfig();

      expect(config.model_name).toBe("embed-english-v3.0");
      expect(config.api_key_env_var).toBe("COHERE_API_KEY");
      expect(config.input_type).toBe("search_document");
      expect(config.truncate).toBeUndefined();
      expect(config.embedding_type).toBeUndefined();
      expect(config.image).toBe(false);
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.COHERE_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new CohereEmbeddingFunction({
        modelName: "custom-model",
        inputType: "search_document",
        truncate: "END",
        embeddingType: "float",
        image: true,
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.input_type).toBe("search_document");
      expect(config.truncate).toBe("END");
      expect(config.image).toBe(true);
      expect(config.embedding_type).toBe("float");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.COHERE_API_KEY;
    delete process.env.COHERE_API_KEY;

    try {
      expect(() => {
        new CohereEmbeddingFunction();
      }).toThrow("Cohere API key is required");
    } finally {
      if (originalEnv) {
        process.env.COHERE_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_COHERE_API_KEY = "test-api-key";

    try {
      const embedder = new CohereEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_COHERE_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_COHERE_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_COHERE_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.COHERE_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config: CohereConfig = {
        model_name: "custom-model",
        api_key_env_var: "COHERE_API_KEY",
        input_type: "search_document",
        truncate: "END",
        embedding_type: "float",
        image: true,
      };

      const embedder = CohereEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.COHERE_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new CohereEmbeddingFunction();
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

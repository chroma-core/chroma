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

      expect(config.model_name).toBe("embed-v5.0-pro");
      expect(config.api_key_env_var).toBe("COHERE_API_KEY");
      expect(config.input_type).toBe("search_document");
      expect(config.truncate).toBeUndefined();
      expect(config.embedding_type).toBeUndefined();
      expect(config.image).toBe(false);
      expect(config.query_model_name).toBeUndefined();
      expect(config.output_dimension).toBeUndefined();
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

  it("should warn when no API key is set", () => {
    const originalEnv = process.env.COHERE_API_KEY;
    delete process.env.COHERE_API_KEY;
    const warn = jest.spyOn(console, "warn").mockImplementation(() => {});

    try {
      new CohereEmbeddingFunction();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("Cohere API key is not set"),
      );
    } finally {
      warn.mockRestore();
      if (originalEnv) {
        process.env.COHERE_API_KEY = originalEnv;
      }
    }
  });

  it("should accept a query model that shares the embedding space", () => {
    process.env.CUSTOM_COHERE_API_KEY = "test-api-key";

    try {
      const embedder = new CohereEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_COHERE_API_KEY",
        modelName: "embed-v5.0-pro",
        queryModelName: "embed-v5.0-fast",
        outputDimension: 1024,
      });

      const config = embedder.getConfig();
      expect(config.query_model_name).toBe("embed-v5.0-fast");
      expect(config.output_dimension).toBe(1024);
    } finally {
      delete process.env.CUSTOM_COHERE_API_KEY;
    }
  });

  it("should reject a query model from a different embedding space", () => {
    process.env.CUSTOM_COHERE_API_KEY = "test-api-key";

    try {
      expect(() => {
        new CohereEmbeddingFunction({
          apiKeyEnvVar: "CUSTOM_COHERE_API_KEY",
          modelName: "embed-v5.0-pro",
          queryModelName: "embed-english-v3.0",
        });
      }).toThrow("queryModelName must share an embedding space");
    } finally {
      delete process.env.CUSTOM_COHERE_API_KEY;
    }
  });

  it("should send search_query for queries and search_document for documents", async () => {
    process.env.CUSTOM_COHERE_API_KEY = "test-api-key";

    try {
      const embedder = new CohereEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_COHERE_API_KEY",
        modelName: "embed-v5.0-pro",
        queryModelName: "embed-v5.0-fast",
      });
      const embed = jest.fn(async (_request: any) => ({
        embeddings: { float: [[0.1, 0.2]] },
      }));
      (embedder as any).client = { embed };

      await embedder.generate(["doc"]);
      await embedder.generateForQueries(["query"]);

      expect(embed.mock.calls[0][0]).toMatchObject({
        model: "embed-v5.0-pro",
        inputType: "search_document",
        embeddingTypes: ["float"],
      });
      expect(embed.mock.calls[1][0]).toMatchObject({
        model: "embed-v5.0-fast",
        inputType: "search_query",
      });
    } finally {
      delete process.env.CUSTOM_COHERE_API_KEY;
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

import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { ForgeEmbeddingFunction } from "./index";

describe("ForgeEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const MODEL = "forge-pro";

  it("should initialize with default parameters", () => {
    const embedder = new ForgeEmbeddingFunction({ apiKey: "test-api-key" });
    expect(embedder.name).toBe("forge");

    const config = embedder.getConfig();
    expect(config.model_name).toBe(MODEL);
    expect(config.api_key_env_var).toBe("FORGE_API_KEY");
    expect(config.api_base).toBe("https://api.voxell.ai/v1");
  });

  it("should initialize with custom parameters", () => {
    const embedder = new ForgeEmbeddingFunction({
      apiKey: "test-api-key",
      modelName: "forge-ultra-4k",
      dimensions: 512,
    });

    const config = embedder.getConfig();
    expect(config.model_name).toBe("forge-ultra-4k");
    expect(config.dimensions).toBe(512);
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_FORGE_API_KEY = "test-api-key";

    try {
      const embedder = new ForgeEmbeddingFunction({
        apiKeyEnvVar: "CUSTOM_FORGE_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe("CUSTOM_FORGE_API_KEY");
    } finally {
      delete process.env.CUSTOM_FORGE_API_KEY;
    }
  });

  it("should build from config", () => {
    const config = {
      api_key_env_var: "FORGE_API_KEY",
      model_name: "forge-turbo",
      api_base: "https://api.voxell.ai/v1",
    };

    process.env.FORGE_API_KEY = "test-api-key";
    try {
      const embedder = ForgeEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig().model_name).toBe(config.model_name);
      expect(embedder.getConfig().api_key_env_var).toBe(config.api_key_env_var);
      expect(embedder.getConfig().api_base).toBe(config.api_base);
    } finally {
      delete process.env.FORGE_API_KEY;
    }
  });

  const generateEmbeddingsTest = "should generate embeddings";
  if (!process.env.FORGE_API_KEY) {
    it.skip(generateEmbeddingsTest, () => {});
  } else {
    it(generateEmbeddingsTest, async () => {
      const embedder = new ForgeEmbeddingFunction({});
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

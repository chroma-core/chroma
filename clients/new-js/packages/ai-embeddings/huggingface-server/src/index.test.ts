import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { HuggingfaceServerEmbeddingFunction } from "./index";

describe("HuggingfaceServerEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";

  it(defaultParametersTest, () => {
    const embedder = new HuggingfaceServerEmbeddingFunction({
      url: "http://127.0.0.1:8080/embed",
    });
    expect(embedder.name).toBe("huggingface-server");

    const config = embedder.getConfig();
    expect(config.url).toBe("http://127.0.0.1:8080/embed");
  });

  it("should initialize with error for a API key", () => {
    expect(() => {
      const embedder = new HuggingfaceServerEmbeddingFunction({
        url: "http://127.0.0.1:8080/embed",
        apiKeyEnvVar: "NON_EXISTS_API_KEY",
      });
    }).toThrow("Could not find API key");
  });

  const buildFromConfigTest = "should build from config";
  it(buildFromConfigTest, () => {
    process.env.HF_SERVER_API_KEY = "API_KEY";

    const config = {
      api_key_env_var: "HF_SERVER_API_KEY",
      url: "http://127.0.0.1:8080/embed",
    };

    const embedder = HuggingfaceServerEmbeddingFunction.buildFromConfig(config);

    expect(embedder.getConfig()).toEqual(config);
  });

  const generateEmbeddingsTest = "should generate embeddings";
  it(generateEmbeddingsTest, async () => {
    const embedder = new HuggingfaceServerEmbeddingFunction({
      url: "http://127.0.0.1:8080/embed",
    });
    const texts = ["Hello world", "Test text"];
    const embeddings = await embedder.generate(texts);

    expect(embeddings.length).toBe(texts.length);

    embeddings.forEach((embedding) => {
      expect(embedding.length).toBeGreaterThan(0);
    });

    expect(embeddings[0]).not.toEqual(embeddings[1]);
  });
});

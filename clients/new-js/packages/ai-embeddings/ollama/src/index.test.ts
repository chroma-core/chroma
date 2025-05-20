import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { OllamaEmbeddingFunction } from "./index";
import { startOllamaContainer } from "../start-ollama";

describe("OllamaEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const defaultParametersTest = "should initialize with default parameters";
  it(defaultParametersTest, () => {
    const embedder = new OllamaEmbeddingFunction();
    expect(embedder.name).toBe("ollama");

    const config = embedder.getConfig();
    expect(config.url).toBe("http://localhost:11434");
    expect(config.model_name).toBe("chroma/all-minilm-l6-v2-f32");
  });

  const buildFromConfigTest = "should build from config";
  it(buildFromConfigTest, () => {
    const config = {
      url: "url",
      model_name: "model",
    };

    const embedder = OllamaEmbeddingFunction.buildFromConfig(config);

    expect(embedder.getConfig()).toEqual(config);
  });

  const generateEmbeddingsTest = "should generate embeddings";
  it(
    generateEmbeddingsTest,
    async () => {
      const { ollamaUrl } = await startOllamaContainer({ verbose: true });
      const embedder = new OllamaEmbeddingFunction({
        url: ollamaUrl,
      });
      const texts = ["Hello world", "Test text"];
      const embeddings = await embedder.generate(texts);

      expect(embeddings.length).toBe(texts.length);

      embeddings.forEach((embedding) => {
        expect(embedding.length).toBeGreaterThan(0);
      });

      expect(embeddings[0]).not.toEqual(embeddings[1]);
    },
    1000000,
  );
});

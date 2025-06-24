import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { OllamaEmbeddingFunction } from "./index";

// Mock the ollama package
const mockEmbed = jest.fn() as jest.MockedFunction<any>;
jest.mock("ollama", () => ({
  Ollama: jest.fn().mockImplementation(() => ({
    embed: mockEmbed,
  })),
}));

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
  it(generateEmbeddingsTest, async () => {
    // Mock the embeddings response
    const mockEmbeddings = [
      Array(384).fill(0).map((_, i) => i / 1000),
      Array(384).fill(0).map((_, i) => (i + 100) / 1000),
    ];

    mockEmbed.mockResolvedValueOnce({
      embeddings: mockEmbeddings,
    });

    const embedder = new OllamaEmbeddingFunction({
      url: "http://localhost:11434",
    });
    const texts = ["Hello world", "Test text"];
    const embeddings = await embedder.generate(texts);

    expect(embeddings.length).toBe(texts.length);

    embeddings.forEach((embedding) => {
      expect(embedding.length).toBeGreaterThan(0);
    });

    expect(embeddings[0]).not.toEqual(embeddings[1]);

    expect(mockEmbed).toHaveBeenCalledWith({
      model: "chroma/all-minilm-l6-v2-f32",
      input: texts,
    });
  });
});

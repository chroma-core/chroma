import { DefaultEmbeddingFunction } from "./index";
import { pipeline } from "@huggingface/transformers";
import { beforeEach, describe, expect, it, jest } from "@jest/globals";

// Mock the transformers pipeline
jest.mock("@huggingface/transformers", () => {
  // Create a mock embeddings result
  const mockEmbeddings = [
    Array(384)
      .fill(0)
      .map((_, i) => i / 1000),
    Array(384)
      .fill(0)
      .map((_, i) => (i + 100) / 1000),
  ];

  // Create the pipeline mock that returns a function
  const pipelineFunction = jest.fn().mockImplementation(async () => {
    // When the pipeline result is called with text, it returns this object with tolist
    return function (texts: string[], options: any) {
      return {
        tolist: () => mockEmbeddings,
      };
    };
  });

  return {
    pipeline: pipelineFunction,
  };
});

describe("DefaultEmbeddingFunction", () => {
  let embedder: DefaultEmbeddingFunction;

  beforeEach(() => {
    embedder = new DefaultEmbeddingFunction();
  });

  it("should initialize with default parameters", () => {
    expect(embedder.name).toBe("default");
    expect(embedder.getConfig().model_name).toBe("Xenova/all-MiniLM-L6-v2");
    expect(embedder.getConfig().revision).toBe("main");
    expect(embedder.getConfig().dtype).toBe("fp32");
  });

  it("should initialize with custom parameters", () => {
    const customEmbedder = new DefaultEmbeddingFunction({
      modelName: "custom-model",
      revision: "custom-revision",
      dtype: "fp16",
    });

    expect(customEmbedder.getConfig().model_name).toBe("custom-model");
    expect(customEmbedder.getConfig().revision).toBe("custom-revision");
    expect(customEmbedder.getConfig().dtype).toBe("fp16");
  });

  it("should handle deprecated quantized parameter", () => {
    const quantizedEmbedder = new DefaultEmbeddingFunction({
      quantized: true,
    });

    expect(quantizedEmbedder.getConfig().dtype).toBe("uint8");
  });

  it("should generate embeddings with correct dimensions", async () => {
    const texts = ["Hello world", "Test text"];
    const embeddings = await embedder.generate(texts);

    // Verify we got the correct number of embeddings
    expect(embeddings.length).toBe(texts.length);

    // Verify each embedding has the correct dimension (384 for MiniLM-L6-v2)
    embeddings.forEach((embedding) => {
      expect(embedding.length).toBe(384);
    });

    // Verify embeddings are different (this works with our mock implementation)
    const [embedding1, embedding2] = embeddings;
    expect(embedding1).not.toEqual(embedding2);
  });

  it("should build from config", () => {
    const config = {
      model_name: "config-model",
      revision: "config-revision",
      dtype: "q8" as const,
    };

    const configEmbedder = DefaultEmbeddingFunction.buildFromConfig(config);

    expect(configEmbedder.getConfig().model_name).toBe("config-model");
    expect(configEmbedder.getConfig().revision).toBe("config-revision");
    expect(configEmbedder.getConfig().dtype).toBe("q8");
  });

  it("should validate config updates", () => {
    const newConfig = { model_name: "model-2" };

    expect(() => {
      new DefaultEmbeddingFunction({
        modelName: "model-1",
      }).validateConfigUpdate(newConfig);
    }).toThrow(
      "The DefaultEmbeddingFunction's 'model' cannot be changed after initialization.",
    );
  });

  describe("pipeline caching", () => {
    const pipelineMock = pipeline as unknown as jest.Mock<
      (...args: unknown[]) => Promise<unknown>
    >;

    beforeEach(() => {
      pipelineMock.mockClear();
    });

    it("should load the pipeline once and reuse it across calls", async () => {
      const embedder = new DefaultEmbeddingFunction({
        modelName: "reuse-across-calls",
      });

      await embedder.generate(["first"]);
      await embedder.generate(["second"]);
      await Promise.all([
        embedder.generate(["third"]),
        embedder.generate(["fourth"]),
      ]);

      expect(pipelineMock).toHaveBeenCalledTimes(1);
    });

    it("should share the pipeline across instances with the same config", async () => {
      await new DefaultEmbeddingFunction({
        modelName: "shared-config",
      }).generate(["a"]);
      await DefaultEmbeddingFunction.buildFromConfig({
        model_name: "shared-config",
      }).generate(["b"]);

      expect(pipelineMock).toHaveBeenCalledTimes(1);
    });

    it("should load separate pipelines for different configs", async () => {
      await new DefaultEmbeddingFunction({
        modelName: "separate-config",
        dtype: "fp32",
      }).generate(["a"]);
      await new DefaultEmbeddingFunction({
        modelName: "separate-config",
        dtype: "q8",
      }).generate(["b"]);

      expect(pipelineMock).toHaveBeenCalledTimes(2);
    });

    it("should retry loading after a failed load", async () => {
      const embedder = new DefaultEmbeddingFunction({
        modelName: "retry-after-failure",
      });
      pipelineMock.mockImplementationOnce(async () => {
        throw new Error("download failed");
      });

      await expect(embedder.generate(["a"])).rejects.toThrow("download failed");
      await expect(embedder.generate(["b"])).resolves.toHaveLength(2);
      expect(pipelineMock).toHaveBeenCalledTimes(2);
    });
  });
});

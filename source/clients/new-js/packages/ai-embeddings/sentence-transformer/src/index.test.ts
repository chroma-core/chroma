import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { SentenceTransformersEmbeddingFunction } from "./index";

// Store reference to mock dispose for test access
let mockDispose: ReturnType<typeof jest.fn>;

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

  // Create a mock dispose function
  mockDispose = jest.fn().mockImplementation(async () => { });

  // Create the pipeline mock that returns a function
  const pipelineFunction = jest.fn().mockImplementation(() => {
    // When the pipeline result is called with text, it returns this object with tolist
    const pipelineInstance = function (texts: string[], options: any) {
      return {
        tolist: () => mockEmbeddings,
      };
    };
    // Add dispose method to the pipeline instance
    pipelineInstance.dispose = mockDispose;
    return pipelineInstance;
  });

  return {
    pipeline: pipelineFunction,
  };
});

describe("SentenceTransformersEmbeddingFunction", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const MODEL = "Xenova/all-MiniLM-L6-v2";

  const defaultParametersTest = "should initialize with default parameters";
  it(defaultParametersTest, () => {
    const embedder = new SentenceTransformersEmbeddingFunction();
    expect(embedder.name).toBe("sentence_transformer");

    const config = embedder.getConfig();
    expect(config.model_name).toBe("all-MiniLM-L6-v2");
    expect(config.device).toBe("cpu");
    expect(config.normalize_embeddings).toBe(false);
    expect(config.kwargs).toEqual({});
  });

  it("should initialize with custom parameters", () => {
    const embedder = new SentenceTransformersEmbeddingFunction({
      modelName: "custom-model",
      device: "cpu",
      normalizeEmbeddings: true,
      kwargs: { test: "value" },
    });

    const config = embedder.getConfig();
    expect(config.model_name).toBe("custom-model");
    expect(config.device).toBe("cpu");
    expect(config.normalize_embeddings).toBe(true);
    expect(config.kwargs).toEqual({ test: "value" });
  });

  it("should throw error for invalid kwargs", () => {
    expect(() => {
      new SentenceTransformersEmbeddingFunction({
        modelName: MODEL,
        kwargs: { invalid: () => { } },
      });
    }).toThrow("Keyword argument 'invalid' has a value of type 'function', which is not supported. Only JSON-serializable values are allowed.");
  });

  const buildFromConfigTest = "should build from config";
  it(buildFromConfigTest, () => {
    const config = {
      model_name: "config-model",
      device: "cpu",
      normalize_embeddings: true,
      kwargs: { test: "value" },
    };

    const embedder = SentenceTransformersEmbeddingFunction.buildFromConfig(
      config,
    );

    expect(embedder.getConfig()).toEqual(config);
  });

  it("should throw error when required fields are missing in buildFromConfig", () => {
    expect(() => {
      SentenceTransformersEmbeddingFunction.buildFromConfig({
        model_name: "test",
      } as any);
    }).toThrow("model_name, device, and normalize_embeddings are required");
  });

  const generateEmbeddingsTest = "should generate embeddings";
  it(generateEmbeddingsTest, async () => {
    const embedder = new SentenceTransformersEmbeddingFunction({
      modelName: MODEL,
    });
    const texts = ["Hello world", "Test text"];
    const embeddings = await embedder.generate(texts);

    expect(embeddings.length).toBe(texts.length);

    embeddings.forEach((embedding) => {
      expect(embedding.length).toBeGreaterThan(0);
      expect(Array.isArray(embedding)).toBe(true);
    });

    expect(embeddings[0]).not.toEqual(embeddings[1]);
  }, 60000); // Increase timeout for model loading

  it("should allow config update", () => {
    const embedder = new SentenceTransformersEmbeddingFunction({
      modelName: MODEL,
    });

    // validateConfigUpdate should not throw (allows updates)
    expect(() => {
      embedder.validateConfigUpdate({
        model_name: "different-model",
        device: "gpu",
        normalize_embeddings: true,
      });
    }).not.toThrow();
  });

  it("should dispose resources correctly", async () => {
    const embedder = new SentenceTransformersEmbeddingFunction({
      modelName: MODEL,
    });

    // Generate embeddings to initialize the pipeline
    await embedder.generate(["test"]);

    // Verify dispose hasn't been called yet
    expect(mockDispose).not.toHaveBeenCalled();

    // Dispose should clean up resources
    await embedder.dispose();

    // Verify dispose was called on the pipeline
    expect(mockDispose).toHaveBeenCalledTimes(1);
  });

  it("should dispose resources even when called during pipeline initialization", async () => {
    const embedder = new SentenceTransformersEmbeddingFunction({
      modelName: MODEL,
    });

    // Start generating embeddings (this starts pipeline initialization)
    const generatePromise = embedder.generate(["test"]);

    // Immediately call dispose while pipeline is still initializing
    // This tests the race condition fix
    await embedder.dispose();

    // Wait for generate to complete (it should handle the disposed state)
    await generatePromise;

    // Verify dispose was called on the pipeline
    expect(mockDispose).toHaveBeenCalledTimes(1);
  });
});

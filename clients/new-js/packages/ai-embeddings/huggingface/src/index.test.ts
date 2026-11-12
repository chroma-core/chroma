import { HuggingfaceEmbeddingFunction } from "./index";
import { afterEach, describe, expect, it } from "@jest/globals";

describe("HuggingfaceEmbeddingFunction", () => {
  afterEach(() => {
    delete process.env.CHROMA_HUGGINGFACE_API_KEY;
    delete process.env.CUSTOM_HF_API_KEY;
  });

  it("initializes with default model and env var (matching the Python provider)", () => {
    const embedder = new HuggingfaceEmbeddingFunction({ apiKey: "test-key" });
    expect(embedder.name).toBe("huggingface");

    expect(embedder.getConfig()).toEqual({
      api_key_env_var: "CHROMA_HUGGINGFACE_API_KEY",
      model_name: "sentence-transformers/all-MiniLM-L6-v2",
    });
  });

  it("honors a custom model name", () => {
    const embedder = new HuggingfaceEmbeddingFunction({
      apiKey: "test-key",
      modelName: "BAAI/bge-small-en-v1.5",
    });
    expect(embedder.getConfig().model_name).toBe("BAAI/bge-small-en-v1.5");
  });

  it("throws when no API key is provided or set in the environment", () => {
    delete process.env.CHROMA_HUGGINGFACE_API_KEY;
    expect(() => {
      new HuggingfaceEmbeddingFunction();
    }).toThrow("HuggingFace API key is required");
  });

  it("resolves the API key from a custom environment variable", () => {
    process.env.CUSTOM_HF_API_KEY = "env-key";
    const embedder = new HuggingfaceEmbeddingFunction({
      apiKeyEnvVar: "CUSTOM_HF_API_KEY",
    });
    expect(embedder.getConfig().api_key_env_var).toBe("CUSTOM_HF_API_KEY");
  });

  it("round-trips through buildFromConfig", () => {
    const config = {
      api_key_env_var: "CHROMA_HUGGINGFACE_API_KEY",
      model_name: "sentence-transformers/all-MiniLM-L6-v2",
    };
    process.env.CHROMA_HUGGINGFACE_API_KEY = "env-key";

    const embedder = HuggingfaceEmbeddingFunction.buildFromConfig(config);
    expect(embedder.getConfig()).toEqual(config);
  });

  it("validates a well-formed config and rejects model_name changes", () => {
    const embedder = new HuggingfaceEmbeddingFunction({ apiKey: "test-key" });
    const config = embedder.getConfig();

    expect(() =>
      HuggingfaceEmbeddingFunction.validateConfig(config),
    ).not.toThrow();
    expect(() =>
      embedder.validateConfigUpdate({
        ...config,
        model_name: "different-model",
      }),
    ).toThrow("Model name cannot be updated");
  });

  it("reports cosine as the default space", () => {
    const embedder = new HuggingfaceEmbeddingFunction({ apiKey: "test-key" });
    expect(embedder.defaultSpace()).toBe("cosine");
    expect(embedder.supportedSpaces()).toEqual(["cosine", "l2", "ip"]);
  });
});

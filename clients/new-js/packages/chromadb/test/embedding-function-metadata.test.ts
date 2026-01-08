import { beforeEach, describe, expect, test } from "@jest/globals";
import { ChromaClient, EmbeddingFunction, registerEmbeddingFunction } from "../src";
import { CohereEmbeddingFunction } from "@chroma-core/cohere";
import { DefaultEmbeddingFunction } from "@chroma-core/default-embed";
import { GoogleGeminiEmbeddingFunction } from "@chroma-core/google-gemini";
import { OllamaEmbeddingFunction } from "@chroma-core/ollama";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";
import { EmbeddingFunctionSpace } from "../src/embedding-function";

// Custom embedding function for testing
class CustomEmbeddingFunction implements EmbeddingFunction {
  constructor(private config: Record<string, any> = {}) {}

  async generate(texts: string[]): Promise<number[][]> {
    return texts.map(() => [1.0, 2.0, 3.0]);
  }

  name = "custom-test-ef";

  getConfig(): Record<string, any> {
    return this.config;
  }

  static buildFromConfig(config: Record<string, any>): EmbeddingFunction {
    return new CustomEmbeddingFunction(config);
  }

  buildFromConfig(config: Record<string, any>): EmbeddingFunction {
    return new CustomEmbeddingFunction(config);
  }

  defaultSpace(): EmbeddingFunctionSpace {
    return "cosine";
  }

  supportedSpaces(): EmbeddingFunctionSpace[] {
    return ["cosine", "l2", "ip"];
  }

  validateConfig?(config: Record<string, any>): void {
    // No validation needed for testing
  }

  validateConfigUpdate?(newConfig: Record<string, any>): void {
    // No validation needed for testing
  }
}

// Register the custom embedding function
registerEmbeddingFunction("custom-test-ef", CustomEmbeddingFunction);

describe("embeddingFunctionMetadata property", () => {
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("returns undefined when no embedding function configured", async () => {
    const collection = await client.createCollection({
      name: "test_no_ef",
      embeddingFunction: null,
    });

    expect(collection.embeddingFunctionMetadata).toBeUndefined();
  });

  test("returns metadata for default embedding function with revision and quantized", async () => {
    const collection = await client.createCollection({
      name: "test_default_ef",
      embeddingFunction: new DefaultEmbeddingFunction(),
    });

    const metadata = collection.embeddingFunctionMetadata;
    expect(metadata).toBeDefined();
    expect(metadata?.type).toBe("default");
    expect(metadata?.model).toBe("Xenova/all-MiniLM-L6-v2");
    expect(metadata?.revision).toBe("main");
    expect(metadata?.quantized).toBe(false);
  });

  test("extracts fields from config with both camelCase and snake_case", async () => {
    const efCamel = new CustomEmbeddingFunction({
      model: "test-model",
      organizationId: "org-123"
    });
    const collectionCamel = await client.createCollection({
      name: "test_camel",
      embeddingFunction: efCamel,
    });

    expect(collectionCamel.embeddingFunctionMetadata?.model).toBe("test-model");
    expect(collectionCamel.embeddingFunctionMetadata?.organizationId).toBe("org-123");

    const efSnake = new CustomEmbeddingFunction({
      model_name: "test-model-2",
      organization_id: "org-456"
    });
    const collectionSnake = await client.createCollection({
      name: "test_snake",
      embeddingFunction: efSnake,
    });

    expect(collectionSnake.embeddingFunctionMetadata?.model).toBe("test-model-2");
    expect(collectionSnake.embeddingFunctionMetadata?.organizationId).toBe("org-456");
  });

  test("handles all metadata fields with provider-like configs", async () => {
    const efComplete = new CustomEmbeddingFunction({
      model: "multi-model",
      revision: "v1.0",
      quantized: true,
      taskType: "RETRIEVAL_QUERY",
      url: "https://api.example.com",
      organizationId: "org-multi",
    });
    const collection = await client.createCollection({
      name: "test_complete",
      embeddingFunction: efComplete,
    });

    const metadata = collection.embeddingFunctionMetadata;
    expect(metadata).toEqual({
      type: "custom-test-ef",
      model: "multi-model",
      revision: "v1.0",
      quantized: true,
      taskType: "RETRIEVAL_QUERY",
      url: "https://api.example.com",
      organizationId: "org-multi",
    });
  });

  test("returns only type when config has no optional fields", async () => {
    const ef = new CustomEmbeddingFunction({ custom_field: "ignored" });
    const collection = await client.createCollection({
      name: "test_minimal",
      embeddingFunction: ef,
    });

    const metadata = collection.embeddingFunctionMetadata;
    expect(metadata?.type).toBe("custom-test-ef");
    expect(metadata?.model).toBeUndefined();
    expect(metadata?.url).toBeUndefined();
  });

  test("persists metadata across getCollection and listCollections", async () => {
    const ef = new CustomEmbeddingFunction({ model: "persistent-model" });
    await client.createCollection({
      name: "test_persistence",
      embeddingFunction: ef,
    });

    const retrieved = await client.getCollection({
      name: "test_persistence",
      embeddingFunction: ef,
    });
    expect(retrieved.embeddingFunctionMetadata?.model).toBe("persistent-model");

    const collections = await client.listCollections();
    const listed = collections.find((c) => c.name === "test_persistence");
    expect(listed?.embeddingFunctionMetadata?.model).toBe("persistent-model");
  });

  test("prefers first variant when multiple field names exist", async () => {
    const ef = new CustomEmbeddingFunction({
      model: "preferred",
      model_name: "ignored",
      url: "https://preferred.com",
      api_url: "https://ignored.com",
    });
    const collection = await client.createCollection({
      name: "test_priority",
      embeddingFunction: ef,
    });

    const metadata = collection.embeddingFunctionMetadata;
    expect(metadata?.model).toBe("preferred");
    expect(metadata?.url).toBe("https://preferred.com");
  });

  test("real embedding functions: OpenAI, Cohere, Google Gemini, Ollama", async () => {
    // OpenAI - demonstrates model + organizationId
    const openai = new OpenAIEmbeddingFunction({
      apiKey: "dummy-key",
      modelName: "text-embedding-3-large",
      organizationId: "org-123456",
      dimensions: 1536,
    });
    const openaiCollection = await client.createCollection({
      name: "test_openai",
      embeddingFunction: openai,
    });
    expect(openaiCollection.embeddingFunctionMetadata).toMatchObject({
      type: "openai",
      model: "text-embedding-3-large",
      organizationId: "org-123456",
    });

    // Cohere - demonstrates model with different provider
    const cohere = new CohereEmbeddingFunction({
      apiKey: "dummy-key",
      modelName: "embed-english-v3.0",
    });
    const cohereCollection = await client.createCollection({
      name: "test_cohere",
      embeddingFunction: cohere,
    });
    expect(cohereCollection.embeddingFunctionMetadata).toMatchObject({
      type: "cohere",
      model: "embed-english-v3.0",
    });

    // Google Gemini - demonstrates model + taskType
    const gemini = new GoogleGeminiEmbeddingFunction({
      apiKey: "dummy-key",
      modelName: "text-embedding-004",
      taskType: "RETRIEVAL_QUERY",
    });
    const geminiCollection = await client.createCollection({
      name: "test_gemini",
      embeddingFunction: gemini,
    });
    expect(geminiCollection.embeddingFunctionMetadata).toMatchObject({
      type: "google-generative-ai",
      model: "text-embedding-004",
      taskType: "RETRIEVAL_QUERY",
    });

    // Ollama - demonstrates model + url
    const ollama = new OllamaEmbeddingFunction({
      url: "http://localhost:11434",
      model: "nomic-embed-text",
    });
    const ollamaCollection = await client.createCollection({
      name: "test_ollama",
      embeddingFunction: ollama,
    });
    expect(ollamaCollection.embeddingFunctionMetadata).toMatchObject({
      type: "ollama",
      model: "nomic-embed-text",
      url: "http://localhost:11434",
    });
  });
});

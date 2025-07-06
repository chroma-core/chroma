import { describe, expect, test } from "@jest/globals";
import { loadSchema, getSchemaVersion } from "../src/schemas";
import { OpenAIEmbeddingFunction } from "../src/embeddings/OpenAIEmbeddingFunction";
import { validateConfigSchema } from "../src/schemas/schemaUtils";

describe("Schema Validation", () => {
  test("should load a schema", () => {
    const schema = loadSchema("openai");
    expect(schema).toBeDefined();
    expect(schema.title).toBe("OpenAI Embedding Function Schema");
  });

  test("should validate a valid config", () => {
    const config = {
      api_key_env_var: "OPENAI_API_KEY",
      model_name: "text-embedding-ada-002",
      organization_id: "",
      dimensions: 1536,
    };
    expect(() => validateConfigSchema(config, "openai")).not.toThrow();
  });

  test("should throw on an invalid config", () => {
    const config = {
      api_key_env_var: "OPENAI_API_KEY",
      model_name: 123, // Should be a string
      organization_id: "",
      dimensions: 1536,
    };
    expect(() => validateConfigSchema(config, "openai")).toThrow();
  });

  test("should get schema version", () => {
    const version = getSchemaVersion("openai");
    expect(version).toBeDefined();
  });

  test("should validate an embedding function", () => {
    process.env.CHROMA_OPENAI_API_KEY = "test-key";

    const embeddingFunction = new OpenAIEmbeddingFunction({});
    expect(() =>
      embeddingFunction.validateConfig(embeddingFunction.getConfig()),
    ).not.toThrow();

    process.env.CHROMA_OPENAI_API_KEY = undefined;
  });
});

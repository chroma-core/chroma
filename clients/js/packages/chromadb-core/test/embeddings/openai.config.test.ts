import { describe, expect, test, beforeEach, afterEach } from "@jest/globals";
import { OpenAIEmbeddingFunction } from "../../src/embeddings/OpenAIEmbeddingFunction";

describe("OpenAIEmbeddingFunction config", () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  test("getConfig persists the env var name, never the API key", () => {
    process.env.MY_OPENAI_KEY = "sk-super-secret-value";
    const ef = new OpenAIEmbeddingFunction({
      openai_api_key_env_var: "MY_OPENAI_KEY",
    });

    const config = ef.getConfig();

    // The stored config must reference the environment variable *name*,
    // not the resolved secret. Leaking the key here would persist it into
    // the collection configuration stored on the server.
    expect(config.api_key_env_var).toBe("MY_OPENAI_KEY");
    expect(JSON.stringify(config)).not.toContain("sk-super-secret-value");
  });

  test("getConfig persists env var name even when key passed directly", () => {
    const ef = new OpenAIEmbeddingFunction({
      openai_api_key: "sk-direct-secret",
      openai_api_key_env_var: "CUSTOM_ENV_VAR",
    });

    const config = ef.getConfig();

    expect(config.api_key_env_var).toBe("CUSTOM_ENV_VAR");
    expect(JSON.stringify(config)).not.toContain("sk-direct-secret");
  });

  test("buildFromConfig round-trips via the env var name", () => {
    process.env.ROUNDTRIP_KEY = "sk-roundtrip-secret";
    const ef = new OpenAIEmbeddingFunction({
      openai_api_key_env_var: "ROUNDTRIP_KEY",
      openai_model: "text-embedding-3-small",
    });

    const rebuilt = ef.buildFromConfig(ef.getConfig());
    const rebuiltConfig = rebuilt.getConfig();

    expect(rebuiltConfig.api_key_env_var).toBe("ROUNDTRIP_KEY");
    expect(rebuiltConfig.model_name).toBe("text-embedding-3-small");
    expect(JSON.stringify(rebuiltConfig)).not.toContain("sk-roundtrip-secret");
  });

  test("dimensions is omitted from config when not configured", () => {
    process.env.CHROMA_OPENAI_API_KEY = "sk-x";
    const ef = new OpenAIEmbeddingFunction({});

    expect(ef.getConfig()).not.toHaveProperty("dimensions");
  });

  test("dimensions is persisted when explicitly configured", () => {
    process.env.CHROMA_OPENAI_API_KEY = "sk-x";
    const ef = new OpenAIEmbeddingFunction({
      openai_model: "text-embedding-3-large",
      openai_embedding_dimensions: 256,
    });

    expect(ef.getConfig().dimensions).toBe(256);
  });

  test("generate omits dimensions for models that do not support it", async () => {
    process.env.CHROMA_OPENAI_API_KEY = "sk-x";
    const ef = new OpenAIEmbeddingFunction({
      openai_model: "text-embedding-ada-002",
    });

    let captured: any;
    // Inject a fake client so loadClient() short-circuits and no real
    // `openai` package/network is required.
    (ef as any).openaiApi = {
      createEmbedding: async (params: any) => {
        captured = params;
        return [[0.1, 0.2, 0.3]];
      },
    };

    await ef.generate(["hello"]);

    expect(captured).toBeDefined();
    expect(captured).not.toHaveProperty("dimensions");
  });

  test("generate forwards dimensions when explicitly configured", async () => {
    process.env.CHROMA_OPENAI_API_KEY = "sk-x";
    const ef = new OpenAIEmbeddingFunction({
      openai_model: "text-embedding-3-small",
      openai_embedding_dimensions: 512,
    });

    let captured: any;
    (ef as any).openaiApi = {
      createEmbedding: async (params: any) => {
        captured = params;
        return [[0.1, 0.2, 0.3]];
      },
    };

    await ef.generate(["hello"]);

    expect(captured.dimensions).toBe(512);
  });
});

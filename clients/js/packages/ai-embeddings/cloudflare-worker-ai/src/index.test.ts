import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { CloudflareWorkerAIEmbeddingFunction } from "./index";

describe("CloudflareWorkerAIEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  const verifyEnvVariables = () => {
    return process.env.CLOUDFLARE_API_KEY && process.env.CLOUDFLARE_ACCOUNT_ID;
  };

  const MODEL = "@cf/baai/bge-m3";
  const ACCOUNT_ID: string = process.env.CLOUDFLARE_ACCOUNT_ID!;

  const defaultParametersTest = "should initialize with default parameters";
  if (!verifyEnvVariables()) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new CloudflareWorkerAIEmbeddingFunction({
        modelName: MODEL,
        accountId: ACCOUNT_ID,
      });

      expect(embedder.name).toBe("cloudflare-worker-ai");
      const config = embedder.getConfig();
      expect(config.account_id).toBe(ACCOUNT_ID);

      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("CLOUDFLARE_API_KEY");
      expect(config.gateway_id).toBeUndefined();
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!verifyEnvVariables()) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      const embedder = new CloudflareWorkerAIEmbeddingFunction({
        apiKey: "custom-key",
        accountId: "custom-account-id",
        modelName: "custom-model",
        apiKeyEnvVar: "custom-key-name",
        gatewayId: "custom-gateway-id",
      });

      const config = embedder.getConfig();
      expect(config.model_name).toBe("custom-model");
      expect(config.account_id).toBe("custom-account-id");
      expect(config.api_key_env_var).toBe("custom-key-name");
      expect(config.gateway_id).toBe("custom-gateway-id");
    });
  }

  it("should initialize with custom error for a API key", () => {
    const originalEnv = process.env.CLOUDFLARE_API_KEY;
    delete process.env.CLOUDFLARE_API_KEY;

    try {
      expect(() => {
        new CloudflareWorkerAIEmbeddingFunction({
          modelName: MODEL,
          accountId: ACCOUNT_ID,
        });
      }).toThrow("Cloudflare API key is required");
    } finally {
      if (originalEnv) {
        process.env.CLOUDFLARE_API_KEY = originalEnv;
      }
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_CLOUDFLARE_API_KEY = "test-api-key";

    try {
      const embedder = new CloudflareWorkerAIEmbeddingFunction({
        modelName: MODEL,
        accountId: ACCOUNT_ID,
        apiKeyEnvVar: "CUSTOM_CLOUDFLARE_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_CLOUDFLARE_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_CLOUDFLARE_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  if (!process.env.CLOUDFLARE_API_KEY) {
    it.skip(buildFromConfigTest, () => {});
  } else {
    it(buildFromConfigTest, () => {
      const config = {
        api_key_env_var: "CLOUDFLARE_API_KEY",
        model_name: "config-model",
        account_id: "account-id",
        gateway_id: "gateway-id",
      };

      const embedder =
        CloudflareWorkerAIEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    });

    const generateEmbeddingsTest = "should generate embeddings";
    if (!process.env.CLOUDFLARE_API_KEY) {
      it.skip(generateEmbeddingsTest, () => {});
    } else {
      it(generateEmbeddingsTest, async () => {
        const embedder = new CloudflareWorkerAIEmbeddingFunction({
          modelName: MODEL,
          accountId: ACCOUNT_ID,
        });

        const texts = ["Hello world", "Test text"];
        const embeddings = await embedder.generate(texts);

        expect(embeddings.length).toBe(texts.length);

        embeddings.forEach((embedding) => {
          expect(embedding.length).toBeGreaterThan(0);
        });

        expect(embeddings[0]).not.toEqual(embeddings[1]);
      });
    }
  }
});

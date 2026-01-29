import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { RunPodEmbeddingFunction, RunPodConfig, RunPodArgs } from "./index";

// Mock the runpod-sdk module
jest.mock("runpod-sdk", () => {
  return {
    default: jest.fn((apiKey: string) => ({
      endpoint: jest.fn((endpointId: string) => ({
        run: jest.fn().mockResolvedValue({
          id: "test-job-id",
          status: "COMPLETED",
        }),
        status: jest.fn().mockResolvedValue({
          status: "COMPLETED",
          output: {
            data: [{ embedding: new Array(1024).fill(0).map(() => Math.random()) }],
          },
        }),
      })),
    })),
  };
});

describe("RunPodEmbeddingFunction", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const ENDPOINT_ID = "test-endpoint-id";
  const MODEL = "test-model-name";

  describe("constructor", () => {
    it("should initialize with default parameters", () => {
      process.env.RUNPOD_API_KEY = "test-api-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: MODEL,
        });
        expect(embedder.name).toBe("runpod");

        const config = embedder.getConfig();
        expect(config.endpoint_id).toBe(ENDPOINT_ID);
        expect(config.model_name).toBe(MODEL);
        expect(config.api_key_env_var).toBe("RUNPOD_API_KEY");
        expect(config.timeout).toBe(300);
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });

    it("should initialize with custom parameters", () => {
      process.env.CUSTOM_RUNPOD_API_KEY = "custom-api-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: "custom-endpoint",
          modelName: "custom-model",
          apiKeyEnvVar: "CUSTOM_RUNPOD_API_KEY",
          timeout: 600,
        });

        const config = embedder.getConfig();
        expect(config.endpoint_id).toBe("custom-endpoint");
        expect(config.model_name).toBe("custom-model");
        expect(config.api_key_env_var).toBe("CUSTOM_RUNPOD_API_KEY");
        expect(config.timeout).toBe(600);
      } finally {
        delete process.env.CUSTOM_RUNPOD_API_KEY;
      }
    });

    it("should throw error for missing API key", () => {
      const originalEnv = process.env.RUNPOD_API_KEY;
      delete process.env.RUNPOD_API_KEY;

      try {
        expect(() => {
          new RunPodEmbeddingFunction({
            endpointId: ENDPOINT_ID,
            modelName: MODEL,
          });
        }).toThrow("RunPod API key is required");
      } finally {
        if (originalEnv) {
          process.env.RUNPOD_API_KEY = originalEnv;
        }
      }
    });

    it("should throw error for empty endpoint ID", () => {
      process.env.RUNPOD_API_KEY = "test-key";

      try {
        expect(() => {
          new RunPodEmbeddingFunction({
            endpointId: "",
            modelName: MODEL,
          });
        }).toThrow("RunPod endpoint ID is required and cannot be empty");
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });

    it("should throw error for empty model name", () => {
      process.env.RUNPOD_API_KEY = "test-key";

      try {
        expect(() => {
          new RunPodEmbeddingFunction({
            endpointId: ENDPOINT_ID,
            modelName: "",
          });
        }).toThrow("RunPod model name is required and cannot be empty");
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });

    it("should accept API key directly", () => {
      const embedder = new RunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
        apiKey: "direct-api-key",
      });

      expect(embedder.name).toBe("runpod");
    });
  });

  describe("generate", () => {
    it("should generate embeddings", async () => {
      process.env.RUNPOD_API_KEY = "test-api-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: MODEL,
        });
        const texts = ["Hello world", "Test text for RunPod"];
        const embeddings = await embedder.generate(texts);

        expect(embeddings.length).toBe(texts.length);
        embeddings.forEach((embedding: number[]) => {
          expect(embedding.length).toBeGreaterThan(0);
          expect(Array.isArray(embedding)).toBe(true);
          embedding.forEach((value: number) => {
            expect(typeof value).toBe("number");
          });
        });
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });

    it("should handle empty input", async () => {
      process.env.RUNPOD_API_KEY = "test-api-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: MODEL,
        });

        const embeddings = await embedder.generate([]);
        expect(embeddings).toEqual([]);
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });
  });

  describe("config", () => {
    it("should build from config", () => {
      process.env.RUNPOD_API_KEY = "test-api-key";

      try {
        const config: RunPodConfig = {
          api_key_env_var: "RUNPOD_API_KEY",
          endpoint_id: ENDPOINT_ID,
          model_name: MODEL,
          timeout: 240,
        };

        const embedder = RunPodEmbeddingFunction.buildFromConfig(config);
        expect(embedder.getConfig()).toEqual(config);
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });

    it("should validate config update correctly", () => {
      process.env.RUNPOD_API_KEY = "test-api-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: MODEL,
        });

        // Should not throw when updating mutable fields only
        expect(() => {
          embedder.validateConfigUpdate({ timeout: 600 });
        }).not.toThrow();

        // Should throw when trying to change model_name
        expect(() => {
          embedder.validateConfigUpdate({ model_name: "different-model" });
        }).toThrow("Model name cannot be updated");

        // Should throw when trying to change endpoint_id
        expect(() => {
          embedder.validateConfigUpdate({ endpoint_id: "different-endpoint" });
        }).toThrow("Endpoint ID cannot be updated");
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });
  });

  describe("spaces", () => {
    it("should return correct default and supported spaces", () => {
      process.env.RUNPOD_API_KEY = "test-key";

      try {
        const embedder = new RunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: MODEL,
        });

        expect(embedder.defaultSpace()).toBe("cosine");
        expect(embedder.supportedSpaces()).toEqual(["cosine", "l2", "ip"]);
      } finally {
        delete process.env.RUNPOD_API_KEY;
      }
    });
  });
});

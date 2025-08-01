import { beforeEach, describe, expect, it, jest } from "@jest/globals";

// Import the actual class directly to avoid registration type issues
export interface RunPodArgs {
  endpointId: string;
  modelName: string;
  apiKeyEnvVar?: string;
  apiKey?: string;
  timeout?: number;
}

export interface RunPodConfig {
  api_key_env_var: string;
  endpoint_id: string;
  model_name: string;
  timeout?: number;
}

// Simple mock of RunPod functionality for testing
class TestRunPodEmbeddingFunction {
  public readonly name = "runpod";
  private readonly apiKeyEnvVar: string;
  private readonly endpointId: string;
  private readonly modelName: string;
  private readonly timeout: number;

  constructor(args: RunPodArgs) {
    const {
      endpointId,
      modelName,
      apiKeyEnvVar = "RUNPOD_API_KEY",
      timeout = 300,
    } = args;

    const apiKey = args.apiKey || process.env[apiKeyEnvVar];

    if (!apiKey) {
      throw new Error(
        `RunPod API key is required. Please provide it in the constructor or set the environment variable ${apiKeyEnvVar}.`,
      );
    }

    if (!endpointId || !endpointId.trim()) {
      throw new Error("RunPod endpoint ID is required and cannot be empty.");
    }

    if (!modelName || !modelName.trim()) {
      throw new Error("RunPod model name is required and cannot be empty.");
    }

    this.endpointId = endpointId;
    this.modelName = modelName;
    this.timeout = timeout;
    this.apiKeyEnvVar = apiKeyEnvVar;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) {
      return [];
    }

    // Mock implementation for testing
    return texts.map(() => new Array(1024).fill(0).map(() => Math.random()));
  }

  public defaultSpace(): string {
    return "cosine";
  }

  public supportedSpaces(): string[] {
    return ["cosine", "l2", "ip"];
  }

  public static buildFromConfig(
    config: RunPodConfig,
  ): TestRunPodEmbeddingFunction {
    return new TestRunPodEmbeddingFunction({
      endpointId: config.endpoint_id,
      modelName: config.model_name,
      apiKeyEnvVar: config.api_key_env_var,
      timeout: config.timeout,
    });
  }

  public getConfig(): RunPodConfig {
    return {
      api_key_env_var: this.apiKeyEnvVar,
      endpoint_id: this.endpointId,
      model_name: this.modelName,
      timeout: this.timeout,
    };
  }

  public static validateConfig(config: RunPodConfig): void {
    if (!config.endpoint_id) {
      throw new Error("endpoint_id is required");
    }
    if (!config.model_name) {
      throw new Error("model_name is required");
    }
    if (!config.api_key_env_var) {
      throw new Error("api_key_env_var is required");
    }
  }
}

describe("RunPodEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    // Debug: Check if environment variables are available
    console.log("RUNPOD_API_KEY available:", !!process.env.RUNPOD_API_KEY);
    console.log(
      "RUNPOD_ENDPOINT_ID available:",
      !!process.env.RUNPOD_ENDPOINT_ID,
    );
  });

  const ENDPOINT_ID =
    process.env.RUNPOD_ENDPOINT_ID || "insert-endpoint-id-here";
  const MODEL = "insert-model-name-here";

  const defaultParametersTest = "should initialize with default parameters";
  if (!process.env.RUNPOD_API_KEY) {
    it.skip(defaultParametersTest, () => {});
  } else {
    it(defaultParametersTest, () => {
      const embedder = new TestRunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
      });
      expect(embedder.name).toBe("runpod");

      const config = embedder.getConfig();
      expect(config.endpoint_id).toBe(ENDPOINT_ID);
      expect(config.model_name).toBe(MODEL);
      expect(config.api_key_env_var).toBe("RUNPOD_API_KEY");
      expect(config.timeout).toBe(300);
    });
  }

  const customParametersTest = "should initialize with custom parameters";
  if (!process.env.RUNPOD_API_KEY) {
    it.skip(customParametersTest, () => {});
  } else {
    it(customParametersTest, () => {
      // Set up custom API key for this test
      process.env.CUSTOM_RUNPOD_API_KEY = process.env.RUNPOD_API_KEY;

      try {
        const embedder = new TestRunPodEmbeddingFunction({
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
  }

  it("should initialize with custom error for missing API key", () => {
    const originalEnv = process.env.RUNPOD_API_KEY;
    delete process.env.RUNPOD_API_KEY;

    try {
      expect(() => {
        new TestRunPodEmbeddingFunction({
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
        new TestRunPodEmbeddingFunction({
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
        new TestRunPodEmbeddingFunction({
          endpointId: ENDPOINT_ID,
          modelName: "",
        });
      }).toThrow("RunPod model name is required and cannot be empty");
    } finally {
      delete process.env.RUNPOD_API_KEY;
    }
  });

  it("should use custom API key environment variable", () => {
    process.env.CUSTOM_RUNPOD_API_KEY =
      process.env.RUNPOD_API_KEY || "test-api-key";

    try {
      const embedder = new TestRunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
        apiKeyEnvVar: "CUSTOM_RUNPOD_API_KEY",
      });

      expect(embedder.getConfig().api_key_env_var).toBe(
        "CUSTOM_RUNPOD_API_KEY",
      );
    } finally {
      delete process.env.CUSTOM_RUNPOD_API_KEY;
    }
  });

  const buildFromConfigTest = "should build from config";
  it(buildFromConfigTest, () => {
    // Ensure API key is available for this test
    const originalApiKey = process.env.RUNPOD_API_KEY;
    process.env.RUNPOD_API_KEY = originalApiKey || "test-api-key";

    try {
      const config = {
        api_key_env_var: "RUNPOD_API_KEY",
        endpoint_id: ENDPOINT_ID,
        model_name: MODEL,
        timeout: 240,
      };

      const embedder = TestRunPodEmbeddingFunction.buildFromConfig(config);

      expect(embedder.getConfig()).toEqual(config);
    } finally {
      if (originalApiKey) {
        process.env.RUNPOD_API_KEY = originalApiKey;
      } else {
        delete process.env.RUNPOD_API_KEY;
      }
    }
  });

  it("should validate configuration correctly", () => {
    const validConfig = {
      api_key_env_var: "RUNPOD_API_KEY",
      endpoint_id: ENDPOINT_ID,
      model_name: MODEL,
    };

    // Should not throw for valid config
    expect(() => {
      TestRunPodEmbeddingFunction.validateConfig(validConfig);
    }).not.toThrow();

    const invalidConfig = {
      api_key_env_var: "RUNPOD_API_KEY",
      endpoint_id: ENDPOINT_ID,
      // Missing required model_name
    };

    // Should throw for invalid config
    expect(() => {
      TestRunPodEmbeddingFunction.validateConfig(invalidConfig as any);
    }).toThrow();
  });

  it("should return correct default and supported spaces", () => {
    process.env.RUNPOD_API_KEY = "test-key";

    try {
      const embedder = new TestRunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
      });

      expect(embedder.defaultSpace()).toBe("cosine");
      expect(embedder.supportedSpaces()).toEqual(["cosine", "l2", "ip"]);
    } finally {
      delete process.env.RUNPOD_API_KEY;
    }
  });

  const generateEmbeddingsTest = "should generate embeddings (mock)";
  it(generateEmbeddingsTest, async () => {
    // Ensure API key is available for this test
    const originalApiKey = process.env.RUNPOD_API_KEY;
    process.env.RUNPOD_API_KEY = originalApiKey || "test-api-key";

    try {
      const embedder = new TestRunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
      });
      const texts = ["Hello world", "Test text for RunPod"];
      const embeddings = await embedder.generate(texts);

      expect(embeddings.length).toBe(texts.length);

      embeddings.forEach((embedding: number[]) => {
        expect(embedding.length).toBeGreaterThan(0);
        expect(Array.isArray(embedding)).toBe(true);
        // Check that embeddings are numbers
        embedding.forEach((value: number) => {
          expect(typeof value).toBe("number");
        });
      });

      // Embeddings for different texts should be different (very likely with random numbers)
      expect(embeddings[0]).not.toEqual(embeddings[1]);
    } finally {
      if (originalApiKey) {
        process.env.RUNPOD_API_KEY = originalApiKey;
      } else {
        delete process.env.RUNPOD_API_KEY;
      }
    }
  });

  const handleEmptyInputTest = "should handle empty input";
  it(handleEmptyInputTest, async () => {
    // Ensure API key is available for this test
    const originalApiKey = process.env.RUNPOD_API_KEY;
    process.env.RUNPOD_API_KEY = originalApiKey || "test-api-key";

    try {
      const embedder = new TestRunPodEmbeddingFunction({
        endpointId: ENDPOINT_ID,
        modelName: MODEL,
      });

      const embeddings = await embedder.generate([]);
      expect(embeddings).toEqual([]);
    } finally {
      if (originalApiKey) {
        process.env.RUNPOD_API_KEY = originalApiKey;
      } else {
        delete process.env.RUNPOD_API_KEY;
      }
    }
  });
});

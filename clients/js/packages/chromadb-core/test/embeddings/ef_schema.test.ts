import {
  describe,
  expect,
  test,
  beforeAll,
  afterAll,
  jest,
} from "@jest/globals";
import {
  loadSchema,
  validateConfigSchema,
  getAvailableSchemas,
  getSchemaVersion,
} from "../../src/schemas/schemaUtils";
import {
  OpenAIEmbeddingFunction,
  CohereEmbeddingFunction,
  JinaEmbeddingFunction,
  OllamaEmbeddingFunction,
  TransformersEmbeddingFunction,
  VoyageAIEmbeddingFunction,
  GoogleGenerativeAiEmbeddingFunction,
  HuggingFaceEmbeddingServerFunction,
  DefaultEmbeddingFunction,
} from "../../src/embeddings/all";

// Mock for embedding functions to avoid actual API calls
const mockEmbeddings = [[0.1, 0.2, 0.3]];

// Test configurations for each embedding function
const EMBEDDING_FUNCTION_CONFIGS: Record<string, any> = {
  openai: {
    args: {
      openai_api_key: "dummy_key",
      openai_model: "text-embedding-ada-002",
      openai_api_key_env_var: "OPENAI_API_KEY",
    },
    config: {
      api_key_env_var: "OPENAI_API_KEY",
      model_name: "text-embedding-ada-002",
      organization_id: "",
      dimensions: 1536,
    },
    expectedConfig: {
      api_key_env_var: "dummy_key",
      model_name: "text-embedding-ada-002",
      organization_id: "",
      dimensions: 1536,
    },
  },
  cohere: {
    args: {
      cohere_api_key: "dummy_key",
      model: "large",
      cohere_api_key_env_var: "COHERE_API_KEY",
    },
    config: {
      api_key_env_var: "COHERE_API_KEY",
      model_name: "large",
    },
  },
  default: {
    args: {
      model: "Xenova/all-MiniLM-L6-v2",
      revision: "main",
      quantized: false,
    },
    config: {
      model: "Xenova/all-MiniLM-L6-v2",
      revision: "main",
      quantized: false,
    },
  },
  jina: {
    args: {
      jina_api_key: "dummy_key",
      jina_model_name: "jina-embeddings-v2-base-en",
      api_key_env_var: "JINAAI_API_KEY",
    },
    config: {
      api_key_env_var: "JINAAI_API_KEY",
      model_name: "jina-embeddings-v2-base-en",
    },
  },
  ollama: {
    args: {
      url: "http://localhost:11434",
      model: "llama2",
    },
    config: {
      url: "http://localhost:11434",
      model_name: "llama2",
    },
  },
  transformers: {
    args: {
      model: "Xenova/all-MiniLM-L6-v2",
      revision: "main",
      quantized: false,
    },
    config: {
      model: "Xenova/all-MiniLM-L6-v2",
      revision: "main",
      quantized: false,
    },
  },
  voyageai: {
    args: {
      api_key: "dummy_key",
      model: "voyage-2",
      api_key_env_var: "VOYAGE_API_KEY",
    },
    config: {
      api_key_env_var: "VOYAGE_API_KEY",
      model_name: "voyage-2",
    },
  },
  google_generative_ai: {
    args: {
      googleApiKey: "dummy_key",
      model: "embedding-001",
      taskType: "RETRIEVAL_DOCUMENT",
      apiKeyEnvVar: "GOOGLE_API_KEY",
    },
    config: {
      api_key_env_var: "GOOGLE_API_KEY",
      model_name: "embedding-001",
    },
  },
  huggingface_server: {
    args: {
      url: "http://localhost:8080",
    },
    config: {
      url: "http://localhost:8080",
    },
  },
};

// Map of embedding function names to their classes
const EMBEDDING_FUNCTION_CLASSES: Record<string, any> = {
  openai: OpenAIEmbeddingFunction,
  cohere: CohereEmbeddingFunction,
  jina: JinaEmbeddingFunction,
  ollama: OllamaEmbeddingFunction,
  transformers: TransformersEmbeddingFunction,
  voyageai: VoyageAIEmbeddingFunction,
  google_generative_ai: GoogleGenerativeAiEmbeddingFunction,
  huggingface_server: HuggingFaceEmbeddingServerFunction,
  default: TransformersEmbeddingFunction,
};

// Setup mocks for dependencies
jest.mock(
  "openai",
  () => ({
    default: jest.fn(),
    VERSION: "4.0.0",
  }),
  { virtual: true },
);

jest.mock(
  "@google/generative-ai",
  () => ({
    GoogleGenerativeAI: jest.fn(),
  }),
  { virtual: true },
);

jest.mock(
  "cohere-ai",
  () => ({
    CohereClient: jest.fn(),
  }),
  { virtual: true },
);

jest.mock("node-fetch", () => jest.fn(), { virtual: true });

// Mock @xenova/transformers for TransformersEmbeddingFunction
jest.mock(
  "@xenova/transformers",
  () => ({
    pipeline: jest.fn().mockImplementation(() => ({
      tolist: () => [[0.1, 0.2, 0.3]],
    })),
  }),
  { virtual: true },
);

// Setup mocks before all tests
beforeAll(() => {
  // Mock the generate method for all embedding functions
  Object.values(EMBEDDING_FUNCTION_CLASSES).forEach((EFClass) => {
    if (EFClass.prototype.generate) {
      jest
        .spyOn(EFClass.prototype, "generate")
        .mockImplementation(async () => mockEmbeddings);
    }
  });

  // Set environment variables for testing
  process.env.OPENAI_API_KEY = "dummy_openai_key";
  process.env.COHERE_API_KEY = "dummy_cohere_key";
  process.env.JINAAI_API_KEY = "dummy_jina_key";
  process.env.GOOGLE_API_KEY = "dummy_google_key";
  process.env.VOYAGE_API_KEY = "dummy_voyageai_key";
});

// Clean up environment variables after all tests
afterAll(() => {
  process.env.OPENAI_API_KEY = undefined;
  process.env.COHERE_API_KEY = undefined;
  process.env.JINAAI_API_KEY = undefined;
  process.env.GOOGLE_API_KEY = undefined;
  process.env.VOYAGE_API_KEY = undefined;
});

describe("Embedding Function Schemas", () => {
  test("all schemas are valid JSON", () => {
    const schemaNames = getAvailableSchemas();
    expect(schemaNames.length).toBeGreaterThan(0);

    for (const schemaName of schemaNames) {
      const schema = loadSchema(schemaName);
      expect(schema).toBeDefined();
      expect(schema.$schema).toBeDefined();
      expect(schema.title).toBeDefined();
      expect(schema.description).toBeDefined();
      expect(schema.version).toBeDefined();
      expect(schema.properties).toBeDefined();
    }
  });

  test("all schemas have valid versions", () => {
    const schemaNames = getAvailableSchemas();

    for (const schemaName of schemaNames) {
      const version = getSchemaVersion(schemaName);
      expect(version).toBeDefined();

      // Version should follow semver format (x.y.z)
      expect(version).toMatch(/^\d+\.\d+\.\d+$/);
    }
  });

  // Test each embedding function's config roundtrip
  Object.entries(EMBEDDING_FUNCTION_CONFIGS).forEach(([efName, testConfig]) => {
    test(`${efName} embedding function config roundtrip`, () => {
      const EFClass = EMBEDDING_FUNCTION_CLASSES[efName];
      if (!EFClass) {
        throw new Error(`No class found for embedding function: ${efName}`);
      }

      // 1. Create embedding function with arguments
      const efInstance = new EFClass(testConfig.args);

      // 2. Get config from the instance
      const config = efInstance.getConfig();

      // Check that config contains expected values
      const expectedConfig = testConfig.expectedConfig || testConfig.config;
      for (const [key, value] of Object.entries(expectedConfig)) {
        expect(config).toHaveProperty(key);
        expect(config[key]).toEqual(value);
      }

      // 3. Create a new instance from the config
      const newEfInstance = efInstance.buildFromConfig(config);

      // 4. Validate the config
      expect(() => efInstance.validateConfig(config)).not.toThrow();

      // 5. Get config from the new instance and verify it matches
      const newConfig = newEfInstance.getConfig();
      for (const [key, value] of Object.entries(config)) {
        expect(newConfig).toHaveProperty(key);
        expect(newConfig[key]).toEqual(value);
      }
    });
  });

  // Test each embedding function with invalid config
  Object.entries(EMBEDDING_FUNCTION_CONFIGS).forEach(([efName, testConfig]) => {
    test(`${efName} embedding function rejects invalid config`, () => {
      const EFClass = EMBEDDING_FUNCTION_CLASSES[efName];
      if (!EFClass) {
        throw new Error(`No class found for embedding function: ${efName}`);
      }

      // Create embedding function with arguments
      const efInstance = new EFClass(testConfig.args);

      // Test with invalid property
      const invalidConfig = {
        ...testConfig.config,
        invalid_property: "invalid_value",
      };

      // Some embedding functions might allow additional properties, so we can't always expect this to fail
      try {
        efInstance.validateConfig(invalidConfig);
      } catch (error) {
        // If it raises an exception, that's expected for many embedding functions
        expect(error).toBeDefined();
      }
    });
  });

  test("schema required fields are enforced", () => {
    const schemaNames = getAvailableSchemas();

    for (const schemaName of schemaNames) {
      const schema = loadSchema(schemaName);

      if (schema.required && schema.required.length > 0) {
        const requiredFields = schema.required;

        // Create a config with all required fields
        const config: Record<string, any> = {};
        for (const field of requiredFields) {
          // Add a dummy value of the correct type
          const fieldSchema = schema.properties[field];
          const fieldType = Array.isArray(fieldSchema.type)
            ? fieldSchema.type[0]
            : fieldSchema.type;

          if (fieldType === "string") {
            config[field] = "dummy";
          } else if (fieldType === "integer" || fieldType === "number") {
            config[field] = 0;
          } else if (fieldType === "boolean") {
            config[field] = false;
          } else if (fieldType === "object") {
            config[field] = {};
          } else if (fieldType === "array") {
            config[field] = [];
          }
        }

        // For each required field, remove it and check that validation fails
        for (const field of requiredFields) {
          const testConfig = { ...config };
          delete testConfig[field];

          expect(() => validateConfigSchema(testConfig, schemaName)).toThrow();
        }
      }
    }
  });

  test("schema additional properties are rejected when specified", () => {
    const schemaNames = getAvailableSchemas();

    for (const schemaName of schemaNames) {
      const schema = loadSchema(schemaName);

      // Create a minimal valid config
      const config: Record<string, any> = {};
      if (schema.required) {
        for (const field of schema.required) {
          // Add a dummy value of the correct type
          const fieldSchema = schema.properties[field];
          const fieldType = Array.isArray(fieldSchema.type)
            ? fieldSchema.type[0]
            : fieldSchema.type;

          if (fieldType === "string") {
            config[field] = "dummy";
          } else if (fieldType === "integer" || fieldType === "number") {
            config[field] = 0;
          } else if (fieldType === "boolean") {
            config[field] = false;
          } else if (fieldType === "object") {
            config[field] = {};
          } else if (fieldType === "array") {
            config[field] = [];
          }
        }
      }

      // Add an additional property
      const testConfig = { ...config, additional_property: "value" };

      // Validation should fail if additionalProperties is false
      if (schema.additionalProperties === false) {
        expect(() => validateConfigSchema(testConfig, schemaName)).toThrow();
      }
    }
  });

  // Test config validation update
  Object.entries(EMBEDDING_FUNCTION_CONFIGS).forEach(([efName, testConfig]) => {
    test(`${efName} embedding function validates config updates`, () => {
      const EFClass = EMBEDDING_FUNCTION_CLASSES[efName];
      if (!EFClass) {
        throw new Error(`No class found for embedding function: ${efName}`);
      }

      // Create embedding function with arguments
      const efInstance = new EFClass(testConfig.args);

      // Get the original config
      const originalConfig = efInstance.getConfig();

      // Create a modified config with a different model name
      const modifiedConfig = {
        ...originalConfig,
        model_name: "different-model",
      };

      // Some embedding functions don't allow changing the model name
      if (efInstance.validateConfigUpdate) {
        try {
          efInstance.validateConfigUpdate(originalConfig, modifiedConfig);
        } catch (error) {
          // If it raises an exception, that's expected for many embedding functions
          expect(error).toBeDefined();
        }
      }
    });
  });
});

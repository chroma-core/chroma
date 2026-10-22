import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { OCIGenAIEmbeddingFunction } from "./index";

const COMPARTMENT_ID =
  process.env.OCI_COMPARTMENT_ID || "ocid1.compartment.oc1..exampleuniqueid";

describe("OCIGenAIEmbeddingFunction", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it("should initialize with default parameters", () => {
    const embedder = new OCIGenAIEmbeddingFunction({
      compartmentId: COMPARTMENT_ID,
    });
    expect(embedder.name).toBe("oci-genai");

    const config = embedder.getConfig();
    expect(config.model_name).toBe("cohere.embed-v4.0");
    expect(config.compartment_id).toBe(COMPARTMENT_ID);
    expect(config.auth_type).toBe("API_KEY");
    expect(config.auth_profile).toBe("DEFAULT");
    expect(config.auth_file_location).toBe("~/.oci/config");
    expect(config.truncate).toBe("END");
    expect(config.service_endpoint).toBeNull();
    expect(config.input_type).toBeNull();
    expect(config.output_dimensions).toBeNull();
  });

  it("should initialize with custom parameters", () => {
    const embedder = new OCIGenAIEmbeddingFunction({
      modelName: "cohere.embed-multilingual-v3.0",
      compartmentId: COMPARTMENT_ID,
      serviceEndpoint:
        "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
      authType: "SECURITY_TOKEN",
      authProfile: "MY_PROFILE",
      authFileLocation: "/etc/oci/config",
      truncate: "START",
      inputType: "CLUSTERING",
      outputDimensions: 512,
    });

    const config = embedder.getConfig();
    expect(config.model_name).toBe("cohere.embed-multilingual-v3.0");
    expect(config.service_endpoint).toBe(
      "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
    );
    expect(config.auth_type).toBe("SECURITY_TOKEN");
    expect(config.auth_profile).toBe("MY_PROFILE");
    expect(config.auth_file_location).toBe("/etc/oci/config");
    expect(config.truncate).toBe("START");
    expect(config.input_type).toBe("CLUSTERING");
    expect(config.output_dimensions).toBe(512);
  });

  it("should require a compartment id", () => {
    expect(() => {
      new OCIGenAIEmbeddingFunction({ compartmentId: "" });
    }).toThrow("compartmentId is required");
  });

  it("should reject unsupported auth types, truncate modes and input types", () => {
    expect(() => {
      new OCIGenAIEmbeddingFunction({
        compartmentId: COMPARTMENT_ID,
        authType: "PASSWORD" as any,
      });
    }).toThrow("Unsupported authType");
    expect(() => {
      new OCIGenAIEmbeddingFunction({
        compartmentId: COMPARTMENT_ID,
        truncate: "MIDDLE" as any,
      });
    }).toThrow("Unsupported truncate");
    expect(() => {
      new OCIGenAIEmbeddingFunction({
        compartmentId: COMPARTMENT_ID,
        inputType: "RERANK" as any,
      });
    }).toThrow("Unsupported inputType");
  });

  it("should build from config and round trip", () => {
    const config = {
      model_name: "cohere.embed-v4.0",
      compartment_id: COMPARTMENT_ID,
      service_endpoint:
        "https://inference.generativeai.us-chicago-1.oci.oraclecloud.com",
      auth_type: "API_KEY",
      auth_profile: "DEFAULT",
      auth_file_location: "~/.oci/config",
      truncate: "END",
      input_type: null,
      output_dimensions: 256,
    };

    const embedder = OCIGenAIEmbeddingFunction.buildFromConfig(config);

    expect(embedder.getConfig()).toEqual(config);
    expect(() =>
      OCIGenAIEmbeddingFunction.validateConfig(embedder.getConfig()),
    ).not.toThrow();
  });

  it("should validate config against the shared schema", () => {
    expect(() =>
      OCIGenAIEmbeddingFunction.validateConfig({
        model_name: "cohere.embed-v4.0",
        compartment_id: COMPARTMENT_ID,
        api_key: "secret",
      } as any),
    ).toThrow("Config validation failed");
    expect(() =>
      OCIGenAIEmbeddingFunction.validateConfig({
        model_name: "cohere.embed-v4.0",
      } as any),
    ).toThrow("Config validation failed");
  });

  it("should reject model and dimension changes in config updates", () => {
    const embedder = new OCIGenAIEmbeddingFunction({
      compartmentId: COMPARTMENT_ID,
    });
    expect(() =>
      embedder.validateConfigUpdate({
        ...embedder.getConfig(),
        model_name: "cohere.embed-english-v3.0",
      }),
    ).toThrow("Model name cannot be updated");
    expect(() =>
      embedder.validateConfigUpdate({
        ...embedder.getConfig(),
        output_dimensions: 256,
      }),
    ).toThrow("Output dimensions cannot be updated");
    expect(() =>
      embedder.validateConfigUpdate({
        ...embedder.getConfig(),
        truncate: "START",
      }),
    ).not.toThrow();
  });

  const generateEmbeddingsTest = "should generate embeddings";
  if (!process.env.OCI_COMPARTMENT_ID) {
    it.skip(generateEmbeddingsTest, () => {});
  } else {
    it(generateEmbeddingsTest, async () => {
      const embedder = new OCIGenAIEmbeddingFunction({
        compartmentId: process.env.OCI_COMPARTMENT_ID!,
        authProfile: process.env.OCI_CONFIG_PROFILE,
        authType: (process.env.OCI_AUTH_TYPE as any) || "API_KEY",
        serviceEndpoint: process.env.OCI_GENAI_SERVICE_ENDPOINT,
      });
      const texts = ["Hello world", "Test text"];
      const embeddings = await embedder.generate(texts);

      expect(embeddings.length).toBe(texts.length);
      embeddings.forEach((embedding) => {
        expect(embedding.length).toBe(1536);
      });
      expect(embeddings[0]).not.toEqual(embeddings[1]);

      const queryEmbeddings = await embedder.generateForQueries([
        "Hello world",
      ]);
      expect(queryEmbeddings[0]).not.toEqual(embeddings[0]);
    });
  }
});

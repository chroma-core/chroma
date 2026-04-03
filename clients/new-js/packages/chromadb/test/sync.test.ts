import { describe, expect, test, beforeEach, afterAll } from "@jest/globals";
import { SyncClient } from "../src/sync-client";
import { ChromaValueError } from "../src/errors";
import { DenseEmbeddingModel, SparseEmbeddingModel } from "../src/sync-types";

// Mock chromaFetch so tests never make real HTTP requests.
// Valid-input tests only need to verify that validation passes (no ChromaValueError).
jest.mock("../src/chroma-fetch", () => ({
  chromaFetch: jest.fn(() =>
    Promise.resolve(
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ),
  ),
}));

describe("SyncClient", () => {
  describe("constructor", () => {
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
    });

    afterAll(() => {
      process.env = originalEnv;
    });

    test("it should throw if no API key is provided", () => {
      delete process.env.CHROMA_API_KEY;
      expect(() => new SyncClient()).toThrow(ChromaValueError);
      expect(() => new SyncClient()).toThrow(/Missing API key/);
    });

    test("it should accept an API key via constructor", () => {
      delete process.env.CHROMA_API_KEY;
      const client = new SyncClient({ apiKey: "test-key" });
      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(SyncClient);
    });

    test("it should accept an API key via environment variable", () => {
      process.env.CHROMA_API_KEY = "env-test-key";
      const client = new SyncClient();
      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(SyncClient);
    });

    test("it should accept a custom host", () => {
      const client = new SyncClient({
        apiKey: "test-key",
        host: "custom-sync.example.com",
      });
      expect(client).toBeDefined();
    });
  });

  describe("methods exist", () => {
    let client: SyncClient;

    beforeEach(() => {
      client = new SyncClient({ apiKey: "test-key" });
    });

    test("source methods are defined", () => {
      expect(typeof client.listSources).toBe("function");
      expect(typeof client.createGitHubSource).toBe("function");
      expect(typeof client.createS3Source).toBe("function");
      expect(typeof client.createWebSource).toBe("function");
      expect(typeof client.getSource).toBe("function");
      expect(typeof client.deleteSource).toBe("function");
    });

    test("invocation methods are defined", () => {
      expect(typeof client.listInvocations).toBe("function");
      expect(typeof client.getInvocation).toBe("function");
      expect(typeof client.cancelInvocation).toBe("function");
      expect(typeof client.createInvocation).toBe("function");
      expect(typeof client.getLatestInvocationsByKeys).toBe("function");
    });

    test("system methods are defined", () => {
      expect(typeof client.health).toBe("function");
    });
  });

  describe("GitHub repository parsing", () => {
    const client = new SyncClient({ apiKey: "test-key" });
    const baseConfig = {
      databaseName: "test-db",
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };

    test("it should accept owner/repo format", async () => {
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: { repository: "chroma-core/chroma" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse a GitHub URL into owner/repo", async () => {
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: { repository: "https://github.com/chroma-core/chroma" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse a GitHub URL with .git suffix", async () => {
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: {
            repository: "https://github.com/chroma-core/chroma.git",
          },
        }),
      ).resolves.not.toThrow();
    });

    test("it should reject invalid repository format", async () => {
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: { repository: "not-valid" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: { repository: "not-valid" },
        }),
      ).rejects.toThrow(/Expected "owner\/repo"/);
    });

    test("it should reject non-GitHub URLs", async () => {
      await expect(
        client.createGitHubSource({
          ...baseConfig,
          github: { repository: "https://gitlab.com/owner/repo" },
        }),
      ).rejects.toThrow(ChromaValueError);
    });
  });

  describe("S3 bucket name parsing", () => {
    const client = new SyncClient({ apiKey: "test-key" });
    const baseConfig = {
      databaseName: "test-db",
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };
    const s3Base = {
      region: "us-east-1",
      collectionName: "docs",
      awsCredentialId: 1,
    };

    test("it should accept a plain bucket name", async () => {
      await expect(
        client.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "my-bucket" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse s3:// URI to bucket name", async () => {
      await expect(
        client.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "s3://my-bucket/some/prefix" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse S3 ARN to bucket name", async () => {
      await expect(
        client.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "arn:aws:s3:::my-bucket" },
        }),
      ).resolves.not.toThrow();
    });
  });

  describe("Web starting URL validation", () => {
    const client = new SyncClient({ apiKey: "test-key" });
    const baseConfig = {
      databaseName: "test-db",
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };

    test("it should accept a valid https URL", async () => {
      await expect(
        client.createWebSource({
          ...baseConfig,
          web: { startingUrl: "https://docs.trychroma.com" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should reject an invalid URL", async () => {
      await expect(
        client.createWebSource({
          ...baseConfig,
          web: { startingUrl: "not a url" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.createWebSource({
          ...baseConfig,
          web: { startingUrl: "not a url" },
        }),
      ).rejects.toThrow(/Invalid starting URL/);
    });

    test("it should reject non-http protocols", async () => {
      await expect(
        client.createWebSource({
          ...baseConfig,
          web: { startingUrl: "ftp://example.com" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.createWebSource({
          ...baseConfig,
          web: { startingUrl: "ftp://example.com" },
        }),
      ).rejects.toThrow(/Only http and https/);
    });
  });

  describe("embedding model enums", () => {
    test("dense embedding model enum has expected value", () => {
      expect(DenseEmbeddingModel.Qwen3Embedding06B).toBe(
        "Qwen/Qwen3-Embedding-0.6B",
      );
    });

    test("sparse embedding model enum has expected values", () => {
      expect(SparseEmbeddingModel.BM25).toBe("Chroma/BM25");
      expect(SparseEmbeddingModel.SpladeV1).toBe("prithivida/Splade_PP_en_v1");
    });
  });
});

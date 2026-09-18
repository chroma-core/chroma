import { describe, expect, test, beforeEach, afterAll } from "@jest/globals";
import { CloudClient } from "../src/cloud-client";
import { chromaFetch } from "../src/chroma-fetch";
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

describe("CloudClient sync", () => {
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
      expect(() => new CloudClient({ database: "test-db" })).toThrow(
        ChromaValueError,
      );
      expect(() => new CloudClient({ database: "test-db" })).toThrow(
        /Missing API key/,
      );
    });

    test("it should accept an API key via constructor", () => {
      delete process.env.CHROMA_API_KEY;
      const client = new CloudClient({
        apiKey: "test-key",
        database: "test-db",
      });
      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(CloudClient);
    });

    test("it should accept an API key via environment variable", () => {
      process.env.CHROMA_API_KEY = "env-test-key";
      const client = new CloudClient({ database: "test-db" });
      expect(client).toBeDefined();
      expect(client).toBeInstanceOf(CloudClient);
    });

    test("it should accept a custom sync host", () => {
      const client = new CloudClient({
        apiKey: "test-key",
        database: "test-db",
        syncHost: "custom-sync.example.com",
      });
      expect(client).toBeDefined();
    });
  });

  describe("methods exist", () => {
    let client: CloudClient;

    beforeEach(() => {
      client = new CloudClient({ apiKey: "test-key", database: "test-db" });
    });

    test("source methods are defined", () => {
      expect(typeof client.sync.listSources).toBe("function");
      expect(typeof client.sync.createGitHubSource).toBe("function");
      expect(typeof client.sync.createS3Source).toBe("function");
      expect(typeof client.sync.createWebSource).toBe("function");
      expect(typeof client.sync.getSource).toBe("function");
      expect(typeof client.sync.deleteSource).toBe("function");
    });

    test("invocation methods are defined", () => {
      expect(typeof client.sync.listInvocations).toBe("function");
      expect(typeof client.sync.getInvocation).toBe("function");
      expect(typeof client.sync.cancelInvocation).toBe("function");
      expect(typeof client.sync.createInvocation).toBe("function");
      expect(typeof client.sync.getLatestInvocationsByKeys).toBe("function");
    });

    test("system methods are defined", () => {
      expect(typeof client.sync.health).toBe("function");
    });

    test("it scopes sync requests to the cloud client database", async () => {
      await client.sync.listSources();

      const fetchMock = chromaFetch as jest.Mock;
      const [requestInfo] = fetchMock.mock.calls.at(-1) as [RequestInfo | URL];
      expect((requestInfo as Request).url).toContain("database_name=test-db");
    });

    test("it prefers source-scoped invocation queries over database-scoped ones", async () => {
      await client.sync.listInvocations({ sourceId: "src-123" });

      const fetchMock = chromaFetch as jest.Mock;
      const [requestInfo] = fetchMock.mock.calls.at(-1) as [RequestInfo | URL];
      const url = (requestInfo as Request).url;
      expect(url).toContain("source_id=src-123");
      expect(url).not.toContain("database_name=");
    });

    test("it forwards non-header fetch options to sync requests", async () => {
      const abortController = new AbortController();
      const clientWithFetchOptions = new CloudClient({
        apiKey: "test-key",
        database: "test-db",
        fetchOptions: {
          signal: abortController.signal,
          credentials: "include",
        },
      });

      await clientWithFetchOptions.sync.listSources();

      const fetchMock = chromaFetch as jest.Mock;
      const [requestInfo] = fetchMock.mock.calls.at(-1) as [RequestInfo | URL];
      const request = requestInfo as Request;
      expect(request.signal).toBe(abortController.signal);
      expect(request.credentials).toBe("include");
    });

    test("it preserves the cloud api key when fetch headers include x-chroma-token", async () => {
      const clientWithHeaderOverride = new CloudClient({
        apiKey: "test-key",
        database: "test-db",
        fetchOptions: {
          headers: {
            "x-test-header": "test-value",
            "x-chroma-token": "wrong-token",
          },
        },
      });

      await clientWithHeaderOverride.sync.listSources();

      const fetchMock = chromaFetch as jest.Mock;
      const [requestInfo] = fetchMock.mock.calls.at(-1) as [RequestInfo | URL];
      const request = requestInfo as Request;
      expect(request.headers.get("x-test-header")).toBe("test-value");
      expect(request.headers.get("x-chroma-token")).toBe("test-key");
    });
  });

  describe("GitHub repository parsing", () => {
    const client = new CloudClient({ apiKey: "test-key", database: "test-db" });
    const baseConfig = {
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };

    test("it should accept owner/repo format", async () => {
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: { repository: "chroma-core/chroma" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse a GitHub URL into owner/repo", async () => {
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: { repository: "https://github.com/chroma-core/chroma" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse a GitHub URL with .git suffix", async () => {
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: {
            repository: "https://github.com/chroma-core/chroma.git",
          },
        }),
      ).resolves.not.toThrow();
    });

    test("it should reject invalid repository format", async () => {
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: { repository: "not-valid" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: { repository: "not-valid" },
        }),
      ).rejects.toThrow(/Expected "owner\/repo"/);
    });

    test("it should reject non-GitHub URLs", async () => {
      await expect(
        client.sync.createGitHubSource({
          ...baseConfig,
          github: { repository: "https://gitlab.com/owner/repo" },
        }),
      ).rejects.toThrow(ChromaValueError);
    });
  });

  describe("S3 bucket name parsing", () => {
    const client = new CloudClient({ apiKey: "test-key", database: "test-db" });
    const baseConfig = {
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };
    const s3Base = {
      region: "us-east-1",
      collectionName: "docs",
      awsCredentialId: 1,
    };

    test("it should accept a plain bucket name", async () => {
      await expect(
        client.sync.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "my-bucket" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse s3:// URI to bucket name", async () => {
      await expect(
        client.sync.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "s3://my-bucket/some/prefix" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should parse S3 ARN to bucket name", async () => {
      await expect(
        client.sync.createS3Source({
          ...baseConfig,
          s3: { ...s3Base, bucketName: "arn:aws:s3:::my-bucket" },
        }),
      ).resolves.not.toThrow();
    });
  });

  describe("Web starting URL validation", () => {
    const client = new CloudClient({ apiKey: "test-key", database: "test-db" });
    const baseConfig = {
      embedding: { dense: { model: DenseEmbeddingModel.Qwen3Embedding06B } },
    };

    test("it should accept a valid https URL", async () => {
      await expect(
        client.sync.createWebSource({
          ...baseConfig,
          web: { startingUrl: "https://docs.trychroma.com" },
        }),
      ).resolves.not.toThrow();
    });

    test("it should reject an invalid URL", async () => {
      await expect(
        client.sync.createWebSource({
          ...baseConfig,
          web: { startingUrl: "not a url" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.sync.createWebSource({
          ...baseConfig,
          web: { startingUrl: "not a url" },
        }),
      ).rejects.toThrow(/Invalid starting URL/);
    });

    test("it should reject non-http protocols", async () => {
      await expect(
        client.sync.createWebSource({
          ...baseConfig,
          web: { startingUrl: "ftp://example.com" },
        }),
      ).rejects.toThrow(ChromaValueError);
      await expect(
        client.sync.createWebSource({
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

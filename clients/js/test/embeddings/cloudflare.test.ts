import { expect, test } from "@jest/globals";
import { DOCUMENTS } from "../data";
import { CloudflareWorkersAIEmbeddingFunction } from "../../src";

if (!process.env.CF_API_TOKEN) {
  test.skip("it should generate Cloudflare embeddings with apiToken and AccountId", async () => {});
} else {
  test("it should generate Cloudflare embeddings with apiToken and AccountId", async () => {
    const embedder = new CloudflareWorkersAIEmbeddingFunction({
      apiToken: process.env.CF_API_TOKEN as string,
      accountId: process.env.CF_ACCOUNT_ID,
    });
    const embeddings = await embedder.generate(DOCUMENTS);
    expect(embeddings).toBeDefined();
    expect(embeddings.length).toBe(DOCUMENTS.length);
  });
}

if (!process.env.CF_API_TOKEN) {
  test.skip("it should generate Cloudflare embeddings with apiToken and AccountId and model", async () => {});
} else {
  test("it should generate Cloudflare embeddings with apiToken and AccountId and model", async () => {
    const embedder = new CloudflareWorkersAIEmbeddingFunction({
      apiToken: process.env.CF_API_TOKEN as string,
      accountId: process.env.CF_ACCOUNT_ID,
      model: "@cf/baai/bge-small-en-v1.5",
    });
    const embeddings = await embedder.generate(DOCUMENTS);
    expect(embeddings).toBeDefined();
    expect(embeddings.length).toBe(DOCUMENTS.length);
  });
}

if (!process.env.CF_API_TOKEN) {
  test.skip("it should generate Cloudflare embeddings with apiToken and gateway", async () => {});
} else {
  test("it should generate Cloudflare embeddings with apiToken and gateway", async () => {
    const embedder = new CloudflareWorkersAIEmbeddingFunction({
      apiToken: process.env.CF_API_TOKEN as string,
      gatewayUrl: process.env.CF_GATEWAY_ENDPOINT,
    });
    const embeddings = await embedder.generate(DOCUMENTS);
    expect(embeddings).toBeDefined();
    expect(embeddings.length).toBe(DOCUMENTS.length);
  });
}

if (!process.env.CF_API_TOKEN) {
  test.skip("it should fail when batch too large", async () => {});
} else {
  test("it should fail when batch too large", async () => {
    const embedder = new CloudflareWorkersAIEmbeddingFunction({
      apiToken: process.env.CF_API_TOKEN as string,
      gatewayUrl: process.env.CF_GATEWAY_ENDPOINT,
    });
    const largeBatch = Array(100)
      .fill([...DOCUMENTS])
      .flat();
    try {
      await embedder.generate(largeBatch);
    } catch (e: any) {
      expect(e.message).toMatch("Batch too large");
    }
  });
}

if (!process.env.CF_API_TOKEN) {
  test.skip("it should fail when gateway endpoint and account id are both provided", async () => {});
} else {
  test("it should fail when gateway endpoint and account id are both provided", async () => {
    try {
      new CloudflareWorkersAIEmbeddingFunction({
        apiToken: process.env.CF_API_TOKEN as string,
        accountId: process.env.CF_ACCOUNT_ID,
        gatewayUrl: process.env.CF_GATEWAY_ENDPOINT,
      });
    } catch (e: any) {
      expect(e.message).toMatch(
        "Please provide either an accountId or a gatewayUrl, not both.",
      );
    }
  });
}

if (!process.env.CF_API_TOKEN) {
  test.skip("it should fail when neither gateway endpoint nor account id are provided", async () => {});
} else {
  test("it should fail when neither gateway endpoint nor account id are provided", async () => {
    try {
      new CloudflareWorkersAIEmbeddingFunction({
        apiToken: process.env.CF_API_TOKEN as string,
      });
    } catch (e: any) {
      expect(e.message).toMatch(
        "Please provide either an accountId or a gatewayUrl.",
      );
    }
  });
}

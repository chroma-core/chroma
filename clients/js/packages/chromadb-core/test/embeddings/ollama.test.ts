import { OllamaEmbeddingFunction } from "../../src/index";
import { describe, expect, test } from "@jest/globals";
import { DOCUMENTS } from "../data";

describe("ollama embedding function", () => {
  if ((globalThis as any).ollamaAvailable) {
    test("it should embed with defaults", async () => {
      const embedder = new OllamaEmbeddingFunction({
        url: process.env.OLLAMA_URL,
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      expect(embeddings).toBeDefined();
      expect(embeddings.length).toBe(DOCUMENTS.length);
      expect(embeddings[0]).toBeDefined();
      expect(embeddings[0].length).toBe(384);
    });
    test("it should embed with model", async () => {
      const embedder = new OllamaEmbeddingFunction({
        url: process.env.OLLAMA_URL,
        model: "nomic-embed-text",
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      expect(embeddings).toBeDefined();
      expect(embeddings.length).toBe(DOCUMENTS.length);
      expect(embeddings[0]).toBeDefined();
      expect(embeddings[0].length).toBe(768);
    });

    test("it should fail with unknown model", async () => {
      const model_name = "not-a-real-model" + Math.floor(Math.random() * 1000);
      const embedder = new OllamaEmbeddingFunction({
        url: process.env.OLLAMA_URL,
        model: model_name,
      });
      try {
        await embedder.generate(DOCUMENTS);
      } catch (e: any) {
        expect(e.message).toContain(`model \"${model_name}\" not found`);
      }
    });

    test("it should fail wrong host", async () => {
      const embedder = new OllamaEmbeddingFunction({
        url: "https://example.com:1234",
      });
      try {
        await embedder.generate(DOCUMENTS);
      } catch (e: any) {
        expect(e.message).toContain(`fetch failed`);
      }
    });
  } else {
    test.skip("ollama not installed", async () => {});
  }
});

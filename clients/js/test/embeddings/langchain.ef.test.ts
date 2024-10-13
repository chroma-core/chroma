import { describe, expect, test } from "@jest/globals";
import {
  LangChainEmbeddingFunction,
  OllamaEmbeddingFunction,
} from "../../src/index";

describe("Langchain Embedding Function Tests", () => {
  if ((globalThis as any).isLangchainInstalled) {
    test("Test LC to Chroma EF", async () => {
      // @ts-ignore
      const importedModule = await import("@langchain/ollama");
      const chromaEmbedding = await LangChainEmbeddingFunction.create({
        langchainEmbeddings: new importedModule.OllamaEmbeddings({
          baseUrl: process.env.OLLAMA_URL,
          model: "chroma/all-minilm-l6-v2-f32",
        }),
      });
      const results = await chromaEmbedding.generate(["Hello World"]);
      expect(results).toBeDefined();
      expect(results).toHaveLength(1);
      expect(results[0]).toHaveLength(384);

      const chromaModule = await import(
        // @ts-ignore
        "@langchain/community/vectorstores/chroma"
      );
      const vectorStore = new chromaModule.Chroma(chromaEmbedding, {
        collectionName: "a-test-collection",
        url: process.env.DEFAULT_CHROMA_INSTANCE_URL,
      });
      const document1 = {
        pageContent: "The powerhouse of the cell is the mitochondria",
        metadata: {},
      };
      const res = await vectorStore.addDocuments([document1], { ids: ["1"] });
      expect(res).toBeDefined();
    });
    test("Test Chroma EF to LC ", async () => {
      const chromaEmbedding = await LangChainEmbeddingFunction.create({
        chromaEmbeddingFunction: new OllamaEmbeddingFunction({
          url:
            `${process.env.OLLAMA_URL}/api/embeddings` ||
            "http://localhost:11434/api/embeddings",
          model: "chroma/all-minilm-l6-v2-f32",
        }),
      });
      const results = await chromaEmbedding.generate(["Hello World"]);
      expect(results).toBeDefined();
      expect(results).toHaveLength(1);
      expect(results[0]).toHaveLength(384);
      const chromaModule = await import(
        // @ts-ignore
        "@langchain/community/vectorstores/chroma"
      );
      const vectorStore = new chromaModule.Chroma(chromaEmbedding, {
        collectionName: "a-test-collection",
        url: process.env.DEFAULT_CHROMA_INSTANCE_URL,
      });
      const document1 = {
        pageContent: "The powerhouse of the cell is the mitochondria",
        metadata: {},
      };
      const res = await vectorStore.addDocuments([document1], { ids: ["1"] });
      expect(res).toBeDefined();
      const retriever = await vectorStore.asRetriever();
      const retrieverResults = await retriever.invoke(
        "Tell me about the mitochondria",
      );
      expect(retrieverResults).toBeDefined();
      expect(retrieverResults).toHaveLength(1);
    });
  } else {
    test.skip("should skip this test if the package is not installed.", () => {
      // This test will be skipped
    });
  }
});

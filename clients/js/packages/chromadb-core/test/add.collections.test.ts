import { expect, test, describe, beforeEach } from "@jest/globals";
import { DOCUMENTS, EMBEDDINGS, IDS } from "./data";
import { METADATAS } from "./data";
import { IncludeEnum } from "../src/types";
import { OpenAIEmbeddingFunction } from "../src/embeddings/OpenAIEmbeddingFunction";
import { CohereEmbeddingFunction } from "../src/embeddings/CohereEmbeddingFunction";
import { VoyageAIEmbeddingFunction } from "../src/embeddings/VoyageAIEmbeddingFunction";
import { ChromaClient } from "../src/ChromaClient";
import { ChromaNotFoundError } from "../src/Errors";

describe("add collections", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should add single embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    const id = "test1";
    const embedding = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const metadata = { test: "test" };
    await collection.add({
      ids: id,
      embeddings: embedding,
      metadatas: metadata,
    });
    const count = await collection.count();
    expect(count).toBe(1);
    var res = await collection.get({
      ids: id,
      include: [IncludeEnum.Embeddings],
    });
    expect(res.embeddings?.[0]).toEqual(embedding);
  });

  test("it should add batch embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      documents: DOCUMENTS,
    });
    const count = await collection.count();
    expect(count).toBe(3);
    var res = await collection.get({
      include: [IncludeEnum.Embeddings],
    });
    expect(res.embeddings).toEqual(EMBEDDINGS);
  });

  if (!process.env.OPENAI_API_KEY) {
    test.skip("it should add OpenAI embeddings", async () => {});
  } else {
    test("it should add OpenAI embeddings", async () => {
      const embedder = new OpenAIEmbeddingFunction({
        openai_api_key: process.env.OPENAI_API_KEY || "",
      });
      const collection = await client.createCollection({
        name: "test",
        embeddingFunction: embedder,
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      await collection.add({ ids: IDS, embeddings: embeddings });
      const count = await collection.count();
      expect(count).toBe(3);
      var res = await collection.get({
        ids: IDS,
        include: [IncludeEnum.Embeddings],
      });
      expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
    });
    test("it should add OpenAI embeddings with dimensions", async () => {
      await client.reset();
      const embedder = new OpenAIEmbeddingFunction({
        openai_api_key: process.env.OPENAI_API_KEY || "",
        openai_embedding_dimensions: 64,
        openai_model: "text-embedding-3-small",
      });
      const collection = await client.createCollection({
        name: "test",
        embeddingFunction: embedder,
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      await collection.add({ ids: IDS, embeddings: embeddings });
      const count = await collection.count();
      expect(count).toBe(3);
      var res = await collection.get({
        ids: IDS,
        include: [IncludeEnum.Embeddings],
      });
      expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
      expect(embeddings[0].length).toBe(64);
    });
    test("it should add OpenAI embeddings with dimensions not supporting old models", async () => {
      await client.reset();
      const embedder = new OpenAIEmbeddingFunction({
        openai_api_key: process.env.OPENAI_API_KEY || "",
        openai_embedding_dimensions: 64,
      });
      const collection = await client.createCollection({
        name: "test",
        embeddingFunction: embedder,
      });

      try {
        await embedder.generate(DOCUMENTS);
      } catch (e: any) {
        expect(e.message).toMatch(
          "This model does not support specifying dimensions.",
        );
      }
    });
  }

  if (!process.env.COHERE_API_KEY) {
    test.skip("it should add Cohere embeddings", async () => {});
  } else {
    test("it should add Cohere embeddings", async () => {
      const embedder = new CohereEmbeddingFunction({
        cohere_api_key: process.env.COHERE_API_KEY || "",
        cohere_api_key_env_var: "COHERE_API_KEY",
      });
      const collection = await client.createCollection({
        name: "test",
        embeddingFunction: embedder,
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      await collection.add({ ids: IDS, embeddings: embeddings });
      const count = await collection.count();
      expect(count).toBe(3);
      var res = await collection.get({
        ids: IDS,
        include: [IncludeEnum.Embeddings],
      });
      expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
    });
  }

  if (!process.env.VOYAGE_API_KEY) {
    test.skip("it should add VoyageAI embeddings", async () => {});
  } else {
    test("it should add VoyageAI embeddings", async () => {
      const embedder = new VoyageAIEmbeddingFunction({
        api_key: process.env.VOYAGE_API_KEY || "",
        model: "voyage-3-large",
        api_key_env_var: "VOYAGE_API_KEY",
      });
      const collection = await client.createCollection({
        name: "test",
        embeddingFunction: embedder,
      });
      const embeddings = await embedder.generate(DOCUMENTS);
      await collection.add({ ids: IDS, embeddings: embeddings });
      const count = await collection.count();
      expect(count).toBe(3);
      var res = await collection.get({
        ids: IDS,
        include: [IncludeEnum.Embeddings],
      });
      expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
    });
  }

  test("add documents", async () => {
    const collection = await client.createCollection({ name: "test" });
    await collection.add({
      ids: IDS,
      embeddings: EMBEDDINGS,
      documents: DOCUMENTS,
    });
    const results = await collection.get({ ids: "test1" });
    expect(results.documents[0]).toBe("This is a test");
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    await expect(async () => {
      await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
    }).rejects.toThrow(ChromaNotFoundError);
  });

  test("It should return an error when inserting duplicate IDs in the same batch", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = IDS.concat(["test1"]);
    const embeddings = EMBEDDINGS.concat([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]);
    const metadatas = METADATAS.concat([{ test: "test1", float_value: 0.1 }]);
    try {
      await collection.add({ ids, embeddings, metadatas });
    } catch (e: any) {
      expect(e.message).toMatch("duplicates");
    }
  });

  test("should error on empty embedding", async () => {
    const collection = await client.createCollection({ name: "test" });
    const ids = ["id1"];
    const embeddings = [[]];
    const metadatas = [{ test: "test1", float_value: 0.1 }];
    try {
      await collection.add({ ids, embeddings, metadatas });
    } catch (e: any) {
      expect(e.message).toMatch("got empty embedding at pos");
    }
  });
});

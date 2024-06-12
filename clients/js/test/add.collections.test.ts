import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { DOCUMENTS, EMBEDDINGS, IDS } from "./data";
import { METADATAS } from "./data";
import { IncludeEnum } from "../src/types";
import { OpenAIEmbeddingFunction } from "../src/embeddings/OpenAIEmbeddingFunction";
import { CohereEmbeddingFunction } from "../src/embeddings/CohereEmbeddingFunction";
import { OllamaEmbeddingFunction } from "../src/embeddings/OllamaEmbeddingFunction";
import { InvalidCollectionError } from "../src/Errors";

test("it should add single embeddings to a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  const ids = "test1";
  const embeddings = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const metadatas = { test: "test" };
  await collection.add({ ids, embeddings, metadatas });
  const count = await collection.count();
  expect(count).toBe(1);
  var res = await collection.get({
    ids: [ids],
    include: [IncludeEnum.Embeddings],
  });
  expect(res.embeddings![0]).toEqual(embeddings);
});

test("it should add batch embeddings to a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
  const count = await collection.count();
  expect(count).toBe(3);
  var res = await collection.get({
    ids: IDS,
    include: [IncludeEnum.Embeddings],
  });
  expect(res.embeddings).toEqual(EMBEDDINGS); // reverse because of the order of the ids
});

if (!process.env.OPENAI_API_KEY) {
  test.skip("it should add OpenAI embeddings", async () => {});
} else {
  test("it should add OpenAI embeddings", async () => {
    await chroma.reset();
    const embedder = new OpenAIEmbeddingFunction({
      openai_api_key: process.env.OPENAI_API_KEY || "",
    });
    const collection = await chroma.createCollection({
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

if (!process.env.COHERE_API_KEY) {
  test.skip("it should add Cohere embeddings", async () => {});
} else {
  test("it should add Cohere embeddings", async () => {
    await chroma.reset();
    const embedder = new CohereEmbeddingFunction({
      cohere_api_key: process.env.COHERE_API_KEY || "",
    });
    const collection = await chroma.createCollection({
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
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  let resp = await collection.add({
    ids: IDS,
    embeddings: EMBEDDINGS,
    documents: DOCUMENTS,
  });
  expect(resp).toBe(true);
  const results = await collection.get({ ids: ["test1"] });
  expect(results.documents[0]).toBe("This is a test");
});

test("should error on non existing collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.deleteCollection({ name: "test" });
  expect(async () => {
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
  }).rejects.toThrow(InvalidCollectionError);
});

test("It should return an error when inserting duplicate IDs in the same batch", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
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
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  const ids = ["id1"];
  const embeddings = [[]];
  const metadatas = [{ test: "test1", float_value: 0.1 }];
  try {
    await collection.add({ ids, embeddings, metadatas });
  } catch (e: any) {
    expect(e.message).toMatch("got empty embedding at pos");
  }
});

if (!process.env.OLLAMA_SERVER_URL) {
  test.skip("it should use ollama EF, OLLAMA_SERVER_URL not defined", async () => {});
} else {
  test("it should use ollama EF", async () => {
    await chroma.reset();
    const embedder = new OllamaEmbeddingFunction({
      url:
        process.env.OLLAMA_SERVER_URL ||
        "http://127.0.0.1:11434/api/embeddings",
      model: "nomic-embed-text",
    });
    const collection = await chroma.createCollection({
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

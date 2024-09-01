import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { DOCUMENTS, EMBEDDINGS, IDS, METADATAS } from "./data";
import { ChromaValueError, InvalidCollectionError } from "../src/Errors";
import { DefaultEmbeddingFunction } from "../src/embeddings/DefaultEmbeddingFunction";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

describe("get collections", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  describe("it should get documents from a collection", () => {
    test("it should get documents from a collection", async () => {
      const collection = await client.createCollection({ name: "test" });
      await collection.add({
        ids: IDS,
        embeddings: EMBEDDINGS,
        metadatas: METADATAS,
      });
      const results = await collection.get({ ids: ["test1"] });
      expect(results?.ids).toHaveLength(1);
      expect(["test1"]).toEqual(expect.arrayContaining(results.ids));
      expect(["test2"]).not.toEqual(expect.arrayContaining(results.ids));
      expect(results.included).toEqual(
        expect.arrayContaining(["metadatas", "documents"]),
      );

      const results2 = await collection.get({
        where: { test: "test1" },
      });
      expect(results2?.ids).toHaveLength(1);
      expect(["test1"]).toEqual(expect.arrayContaining(results2.ids));
    });
  });

  describe("wrong code returns an error", () => {
    test("wrong code returns an error", async () => {
      const collection = await client.createCollection({ name: "test" });
      await collection.add({
        ids: IDS,
        embeddings: EMBEDDINGS,
        metadatas: METADATAS,
      });
      try {
        await collection.get({
          where: {
            //@ts-ignore supposed to fail
            test: { $contains: "hello" },
          },
        });
      } catch (error: any) {
        expect(error).toBeDefined();
        expect(error).toBeInstanceOf(ChromaValueError);
        expect(error.message).toMatchInlineSnapshot(
          `"Expected where operator to be one of $gt, $gte, $lt, $lte, $ne, $eq, $in, $nin, got $contains"`,
        );
      }
    });
  });

  describe("it should get embedding with matching documents", () => {
    test("it should get embedding with matching documents", async () => {
      const collection = await client.createCollection({ name: "test" });
      await collection.add({
        ids: IDS,
        embeddings: EMBEDDINGS,
        metadatas: METADATAS,
        documents: DOCUMENTS,
      });
      const results2 = await collection.get({
        whereDocument: { $contains: "This is a test" },
      });
      expect(results2?.ids).toHaveLength(1);
      expect(["test1"]).toEqual(expect.arrayContaining(results2.ids));
    });
  });

  describe("it should get records not matching", () => {
    test("it should get records not matching", async () => {
      const collection = await client.createCollection({ name: "test" });
      await collection.add({
        ids: IDS,
        embeddings: EMBEDDINGS,
        metadatas: METADATAS,
        documents: DOCUMENTS,
      });
      const results2 = await collection.get({
        whereDocument: { $not_contains: "This is another" },
      });
      expect(results2?.ids).toHaveLength(2);
      expect(["test1", "test3"]).toEqual(expect.arrayContaining(results2.ids));
    });
  });

  describe("test gt, lt, in a simple small way", () => {
    test("test gt, lt, in a simple small way", async () => {
      const collection = await client.createCollection({ name: "test" });
      await collection.add({
        ids: IDS,
        embeddings: EMBEDDINGS,
        metadatas: METADATAS,
      });
      const items = await collection.get({
        where: { float_value: { $gt: -1.4 } },
      });
      expect(items.ids).toHaveLength(2);
      expect(["test2", "test3"]).toEqual(expect.arrayContaining(items.ids));
    });
  });

  describe("should error on non existing collection", () => {
    test("should error on non existing collection", async () => {
      const collection = await client.createCollection({ name: "test" });
      await client.deleteCollection({ name: "test" });
      expect(async () => {
        await collection.get({ ids: IDS });
      }).rejects.toThrow(InvalidCollectionError);
    });
  });

  describe("it should throw an error if the collection does not exist", () => {
    test("it should throw an error if the collection does not exist", async () => {
      await expect(
        async () =>
          await client.getCollection({
            name: "test",
            embeddingFunction: new DefaultEmbeddingFunction(),
          }),
      ).rejects.toThrow(Error);
    });
  });
});

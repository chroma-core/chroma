import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "@jest/globals";
import { EMBEDDINGS, IDS, METADATAS } from "./data";
import { InvalidCollectionError } from "../src/Errors";
import { StartedTestContainer } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";
import { startChromaContainer } from "./startChromaContainer";

describe("delete collection", () => {
  // connects to the unauthenticated chroma instance started in
  // the global jest setup file.
  const client = new ChromaClient({
    path: process.env.DEFAULT_CHROMA_INSTANCE_URL,
  });

  beforeEach(async () => {
    await client.reset();
  });

  test("it should delete documents from a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.addRecords(collection, {
      ids: IDS,
      embeddings: EMBEDDINGS,
      metadatas: METADATAS,
    });
    let count = await client.countRecords(collection);
    expect(count).toBe(3);
    await client.deleteRecords(collection, {
      where: { test: "test1" },
    });
    count = await client.countRecords(collection);
    expect(count).toBe(2);

    const remainingEmbeddings = await client.getRecords(collection);
    expect(remainingEmbeddings?.ids).toEqual(
      expect.arrayContaining(["test2", "test3"]),
    );
  });

  test("should error on non existing collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    await client.deleteCollection({ name: "test" });
    expect(async () => {
      await client.deleteRecords(collection, { where: { test: "test1" } });
    }).rejects.toThrow(InvalidCollectionError);
  });
});

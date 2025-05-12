import { expect, test, describe, beforeEach } from "@jest/globals";
import { DOCUMENTS, EMBEDDINGS, IDS, METADATAS } from "./data";
import { ChromaClient } from "../src";

describe("add collections", () => {
  const client = new ChromaClient();

  beforeEach(async () => {
    await client.reset();
  });

  test("it should add single embeddings to a collection", async () => {
    const collection = await client.createCollection({ name: "test" });
    console.log(collection);
    const id = "test1";
    // const embedding = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    // const metadata = { test: "test" };
    // await collection.add({
    //   ids: [id],
    //   embeddings: [embedding],
    //   metadatas: [metadata],
    // });
    // const count = await collection.count();
    // expect(count).toBe(1);
    // const res = await collection.get({
    //   ids: [id],
    //   include: ["embeddings"],
    // });
    // expect(res.embeddings?.[0]).toEqual(embedding);
  });
});

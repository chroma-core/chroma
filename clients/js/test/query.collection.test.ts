import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { IncludeEnum } from "../src/types";
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from "./data";

test("it should query a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS });
  const results = await collection.query({ queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], nResults: 2 });
  expect(results).toBeDefined();
  expect(results).toBeInstanceOf(Object);
  expect(["test1", "test2"]).toEqual(expect.arrayContaining(results.ids[0]));
  expect(["test3"]).not.toEqual(expect.arrayContaining(results.ids[0]));
});

// test where_document
test("it should get embedding with matching documents", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS, documents: DOCUMENTS });

  const results = await collection.query({
    queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    nResults: 3,
    whereDocument: { $contains: "This is a test" }
  });

  // it should only return doc1
  expect(results).toBeDefined();
  expect(results).toBeInstanceOf(Object);
  expect(results.ids.length).toBe(1);
  expect(["test1"]).toEqual(expect.arrayContaining(results.ids[0]));
  expect(["test2"]).not.toEqual(expect.arrayContaining(results.ids[0]));
  expect(["This is a test"]).toEqual(
    expect.arrayContaining(results.documents[0])
  );

  const results2 = await collection.query({
    queryEmbeddings: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    nResults: 3,
    whereDocument: { $contains: "This is a test" },
    include: [IncludeEnum.Embeddings]
  });

  // expect(results2.embeddings[0][0]).toBeInstanceOf(Array);
  expect(results2.embeddings![0].length).toBe(1);
  expect(results2.embeddings![0][0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
});

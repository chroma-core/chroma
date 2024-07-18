import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { InvalidCollectionError } from "../src/Errors";

test("it should upsert embeddings to a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  const ids = ["test1", "test2"];
  const embeddings = [
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
  ];
  await chroma.addRecords(collection, { ids, embeddings });
  const count = await chroma.countRecords(collection);
  expect(count).toBe(2);

  const ids2 = ["test2", "test3"];
  const embeddings2 = [
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 15],
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  ];

  await chroma.upsertRecords(collection, {
    ids: ids2,
    embeddings: embeddings2,
  });

  const count2 = await chroma.countRecords(collection);
  expect(count2).toBe(3);
});

test("should error on non existing collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.deleteCollection({ name: "test" });
  expect(async () => {
    await chroma.upsertRecords(collection, {
      ids: ["test1"],
      embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
      metadatas: [{ test: "meta1" }],
      documents: ["doc1"],
    });
  }).rejects.toThrow(InvalidCollectionError);
});

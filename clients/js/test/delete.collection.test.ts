import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { EMBEDDINGS, IDS, METADATAS } from "./data";
import { InvalidCollectionError } from "../src/Errors";

test("it should delete documents from a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.addDocuments(collection, {
    ids: IDS,
    embeddings: EMBEDDINGS,
    metadatas: METADATAS,
  });
  let count = await chroma.countDocuments(collection);
  expect(count).toBe(3);
  await chroma.deleteDocuments(collection, {
    where: { test: "test1" },
  });
  count = await chroma.countDocuments(collection);
  expect(count).toBe(2);

  const remainingEmbeddings = await chroma.getDocuments(collection);
  expect(remainingEmbeddings?.ids).toEqual(
    expect.arrayContaining(["test2", "test3"]),
  );
});

test("should error on non existing collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.deleteCollection({ name: "test" });
  expect(async () => {
    await chroma.deleteDocuments(collection, { where: { test: "test1" } });
  }).rejects.toThrow(InvalidCollectionError);
});

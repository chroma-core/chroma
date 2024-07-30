import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { IDS, EMBEDDINGS } from "./data";
import { InvalidCollectionError } from "../src/Errors";

test("it should peek a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.addRecords(collection, { ids: IDS, embeddings: EMBEDDINGS });
  const results = await chroma.peekRecords(collection, { limit: 2 });
  expect(results).toBeDefined();
  expect(typeof results).toBe("object");
  expect(results.ids.length).toBe(2);
  expect(["test1", "test2"]).toEqual(expect.arrayContaining(results.ids));
});

test("should error on non existing collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await chroma.deleteCollection({ name: "test" });
  expect(async () => {
    await chroma.peekRecords(collection);
  }).rejects.toThrow(InvalidCollectionError);
});

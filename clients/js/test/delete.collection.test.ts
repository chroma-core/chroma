import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { EMBEDDINGS, IDS, METADATAS } from "./data";

test("it should delete a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });
  let count = await collection.count();
  expect(count).toBe(3);
  var resp = await collection.delete({ where: { test: "test1" } });
  count = await collection.count();
  expect(count).toBe(2);

  var remainingEmbeddings = await collection.get();
  expect(["test2", "test3"]).toEqual(
    expect.arrayContaining(remainingEmbeddings.ids)
  );
});

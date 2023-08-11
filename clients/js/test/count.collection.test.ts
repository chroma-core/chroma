import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { EMBEDDINGS, IDS, METADATAS } from "./data";

test("it should return number of embeddings in a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });
  let count = await collection.count();
  expect(count).toBe(3);

});

test("test gt, lt, in a simple small way", async () => {
    await chroma.reset();
    const collection = await chroma.createCollection({ name: "test" });
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });
    const items = await collection.count({ where: { float_value: { $gt: -1.4 } } });
    expect(items).toBe(2)
    const items2 = await collection.count({ where: { float_value: { $lt: -1.4 } } });
    expect(items2).toBe(1)
  });

test("simple test of count with where_document", async () => {
    await chroma.reset();
    const collection = await chroma.createCollection({ name: "test" });
    await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS });
    const items = await collection.count({whereDocument: { $contains: "test"} } )
    expect(items).toBe(3)
})
  
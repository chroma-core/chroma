import { expect, test } from '@jest/globals';
import chroma from './initClient'

test("verify that server is running and functional within runner", async () => {
  const collection = await chroma.createCollection({ name: "test" });
  const preliminaryCount = await collection.count();
  expect(preliminaryCount).toBe(0);
  await collection.add({
    ids: ["id1"],
    metadatas: [{"chapter": "3", "verse": "16"}], 
    documents: ["lorem ipsum..."],
    embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]] 
  })
  const getAll = await collection.get()
  console.log("getAll:\n", getAll)
  const count = await collection.count();
  expect(count).toBe(1);
});
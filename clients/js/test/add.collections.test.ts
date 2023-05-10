import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { DOCUMENTS, EMBEDDINGS, IDS } from './data';
import { METADATAS } from './data';
import { IncludeEnum } from "../src/types";

test("it should add single embeddings to a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection("test");
  const id = "test1";
  const embedding = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const metadata = { test: "test" };
  await collection.add(id, embedding, metadata);
  const count = await collection.count();
  expect(count).toBe(1);
  var res = await collection.get([id], undefined, undefined, undefined, [
    IncludeEnum.Embeddings,
  ]);
  expect(res.embeddings[0]).toEqual(embedding);
});

test("it should add batch embeddings to a collection", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection("test");
  await collection.add(IDS, EMBEDDINGS);
  const count = await collection.count();
  expect(count).toBe(3);
  var res = await collection.get(IDS, undefined, undefined, undefined, [
    IncludeEnum.Embeddings,
  ]);
  expect(res.embeddings).toEqual(EMBEDDINGS); // reverse because of the order of the ids
});

test("add documents", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection("test");
  await collection.add(IDS, EMBEDDINGS, undefined, DOCUMENTS);
  const results = await collection.get(["test1"]);
  expect(results.documents[0]).toBe("This is a test");
});

test('it should return an error when inserting an ID that alreay exists in the Collection', async () => {
  await chroma.reset()
  const collection = await chroma.createCollection('test')
  await collection.add(IDS, EMBEDDINGS, METADATAS)
  const results = await collection.add(IDS, EMBEDDINGS, METADATAS);
  expect(results.error).toBeDefined()
  expect(results.error).toContain("IDAlreadyExists")
})

test('It should return an error when inserting duplicate IDs in the same batch', async () => {
  await chroma.reset()
  const collection = await chroma.createCollection('test')
  const ids = IDS.concat(["test1"])
  const embeddings = EMBEDDINGS.concat([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
  const metadatas = METADATAS.concat([{ test: 'test1', 'float_value': 0.1 }])
  try {
    await collection.add(ids, embeddings, metadatas);
  } catch (e: any) {
    expect(e.message).toMatch('duplicates')
  }
})

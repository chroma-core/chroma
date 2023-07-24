import { expect, test } from "@jest/globals";
import chroma from "./initClient";
import { IncludeEnum } from "../src/types";
import { IDS, DOCUMENTS, EMBEDDINGS, METADATAS } from "./data";

test("it should get embedding with matching documents", async () => {
  await chroma.reset();
  const collection = await chroma.createCollection({ name: "test" });
  await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS, documents: DOCUMENTS });

  const results = await collection.get({
    ids: ["test1"],
    include: [
      IncludeEnum.Embeddings,
      IncludeEnum.Metadatas,
      IncludeEnum.Documents,
    ]
  });
  expect(results).toBeDefined();
  expect(results).toBeInstanceOf(Object);
  expect(results.embeddings![0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

  await collection.update({
    ids: ["test1"],
    embeddings: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
    metadatas: [{ test: "test1new" }],
    documents: ["doc1new"]
  });

  const results2 = await collection.get({
    ids: ["test1"],
    include: [
      IncludeEnum.Embeddings,
      IncludeEnum.Metadatas,
      IncludeEnum.Documents,
    ]
  });
  expect(results2).toBeDefined();
  expect(results2).toBeInstanceOf(Object);
  expect(results2.embeddings![0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 11]);
  expect(results2.metadatas[0]).toEqual({ test: "test1new", float_value: -2 });
  expect(results2.documents[0]).toEqual("doc1new");
});

// this currently fails
// test("it should update metadata or documents to array of Nones", async () => {
//   await chroma.reset();
//   const collection = await chroma.createCollection({ name: "test" });
//   await collection.add({ ids: IDS, embeddings: EMBEDDINGS, metadatas: METADATAS, documents: DOCUMENTS });

//   await collection.update({
//     ids: ["test1"],
//     metadatas: [undefined],
//   });

//   const results3 = await collection.get({
//     ids: ["test1"],
//     include: [
//       IncludeEnum.Embeddings,
//       IncludeEnum.Metadatas,
//       IncludeEnum.Documents,
//     ]
//   });
//   expect(results3).toBeDefined();
//   expect(results3).toBeInstanceOf(Object);
//   expect(results3.metadatas[0]).toEqual({});
// });

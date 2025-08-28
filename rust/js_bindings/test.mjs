import { Bindings } from "./index.js"

const bindings = await Bindings.create(true, 2000, "./test_data");

const collection = await bindings.createCollection("test")
console.log("Created collection:", collection)

await bindings.add(
  ["foo", "bar"],
  collection.id,
  [
    new Float32Array([0.1, 0.2, 0.3]),
    new Float32Array([0.4, 0.5, 0.6])
  ],
  ["doc1", "doc2"],
  collection.tenant,
  collection.database,
)

const queryResult = await bindings.query(
  collection.id,
  null,
  [new Float32Array([0.1, 0.2, 0.3])],
  2,
  null,
  null,
  ["embeddings", "documents", "metadatas", "distances"],
  collection.tenant,
  collection.database,
)
console.log("Query result:", queryResult)

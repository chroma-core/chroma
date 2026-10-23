const IDS = ["test1", "test2", "test3"];
const EMBEDDINGS = [
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
];
const METADATAS = [
  { test: "test1", float_value: -2 },
  { test: "test2", float_value: 0 },
  { test: "test3", float_value: 2 },
];
const DOCUMENTS = [
  "This is a test",
  "This is another test",
  "This is a third test",
];

export { IDS, EMBEDDINGS, METADATAS, DOCUMENTS };

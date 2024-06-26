var fs = require("fs");
var path = require("path");

var express = require("express");
var chroma = require("chromadb");

var app = express();
app.get("/", async (req, res) => {
  const cc = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await cc.reset();

  const google = new chroma.GoogleGenerativeAiEmbeddingFunction({
    googleApiKey: "<APIKEY>",
  });

  const collection = await cc.collection({
    name: "test-from-js",
    embeddingFunction: google,
  });

  await cc.addDocuments(collection, [
    { id: "doc1", contents: "doc1" },
    { id: "doc2", contents: "doc2" },
  ]);

  let count = await cc.countDocuments(collection);
  console.log("count", count);

  const googleQuery = new chroma.GoogleGenerativeAiEmbeddingFunction({
    googleApiKey: "<APIKEY>",
    taskType: "RETRIEVAL_QUERY",
  });

  const queryCollection = await cc.collection({
    name: "test-from-js",
    embeddingFunction: googleQuery,
  });

  const query = await cc.queryDocuments(queryCollection, {
    query: "doc1",
    nResults: 1,
  });
  console.log("query", query);

  console.log("COMPLETED");

  const collections = await cc.listCollections();
  console.log("collections", collections);

  res.send("Hello World!");
});
app.listen(3000, function () {
  console.log("Example app listening on port 3000!");
});

var fs = require("fs");
var path = require("path");

var express = require("express");
var chroma = require("chromadb");

var app = express();
app.get("/", async (req, res) => {
  const cc = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await cc.reset();

  // If you have a Google API key, you can use the GoogleGenerativeAiEmbeddingFunction
  // and replace the default embedding function with this one.
  // const google = new chroma.GoogleGenerativeAiEmbeddingFunction({
  //   googleApiKey: "<APIKEY>",
  // });

  const collection = await cc.createCollection({
    name: "test-from-js",
    embeddingFunction: new chroma.DefaultEmbeddingFunction(),
  });

  await collection.add({
    ids: ["doc1", "doc2"],
    documents: ["doc1", "doc2"],
  });

  let count = await collection.count();
  console.log("count", count);

  // const googleQuery = new chroma.GoogleGenerativeAiEmbeddingFunction({
  //   googleApiKey: "<APIKEY>",
  //   taskType: "RETRIEVAL_QUERY",
  // });

  const queryCollection = await collection.get({
    name: "test-from-js",
    embeddingFunction: new chroma.DefaultEmbeddingFunction(),
  });

  const query = await collection.query({
    queryTexts: "doc1",
    nResults: 1,
  });
  console.log("query", query);

  const collections = await cc.listCollections();
  console.log("collections", collections);

  console.log("SUCCESS!");

  res.send(query);
});
app.listen(3000, function () {
  console.log("Example app listening on port 3000!");
});

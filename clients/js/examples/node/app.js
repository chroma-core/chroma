var fs = require("fs");
var path = require("path");

// var pathToClient = path.join(__dirname, '..', '..', 'dist', 'main', 'index');

var express = require("express");
// var chroma = require(pathToClient);
var chroma = require("chromadb");
var openai = require("openai");

var app = express();
app.get("/", async (req, res) => {
  const cc = new chroma.ChromaClient("http://localhost:8000");
  await cc.reset();

  // const openAIembedder = new chroma.OpenAIEmbeddingFunction("key")
  const cohereAIEmbedder = new chroma.CohereEmbeddingFunction("key");

  const collection = await cc.createCollection(
    "test-from-js",
    undefined,
    cohereAIEmbedder
  );

  await collection.add(["doc1", "doc2"], undefined, undefined, [
    "doc1",
    "doc2",
  ]);

  let count = await collection.count();
  console.log("count", count);

  const query = await collection.query(undefined, 1, undefined, "doc1");
  console.log("query", query);

  console.log("COMPLETED");

  // const collections = await cc.listCollections();
  // console.log('collections', collections)

  // res.send('Hello World!');
});
app.listen(3000, function () {
  console.log("Example app listening on port 3000!");
});

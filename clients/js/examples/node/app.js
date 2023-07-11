var fs = require("fs");
var path = require("path");

var express = require("express");
var chroma = require("chromadb");
var openai = require("openai");

var app = express();
app.get("/", async (req, res) => {
  const cc = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await cc.reset();

  const openAIembedder = new chroma.OpenAIEmbeddingFunction("key")
  const cohereAIEmbedder = new chroma.OpenAIEmbeddingFunction({ openai_api_key: "API_KEY" });

  const collection = await cc.createCollection({
    name: "test-from-js",
    embeddingFunction: cohereAIEmbedder,
  });

  await collection.add({
    ids: ["doc1", "doc2"],
    documents: [
      "doc1",
      "doc2",
    ]
  });

  let count = await collection.count();
  console.log("count", count);

  const query = await collection.query({
    queryTexts: ["doc1"],
    nResults: 1
  });
  console.log("query", query);

  console.log("COMPLETED");

  const collections = await cc.listCollections();
  console.log('collections', collections)

  res.send('Hello World!');
});
app.listen(3000, function () {
  console.log("Example app listening on port 3000!");
});

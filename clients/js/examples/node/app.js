var express = require("express");
var chroma = require("chromadb");
var dotenv = require("dotenv");

dotenv.config();

if (!process.env.GOOGLE_API_KEY) {
  console.error(
    "GOOGLE_API_KEY is not set. Please create a .env file and set it there."
  );
  process.exit(0);
}

var app = express();
app.get("/", async (req, res) => {
  const cc = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await cc.reset();

  const google = new chroma.GoogleGenerativeAiEmbeddingFunction({
    googleApiKey: process.env.GOOGLE_API_KEY,
  });

  const collection = await cc.createCollection({
    name: "test-from-js",
    embeddingFunction: google,
  });

  await collection.add({
    ids: ["doc1", "doc2"],
    documents: ["doc1", "doc2"],
  });

  let count = await collection.count();
  console.log("count", count);

  const googleQuery = new chroma.GoogleGenerativeAiEmbeddingFunction({
    googleApiKey: "<APIKEY>",
    taskType: "RETRIEVAL_QUERY",
  });

  const queryCollection = await cc.getCollection({
    name: "test-from-js",
    embeddingFunction: googleQuery,
  });

  const query = await collection.query({
    queryTexts: ["doc1"],
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

var express = require('express');
var chroma = require('chromadb');

var app = express();
app.get('/', async (req, res) => {
  const cc = chroma.chromaClient("http://localhost:8000");
  await cc.reset()

  const collection = await cc.createCollection("test-from-js");
  let count = await collection.count();
  console.log('count', count)

  const collections = await cc.listCollections();
  console.log('collections', collections)

  res.send('Hello World!');
});
app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});


var express = require('express');

// import chromaClient from ../chromaClient.ts
var chromaClient = require('chromadb');


var app = express();
app.get('/', function (req, res) {
  res.send('Hello World!');
  let collections = chromaClient.listCollections();
  console.log(collections);
});
app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});
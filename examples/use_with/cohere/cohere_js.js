/*

## Cohere

First run Chroma

```
git clone git@github.com:chroma-core/chroma.git
cd chroma
docker-compose up -d --build
```

Then install chroma and cohere
```
npm install chromadb
npm install cohere-ai
```

Then set your API KEY

### Basic Example

*/

// import chroma
const chroma = require("chromadb");
const cohere = require("cohere-ai");

const main = async () => {

  const client = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await client.reset();

  const cohereAIEmbedder = new chroma.CohereEmbeddingFunction({ cohere_api_key: "APIKEY" });

  const collection = await client.createCollection({
    name: "cohere_js",
    embeddingFunction: cohereAIEmbedder
  });

  console.log(await cohereAIEmbedder.generate(["I like apples", "I like bananas", "I like oranges"]))

  // await collection.add({
  //   ids: ["1", "2", "3"],
  //   documents: ["I like apples", "I like bananas", "I like oranges"],
  //   metadatas: [{ "fruit": "apple" }, { "fruit": "banana" }, { "fruit": "orange" }],
  // });

  // console.log(await collection.query({
  //   queryTexts: ["citrus"],
  //   nResults: 1
  // }));

}

main();

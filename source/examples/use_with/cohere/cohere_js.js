/*

## Cohere

First run Chroma

```
git clone git@github.com:chroma-core/chroma.git
cd chroma
chroma run --path /chroma_db_path
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

  const COHERE_API_KEY = "COHERE_API_KEY";

  const client = new chroma.ChromaClient({ path: "http://localhost:8000" });
  await client.reset();

  const cohereAIEmbedder = new chroma.CohereEmbeddingFunction({ cohere_api_key: COHERE_API_KEY });

  const collection = await client.createCollection({
    name: "cohere_js",
    embeddingFunction: cohereAIEmbedder
  });

  await collection.add({
    ids: ["1", "2", "3"],
    documents: ["I like apples", "I like bananas", "I like oranges"],
    metadatas: [{ "fruit": "apple" }, { "fruit": "banana" }, { "fruit": "orange" }],
  });

  console.log(await collection.query({
    queryTexts: ["citrus"],
    nResults: 1
  }));

  // Multilingual Example

  const cohereAIMulitlingualEmbedder = new chroma.CohereEmbeddingFunction({ cohere_api_key: COHERE_API_KEY, model: "multilingual-22-12" });

  const collection_multilingual = await client.createCollection({
    name: "cohere_js_multilingual",
    embeddingFunction: cohereAIMulitlingualEmbedder
  });

  // # 나는 오렌지를 좋아한다 is "I like oranges" in Korean
  multilingual_texts = ['Hello from Cohere!', 'مرحبًا من كوهير!',
    'Hallo von Cohere!', 'Bonjour de Cohere!',
    '¡Hola desde Cohere!', 'Olá do Cohere!',
    'Ciao da Cohere!', '您好，来自 Cohere！',
    'कोहेरे से नमस्ते!', '나는 오렌지를 좋아한다']

  let ids = Array.from({ length: multilingual_texts.length }, (_, i) => String(i));

  await collection.add({
    ids: ids,
    documents: multilingual_texts
  })

  console.log(await collection.query({ queryTexts: ["citrus"], nResults: 1 }))

}

main();

---
title: '🧬 Embeddings'
---

Embeddings are the A.I-native way to represent any kind of data, making them the perfect fit for working with all kinds of A.I-powered tools and algorithms. They can represent text, images, and soon audio and video. There are many options for creating embeddings, whether locally using an installed library, or by calling an API.

Chroma provides lightweight wrappers around popular embedding providers, making it easy to use them in your apps. You can set an embedding function when you create a Chroma collection, which will be used automatically, or you can call them directly yourself.

{% special_table %}
{% /special_table %}

|              | Python | JS |
|--------------|-----------|---------------|
| [OpenAI](/integrations/openai) | ✅  | ✅ |
| [Google Generative AI](/integrations/google-gemini) | ✅  | ✅ |
| [Cohere](/integrations/cohere) | ✅  | ✅ |
| [Hugging Face](/integrations/hugging-face) | ✅  | ➖ |
| [Instructor](/integrations/instructor) | ✅  | ➖ |
| [Hugging Face Embedding Server](/integrations/hugging-face-server) | ✅  | ✅ |
| [Jina AI](/integrations/jinaai) | ✅  | ✅ |
| [FastEmbed](/integrations/fastembed) | ✅ | ➖ |

We welcome pull requests to add new Embedding Functions to the community.

***

## Default: all-MiniLM-L6-v2

By default, Chroma uses the [Sentence Transformers](https://www.sbert.net/) `all-MiniLM-L6-v2` model to create embeddings. This embedding model can create sentence and document embeddings that can be used for a wide variety of tasks. This embedding function runs locally on your machine, and may require you download the model files (this will happen automatically).

```python
from chromadb.utils import embedding_functions
default_ef = embedding_functions.DefaultEmbeddingFunction()
```

{% note type="default" %}
Embedding functions can be linked to a collection and used whenever you call `add`, `update`, `upsert` or `query`. You can also use them directly which can be handy for debugging.
```py
val = default_ef(["foo"])
```
-> [[0.05035809800028801, 0.0626462921500206, -0.061827320605516434...]]
{% /note %}


<!--
## Transformers.js

Chroma can use [Transformers.js](https://github.com/xenova/transformers.js) to create embeddings locally on the machine. Transformers uses the 'Xenova/all-MiniLM-L6-v2' model. Make sure you have installed Transformers.js by running ```npm install @xenova/transformers``` from the commandline.

```javascript
const {ChromaClient} = require('chromadb');
const client = new ChromaClient({path: "http://localhost:8000"});
const {TransformersEmbeddingFunction} = require('chromadb');
const embedder = new TransformersEmbeddingFunction();

(async () => {
    // create the collection called name
    const collection = await client.getOrCreateCollection({name: "name", embeddingFunction: embedder})

    // add documents to the collection
    await collection.add({
        ids: ["id1", "id2", "id3"],
        metadatas: [{"chapter": "3", "verse": "16"}, {"chapter": "3", "verse": "5"}, {"chapter": "29", "verse": "11"}],
        documents: ["lorem ipsum...", "doc2", "doc3"],
    })

    // query the collection
    const results = await collection.query({
        nResults: 2,
        queryTexts: ["lorem ipsum"]
    })
})();

``` -->

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

## Sentence Transformers

Chroma can also use any [Sentence Transformers](https://www.sbert.net/) model to create embeddings.

```python
sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
```

You can pass in an optional `model_name` argument, which lets you choose which Sentence Transformers model to use. By default, Chroma uses `all-MiniLM-L6-v2`. You can see a list of all available models [here](https://www.sbert.net/docs/pretrained_models.html).

{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}


***


## Custom Embedding Functions

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

You can create your own embedding function to use with Chroma, it just needs to implement the `EmbeddingFunction` protocol.

```python
from chromadb import Documents, EmbeddingFunction, Embeddings

class MyEmbeddingFunction(EmbeddingFunction):
    def __call__(self, input: Documents) -> Embeddings:
        # embed the documents somehow
        return embeddings
```

We welcome contributions! If you create an embedding function that you think would be useful to others, please consider [submitting a pull request](https://github.com/chroma-core/chroma) to add it to Chroma's `embedding_functions` module.


{% /tab %}
{% tab label="Javascript" %}

You can create your own embedding function to use with Chroma, it just needs to implement the `EmbeddingFunction` protocol. The `.generate` method in a class is strictly all you need.

```javascript
class MyEmbeddingFunction {
  private api_key: string;

  constructor(api_key: string) {
    this.api_key = api_key;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    // do things to turn texts into embeddings with an api_key perhaps
    return embeddings;
  }
}
```

{% /tab %}
{% /tabs %}

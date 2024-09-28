---
id: google-gemini
name: "Google Gemini"
---

# Google Gemini

{% tabs group="code-lang" hideContent=true %}
{% tab label="Python" %}
{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}

Chroma provides a convenient wrapper around Google's Generative AI embedding API. This embedding function runs remotely on Google's servers, and requires an API key.

You can get an API key by signing up for an account at [Google MakerSuite](https://makersuite.google.com/).

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

This embedding function relies on the `google-generativeai` python package, which you can install with `pip install google-generativeai`.

```python
# import
import chromadb.utils.embedding_functions as embedding_functions

# use directly
google_ef  = embedding_functions.GoogleGenerativeAiEmbeddingFunction(api_key="YOUR_API_KEY")
google_ef(["document1","document2"])

# pass documents to query for .add and .query
collection = client.create_collection(name="name", embedding_function=google_ef)
collection = client.get_collection(name="name", embedding_function=google_ef)
```

You can view a more [complete example](https://github.com/chroma-core/chroma/tree/main/examples/gemini) chatting over documents with Gemini embedding and langauge models.

For more info - please visit the [official Google python docs](https://ai.google.dev/tutorials/python_quickstart).

{% /tab %}
{% tab label="Javascript" %}

This embedding function relies on the `@google/generative-ai` npm package, which you can install with e.g. `npm install @google/generative-ai`.

```javascript
import { ChromaClient, GoogleGenerativeAiEmbeddingFunction } from "chromadb";
const embedder = new GoogleGenerativeAiEmbeddingFunction({
  googleApiKey: "<YOUR API KEY>",
});

// use directly
const embeddings = await embedder.generate(["document1", "document2"]);

// pass documents to query for .add and .query
const collection = await client.createCollection({
  name: "name",
  embeddingFunction: embedder,
});
const collectionGet = await client.getCollection({
  name: "name",
  embeddingFunction: embedder,
});
```

You can view a more [complete example using Node](https://github.com/chroma-core/chroma/blob/main/clients/js/examples/node/app.js).

For more info - please visit the [official Google JS docs](https://ai.google.dev/tutorials/node_quickstart).

{% /tab %}
{% /tabs %}

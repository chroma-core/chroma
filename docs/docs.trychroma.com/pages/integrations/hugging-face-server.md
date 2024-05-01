---
title: 'Hugging Face Text Embedding Server'
---

{% tabs group="code-lang" hideContent=true %}
{% tab label="Python" %}
{% /tab %}
{% tab label="Javascript" %}
{% /tab %}
{% /tabs %}

Chroma provides a convenient wrapper for HuggingFace Text Embedding Server, a standalone server that provides text embeddings via a REST API. You can read more about it [**here**](https://github.com/huggingface/text-embeddings-inference).

## Setting Up The Server

To run the embedding server locally you can run the following command from the root of the Chroma repository. The docker compose command will run Chroma and the embedding server together.

```bash
docker compose -f examples/server_side_embeddings/huggingface/docker-compose.yml up -d
```

or

```bash
docker run -p 8001:80 -d -rm --name huggingface-embedding-server ghcr.io/huggingface/text-embeddings-inference:cpu-0.3.0 --model-id BAAI/bge-small-en-v1.5 --revision -main
```

{% note type="note" %}
The above docker command will run the server with the `BAAI/bge-small-en-v1.5` model. You can find more information about running the server in docker [**here**](https://github.com/huggingface/text-embeddings-inference#docker).
{% /note %}

## Usage

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

This embedding function relies on the `requests` python package, which you can install with `pip install requests`.

```python
from chromadb.utils.embedding_functions import HuggingFaceEmbeddingServer
huggingface_ef = HuggingFaceEmbeddingServer(url="http://localhost:8001/embed")
```

The embedding model is configured on the server side. Check the docker-compose file in `examples/server_side_embeddings/huggingface/docker-compose.yml` for an example of how to configure the server.

{% /tab %}
{% tab label="Javascript" %}


```javascript
import  {HuggingFaceEmbeddingServerFunction} from 'chromadb';
const embedder = new HuggingFaceEmbeddingServerFunction({url:"http://localhost:8001/embed"})

// use directly
const embeddings = embedder.generate(["document1","document2"])

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collection = await client.getCollection({name: "name", embeddingFunction: embedder})
```

{% /tab %}
{% /tabs %}

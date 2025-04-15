---
id: 'cohere'
name: 'Cohere'
---

# Cohere

Chroma also provides a convenient wrapper around Cohere's embedding API. This embedding function runs remotely on Cohere’s servers, and requires an API key. You can get an API key by signing up for an account at [Cohere](https://dashboard.cohere.ai/welcome/register).

{% Tabs %}
{% Tab label="python" %}

This embedding function relies on the `cohere` python package, which you can install with `pip install cohere`.

```python
import chromadb.utils.embedding_functions as embedding_functions
cohere_ef  = embedding_functions.CohereEmbeddingFunction(api_key="YOUR_API_KEY",  model_name="large")
cohere_ef(input=["document1","document2"])
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { CohereEmbeddingFunction } from 'chromadb';

const embedder = new CohereEmbeddingFunction("apiKey")

// use directly
const embeddings = embedder.generate(["document1","document2"])

// pass documents to query for .add and .query
const collection = await client.createCollection({name: "name", embeddingFunction: embedder})
const collectionGet = await client.getCollection({name:"name", embeddingFunction: embedder})
```

{% /Tab %}

{% /Tabs %}

You can pass in an optional `model_name` argument, which lets you choose which Cohere embeddings model to use. By default, Chroma uses `large` model. You can see the available models under `Get embeddings` section [here](https://docs.cohere.ai/reference/embed).

### Multilingual model example

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
cohere_ef  = embedding_functions.CohereEmbeddingFunction(
        api_key="YOUR_API_KEY",
        model_name="multilingual-22-12")

multilingual_texts  = [ 'Hello from Cohere!', 'مرحبًا من كوهير!',
        'Hallo von Cohere!', 'Bonjour de Cohere!',
        '¡Hola desde Cohere!', 'Olá do Cohere!',
        'Ciao da Cohere!', '您好，来自 Cohere！',
        'कोहिअर से नमस्ते!'  ]

cohere_ef(input=multilingual_texts)

```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { CohereEmbeddingFunction } from 'chromadb';

const embedder = new CohereEmbeddingFunction("apiKey")

multilingual_texts  = [ 'Hello from Cohere!', 'مرحبًا من كوهير!',
        'Hallo von Cohere!', 'Bonjour de Cohere!',
        '¡Hola desde Cohere!', 'Olá do Cohere!',
        'Ciao da Cohere!', '您好，来自 Cohere！',
        'कोहिअर से नमस्ते!'  ]

const embeddings = embedder.generate(multilingual_texts)

```

{% /Tab %}

{% /TabbedCodeBlock %}

For more information on multilingual model you can read [here](https://docs.cohere.ai/docs/multilingual-language-models).


### Multimodal model example

{% tabs group="code-lang" hideTabs=true %}
{% Tab label="python" %}

```python

import os
from datasets import load_dataset, Image


dataset = load_dataset(path="detection-datasets/coco", split="train", streaming=True)

IMAGE_FOLDER = "images"
N_IMAGES = 5

# Write the images to a folder
dataset_iter = iter(dataset)
os.makedirs(IMAGE_FOLDER, exist_ok=True)
for i in range(N_IMAGES):
    image = next(dataset_iter)['image']
    image.save(f"images/{i}.jpg")


multimodal_cohere_ef = CohereEmbeddingFunction(
    model_name="embed-english-v3.0",
    api_key="YOUR_API_KEY",
)
image_loader = ImageLoader()

multimodal_collection = client.create_collection(
    name="multimodal",
    embedding_function=multimodal_cohere_ef,
    data_loader=image_loader)

image_uris = sorted([os.path.join(IMAGE_FOLDER, image_name) for image_name in os.listdir(IMAGE_FOLDER)])
ids = [str(i) for i in range(len(image_uris))]
for i in range(len(image_uris)):
    # max images per add is 1, see cohere docs https://docs.cohere.com/v2/reference/embed#request.body.images
    multimodal_collection.add(ids=[str(i)], uris=[image_uris[i]])

retrieved = multimodal_collection.query(query_texts=["animals"], include=['data'], n_results=3)

```

{% /Tab %}
{% /tabs %}

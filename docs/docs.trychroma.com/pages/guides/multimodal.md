---
title: "🖼️ Multimodal"
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

---

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Chroma supports multimodal collections, i.e. collections which can store, and can be queried by, multiple modalities of data.

Try it out in Colab: [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://githubtocolab.com/chroma-core/chroma/blob/main/examples/multimodal/multimodal_retrieval.ipynb)

## Multi-modal Embedding Functions

Chroma supports multi-modal embedding functions, which can be used to embed data from multiple modalities into a single embedding space.

Chroma has the OpenCLIP embedding function built in, which supports both text and images.

```python
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
embedding_function = OpenCLIPEmbeddingFunction()
```

## Data Loaders

Chroma supports data loaders, for storing and querying with data stored outside Chroma itself, via URI. Chroma will not store this data, but will instead store the URI, and load the data from the URI when needed.

Chroma has an data loader for loading images from a filesystem built in.

```python
from chromadb.utils.data_loaders import ImageLoader
data_loader = ImageLoader()
```

## Multi-modal Collections

You can create a multi-modal collection by passing in a multi-modal embedding function. In order to load data from a URI, you must also pass in a data loader.

```python
import chromadb

client = chromadb.Client()

collection = client.create_collection(
    name='multimodal_collection',
    embedding_function=embedding_function,
    data_loader=data_loader)

```

### Adding data

You can add data to a multi-modal collection by specifying the data modality. For now, images are supported:

```python
collection.add(
    ids=['id1', 'id2', 'id3'],
    images=[...] # A list of numpy arrays representing images
)
```

Note that Chroma will not store the data for you, and you will have to maintain a mapping from IDs to data yourself.

However, you can use Chroma in combination with data stored elsewhere, by adding it via URI. Note that this requires that you have specified a data loader when creating the collection.

```python
collection.add(
    ids=['id1', 'id2', 'id3'],
    uris=[...] #  A list of strings representing URIs to data
)
```

Since the embedding function is multi-modal, you can also add text to the same collection:

```python
collection.add(
    ids=['id4', 'id5', 'id6'],
    texts=["This is a document", "This is another document", "This is a third document"]
)
```

### Querying

You can query a multi-modal collection with any of the modalities that it supports. For example, you can query with images:

```python
results = collection.query(
    query_images=[...] # A list of numpy arrays representing images
)
```

Or with text:

```python
results = collection.query(
    query_texts=["This is a query document", "This is another query document"]
)
```

If a data loader is set for the collection, you can also query with URIs which reference data stored elsewhere of the supported modalities:

```python
results = collection.query(
    query_uris=[...] # A list of strings representing URIs to data
)
```

Additionally, if a data loader is set for the collection, and URIs are available, you can include the data in the results:

```python
results = collection.query(
    query_images=[...], # # list of numpy arrays representing images
    includes=['data']
)
```

This will automatically call the data loader for any available URIs, and include the data in the results. `uris` are also available as an `includes` field.

### Updating

You can update a multi-modal collection by specifying the data modality, in the same way as `add`. For now, images are supported:

```python
collection.update(
    ids=['id1', 'id2', 'id3'],
    images=[...] # A list of numpy arrays representing images
)
```

Note that a given entry with a specific ID can only have one associated modality at a time. Updates will over-write the existing modality, so for example, an entry which originally has corresponding text and updated with an image, will no longer have that text after an update with images.

{% /tab %}
{% tab label="Javascript" %}

Support for multi-modal retrieval for Chroma's JavaScript client is coming soon!

{% /tab %}

{% /tabs %}

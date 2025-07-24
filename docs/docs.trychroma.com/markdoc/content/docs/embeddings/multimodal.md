# Multimodal

{% Banner type="note" %}
Multimodal support is currently available only in Python. Javascript/Typescript support coming soon! 
{% /Banner %}

You can create multimodal Chroma collections; these are collections which can store, and can be queried by, multiple modalities of data.

[Try it out in Colab](https://githubtocolab.com/chroma-core/chroma/blob/main/examples/multimodal/multimodal_retrieval.ipynb)

## Multi-modal Embedding Functions

Chroma supports multi-modal embedding functions, which can be used to embed data from multiple modalities into a single embedding space.

Chroma ships with the OpenCLIP embedding function built in, which supports both text and images.

```python
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
embedding_function = OpenCLIPEmbeddingFunction()
```

## Adding Multimodal Data and Data Loaders

You can add embedded data of modalities different from text directly to Chroma. For now images are supported:

```python
collection.add(
    ids=['id1', 'id2', 'id3'],
    images=[[1.0, 1.1, 2.1, ...], ...] # A list of numpy arrays representing images
)
```

Unlike with text documents, which are stored in Chroma, we will not store your original images, or data of other modalities. Instead, for each of your multimodal records you can specify a URI where the original format is stored, and a **data loader**. For each URI you add, Chroma will use the data loader to retrieve the original data, embed it, and store the embedding.

For example, Chroma ships with a data loader, `ImageLoader`, for loading images from a local filesystem. We can create a collection set up with the `ImageLoader`:

```python
import chromadb
from chromadb.utils.data_loaders import ImageLoader
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction

client = chromadb.Client()

data_loader = ImageLoader()
embedding_function = OpenCLIPEmbeddingFunction()

collection = client.create_collection(
    name='multimodal_collection',
    embedding_function=embedding_function,
    data_loader=data_loader
)
```

Now, we can use the `.add` method to add records to this collection. The collection's data loader will grab the images using the URIs, embed them using the `OpenCLIPEmbeddingFunction`, and store the embeddings in Chroma.

```python
collection.add(
    ids=["id1", "id2"],
    uris=["path/to/file/1", "path/to/file/2"]
)
```

If the embedding function you use is multi-modal (like `OpenCLIPEmbeddingFunction`), you can also add text to the same collection:

```python
collection.add(
    ids=["id3", "id4"],
    documents=["This is a document", "This is another document"]
)
```

## Querying

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
    include=['data']
)
```

This will automatically call the data loader for any available URIs, and include the data in the results. `uris` are also available as an `include` field.

## Updating

You can update a multi-modal collection by specifying the data modality, in the same way as `add`. For now, images are supported:

```python
collection.update(
    ids=['id1', 'id2', 'id3'],
    images=[...] # A list of numpy arrays representing images
)
```

Note that a given entry with a specific ID can only have one associated modality at a time. Updates will over-write the existing modality, so for example, an entry which originally has corresponding text and updated with an image, will no longer have that text after an update with images.


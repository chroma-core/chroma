# CIP-07302024: ID Generators

## Status

Current Status: `Under Discussion`

## Motivation

While leaving ID generation to users is a sensible approach, Chroma does not make a conscious effort to help or guide
users on how to generate IDs and what are the implications of choosing different ID generation strategies. A few sources
of ID generations:

- Langchain🦜🔗 - https://github.com/langchain-ai/langchain/blob/b7bbfc7c67f2c05d1a980ef4b600388cbe037efe/libs/partners/chroma/langchain_chroma/vectorstores.py#L385-L386
- LlamaIndex - https://github.com/run-llama/llama_index/blob/9a6ac56b3f88e3b3d6e03e6e3c49fb6b29041c19/llama-index-integrations/vector_stores/llama-index-vector-stores-chroma/llama_index/vector_stores/chroma/base.py#L292
- https://cookbook.chromadb.dev/core/document-ids/ (web analytics also show that this is in the top 10 resources user
  visit)
- https://github.com/amikos-tech/chromadbx/?tab=readme-ov-file#id-generation

The latter sources are helpful, but incomplete sources for a cohesive approach to ID generation.

ID generators can be useful for several reasons:

- DX - allowing developers to either choose from existing implementations or create their own
- General system extensibility - allowing for more complex ID generation strategies
- Sensible defaults - providing a default ID generation strategy that aligns with good practices and rest of the ecosystem.

## Public Interfaces

We propose the introduction of the following publicly facing interfaces:

- IDGenerator protocol
- A new setting to configure the class of the ID generator

The IDGenerator protocol will be akin to the EmbeddingFunction in such that it can be easily extended by users to
accommodate their specific needs. The initial implementation will also ship with a default implementation of UUID ID
generator.

The new setting `id_generator_impl`, which is the FQN or the instance of the Python class that implements the
IDGenerator protocol. It defaults to the UUID ID generator, but allows users to override it with their own or
Chroma-provided ID generators with client settings.

## Proposed Changes

### IDGenerator

ID Generators, ideally, are dumb classes that generate IDs given a sequence of documents and/or metadatas, but they
don’t always have to be, which is why we suggest allowing for stateful generators to be passed to Settings (see below):

Why is it beneficial to base the ID generation on the documents and/or metadata:

- Documents and/or metadatas can be used to form IDs via means of text transforms.
- Documents and/or metadatas can be used as sources of entropy for generating IDs

> **Note**: Some generators (e.g. UUIDs) may not use either docs or metadatas at all.

```python
from typing import Generator, Optional, TypeVar
from typing_extensions import Protocol, runtime_checkable
from chromadb.api.types import ID, OneOrMany, Metadata,Embeddable

D = TypeVar("D", bound=Embeddable, contravariant=True)

@runtime_checkable
class IDGenerator(Protocol[D]):
    def generator(self, docs: Optional[OneOrMany[Embeddable]] = None,
                  metadatas: Optional[OneOrMany[Metadata]] = None) -> Generator[ID, None, None]:
        ...

```

> **Note**: We can also make the protocol accept embeddings, but the above is a good start that provides plenty of
> extensibility to users.

Sample UUID Generation:

```python
class UUIDGenerator(IDGenerator[Embeddable]):
    def __init__(self):
        ...

    def generator(self, docs: Optional[OneOrMany[Embeddable]] = None,
                  metadatas: Optional[OneOrMany[Metadata]] = None) -> Generator[ID, None, None]:
        if docs:
            for _ in docs:
                yield f"{uuid.uuid4()}"
        elif metadatas:
            for _ in metadatas:
                yield f"{uuid.uuid4()}"
        else:
            while True:
                yield f"{uuid.uuid4()}"
```

Example Code change to `Collection.add` :

```python
def add(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        embeddings: Optional[  # type: ignore[type-arg]
            Union[
                OneOrMany[Embedding],
                OneOrMany[np.ndarray],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
) -> None:
    """Add embeddings to the data store.
    Args:
        ids: The ids of the embeddings you wish to add
        embeddings: The embeddings to add. If None, embeddings will be computed based on the documents or images using the embedding_function set for the Collection. Optional.
        metadatas: The metadata to associate with the embeddings. When querying, you can filter on this metadata. Optional.
        documents: The documents to associate with the embeddings. Optional.
        images: The images to associate with the embeddings. Optional.
        uris: The uris of the images to associate with the embeddings. Optional.

    Returns:
        None

    Raises:
        ValueError: If you don't provide either embeddings or documents
        ValueError: If the length of ids, embeddings, metadatas, or documents don't match
        ValueError: If you don't provide an embedding function and don't provide embeddings
        ValueError: If you provide both embeddings and documents
        ValueError: If you provide an id that already exists

    """
    if ids is None:
        _gen = get_id_generator_instance(self._client.get_settings().id_generator_impl)
        generator = _gen.generator(documents=documents, metadatas=metadatas)
        ids = [next(generator) for _ in range(len(documents))]
    ...
```

## **Compatibility, Deprecation, and Migration Plan**

This change does not introduce any breaking changes. The change is client-side only therefore will not cause any
cross-version client/server compatibility issues.

## **Test Plan**

The following tests will be added:

- Verify backward compatibility with provided ID (with the new setting)
- Verify shipped ID generators work when IDs are not supplied
- Verify custom ID generator can be configured
- Verify default behaviour

## **Rejected Alternatives**

TBD

## Future Work

- Store the ID generator as a configuration on a collection?
- JS Implementation (once refactor is done)

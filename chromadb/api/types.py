from typing import Union, Sequence, Dict, TypedDict, Protocol


ID = str
IDs = list[ID]

Embedding = Sequence[float]
# TODO: Use generic one or many type
Embeddings = Union[Sequence[Embedding], Embedding]

Metadata = Dict[str, str]
Metadatas = Union[Metadata, Sequence[Metadata]]

Where = Dict[str, str]
Document = str
Documents = Union[Document, Sequence[Document]]


class GetResult(TypedDict):
    ids: IDs
    embeddings: list[Embedding]
    documents: list[Document]
    metadatas: list[Metadata]


class QueryResult(TypedDict):
    ids: list[IDs]
    embeddings: list[list[Embedding]]
    documents: list[list[Document]]
    metadatas: list[list[Metadata]]
    distances: list[list[float]]


class EmbeddingFunction(Protocol):
    def __call__(self, texts: Documents) -> Embeddings:
        ...
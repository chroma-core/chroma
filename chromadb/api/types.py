from typing import Union, Dict, Sequence, TypedDict, Protocol, TypeVar

ID = str
IDs = list[ID]

Embedding = list[float]
Embeddings = list[Embedding]

Metadata = Dict[str, str]
Metadatas = list[Metadata]

Document = str
Documents = list[Document]

Parameter = TypeVar("Parameter", Embedding, Document, Metadata, ID)
T = TypeVar("T")
OneOrMany = Union[T, list[T]]

Where = Dict[str, str]


class GetResult(TypedDict):
    ids: list[ID]
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


def maybe_cast_one_to_many(
    target: OneOrMany[Parameter],
) -> list[Parameter]:
    """Infers if target is Embedding, Metadata, or Document and casts it to a many object if its one"""

    if isinstance(target, Sequence):
        # One Document or ID
        if isinstance(target, str) and target != None:
            return [target]  # type: ignore
        # One Embedding
        if isinstance(target[0], float):
            return [target]  # type: ignore
    # One Metadata dict
    if isinstance(target, dict):
        return [target]
    # Already a sequence
    return target  # type: ignore

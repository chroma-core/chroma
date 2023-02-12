from typing import Union, Dict, Sequence, TypedDict, Protocol, TypeVar, List

ID = str
IDs = List[ID]

Embedding = List[float]
Embeddings = List[Embedding]

Metadata = Dict[str, str]
Metadatas = List[Metadata]

Document = str
Documents = List[Document]

Parameter = TypeVar("Parameter", Embedding, Document, Metadata, ID)
T = TypeVar("T")
OneOrMany = Union[T, List[T]]

Where = Dict[str, str]


class GetResult(TypedDict):
    ids: List[ID]
    embeddings: List[Embedding]
    documents: List[Document]
    metadatas: List[Metadata]


class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: List[List[Embedding]]
    documents: List[List[Document]]
    metadatas: List[List[Metadata]]
    distances: List[List[float]]


class EmbeddingFunction(Protocol):
    def __call__(self, texts: Documents) -> Embeddings:
        ...


def maybe_cast_one_to_many(
    target: OneOrMany[Parameter],
) -> List[Parameter]:
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

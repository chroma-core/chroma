from typing import Union, Sequence, Dict, TypedDict

ID = str
IDs = Sequence[ID]

Embedding = Sequence[float]
Embeddings = Union[Sequence[Embedding], Embedding]

Metadata = Dict[str, str]
Metadatas = Union[Metadata, Sequence[Metadata]]

Where = Dict[str, str]
Documents = Union[str, Sequence[str]]

Item = Dict


class NearestNeighborsResult(TypedDict):
    items: list[Item]
    distances: Sequence[float]

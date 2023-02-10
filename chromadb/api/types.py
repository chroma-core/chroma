from typing import Union, Sequence, Dict

ID = str
IDs = list[ID]

Embedding = Sequence[float]
Embeddings = Union[Sequence[Embedding], Embedding]

Metadata = Dict[str, str]
Metadatas = Union[Metadata, Sequence[Metadata]]

Where = Dict[str, str]
Documents = Union[str, Sequence[str]]

Item = Dict

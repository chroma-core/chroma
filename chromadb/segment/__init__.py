from enum import Enum, auto
from typing import TypedDict, Union, Optional, Sequence, Protocol


class SequentialID(Protocol):
    def serialize(self) -> bytes: ...
    def __eq__(self, other) -> bool: ...
    def __lt__(self, other) -> bool: ...
    def __gt__(self, other) -> bool: ...
    def __le__(self, other) -> bool: ...
    def __ge__(self, other) -> bool: ...
    def __ne__(self, other) -> bool: ...


class InsertType(Enum):
    ADD_ONLY = auto()
    UPDATE_ONLY = auto()
    ADD_OR_UPDATE = auto()


Vector = Union[Sequence[float], Sequence[int]]

class EmbeddingRecord(TypedDict):
    id: str
    sequence: SequentialID
    embedding: Optional[Vector]
    metadata: Optional[dict[str, str]]



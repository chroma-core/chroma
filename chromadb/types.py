from typing import TypedDict, Optional, Literal, Protocol, Union, Sequence
from uuid import UUID
from enum import Enum


class ScalarEncoding(Enum):
    FLOAT32 = "FLOAT32"
    INT32 = "INT32"


class EmbeddingFunction(TypedDict):
    name: str
    dimension: int
    scalar_encoding: ScalarEncoding


class Topic(TypedDict):
    name: str
    embedding_function: Optional[str]
    metadata: Optional[dict[str, Union[str, int, float]]]


class Segment(TypedDict):
    id: UUID
    type: str
    scope: Literal["vector", "metadata"]
    topic: Optional[str]
    metadata: Optional[dict[str, Union[str, int, float]]]


class SeqId(Protocol):
    def serialize(self) -> bytes:
        ...

    def __eq__(self, other) -> bool:
        ...

    def __lt__(self, other) -> bool:
        ...

    def __gt__(self, other) -> bool:
        ...

    def __le__(self, other) -> bool:
        ...

    def __ge__(self, other) -> bool:
        ...

    def __ne__(self, other) -> bool:
        ...


class InsertType(Enum):
    ADD_ONLY = "ADD_ONLY"
    UPDATE_ONLY = "UPDATE_ONLY"
    ADD_OR_UPDATE = "ADD_OR_UPDATE"


Vector = Union[Sequence[float], Sequence[int]]


class BaseEmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId


class VectorEmbeddingRecord(BaseEmbeddingRecord):
    embedding: Vector


class MetadataEmbeddingRecord(BaseEmbeddingRecord):
    metadata: dict[str, Union[str, int, float]]


class EmbeddingRecord(VectorEmbeddingRecord, MetadataEmbeddingRecord):
    pass


class InsertEmbeddingRecord(TypedDict):
    id: str
    embedding: Vector
    metadata: Optional[dict[str, Union[str, int, float]]]
    insert_type: InsertType


class VectorQuery(TypedDict):
    """A KNN/ANN query"""

    vector: Vector
    k: int
    allowed_ids: Optional[Sequence[str]]
    options: Optional[dict[str, Union[str, int, float]]]


class VectorQueryResult(VectorEmbeddingRecord):
    """A KNN/ANN query result"""

    distance: float


# Metadata Query Grammar
LiteralValue = Union[str, int, float]
LogicalOperator = Literal["$and", "$or"]
WhereOperator = Literal["$gt", "$gte", "$lt", "$lte", "$ne", "$eq"]
OperatorExpression = dict[Union[WhereOperator, LogicalOperator], LiteralValue]

Where = dict[Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, list["Where"]]]

WhereDocumentOperator = Literal["$contains", LogicalOperator]
WhereDocument = dict[WhereDocumentOperator, Union[str, list["WhereDocument"]]]

from typing import TypeVar, Optional, Union, Sequence, Dict, Any
from typing_extensions import Literal, Protocol, TypedDict
from uuid import UUID
from enum import Enum

Metadata = Dict[str, Union[str, int, float]]

# Namespaced Names are mechanically just strings, but we use this type to indicate that
# the intent is for the value to be globally unique and semantically meaningful.
NamespacedName = str


class ScalarEncoding(Enum):
    FLOAT32 = "FLOAT32"
    INT32 = "INT32"


class SegmentScope(Enum):
    VECTOR = "VECTOR"
    METADATA = "METADATA"


# Note: This is the data model for identifying and describing an embedding function,
# not the actual function implementation.
class EmbeddingFunction(TypedDict):
    name: NamespacedName
    dimension: int
    scalar_encoding: ScalarEncoding


class Collection(TypedDict):
    id: UUID
    name: str
    topic: str
    metadata: Optional[Metadata]


class Segment(TypedDict):
    id: UUID
    type: NamespacedName
    scope: SegmentScope
    # If a segment has a topic, it implies that this segment is a consumer of the topic
    # and indexes the contents of the topic.
    topic: Optional[str]
    # If a segment has a collection, it implies that this segment implements the full
    # collection and can be used to service queries (for it's given scope.)
    collection: Optional[UUID]
    metadata: Optional[Metadata]


S = TypeVar("S", bound="SeqId")


class SeqId(Protocol):
    def __eq__(self, other: Any) -> bool:
        ...

    def __lt__(self: S, other: S) -> bool:
        ...


class InsertType(Enum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


Vector = Union[Sequence[float], Sequence[int]]


class VectorEmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    embedding: Vector


class MetadataEmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    metadata: Dict[str, Metadata]


class EmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    embedding: Vector
    metadata: Optional[Metadata]


class InsertEmbeddingRecord(TypedDict):
    id: str
    embedding: Vector
    metadata: Optional[Metadata]
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
LogicalOperator = Union[Literal["$and"], Literal["$or"]]
WhereOperator = Union[
    Literal["$gt"],
    Literal["$gte"],
    Literal["$lt"],
    Literal["$lte"],
    Literal["$ne"],
    Literal["$eq"],
]
OperatorExpression = Dict[Union[WhereOperator, LogicalOperator], LiteralValue]

Where = dict[
    Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, list["Where"]]
]

WhereDocumentOperator = Union[Literal["$contains"], LogicalOperator]
WhereDocument = dict[WhereDocumentOperator, Union[str, list["WhereDocument"]]]

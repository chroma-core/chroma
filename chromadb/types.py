from typing import Optional, Union, Sequence, Any, Mapping
from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum

Metadata = Mapping[str, Union[str, int, float]]

# Namespaced Names are mechanically just strings, but we use this type to indicate that
# the intent is for the value to be globally unique and semantically meaningful.
NamespacedName = str


class ScalarEncoding(Enum):
    FLOAT32 = "FLOAT32"
    INT32 = "INT32"


class SegmentScope(Enum):
    VECTOR = "VECTOR"
    METADATA = "METADATA"


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


# The desire here is for SeqID to be any type that can be compared to other values of
# the same type to establish a linear order.

# This is surprisingly difficult to express in Python. ints, for example, do not
# "support" __eq__ and __lt__ so using a protocol won't work.
SeqId = Any


class InsertType(Enum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


Vector = Union[Sequence[float], Sequence[int]]


class VectorEmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    embedding: Vector
    encoding: ScalarEncoding


class MetadataEmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    metadata: Optional[Metadata]


class EmbeddingRecord(TypedDict):
    id: str
    seq_id: SeqId
    embedding: Vector
    encoding: ScalarEncoding
    metadata: Optional[Metadata]


class InsertEmbeddingRecord(TypedDict):
    id: str
    embedding: Vector
    encoding: ScalarEncoding
    metadata: Optional[Metadata]
    insert_type: InsertType


class DeleteEmbeddingRecord(TypedDict):
    delete_id: str


class EmbeddingDeleteRecord(TypedDict):
    delete_id: str
    seq_id: SeqId


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

Where = dict[
    Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, list["Where"]]
]

WhereDocumentOperator = Literal["$contains", LogicalOperator]
WhereDocument = dict[WhereDocumentOperator, Union[str, list["WhereDocument"]]]


class Unspecified:
    """A sentinel value used to indicate that a value should not be updated"""

    _instance: Optional["Unspecified"] = None

    def __new__(cls) -> "Unspecified":
        if cls._instance is None:
            cls._instance = super(Unspecified, cls).__new__(cls)

        return cls._instance


T = TypeVar("T")
OptionalArgument = Union[T, Unspecified]

UpdateMetadata = Mapping[str, Union[int, float, str, None]]

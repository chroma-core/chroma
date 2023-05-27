from typing import Optional, Union, Sequence, Mapping
from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum

Metadata = Mapping[str, Union[str, int, float]]
UpdateMetadata = Mapping[str, Union[int, float, str, None]]

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


# SeqID can be one of three types of value in our current and future plans:
# 1. A Pulsar MessageID encoded as a 192-bit integer
# 2. A Pulsar MessageIndex (a 64-bit integer)
# 3. A SQL RowID (a 64-bit integer)

# All three of these types can be expressed as a Python int, so that is the type we
# use in the internal Python API. However, care should be taken that the larger 192-bit
# values are stored correctly when persisting to DBs.
SeqId = int


class Operation(Enum):
    ADD = "ADD"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"
    DELETE = "DELETE"


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
    embedding: Optional[Vector]
    encoding: Optional[ScalarEncoding]
    metadata: Optional[UpdateMetadata]
    operation: Operation


class SubmitEmbeddingRecord(TypedDict):
    id: str
    embedding: Optional[Vector]
    encoding: Optional[ScalarEncoding]
    metadata: Optional[UpdateMetadata]
    operation: Operation


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

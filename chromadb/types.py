from typing import Optional, Union, Sequence, Dict, Mapping, List

from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum


Metadata = Mapping[str, Union[str, int, float, bool]]
UpdateMetadata = Mapping[str, Union[int, float, str, bool, None]]

# Namespaced Names are mechanically just strings, but we use this type to indicate that
# the intent is for the value to be globally unique and semantically meaningful.
NamespacedName = str


class ScalarEncoding(Enum):
    FLOAT32 = "FLOAT32"
    INT32 = "INT32"


class SegmentScope(Enum):
    VECTOR = "VECTOR"
    METADATA = "METADATA"
    RECORD = "RECORD"


class Collection(TypedDict):
    id: UUID
    name: str
    metadata: Optional[Metadata]
    dimension: Optional[int]
    tenant: str
    database: str
    # The version is only used in the distributed version of chroma
    # in single-node chroma, this field is always 0
    version: int


class Database(TypedDict):
    id: UUID
    name: str
    tenant: str


class Tenant(TypedDict):
    name: str


class Segment(TypedDict):
    id: UUID
    type: NamespacedName
    scope: SegmentScope
    # If a segment has a collection, it implies that this segment implements the full
    # collection and can be used to service queries (for it's given scope.)
    collection: Optional[UUID]
    metadata: Optional[Metadata]


# SeqID can be one of three types of value in our current and future plans:
# 1. A Pulsar MessageID encoded as a 192-bit integer - This is no longer used as we removed pulsar
# 2. A Pulsar MessageIndex (a 64-bit integer) -  This is no longer used as we removed pulsar
# 3. A SQL RowID (a 64-bit integer) - This is used by both sqlite and the new log-service

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
    embedding: Vector


class MetadataEmbeddingRecord(TypedDict):
    id: str
    metadata: Optional[Metadata]


class OperationRecord(TypedDict):
    id: str
    embedding: Optional[Vector]
    encoding: Optional[ScalarEncoding]
    metadata: Optional[UpdateMetadata]
    operation: Operation


class LogRecord(TypedDict):
    log_offset: int
    record: OperationRecord


class VectorQuery(TypedDict):
    """A KNN/ANN query"""

    vectors: Sequence[Vector]
    k: int
    allowed_ids: Optional[Sequence[str]]
    include_embeddings: bool
    options: Optional[Dict[str, Union[str, int, float, bool]]]


class VectorQueryResult(TypedDict):
    """A KNN/ANN query result"""

    id: str
    distance: float
    embedding: Optional[Vector]


# Metadata Query Grammar
LiteralValue = Union[str, int, float, bool]
LogicalOperator = Union[Literal["$and"], Literal["$or"]]
WhereOperator = Union[
    Literal["$gt"],
    Literal["$gte"],
    Literal["$lt"],
    Literal["$lte"],
    Literal["$ne"],
    Literal["$eq"],
    Literal["$like"],
    Literal["$nlike"]
]

InclusionExclusionOperator = Union[Literal["$in"], Literal["$nin"]]
OperatorExpression = Union[
    Dict[Union[WhereOperator, LogicalOperator], LiteralValue],
    Dict[InclusionExclusionOperator, List[LiteralValue]]
]

Where = Dict[
    Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, List["Where"]]
]

WhereDocumentOperator = Union[
    Literal["$contains"], Literal["$not_contains"], LogicalOperator
]
WhereDocument = Dict[WhereDocumentOperator, Union[str, List["WhereDocument"]]]


class Unspecified:
    """A sentinel value used to indicate that a value should not be updated"""

    _instance: Optional["Unspecified"] = None

    def __new__(cls) -> "Unspecified":
        if cls._instance is None:
            cls._instance = super(Unspecified, cls).__new__(cls)

        return cls._instance


T = TypeVar("T")
OptionalArgument = Union[T, Unspecified]

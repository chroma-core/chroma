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


class Collection(TypedDict):
    id: UUID
    name: str
    topic: str
    metadata: Optional[Metadata]
    dimension: Optional[int]
    tenant: str
    database: str


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
    # The collection the operation is being performed on
    # This is optional because in the single node version,
    # topics are 1:1 with collections. So consumers of the ingest queue
    # implicitly know this mapping. However, in the multi-node version,
    # topics are shared between collections, so we need to explicitly
    # specify the collection.
    # For backwards compatability reasons, we can't make this a required field on
    # single node, since data written with older versions of the code won't be able to
    # populate it.
    collection_id: Optional[UUID]


class SubmitEmbeddingRecord(TypedDict):
    id: str
    embedding: Optional[Vector]
    encoding: Optional[ScalarEncoding]
    metadata: Optional[UpdateMetadata]
    operation: Operation
    collection_id: UUID  # The collection the operation is being performed on


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
    seq_id: SeqId
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
]
InclusionExclusionOperator = Union[Literal["$in"], Literal["$nin"]]
OperatorExpression = Union[
    Dict[Union[WhereOperator, LogicalOperator], LiteralValue],
    Dict[InclusionExclusionOperator, List[LiteralValue]],
]

Where = Dict[
    Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, List["Where"]]
]

WhereDocumentOperator = Union[Literal["$contains"], LogicalOperator]
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

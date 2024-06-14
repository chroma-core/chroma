from abc import ABC, abstractmethod
from typing import Any, Optional, Union, Sequence, Dict, Mapping, List, Generic

from typing_extensions import Self

from overrides import override
from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum
from pydantic import BaseModel

from chromadb.api.configuration import (
    CollectionConfiguration,
    Configuration,
)
from chromadb.serde import BaseModelJSONSerializable


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


C = TypeVar("C", bound=Configuration)


class Configurable(Generic[C], ABC):
    """A mixin that allows a class to be configured with a configuration object"""

    @abstractmethod
    def get_configuration(self) -> C:
        raise NotImplementedError()

    @abstractmethod
    def set_configuration(self, configuration: C) -> None:
        raise NotImplementedError()


class Collection(
    BaseModel,
    Configurable[CollectionConfiguration],
    BaseModelJSONSerializable["Collection"],
):
    """A model of a collection used for transport, serialization, and storage"""

    id: UUID
    name: str
    configuration_json: Dict[str, Any]
    metadata: Optional[Metadata]
    dimension: Optional[int]
    tenant: str
    database: str
    # The version is only used in the distributed version of chroma
    # in single-node chroma, this field is always 0
    version: int

    def __init__(
        self,
        id: UUID,
        name: str,
        configuration: CollectionConfiguration,
        metadata: Optional[Metadata],
        dimension: Optional[int],
        tenant: str,
        database: str,
        version: int = 0,
    ):
        super().__init__(
            id=id,
            name=name,
            metadata=metadata,
            configuration_json=configuration.to_json(),
            dimension=dimension,
            tenant=tenant,
            database=database,
            version=version,
        )

    def modify(
        self,
        name: Optional[str] = None,
        metadata: Optional[Metadata] = None,
        configuration: Optional[CollectionConfiguration] = None,
    ) -> None:
        """

        Modifes the collection and returns a new instance of the collection. This does not partially update the
        metadata or the configuration, but rather replaces them with the provided values.

        """
        if name is not None:
            self.name = name
        if configuration is not None:
            self.configuration_json = configuration.to_json()
        if metadata is not None:
            self.metadata = metadata

    # TODO: This throws away type information.
    def __getitem__(self, key: str) -> Optional[Any]:
        """Allows the collection to be treated as a dictionary"""
        if key == "configuration":
            return self.get_configuration()
        # For the other model attributes we allow the user to access them directly
        if key in self.model_fields:
            return getattr(self, key)
        return None

    # TODO: This doesn't check types.
    def __setitem__(self, key: str, value: Any) -> None:
        """Allows the collection to be treated as a dictionary"""
        # For the model attributes we allow the user to access them directly
        if key == "configuration":
            self.set_configuration(value)
        if key in self.model_fields:
            setattr(self, key, value)
        else:
            raise KeyError(f"No such key: {key}, valid keys are: {self.model_fields}")

    def __eq__(self, __value: object) -> bool:
        # Check that all the model fields are equal
        if not isinstance(__value, Collection):
            return False
        for field in self.model_fields:
            if getattr(self, field) != getattr(__value, field):
                return False
        return True

    def get_configuration(self) -> CollectionConfiguration:
        """Returns the configuration of the collection"""
        return CollectionConfiguration.from_json(self.configuration_json)

    def set_configuration(self, configuration: CollectionConfiguration) -> None:
        """Sets the configuration of the collection"""
        self.configuration_json = configuration.to_json()

    @classmethod
    @override
    def from_json(cls, json_map: Dict[str, Any]) -> Self:
        """Deserializes a Collection object from JSON"""

        # Get the CollectionConfiguration from the JSON map, and remove it from the map
        configuration = CollectionConfiguration.from_json(
            json_map.pop("configuration_json")
        )

        return cls(configuration=configuration, **json_map)


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
]
InclusionExclusionOperator = Union[Literal["$in"], Literal["$nin"]]
OperatorExpression = Union[
    Dict[Union[WhereOperator, LogicalOperator], LiteralValue],
    Dict[InclusionExclusionOperator, List[LiteralValue]],
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

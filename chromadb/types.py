from abc import ABC, abstractmethod
from typing import Any, Optional, Union, Sequence, Dict, Mapping, List, cast

from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum
from pydantic import BaseModel

from chromadb.api.configuration import CollectionConfiguration, Configuration


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


class JSONSerializable(ABC):
    """A mixin that allows a class to be serialized to JSON"""

    @abstractmethod
    def to_json(self) -> str:
        """Serializes the object to JSON"""
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_json(cls, json: Dict[str, Any]) -> "JSONSerializable":
        """Deserializes the object from JSON"""
        raise NotImplementedError()


class Configurable:
    """A mixin that allows a class to be configured with a configuration object"""

    _configuration: Optional[Configuration] = None

    @property
    def configuration(self) -> Optional[Configuration]:
        return self._configuration

    @configuration.setter
    def configuration(self, configuration: Configuration) -> None:
        self._configuration = configuration


CONFIGURATION_METADATA_PREFIX = "chroma:"


class Collection(BaseModel, Configurable, JSONSerializable):
    """A model of a collection used for transport, serialization, and storage"""

    id: UUID
    name: str
    topic: Optional[str]  # The SysDB service is responsible for populating this field
    metadata: Optional[Metadata]
    dimension: Optional[int]
    tenant: str
    database: str

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
        # Update the configuration, so that we can coalesce it into the metadata
        if configuration is not None:
            self.configuration = configuration
        if metadata is not None:
            if self._configuration is not None:
                metadata = Collection.populate_metadata_from_configuration(
                    metadata, cast(CollectionConfiguration, self._configuration)
                )
            self.metadata = metadata
            # TODO: do the update rules allow metadata to be None

    @staticmethod
    def with_configuration(
        configuration: CollectionConfiguration, **kwargs: Any
    ) -> "Collection":
        """Creates an instance of the class with a given configuration"""
        # Overrides the base configurable's from_configuration method to set the configuration by populating the
        # metadata field
        if "metadata" not in kwargs:
            raise ValueError(
                "A argument to metadata, even if none, must be provided when creating a collection with a configuration"
            )
        metadata = kwargs["metadata"]
        if metadata is None:
            metadata = {}
        Collection.populate_metadata_from_configuration(metadata, configuration)
        instance = Collection(**kwargs)
        instance.configuration = configuration
        return instance

    @staticmethod
    def populate_metadata_from_configuration(
        metadata: Metadata, configuration: CollectionConfiguration
    ) -> None:
        """Collections store their configuration in their metadata field in order to preserve flexibility
        and remove the need for schema changes when adding new configuration parameters, segment types etc.
        This method populates the metadata field with the configuration parameters, prefixed by a namespace
        """

        for parameter in configuration.get_parameters():
            metadata[  # type: ignore
                f"{CONFIGURATION_METADATA_PREFIX}{parameter.name}"
            ] = parameter.value

    @property
    def sanitized_metadata(self) -> Optional[Metadata]:
        """
        Returns a sanitized version of the metadata that does not include configuration parameters for user
        facing APIs
        """

        if self.metadata is None:
            return self.metadata

        sanitized_metadata = {}
        for key, value in self.metadata.items():
            if not key.startswith(CONFIGURATION_METADATA_PREFIX):
                sanitized_metadata[key] = value
        if len(sanitized_metadata) == 0:
            return None
        return sanitized_metadata

    def __getitem__(self, key: str) -> Optional[Any]:
        """Allows the collection to be treated as a dictionary"""
        # For the model attributes we allow the user to access them directly
        if key in self.model_fields:
            return getattr(self, key)
        return None

    def __setitem__(self, key: str, value: Any) -> None:
        """Allows the collection to be treated as a dictionary"""
        # For the model attributes we allow the user to access them directly
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

    def to_json(self) -> str:
        """Serializes the collection to JSON"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "Collection":
        """Deserializes the collection from JSON"""
        return cls(**json_dict)


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

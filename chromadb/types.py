from abc import ABC, abstractmethod
from typing import Optional, Union, Sequence, Dict, Mapping, List, Generic, Any, cast

from typing_extensions import Literal, TypedDict, TypeVar
from uuid import UUID
from enum import Enum
from pydantic import BaseModel

from chromadb.api.configuration import (
    Configuration,
    CollectionConfiguration,
    ConfigurationParameter,
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


C = TypeVar("C", bound="Configuration")


class Configurable(Generic[C], ABC):
    """A mixin that allows a class to be configured with a configuration object"""

    @abstractmethod
    def get_configuration(self) -> C:
        raise NotImplementedError()

    @abstractmethod
    def set_configuration(self, configuration: C) -> None:
        raise NotImplementedError()


CONFIGURATION_METADATA_PREFIX = "chroma:"


class Collection(
    BaseModel, Configurable[CollectionConfiguration], BaseModelJSONSerializable
):
    """A model of a collection used for transport, serialization, and storage"""

    id: UUID
    name: str
    topic: Optional[str]  # The SysDB service is responsible for populating this field
    metadata: Optional[Metadata]
    dimension: Optional[int]
    tenant: str
    database: str

    def __init__(
        self,
        id: UUID,
        name: str,
        topic: Optional[str],
        metadata: Optional[Metadata],
        dimension: Optional[int],
        tenant: str,
        database: str,
    ):
        super().__init__(
            id=id,
            name=name,
            topic=topic,
            metadata=metadata,
            dimension=dimension,
            tenant=tenant,
            database=database,
        )

    def modify(
        self,
        name: Optional[str] = None,
        metadata: Optional[Metadata] = None,
        configuration: Optional[CollectionConfiguration] = None,
    ) -> None:
        """
        Modifies the collection and returns a new instance of the collection. This does not partially update the
        metadata or the configuration, but rather replaces them with the provided values.
        """
        if name is not None:
            self.name = name
        # Update the configuration, so that we can coalesce it into the metadata
        if configuration is not None:
            self.configuration = configuration
        if metadata is not None:
            Collection._populate_metadata_from_configuration(
                metadata, cast(CollectionConfiguration, self.get_configuration())
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
        if kwargs["metadata"] is None:
            kwargs["metadata"] = {}
        Collection._populate_metadata_from_configuration(
            kwargs["metadata"], configuration
        )
        instance = Collection(**kwargs)
        return instance

    @staticmethod
    def _populate_metadata_from_configuration(
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

    @staticmethod
    def _extract_configuration_from_metadata(
        metadata: Metadata,
    ) -> CollectionConfiguration:
        """Extracts the configuration from the metadata field, if it exists"""
        configuration_parameters = []
        for key, value in metadata.items():
            if key.startswith(CONFIGURATION_METADATA_PREFIX):
                configuration_parameters.append(
                    ConfigurationParameter(
                        name=key[len(CONFIGURATION_METADATA_PREFIX) :], value=value
                    )
                )
        return CollectionConfiguration(configuration_parameters)

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

    def get_configuration(self) -> CollectionConfiguration:
        """Returns the configuration of the collection"""
        if self.metadata is None:
            return CollectionConfiguration()
        return Collection._extract_configuration_from_metadata(self.metadata)

    def set_configuration(self, configuration: Configuration) -> None:
        """Sets the configuration of the collection"""
        if self.metadata is None:
            self.metadata = {}
        Collection._populate_metadata_from_configuration(
            self.metadata, cast(CollectionConfiguration, configuration)
        )


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

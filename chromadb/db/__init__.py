from abc import ABC, abstractmethod
from typing import Optional, TypedDict, Union, Literal
from uuid import UUID
from collections.abc import Sequence
import numpy.typing as npt


StrDict = Optional[dict[str, str]]
NameOrID = Union[str, UUID]


class Collection(TypedDict):
    id: UUID
    name: str
    metadata: StrDict = None


class Segment(TypedDict):
    id: UUID
    python_class: str
    metadata: StrDict


class System(ABC):
    """Interface for Chroma's System storage backend"""


    @abstractmethod
    def create_collection(self,
                          name: str,
                          metadata: StrDict = None,
                          get_or_create: bool = False) -> Collection:
       """Create a new collection"""
       pass


    @abstractmethod
    def get_collection(self,
                       name_or_id: NameOrID) -> Collection:
        """Get a collection"""
        pass


    @abstractmethod
    def list_collections(self) -> Sequence[Collection]:
        """List all collections"""
        pass


    @abstractmethod
    def update_collection(self,
                          name_or_id: NameOrID,
                          new_name: Optional[str] = None,
                          new_metadata: StrDict = None) -> Collection:
        """Update a collection's name and/or metadata"""
        pass


    @abstractmethod
    def delete_collection(self,  name_or_id: NameOrID) -> None:
        """Delete a collection by name or id. Returns None on success."""
        pass


    # TODO: This method signature needs more thought put into it, and will likely
    # need to change or be split into multiple methods as the system grows.
    @abstractmethod
    def get_segment(self,
                    metadata: dict[str, str],
                    collection_id: Optional[UUID] = None,
                    create: bool = False) -> Segment:
        """Obtain a segment that can handle the given embedding
        metadata. Optionally, create it if it doesn't already exist."""
        pass


# Query Grammar
LiteralValue = Union[str, int, float]
LogicalOperator = Literal["$and", "$or"]
WhereOperator = Literal["$gt", "$gte", "$lt", "$lte", "$ne", "$eq"]
OperatorExpression = dict[Union[WhereOperator, LogicalOperator], LiteralValue]

Where = dict[Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, list["Where"]]]

WhereDocumentOperator = Literal["$contains", LogicalOperator]
WhereDocument = dict[WhereDocumentOperator, Union[str, list["WhereDocument"]]]


class EmbeddingMetadata(TypedDict):
    id: UUID
    sequence: int
    metadata: StrDict


class Metadata(ABC):
    """Interface for Chroma's Embedding StrDict storage backend."""

    @abstractmethod
    def append(self,
               collection_id: UUID,
               metadata: Sequence[EmbeddingMetadata]) -> None:
        """Add embedding metadata to a collection. If embeddings are
        already in the collection, they will be updated."""
        pass


    @abstractmethod
    def get(self,
            collection_id: UUID,
            where: Where = {},
            where_document: WhereDocument = {},
            max_sequence: int = -1,
            ids: Optional[Sequence[UUID]] = None,
            sort: Optional[str] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None) -> Sequence[EmbeddingMetadata]:
        """Get embedding metadata from a collection. Returns sequence
        of id, metadata pairs."""
        pass


    @abstractmethod
    def count(self, collection_id: UUID) -> int:
        """Get the number of embeddings in a collection."""
        pass


    @abstractmethod
    def delete(self, collection_id: UUID) -> None:
        """Soft-delete embeddings from a collection by ID."""
        pass


class DB(ABC):
    """Existing DB interface, retained for backwards compatibility"""


    # TODO: get rid of this! Ripe for sql injection attacks.
    @abstractmethod
    def raw_sql(self, raw_sql) -> Sequence:
        """Execute a SQL string and return the results"""
        pass

    # TODO: get rid of this! Shouldn't be necessary for clients to
    # worry about explicitly.
    @abstractmethod
    def persist(self):
        pass

    # TODO: get rid of this! Dropping the whole database should not be
    # available via the API.
    @abstractmethod
    def reset(self):
        pass


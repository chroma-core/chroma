from typing import Protocol, Optional, Iterable, Sequence, Literal, Any, List, Tuple, Dict
from abc import ABC, abstractmethod
from uuid import UUID
from collections.abc import Sequence
from enum import Enum
import pypika
from chromadb.types import Segment, Topic, EmbeddingFunction
from chromadb.api.types import Embeddings, Metadatas, Documents, IDs, Where, WhereDocument
import numpy.typing as npt
from overrides import EnforceOverrides


class Cursor(Protocol):
    def execute(self, sql: str, params: Optional[tuple] = None):
        ...

    def executemany(self, sql: str, params: Optional[Sequence] = None):
        ...

    def fetchone(self) -> tuple[Any]:
        ...

    def fetchall(self) -> Iterable[tuple]:
        ...


class TxWrapper(ABC, EnforceOverrides):
    """Wrapper class for DBAPI 2.0 Connection objects, with which clients can implement transactions.
    Makes two guarantees that basic DBAPI 2.0 connections do not:

    - __enter__ returns a Cursor object consistently (instead of a Connection like some do)
    - Always re-raises an exception if one was thrown from the body
    """

    @abstractmethod
    def __enter__(self) -> Cursor:
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass


class SqlDB(ABC, EnforceOverrides):
    """DBAPI 2.0 interface wrapper to ensure consistent behavior between implementations"""

    @abstractmethod
    def tx(self) -> TxWrapper:
        """Return a transaction wrapper"""
        pass

    @staticmethod
    @abstractmethod
    def querybuilder() -> type[pypika.Query]:
        """Return a PyPika Query class of an appropriate subtype for this database implementation"""
        pass

    @staticmethod
    @abstractmethod
    def parameter_format() -> str:
        """Return the appropriate parameter format for this database implementation.
        Will be called with str.format(i) where i is the numeric index of the parameter."""
        pass


class SysDB(ABC, EnforceOverrides):
    """Data interface for Chroma's System storage backend"""

    @abstractmethod
    def create_segment(self, segment: Segment) -> Segment:
        """Create a new segment."""
        pass

    @abstractmethod
    def get_segments(
        self,
        id: Optional[UUID] = None,
        scope: Optional[str] = None,
        topic: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> Sequence[Segment]:
        """Find segments by id, embedding function, and/or metadata"""
        pass

    @abstractmethod
    def get_topics(
        self,
        name: Optional[str] = None,
        embedding_function: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> Sequence[Topic]:
        """Get topics by name, embedding function or metadata"""
        pass

    @abstractmethod
    def create_topic(self, topic: Topic) -> None:
        """Create a new topic"""
        pass

    @abstractmethod
    def delete_topic(self, topic_name: str) -> None:
        """Delete a topic and all associated segments from the SysDB"""
        pass

    @abstractmethod
    def get_embedding_functions(self, name: Optional[str]) -> Sequence[EmbeddingFunction]:
        """Find embedding functions"""
        pass

    @abstractmethod
    def create_embedding_function(self, embedding_function: EmbeddingFunction) -> None:
        """Create a new embedding function"""
        pass

    @abstractmethod
    def reset(self):
        """Delete all tables and data. For testing only, implementations intended for production
        may throw an exception instead of implementing this method."""
        pass


class DB(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def create_collection(
        self, name: str, metadata: Optional[Dict] = None, get_or_create: bool = False
    ) -> Sequence:
        pass

    @abstractmethod
    def get_collection(self, name: str) -> Sequence:
        pass

    @abstractmethod
    def list_collections(self) -> Sequence:
        pass

    @abstractmethod
    def update_collection(
        self, current_name: str, new_name: Optional[str] = None, new_metadata: Optional[Dict] = None
    ):
        pass

    @abstractmethod
    def delete_collection(self, name: str):
        pass

    @abstractmethod
    def get_collection_uuid_from_name(self, collection_name: str) -> str:
        pass

    @abstractmethod
    def add(
        self,
        collection_uuid: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas],
        documents: Optional[Documents],
        ids: List[UUID],
    ) -> List[UUID]:
        pass

    @abstractmethod
    def add_incremental(self, collection_uuid: str, ids: List[UUID], embeddings: Embeddings):
        pass

    @abstractmethod
    def get(
        self,
        where: Where = {},
        collection_name: Optional[str] = None,
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: WhereDocument = {},
        columns: Optional[List[str]] = None,
    ) -> Sequence:
        pass

    @abstractmethod
    def update(
        self,
        collection_uuid: str,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ):
        pass

    @abstractmethod
    def count(self, collection_name: str):
        pass

    @abstractmethod
    def delete(
        self,
        where: Where = {},
        collection_uuid: Optional[str] = None,
        ids: Optional[IDs] = None,
        where_document: WhereDocument = {},
    ) -> List:
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def get_nearest_neighbors(
        self, collection_name, where, embeddings, n_results, where_document
    ) -> Tuple[List[List[UUID]], npt.NDArray]:
        pass

    @abstractmethod
    def get_by_ids(self, uuids, columns=None) -> Sequence:
        pass

    @abstractmethod
    def raw_sql(self, raw_sql):
        pass

    @abstractmethod
    def create_index(self, collection_uuid: str):
        pass

    @abstractmethod
    def persist(self):
        pass

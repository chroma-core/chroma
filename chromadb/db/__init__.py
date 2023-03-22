from typing import Protocol, Optional, Iterable, Sequence, Literal, Any
from abc import ABC, abstractmethod
from typing import Optional, TypedDict
from uuid import UUID
from collections.abc import Sequence
from enum import Enum
import pypika


class Segment(TypedDict):
    id: UUID
    type: str
    scope: Literal["vector", "metadata"]
    embedding_function: str
    metadata: Optional[dict[str, str]]


class Cursor(Protocol):
    def execute(self, sql: str, params: Optional[tuple] = None):
        ...

    def executemany(self, sql: str, params: Optional[Sequence] = None):
        ...

    def fetchone(self) -> tuple[Any]:
        ...

    def fetchall(self) -> Iterable[tuple]:
        ...


class TxWrapper(ABC):
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


class SqlDB(ABC):
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


class SysDB(ABC):
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
        embedding_function: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> Sequence[Segment]:
        """Find segments by id, embedding function, and/or metadata"""
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

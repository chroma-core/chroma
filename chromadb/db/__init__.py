from abc import ABC, abstractmethod
from typing import Optional, TypedDict, Union, Literal
from uuid import UUID
from collections.abc import Sequence
from enum import Enum, auto


StrDict = Optional[dict[str, str]]


class ScalarType(Enum):
    FLOAT64 = "float64"
    FLOAT32 = "float32"
    FLOAT16 = "float16"
    INT64 = "int64"
    INT32 = "int32"
    INT16 = "int16"
    INT8 = "int8"


class EmbeddingFunction(TypedDict):
    name: str
    dimension: int
    scalar_type: ScalarType
    metadata: StrDict


class Segment(TypedDict):
    id: UUID
    type: str
    embedding_function: EmbeddingFunction
    children: Sequence[UUID]
    metadata: StrDict


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
        embedding_function: Optional[str] = None,
        metadata: StrDict = None,
    ) -> Sequence[Segment]:
        """Find segments by id, embedding function, or metadata"""
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

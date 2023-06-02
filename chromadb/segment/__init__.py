from typing import Optional, Sequence, Set, TypeVar, Type
from abc import ABC, abstractmethod
from chromadb.types import (
    Collection,
    MetadataEmbeddingRecord,
    VectorEmbeddingRecord,
    Where,
    WhereDocument,
    VectorQuery,
    VectorQueryResult,
    Segment,
    SeqId,
)
from chromadb.config import Component, System
from overrides import EnforceOverrides
from uuid import UUID


class SegmentImplementation(ABC, EnforceOverrides):
    @abstractmethod
    def __init__(self, sytstem: System, segment: Segment):
        pass

    @abstractmethod
    def count(self) -> int:
        """Get the number of embeddings in this segment"""
        pass

    @abstractmethod
    def max_seqid(self) -> SeqId:
        """Get the maximum SeqID currently indexed by this segment"""
        pass


class MetadataReader(SegmentImplementation):
    """Embedding Metadata segment interface"""

    @abstractmethod
    def get_metadata(
        self,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        ids: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        """Query for embedding metadata."""
        pass


class VectorReader(SegmentImplementation):
    """Embedding Vector segment interface"""

    @abstractmethod
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        """Get embeddings from the segment. If no IDs are provided, all embeddings are
        returned."""
        pass

    @abstractmethod
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        """Given a vector query, return the top-k nearest neighbors for vector in the
        query."""
        pass


class SegmentManager(Component):
    """Interface for a pluggable strategy for creating, retrieving and instantiating
    segments as required"""

    @abstractmethod
    def create_segments(self, collection: Collection) -> Set[Segment]:
        """Create the segments required for a new collection."""
        pass

    @abstractmethod
    def delete_segments(self, collection_id: UUID) -> None:
        """Delete all the segments associated with a collection"""
        pass

    T = TypeVar("T", bound="SegmentImplementation")

    # Future Note: To support time travel, add optional parameters to this method to
    # retrieve Segment instances that are bounded to events from a specific range of
    # time
    @abstractmethod
    def get_segment(self, collection_id: UUID, type: Type[T]) -> SegmentImplementation:
        """Return the segment that should be used for servicing queries to a collection.
        Implementations should cache appropriately; clients are intended to call this
        method repeatedly rather than storing the result (thereby giving this
        implementation full control over which segment impls are in or out of memory at
        a given time.)"""
        pass

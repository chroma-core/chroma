from typing import Optional, Sequence, TypeVar, Type
from abc import abstractmethod
from chromadb.types import (
    Collection,
    MetadataEmbeddingRecord,
    Operation,
    VectorEmbeddingRecord,
    Where,
    WhereDocument,
    VectorQuery,
    VectorQueryResult,
    Segment,
    SeqId,
    Metadata,
)
from chromadb.config import Component, System
from uuid import UUID


class SegmentImplementation(Component):
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

    @staticmethod
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        """Given an arbitrary metadata map (e.g, from a collection), validate it and
        return metadata (if any) that is applicable and should be applied to the
        segment. Validation errors will be reported to the user."""
        return None

    @abstractmethod
    def delete(self) -> None:
        """Delete the segment and all its data"""
        ...


S = TypeVar("S", bound=SegmentImplementation)


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
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        """Return the segments required for a new collection. Returns only segment data,
        does not persist to the SysDB"""
        pass

    @abstractmethod
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        """Delete any local state for all the segments associated with a collection, and
        returns a sequence of their IDs. Does not update the SysDB."""
        pass

    # Future Note: To support time travel, add optional parameters to this method to
    # retrieve Segment instances that are bounded to events from a specific range of
    # time
    @abstractmethod
    def get_segment(self, collection_id: UUID, type: Type[S]) -> S:
        """Return the segment that should be used for servicing queries to a collection.
        Implementations should cache appropriately; clients are intended to call this
        method repeatedly rather than storing the result (thereby giving this
        implementation full control over which segment impls are in or out of memory at
        a given time.)"""
        pass

    @abstractmethod
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        """Signal to the segment manager that a collection is about to be used, so that
        it can preload segments as needed. This is only a hint, and implementations are
        free to ignore it."""
        pass

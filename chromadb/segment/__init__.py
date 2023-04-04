from typing import TypedDict, Optional, Sequence
from abc import ABC, abstractmethod
from chromadb.types import (
    Collection,
    Vector,
    EmbeddingRecord,
    MetadataEmbeddingRecord,
    VectorEmbeddingRecord,
    Where,
    WhereDocument,
    VectorQuery,
    VectorQueryResult,
    Segment,
)
from overrides import EnforceOverrides
from uuid import UUID


class SegmentImplementation(ABC, EnforceOverrides):
    pass


class MetadataReader(SegmentImplementation):
    """Embedding Metadata segment interface"""

    @abstractmethod
    def get_metadata(
        self,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        ids: Optional[Sequence[str]] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        """Query for embedding metadata."""
        pass

    @abstractmethod
    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        pass


class VectorReader(SegmentImplementation):
    """Embedding Vector segment interface"""

    @abstractmethod
    def get_vectors(self, ids: Optional[Sequence[str]]) -> Sequence[VectorEmbeddingRecord]:
        """Get embeddings from the segment. If no IDs are provided,
        all embeddings are returned."""
        pass

    @abstractmethod
    def query_vectors(
        self, queries: Sequence[VectorQuery]
    ) -> Sequence[Sequence[VectorQueryResult]]:
        """Given a list of vector queries, return the top-k nearest
        neighbors for each query."""
        pass


class SegmentManager(ABC, EnforceOverrides):
    """Interface for a pluggable strategy for creating, retrieving and instantiating segments as required"""

    @abstractmethod
    def create_collection(self, collection: Collection) -> None:
        """Create and initialize the components (topics and segments) required for a new collection"""
        pass

    @abstractmethod
    def delete_collection(self, id: UUID) -> None:
        """Delete all the components associated with a collection"""
        pass

    @abstractmethod
    def initialize_all(self) -> None:
        """Initialize all segments for which this instance is responsible"""
        pass

    @abstractmethod
    def get_instance(self, segment: Segment) -> SegmentImplementation:
        """Return an instance of the given segment, initializing if necessary"""
        pass

    @abstractmethod
    def reset(self):
        """Delete all segments. Should be used for testing only, implementations intended for production
        may throw an exception instead of implementing this method."""
        pass

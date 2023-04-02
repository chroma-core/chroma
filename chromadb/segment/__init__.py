from typing import TypedDict, Optional, Sequence
from abc import ABC, abstractmethod
from chromadb.types import (
    Topic,
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


class SegmentImplementation(ABC):
    pass


class MetadataReader(SegmentImplementation):
    """Embedding Metadata segment interface"""

    @abstractmethod
    def get_metadata(
        self,
        where: Where = {},
        where_document: WhereDocument = {},
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


class SegmentManager(ABC):
    """Interface for a pluggable strategy for creating, retrieving and instantiating segments as required"""

    @abstractmethod
    def create_topic_segments(self, topic: Topic) -> None:
        """Create and initialize the segments required for a new topic"""
        pass

    @abstractmethod
    def delete_topic_segments(self, name: str) -> None:
        """Delete all the segments associated with a collection"""
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

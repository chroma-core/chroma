from typing import TypedDict, Optional, Sequence
from abc import ABC, abstractmethod
from chromadb.types import (
    Vector,
    EmbeddingRecord,
    PersistentEmbeddingRecord,
    Where,
    WhereDocument,
    VectorQuery,
    VectorQueryResult,
)


class MetadataReader(ABC):
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
    ) -> Sequence[PersistentEmbeddingRecord]:
        """Query for embedding metadata."""
        pass

    @abstractmethod
    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        pass


class VectorReader(ABC):
    """Embedding Vector segment interface"""

    @abstractmethod
    def get_vectors(self, ids: Optional[Sequence[str]]) -> Sequence[PersistentEmbeddingRecord]:
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
    """Pluggable strategy for creating new segments"""

    @abstractmethod
    def create_collection(
        self, name: str, embedding_function: str, metadata: dict[str, str]
    ) -> None:
        """Create and initialize the segments required for a new collection"""
        pass

    @abstractmethod
    def reset(self):
        """Delete all segments. Should be used for testing only, implementations intended for production
        may throw an exception instead of implementing this method."""
        pass

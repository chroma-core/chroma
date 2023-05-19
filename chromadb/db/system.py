from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence
from uuid import UUID
from overrides import EnforceOverrides
from chromadb.types import (
    Collection,
    EmbeddingFunction,
    Metadata,
    NamespacedName,
    Segment,
)


class SysDB(ABC, EnforceOverrides):
    """Data interface for Chroma's System database"""

    @abstractmethod
    def create_segment(self, segment: Segment) -> Segment:
        """Create a new segment in the System database."""
        pass

    @abstractmethod
    def get_segments(
        self,
        id: Optional[UUID] = None,
        scope: Optional[str] = None,
        topic: Optional[str] = None,
        collection: Optional[UUID] = None,
        metadata: Optional[Dict[str, Metadata]] = None,
    ) -> Sequence[Segment]:
        """Find segments by id, embedding function, and/or metadata"""
        pass

    @abstractmethod
    def get_collections(
        self,
        id: Optional[UUID] = None,
        topic: Optional[str] = None,
        name: Optional[str] = None,
        embedding_function: Optional[NamespacedName] = None,
        metadata: Optional[dict[str, Metadata]] = None,
    ) -> Sequence[Collection]:
        """Get collections by name, embedding function and/or metadata"""
        pass

    @abstractmethod
    def create_collection(self, collection: Collection) -> None:
        """Create a new topic"""
        pass

    @abstractmethod
    def delete_collection(self, id: UUID) -> None:
        """Delete a topic and all associated segments from the SysDB"""
        pass

    @abstractmethod
    def get_embedding_functions(
        self, name: Optional[str]
    ) -> Sequence[EmbeddingFunction]:
        """Find embedding functions"""
        pass

    @abstractmethod
    def create_embedding_function(self, embedding_function: EmbeddingFunction) -> None:
        """Create a new embedding function"""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Delete all data. Should be used for testing only, implementations
        intended for production may throw an exception instead of implementing this
        method."""
        pass

from abc import ABC, abstractmethod
from typing import Optional, Sequence
from uuid import UUID
from overrides import EnforceOverrides
from chromadb.types import Collection, Segment, SegmentScope


class SysDB(ABC, EnforceOverrides):
    """Data interface for Chroma's System database"""

    @abstractmethod
    def create_segment(self, segment: Segment) -> None:
        """Create a new segment in the System database. Raises DuplicateError if the ID
        already exists."""
        pass

    @abstractmethod
    def delete_segment(self, id: UUID) -> None:
        """Create a new segment in the System database."""
        pass

    @abstractmethod
    def get_segments(
        self,
        id: Optional[UUID] = None,
        type: Optional[str] = None,
        scope: Optional[SegmentScope] = None,
        topic: Optional[str] = None,
        collection: Optional[UUID] = None,
    ) -> Sequence[Segment]:
        """Find segments by id, type, scope, topic or collection."""
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
    def get_collections(
        self,
        id: Optional[UUID] = None,
        topic: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Sequence[Collection]:
        """Find collections by id, topic or name"""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Delete all data. Should be used for testing only, implementations
        intended for production may throw an exception instead of implementing this
        method."""
        pass

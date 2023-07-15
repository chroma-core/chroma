from abc import abstractmethod
from typing import Optional, Sequence
from uuid import UUID
from chromadb.types import (
    Collection,
    Segment,
    SegmentScope,
    OptionalArgument,
    Unspecified,
    UpdateMetadata,
)
from chromadb.config import Component


class SysDB(Component):
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
    def update_segment(
        self,
        id: UUID,
        topic: OptionalArgument[Optional[str]] = Unspecified(),
        collection: OptionalArgument[Optional[UUID]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        """Update a segment. Unspecified fields will be left unchanged. For the
        metadata, keys with None values will be removed and keys not present in the
        UpdateMetadata dict will be left unchanged."""
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
    def update_collection(
        self,
        id: UUID,
        topic: OptionalArgument[str] = Unspecified(),
        name: OptionalArgument[str] = Unspecified(),
        dimension: OptionalArgument[Optional[int]] = Unspecified(),
        metadata: OptionalArgument[Optional[UpdateMetadata]] = Unspecified(),
    ) -> None:
        """Update a collection. Unspecified fields will be left unchanged. For metadata,
        keys with None values will be removed and keys not present in the UpdateMetadata
        dict will be left unchanged."""
        pass

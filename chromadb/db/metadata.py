from abc import ABC, abstractmethod
from typing import Optional, Sequence, Union
from uuid import UUID
from overrides import EnforceOverrides
from chromadb.types import (
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
    EmbeddingRecord,
    EmbeddingDeleteRecord,
)


class MetadataDB(ABC, EnforceOverrides):
    """Data interface for a database to store and retrieve Embedding Metadata. Intended
    for use as a storage layer for implementations of chromadb.segment.MetadataReader.
    """

    @abstractmethod
    def count_metadata(self, segment_id: UUID) -> int:
        pass

    @abstractmethod
    def get_metadata(
        self,
        segment_id: UUID,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        ids: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        pass

    @abstractmethod
    def write_metadata(
        self,
        segment_id: UUID,
        metadata: Sequence[Union[EmbeddingRecord, EmbeddingDeleteRecord]],
        replace: bool = False,
    ) -> None:
        pass

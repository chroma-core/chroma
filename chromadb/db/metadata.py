from abc import ABC, abstractmethod
from typing import Optional, Sequence
from uuid import UUID
from overrides import EnforceOverrides
from chromadb.types import (
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
    EmbeddingRecord,
    SeqId,
)


class OutdatedOperationError(Exception):
    """Raised when a write operation is attempted with a SeqID less than one already
    present in the DB for the topic."""

    def __init__(self, seq_id: SeqId, max_seq_id: SeqId):
        super().__init__(
            f"Operation for seq ${seq_id} is outdated, max seq_id is {max_seq_id}"
        )

    pass


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
        records: Sequence[EmbeddingRecord],
    ) -> None:
        pass

    @abstractmethod
    def max_seq_id(self, segment_id: UUID) -> SeqId:
        pass

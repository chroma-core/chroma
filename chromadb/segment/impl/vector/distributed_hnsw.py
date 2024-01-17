from typing import Optional, Sequence
from overrides import override
from chromadb.config import System
from chromadb.segment import VectorReader
from chromadb.types import (
    Metadata,
    Segment,
    SeqId,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)


class DistributedHNSWSegment(VectorReader):
    def __init__(self, system: System, segment: Segment):
        pass

    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        raise NotImplementedError()

    @override
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        raise NotImplementedError()

    @override
    def count(self) -> int:
        raise NotImplementedError()

    @override
    def max_seqid(self) -> SeqId:
        raise NotImplementedError()

    @override
    def delete(self) -> None:
        raise NotImplementedError()

    @override
    @staticmethod
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        raise NotImplementedError()

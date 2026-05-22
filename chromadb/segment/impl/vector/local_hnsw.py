"""Compatibility shim for removed Python vector segment implementation."""

from chromadb.segment import VectorReader
from chromadb.config import System
from typing import Optional, Sequence
from chromadb.types import (
    Metadata,
    RequestVersionContext,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)

ERROR_MSG = (
    "Python local HNSW vector segment implementation is no longer supported."
    " Use the Rust backend instead."
)


class LocalHnswSegment(VectorReader):
    DEFAULT_CAPACITY = 1000

    def __init__(self, system: System, segment):
        super().__init__(system, segment)  # type: ignore[misc]
        raise RuntimeError(ERROR_MSG)

    @staticmethod
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        raise RuntimeError(ERROR_MSG)

    def start(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def stop(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def count(self, request_version_context: RequestVersionContext) -> int:
        raise RuntimeError(ERROR_MSG)

    def max_seqid(self) -> int:
        raise RuntimeError(ERROR_MSG)

    def get_vectors(
        self,
        request_version_context: RequestVersionContext,
        ids: Optional[Sequence[str]] = None,
    ) -> Sequence[VectorEmbeddingRecord]:
        raise RuntimeError(ERROR_MSG)

    def query_vectors(self, query: VectorQuery) -> Sequence[Sequence[VectorQueryResult]]:
        raise RuntimeError(ERROR_MSG)

    def delete(self) -> None:
        raise RuntimeError(ERROR_MSG)

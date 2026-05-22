"""Compatibility shim for removed persistent local HNSW implementation."""

from typing import Optional, Sequence

from chromadb.config import System
from chromadb.segment.impl.vector.local_hnsw import LocalHnswSegment
from chromadb.types import (
    RequestVersionContext,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)

ERROR_MSG = (
    "Python persistent HNSW segment implementation is no longer supported."
    " Use the Rust backend instead."
)


class PersistentLocalHnswSegment(LocalHnswSegment):
    METADATA_FILE = "index_metadata.pickle"

    @staticmethod
    def get_file_handle_count() -> int:
        raise RuntimeError(ERROR_MSG)

    def __init__(self, system: System, segment):
        super().__init__(system, segment)  # type: ignore[misc]
        raise RuntimeError(ERROR_MSG)

    @staticmethod
    def propagate_collection_metadata(metadata):
        raise RuntimeError(ERROR_MSG)

    def start(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def stop(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def open_persistent_index(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def close_persistent_index(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def count(self, request_version_context: RequestVersionContext) -> int:
        raise RuntimeError(ERROR_MSG)

    def get_vectors(
        self,
        request_version_context: RequestVersionContext,
        ids: Optional[Sequence[str]] = None,
    ) -> Sequence[VectorEmbeddingRecord]:
        raise RuntimeError(ERROR_MSG)

    def query_vectors(self, query: VectorQuery) -> Sequence[Sequence[VectorQueryResult]]:
        raise RuntimeError(ERROR_MSG)

    def reset_state(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def delete(self) -> None:
        raise RuntimeError(ERROR_MSG)

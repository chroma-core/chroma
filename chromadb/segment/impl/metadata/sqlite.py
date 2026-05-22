"""Compatibility shim for removed Python metadata segment implementation."""

from chromadb.segment import MetadataReader
from chromadb.config import System
from typing import Optional, Sequence, Mapping

from chromadb.types import Metadata, MetadataEmbeddingRecord, RequestVersionContext, VectorQuery

ERROR_MSG = (
    "Python metadata segment implementation is no longer supported."
    " Use the Rust backend instead."
)


class SqliteMetadataSegment(MetadataReader):
    def __init__(self, system: System, segment):
        super().__init__(system, segment)  # type: ignore[misc]
        raise RuntimeError(ERROR_MSG)

    @staticmethod
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        raise RuntimeError(ERROR_MSG)

    def count(self, request_version_context: RequestVersionContext) -> int:
        raise RuntimeError(ERROR_MSG)

    def max_seqid(self):
        raise RuntimeError(ERROR_MSG)

    def start(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def stop(self) -> None:
        raise RuntimeError(ERROR_MSG)

    def get_metadata(
        self,
        request_version_context: RequestVersionContext,
        where=None,
        where_document=None,
        ids=None,
        limit=None,
        offset=None,
        include_metadata: bool = True,
    ) -> Sequence[MetadataEmbeddingRecord]:
        raise RuntimeError(ERROR_MSG)

    def delete(self) -> None:
        raise RuntimeError(ERROR_MSG)

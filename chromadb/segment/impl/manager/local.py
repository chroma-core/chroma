"""Compatibility shim for removed Python local segment manager.

Legacy collection segment creation and query execution now run entirely in the
Rust backend. This shim keeps imports stable while making failures explicit.
"""

from typing import Dict, Optional, Sequence, TypeVar
from uuid import UUID

from chromadb.config import System
from chromadb.segment import Segment, SegmentManager, SegmentType, MetadataReader, VectorReader

ERROR_MSG = (
    "Python local segment manager has been removed in this backend."
    " Use Rust-backed execution (RustBindingsAPI) instead."
)

S = TypeVar("S", bound=SegmentManager)


class LocalSegmentManager(SegmentManager):
    def __init__(self, system: System):
        super().__init__(system)
        raise RuntimeError(ERROR_MSG)

    def prepare_segments_for_new_collection(self, collection) -> Sequence[Segment]:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)

    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)

    def hint_use_collection(self, collection_id: UUID, hint_type) -> None:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)


def _segment(type: SegmentType, scope, collection) -> Segment:  # pragma: no cover
    raise RuntimeError(ERROR_MSG)

from threading import Lock
from chromadb.segment import (
    SegmentImplementation,
    SegmentManager,
    MetadataReader,
    SegmentType,
    VectorReader,
    S,
)
from chromadb.config import System, get_class
from chromadb.db.system import SysDB
from overrides import override
from enum import Enum
from chromadb.types import Collection, Operation, Segment, SegmentScope, Metadata
from typing import Dict, Type, Sequence, Optional, cast
from uuid import UUID, uuid4
from collections import defaultdict

# TODO: it is odd that the segment manager is different for distributed vs local
# implementations.  This should be refactored to be more consistent and shared.
# needed in this is the ability to specify the desired segment types for a collection


class DistributedSegmentManager(SegmentManager):
    @override
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        return super().create_segments(collection)

    @override
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        return super().delete_segments(collection_id)

    @override
    def get_segment(self, collection_id: UUID, type: type[S]) -> S:
        return super().get_segment(collection_id, type)

    @override
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        return super().hint_use_collection(collection_id, hint_type)

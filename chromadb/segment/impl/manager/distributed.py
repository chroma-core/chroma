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

SEGMENT_TYPE_IMPLS = {
    SegmentType.HNSW_DISTRIBUTED: "chromadb.segment.impl.vector.grpc_segment.GrpcSegment",
}


class DistributedSegmentManager(SegmentManager):
    @override
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        vector_segment = _segment(
            SegmentType.HNSW_DISTRIBUTED, SegmentScope.VECTOR, collection
        )

    @override
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        return super().delete_segments(collection_id)

    @override
    def get_segment(self, collection_id: UUID, type: type[S]) -> S:
        return super().get_segment(collection_id, type)

    @override
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        return super().hint_use_collection(collection_id, hint_type)


# TODO: rethink duplication from local segment manager
def _segment(type: SegmentType, scope: SegmentScope, collection: Collection) -> Segment:
    """Create a metadata dict, propagating metadata correctly for the given segment type."""
    cls = get_class(SEGMENT_TYPE_IMPLS[type], SegmentImplementation)
    collection_metadata = collection.get("metadata", None)
    metadata: Optional[Metadata] = None
    if collection_metadata:
        metadata = cls.propagate_collection_metadata(collection_metadata)

    return Segment(
        id=uuid4(),
        type=type.value,
        scope=scope,
        topic=collection["topic"],
        collection=collection["id"],
        metadata=metadata,
    )

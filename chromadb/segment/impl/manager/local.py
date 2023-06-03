from chromadb.segment import (
    SegmentImplementation,
    SegmentManager,
    MetadataReader,
    VectorReader,
    S,
)
from chromadb.config import System, get_class
from chromadb.db.system import SysDB
from overrides import override
from enum import Enum
from chromadb.types import Collection, Segment, SegmentScope, Metadata
from typing import Dict, Type, Sequence, Optional, cast
from uuid import UUID, uuid4
from collections import defaultdict


class SegmentType(Enum):
    SQLITE = "urn:chroma:segment/metadata/sqlite"
    HNSW_LOCAL_MEMORY = "urn:chroma:segment/vector/hnsw-local-memory"


SEGMENT_TYPE_IMPLS = {
    SegmentType.SQLITE: "chromadb.segment.impl.metadata.sqlite.SqliteMetadataSegment",
    SegmentType.HNSW_LOCAL_MEMORY: "chromadb.segment.impl.vector.local_hnsw.LocalHnswSegment",
}


class LocalSegmentManager(SegmentManager):
    _sysdb: SysDB
    _system: System
    _instances: Dict[UUID, SegmentImplementation]
    _segment_cache: Dict[UUID, Dict[SegmentScope, Segment]]

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._system = system
        self._instances = {}
        self._segment_cache = defaultdict(dict)

    @override
    def start(self) -> None:
        for instance in self._instances.values():
            instance.start()
        super().start()

    @override
    def stop(self) -> None:
        for instance in self._instances.values():
            instance.stop()
        super().stop()

    @override
    def reset_state(self) -> None:
        for instance in self._instances.values():
            instance.stop()
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        super().reset_state()

    @override
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        vector_segment = _segment(
            SegmentType.HNSW_LOCAL_MEMORY, SegmentScope.VECTOR, collection
        )
        metadata_segment = _segment(
            SegmentType.SQLITE, SegmentScope.METADATA, collection
        )
        return [vector_segment, metadata_segment]

    @override
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        segments = self._sysdb.get_segments(collection=collection_id)
        for segment in segments:
            if segment["id"] in self._instances:
                del self._instances[segment["id"]]
            if collection_id in self._segment_cache:
                if segment["scope"] in self._segment_cache[collection_id]:
                    del self._segment_cache[collection_id][segment["scope"]]
                del self._segment_cache[collection_id]
        return [s["id"] for s in segments]

    @override
    def get_segment(self, collection_id: UUID, type: Type[S]) -> S:
        if type == MetadataReader:
            scope = SegmentScope.METADATA
        elif type == VectorReader:
            scope = SegmentScope.VECTOR
        else:
            raise ValueError(f"Invalid segment type: {type}")

        if scope not in self._segment_cache[collection_id]:
            segments = self._sysdb.get_segments(collection=collection_id, scope=scope)
            known_types = set([k.value for k in SEGMENT_TYPE_IMPLS.keys()])
            # Get the first segment of a known type
            segment = next(filter(lambda s: s["type"] in known_types, segments))
            self._segment_cache[collection_id][scope] = segment

        instance = self._instance(self._segment_cache[collection_id][scope])
        return cast(S, instance)

    def _cls(self, segment: Segment) -> Type[SegmentImplementation]:
        classname = SEGMENT_TYPE_IMPLS[SegmentType(segment["type"])]
        cls = get_class(classname, SegmentImplementation)
        return cls

    def _instance(self, segment: Segment) -> SegmentImplementation:
        if segment["id"] not in self._instances:
            cls = self._cls(segment)
            instance = cls(self._system, segment)
            instance.start()
            self._instances[segment["id"]] = instance
        return self._instances[segment["id"]]


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

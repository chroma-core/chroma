from chromadb.segment import (
    SegmentImplementation,
    SegmentManager,
    MetadataReader,
    VectorReader,
)
from chromadb.config import System, get_class
from chromadb.db.system import SysDB
from overrides import override
from enum import Enum
from chromadb.types import Collection, Segment, SegmentScope
from typing import Dict, Set, Type, TypeVar
from uuid import UUID, uuid4
from collections import defaultdict
import re


class SegmentType(Enum):
    SQLITE = "urn:chroma:segment/metadata/sqlite"
    HNSW_LOCAL_MEMORY = "urn:chroma:segment/vector/hnsw-local-memory"


SEGMENT_TYPE_IMPLS = {
    SegmentType.SQLITE: "chromadb.segment.impl.sqlite.SqliteMetadataReader",
    SegmentType.HNSW_LOCAL_MEMORY: "chromadb.segment.impl.vector.local_hnsw.LocalHnswSegment",
}

PROPAGATE_METADATA = {
    SegmentType.HNSW_LOCAL_MEMORY: [r"^hnsw:.*"],
}


class LocalSegmentManager(SegmentManager):
    _sysdb: SysDB
    _system: System
    _instances: Dict[UUID, SegmentImplementation]
    _segment_cache: Dict[UUID, Dict[SegmentScope, Segment]]

    def __init__(self, system: System):
        self._sysdb = self.require(SysDB)
        self._system = system
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        super().__init__(system)

    @override
    def start(self) -> None:
        super().start()

    @override
    def stop(self) -> None:
        super().stop()

    @override
    def reset(self) -> None:
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        super().reset()

    @override
    def create_segments(self, collection: Collection) -> Set[Segment]:
        vector_segment = _segment(
            SegmentType.HNSW_LOCAL_MEMORY, SegmentScope.VECTOR, collection
        )
        metadata_segment = _segment(
            SegmentType.SQLITE, SegmentScope.METADATA, collection
        )
        self._sysdb.create_segment(vector_segment)
        self._sysdb.create_segment(metadata_segment)
        return {vector_segment, metadata_segment}

    @override
    def delete_segments(self, collection_id: UUID) -> None:
        segments = self._sysdb.get_segments(collection=collection_id)
        for segment in segments:
            self._sysdb.delete_segment(segment["id"])
            del self._instances[segment["id"]]
            del self._segment_cache[collection_id][segment["scope"]]
            del self._segment_cache[collection_id]

    T = TypeVar("T", bound="SegmentImplementation")

    @override
    def get_segment(self, collection_id: UUID, type: Type[T]) -> SegmentImplementation:
        if type == Type[MetadataReader]:
            scope = SegmentScope.METADATA
        elif type == Type[VectorReader]:
            scope = SegmentScope.VECTOR
        else:
            raise ValueError(f"Invalid segment type: {type}")

        if scope not in self._segment_cache[collection_id]:
            segments = self._sysdb.get_segments(collection=collection_id, scope=scope)
            known_types = set([k.value for k in SEGMENT_TYPE_IMPLS.keys()])
            # Get the first segment of a known type
            segment = next(filter(lambda s: s["type"] in known_types, segments))
            self._segment_cache[collection_id][scope] = segment

        return self._instance(self._segment_cache[collection_id][scope])

    def _instance(self, segment: Segment) -> SegmentImplementation:
        if segment["id"] not in self._instances:
            classname = SEGMENT_TYPE_IMPLS[SegmentType(segment["type"])]
            cls = get_class(classname, SegmentImplementation)
            self._instances[segment["id"]] = cls(self._system, segment)
        return self._instances[segment["id"]]


def _segment(type: SegmentType, scope: SegmentScope, collection: Collection) -> Segment:
    """Create a metadata dict, propagating metadata correctly for the given segment type."""
    metadata = {}
    regexes = PROPAGATE_METADATA.get(type, [])
    if collection["metadata"]:
        for key, value in collection["metadata"].items():
            for regex in regexes:
                if re.match(regex, key):
                    metadata[key] = value
                    break

    return Segment(
        id=uuid4(),
        type=type.value,
        scope=scope,
        topic=collection["topic"],
        collection=collection["id"],
        metadata=metadata,
    )

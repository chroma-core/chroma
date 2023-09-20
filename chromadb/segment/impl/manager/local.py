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
from chromadb.segment.impl.vector.local_persistent_hnsw import (
    PersistentLocalHnswSegment,
)
from chromadb.types import Collection, Operation, Segment, SegmentScope, Metadata
from typing import Dict, Type, Sequence, Optional, cast
from uuid import UUID, uuid4
from collections import defaultdict
import platform

from chromadb.utils.lru_cache import LRUCache

if platform.system() != "Windows":
    import resource
elif platform.system() == "Windows":
    import ctypes


SEGMENT_TYPE_IMPLS = {
    SegmentType.SQLITE: "chromadb.segment.impl.metadata.sqlite.SqliteMetadataSegment",
    SegmentType.HNSW_LOCAL_MEMORY: "chromadb.segment.impl.vector.local_hnsw.LocalHnswSegment",
    SegmentType.HNSW_LOCAL_PERSISTED: "chromadb.segment.impl.vector.local_persistent_hnsw.PersistentLocalHnswSegment",
}


class LocalSegmentManager(SegmentManager):
    _sysdb: SysDB
    _system: System
    _instances: Dict[UUID, SegmentImplementation]
    _vector_instances_file_handle_cache: LRUCache[
        UUID, PersistentLocalHnswSegment
    ]  # LRU cache to manage file handles across vector segment instances
    _segment_cache: Dict[
        UUID, Dict[SegmentScope, Segment]
    ]  # Tracks which segments are loaded for a given collection
    _vector_segment_type: SegmentType = SegmentType.HNSW_LOCAL_MEMORY
    _lock: Lock
    _max_file_handles: int

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._system = system
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        self._lock = Lock()

        # TODO: prototyping with distributed segment for now, but this should be a configurable option
        # we need to think about how to handle this configuration
        if self._system.settings.require("is_persistent"):
            self._vector_segment_type = SegmentType.HNSW_LOCAL_PERSISTED
            if platform.system() != "Windows":
                self._max_file_handles = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            else:
                self._max_file_handles = ctypes.windll.msvcrt._getmaxstdio()  # type: ignore
            segment_limit = (
                self._max_file_handles
                // PersistentLocalHnswSegment.get_file_handle_count()
            )
            self._vector_instances_file_handle_cache = LRUCache(
                segment_limit, callback=lambda _, v: v.close_persistent_index()
            )

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
            instance.reset_state()
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        super().reset_state()

    @override
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        vector_segment = _segment(
            self._vector_segment_type, SegmentScope.VECTOR, collection
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
                if segment["type"] == SegmentType.HNSW_LOCAL_PERSISTED.value:
                    instance = self.get_segment(collection_id, VectorReader)
                    instance.delete()
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

        # Instances must be atomically created, so we use a lock to ensure that only one thread
        # creates the instance.
        with self._lock:
            instance = self._instance(self._segment_cache[collection_id][scope])
        return cast(S, instance)

    @override
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        # The local segment manager responds to hints by pre-loading both the metadata and vector
        # segments for the given collection.
        for type in [MetadataReader, VectorReader]:
            # Just use get_segment to load the segment into the cache
            instance = self.get_segment(collection_id, type)
            # If the segment is a vector segment, we need to keep segments in an LRU cache
            # to avoid hitting the OS file handle limit.
            if type == VectorReader and self._system.settings.require("is_persistent"):
                instance = cast(PersistentLocalHnswSegment, instance)
                instance.open_persistent_index()
                self._vector_instances_file_handle_cache.set(collection_id, instance)

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

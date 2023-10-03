from threading import Lock

import grpc
from chromadb.proto.chroma_pb2_grpc import SegmentServerStub
from chromadb.proto.convert import to_proto_segment
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
from chromadb.segment import SegmentDirectory
from chromadb.types import Collection, Operation, Segment, SegmentScope, Metadata
from typing import Dict, List, Type, Sequence, Optional, cast
from uuid import UUID, uuid4
from collections import defaultdict

# TODO: it is odd that the segment manager is different for distributed vs local
# implementations.  This should be refactored to be more consistent and shared.
# needed in this is the ability to specify the desired segment types for a collection
# It is odd that segment manager is coupled to the segment implementation. We need to rethink
# this abstraction.

SEGMENT_TYPE_IMPLS = {
    SegmentType.SQLITE: "chromadb.segment.impl.metadata.sqlite.SqliteMetadataSegment",
    SegmentType.HNSW_DISTRIBUTED: "chromadb.segment.impl.vector.grpc_segment.GrpcVectorSegment",
}


class DistributedSegmentManager(SegmentManager):
    _sysdb: SysDB
    _system: System
    _instances: Dict[UUID, SegmentImplementation]
    _segment_cache: Dict[
        UUID, Dict[SegmentScope, Segment]
    ]  # collection_id -> scope -> segment
    _segment_directory: SegmentDirectory
    _lock: Lock
    _segment_server_stubs: Dict[str, SegmentServerStub]  # grpc_url -> grpc stub

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._segment_directory = self.require(SegmentDirectory)
        self._system = system
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        self._segment_server_stubs = {}
        self._lock = Lock()

    @override
    def create_segments(self, collection: Collection) -> Sequence[Segment]:
        vector_segment = _segment(
            SegmentType.HNSW_DISTRIBUTED, SegmentScope.VECTOR, collection
        )
        metadata_segment = _segment(
            SegmentType.SQLITE, SegmentScope.METADATA, collection
        )
        return [vector_segment, metadata_segment]

    @override
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        raise NotImplementedError()

    @override
    def get_segment(self, collection_id: UUID, type: type[S]) -> S:
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
            grpc_url = self._segment_directory.get_segment_endpoint(segment)
            if segment["metadata"] is not None:
                segment["metadata"]["grpc_url"] = grpc_url  # type: ignore
            else:
                segment["metadata"] = {"grpc_url": grpc_url}
            # TODO: Register a callback to update the segment when it gets moved
            # self._segment_directory.register_updated_segment_callback()
            self._segment_cache[collection_id][scope] = segment

        # Instances must be atomically created, so we use a lock to ensure that only one thread
        # creates the instance.
        with self._lock:
            instance = self._instance(self._segment_cache[collection_id][scope])
        return cast(S, instance)

    @override
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        # TODO: this should call load/release on the target node, node should be stored in metadata
        # for now this is fine, but cache invalidation is a problem btwn sysdb and segment manager
        types = [MetadataReader, VectorReader]
        for type in types:
            self.get_segment(
                collection_id, type
            )  # TODO: this is a hack that mirrors local segment manager to force load the relevant instances
            if type == VectorReader:
                # Load the remote segment
                segments = self._sysdb.get_segments(
                    collection=collection_id, scope=SegmentScope.VECTOR
                )
                known_types = set([k.value for k in SEGMENT_TYPE_IMPLS.keys()])
                segment = next(filter(lambda s: s["type"] in known_types, segments))
                grpc_url = self._segment_directory.get_segment_endpoint(segment)

                if grpc_url not in self._segment_server_stubs:
                    channel = grpc.insecure_channel(grpc_url)
                    self._segment_server_stubs[grpc_url] = SegmentServerStub(channel)  # type: ignore

                self._segment_server_stubs[grpc_url].LoadSegment(
                    to_proto_segment(segment)
                )

    # TODO: rethink duplication from local segment manager
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

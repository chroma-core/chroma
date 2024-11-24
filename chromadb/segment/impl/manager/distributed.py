from collections import defaultdict
from threading import Lock
from typing import Dict, Sequence
from uuid import UUID, uuid4

from overrides import override

from chromadb.config import System
from chromadb.db.system import SysDB
from chromadb.segment import (
    SegmentImplementation,
    SegmentManager,
    SegmentType,
)
<<<<<<< HEAD
from chromadb.config import System, get_class
from chromadb.db.system import SysDB
from chromadb.errors import InvalidArgumentError
from overrides import override
=======
>>>>>>> main
from chromadb.segment.distributed import SegmentDirectory
from chromadb.segment.impl.vector.hnsw_params import PersistentHnswParams
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.types import Collection, Operation, Segment, SegmentScope


class DistributedSegmentManager(SegmentManager):
    _sysdb: SysDB
    _system: System
    _opentelemetry_client: OpenTelemetryClient
    _instances: Dict[UUID, SegmentImplementation]
    _segment_cache: Dict[
        UUID, Dict[SegmentScope, Segment]
    ]  # collection_id -> scope -> segment
    _segment_directory: SegmentDirectory
    _lock: Lock
    # _segment_server_stubs: Dict[str, SegmentServerStub]  # grpc_url -> grpc stub

    def __init__(self, system: System):
        super().__init__(system)
        self._sysdb = self.require(SysDB)
        self._segment_directory = self.require(SegmentDirectory)
        self._system = system
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        self._instances = {}
        self._segment_cache = defaultdict(dict)
        self._lock = Lock()

    @trace_method(
        "DistributedSegmentManager.prepare_segments_for_new_collection",
        OpenTelemetryGranularity.OPERATION_AND_SEGMENT,
    )
    @override
    def prepare_segments_for_new_collection(self, collection: Collection) -> Sequence[Segment]:
        vector_segment = Segment(
            id=uuid4(),
            type=SegmentType.HNSW_DISTRIBUTED.value,
            scope=SegmentScope.VECTOR,
            collection=collection.id,
            metadata=PersistentHnswParams.extract(collection.metadata)
            if collection.metadata
            else None,
        )
        metadata_segment = Segment(
            id=uuid4(),
            type=SegmentType.BLOCKFILE_METADATA.value,
            scope=SegmentScope.METADATA,
            collection=collection.id,
            metadata=None,
        )
        record_segment = Segment(
            id=uuid4(),
            type=SegmentType.BLOCKFILE_RECORD.value,
            scope=SegmentScope.RECORD,
            collection=collection.id,
            metadata=None,
        )
        return [vector_segment, record_segment, metadata_segment]

    @override
    def delete_segments(self, collection_id: UUID) -> Sequence[UUID]:
        segments = self._sysdb.get_segments(collection=collection_id)
        return [s["id"] for s in segments]

    @trace_method(
        "DistributedSegmentManager.get_segment",
        OpenTelemetryGranularity.OPERATION_AND_SEGMENT,
    )
<<<<<<< HEAD
    def get_segment(self, collection_id: UUID, type: Type[S]) -> S:
        if type == MetadataReader:
            scope = SegmentScope.METADATA
        elif type == VectorReader:
            scope = SegmentScope.VECTOR
        else:
            raise InvalidArgumentError(f"Invalid segment type: {type}")

=======
    def get_segment(self, collection_id: UUID, scope: SegmentScope) -> Segment:
>>>>>>> main
        if scope not in self._segment_cache[collection_id]:
            # For now, there is exactly one segment per scope for a given collection
            segment = self._sysdb.get_segments(collection=collection_id, scope=scope)[0]
            # TODO: Register a callback to update the segment when it gets moved
            # self._segment_directory.register_updated_segment_callback()
            self._segment_cache[collection_id][scope] = segment
        return self._segment_cache[collection_id][scope]

    @trace_method(
        "DistributedSegmentManager.get_endpoint",
        OpenTelemetryGranularity.OPERATION_AND_SEGMENT,
    )
    def get_endpoint(self, collection_id: UUID) -> str:
        # Get grpc endpoint from record segment. Since grpc endpoint is endpoint is
        # determined by collection uuid, the endpoint should be the same for all
        # segments of the same collection
        record_segment = self.get_segment(collection_id, SegmentScope.RECORD)
        return self._segment_directory.get_segment_endpoint(record_segment)

    @trace_method(
        "DistributedSegmentManager.hint_use_collection",
        OpenTelemetryGranularity.OPERATION_AND_SEGMENT,
    )
    @override
    def hint_use_collection(self, collection_id: UUID, hint_type: Operation) -> None:
        pass

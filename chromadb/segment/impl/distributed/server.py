from typing import Dict, Type
from uuid import UUID
from chromadb.config import Settings, System, get_class
from chromadb.proto.chroma_pb2_grpc import (
    SegmentServerServicer,
    add_SegmentServerServicer_to_server,
    VectorReaderServicer,
    add_VectorReaderServicer_to_server,
)
import chromadb.proto.chroma_pb2 as proto
import grpc
from concurrent import futures
from chromadb.proto.convert import from_proto_segment
from chromadb.segment import SegmentImplementation, SegmentType
from chromadb.config import System
from chromadb.types import Segment, SegmentScope


# Run this with python -m chromadb.segment.impl.distributed.server

# TODO: for now the distirbuted segment type is serviced by a persistent local segment, since
# the only real material difference is the way the segment is loaded and persisted.
# we should refactor our the index logic from the segment logic, and then we can have a
# distributed segment implementation that uses the same index impl but has a different segment wrapper
# that handles the distributed logic and storage

SEGMENT_TYPE_IMPLS = {
    SegmentType.HNSW_DISTRIBUTED: "chromadb.segment.impl.vector.local_persistent_hnsw.PersistentLocalHnswSegment",
}


class SegmentServer(SegmentServerServicer, VectorReaderServicer):
    _segment_cache: Dict[UUID, SegmentImplementation] = {}
    _system: System

    def __init__(self, system: System) -> None:
        super().__init__()
        self._system = system

    def LoadSegment(self, request: proto.Segment, context):
        id = UUID(hex=request.id)
        if id in self._segment_cache:
            # TODO: already loaded state
            return proto.SegmentServerResponse(
                success=True,
            )
        else:
            if request.scope == SegmentScope.METADATA:
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details("Metadata segments are not yet implemented")
                return proto.SegmentServerResponse(success=False)
            elif request.scope == SegmentScope.VECTOR:
                if request.type == SegmentType.HNSW_DISTRIBUTED:
                    self._create_instance(from_proto_segment(request))
                else:
                    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                    context.set_details("Segment type not implemented")
                    return proto.SegmentServerResponse(success=False)

    def ReleaseSegment(self, request, context):
        return super().ReleaseSegment(request, context)

    def QueryVectors(self, request, context):
        return super().QueryVectors(request, context)

    def GetVectors(self, request, context):
        return super().GetVectors(request, context)

    def _cls(self, segment: Segment) -> Type[SegmentImplementation]:
        classname = SEGMENT_TYPE_IMPLS[SegmentType(segment["type"])]
        cls = get_class(classname, SegmentImplementation)
        return cls

    def _create_instance(self, segment: Segment) -> None:
        if segment["id"] not in self._segment_cache:
            cls = self._cls(segment)
            instance = cls(self._system, segment)
            instance.start()
            self._segment_cache[segment["id"]] = instance


if __name__ == "__main__":
    # TODO: parameterize the setings from env
    settings = Settings(
        is_persistent=True,
        chroma_producer_impl="chromadb.ingest.impl.pulsar.PulsarProducer",
        chroma_consumer_impl="chromadb.ingest.impl.pulsar.PulsarConsumer",
        pulsar_broker_url="pulsar",
        pulsar_broker_port="6650",
        pulsar_admin_port="8080",
    )
    system = System(settings)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    segment_server = SegmentServer(system)
    add_SegmentServerServicer_to_server(segment_server, server)
    add_VectorReaderServicer_to_server(segment_server, server)
    # TODO: parameterize the port from env
    server.add_insecure_port("[::]:50051")
    system.start()
    server.start()
    server.wait_for_termination()

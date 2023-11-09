from typing import Any, Dict, List, Type, cast
from uuid import UUID
from chromadb.config import Settings, System, get_class
from chromadb.ingest import CollectionAssignmentPolicy
from chromadb.proto.chroma_pb2_grpc import (
    SegmentServerServicer,
    add_SegmentServerServicer_to_server,
    VectorReaderServicer,
    add_VectorReaderServicer_to_server,
)
import chromadb.proto.chroma_pb2 as proto
import grpc
from concurrent import futures
from chromadb.proto.convert import (
    from_proto_segment,
    to_proto_seq_id,
    to_proto_vector,
    to_proto_vector_embedding_record,
)
from chromadb.segment import SegmentImplementation, SegmentType, VectorReader
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.types import ScalarEncoding, Segment, SegmentScope
from chromadb.segment.distributed import MemberlistProvider, Memberlist
from chromadb.utils.rendezvous_hash import assign, murmur3hasher
import logging
import os

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
    _opentelemetry_client: OpenTelemetryClient
    _memberlist_provider: MemberlistProvider
    _curr_memberlist: Memberlist
    _assigned_topics: List[str]

    def __init__(self, system: System) -> None:
        super().__init__()
        self._system = system
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        # TODO: add term and epoch to segment server
        self._memberlist_provider = system.require(MemberlistProvider)
        self._memberlist_provider.set_memberlist_name("worker-memberlist")
        self._assignment_policy = system.require(CollectionAssignmentPolicy)
        self._curr_memberlist = self._memberlist_provider.get_memberlist()
        self._memberlist_provider.register_updated_memberlist_callback(
            self._on_memberlist_update
        )
        self._assigned_topics = []

    def _compute_assigned_topics(self) -> None:
        """Uses rendezvous hashing to compute the topics that this node is responsible for"""
        topics = self._assignment_policy.get_topics()
        my_ip = os.environ["MY_POD_IP"]
        new_assignments = []
        for topic in topics:
            assigned = assign(topic, self._curr_memberlist, murmur3hasher)
            if assigned == my_ip:
                new_assignments.append(topic)
        # TODO: We need to lock this assignment
        self._assigned_topics = new_assignments
        print("Memberlist: ", self._curr_memberlist)
        print("Assigned topics: ", self._assigned_topics)

    def _on_memberlist_update(self, memberlist: Memberlist) -> None:
        """Called when the memberlist is updated"""
        print("Memberlist updated ", memberlist)
        self._curr_memberlist = memberlist
        if len(self._curr_memberlist) > 0:
            self._compute_assigned_topics()
        else:
            # In this case we'd want to warn that there are no members but
            # this is not an error, as it could be that the cluster is just starting up
            print("Memberlist is empty")

    # def QueryVectors(
    #     self, request: proto.QueryVectorsRequest, context: Any
    # ) -> proto.QueryVectorsResponse:
    #     context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    #     context.set_details("Query segment not implemented yet")
    #     return proto.QueryVectorsResponse()

    # @trace_method(
    #     "SegmentServer.GetVectors", OpenTelemetryGranularity.OPERATION_AND_SEGMENT
    # )
    # def GetVectors(
    #     self, request: proto.GetVectorsRequest, context: Any
    # ) -> proto.GetVectorsResponse:
    #     segment_id = UUID(hex=request.segment_id)
    #     if segment_id not in self._segment_cache:
    #         context.set_code(grpc.StatusCode.NOT_FOUND)
    #         context.set_details("Segment not found")
    #         return proto.GetVectorsResponse()
    #     else:
    #         segment = self._segment_cache[segment_id]
    #         segment = cast(VectorReader, segment)
    #         segment_results = segment.get_vectors(request.ids)
    #         return_records = []
    #         for record in segment_results:
    #             # TODO: encoding should be based on stored encoding for segment
    #             # For now we just assume float32
    #             return_record = to_proto_vector_embedding_record(
    #                 record, ScalarEncoding.FLOAT32
    #             )
    #             return_records.append(return_record)
    #         return proto.GetVectorsResponse(records=return_records)

    # def _cls(self, segment: Segment) -> Type[SegmentImplementation]:
    #     classname = SEGMENT_TYPE_IMPLS[SegmentType(segment["type"])]
    #     cls = get_class(classname, SegmentImplementation)
    #     return cls

    # def _create_instance(self, segment: Segment) -> None:
    #     if segment["id"] not in self._segment_cache:
    #         cls = self._cls(segment)
    #         instance = cls(self._system, segment)
    #         instance.start()
    #         self._segment_cache[segment["id"]] = instance


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    system = System(Settings())
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    segment_server = SegmentServer(system)
    add_SegmentServerServicer_to_server(segment_server, server)  # type: ignore
    add_VectorReaderServicer_to_server(segment_server, server)  # type: ignore
    server.add_insecure_port(
        f"[::]:{system.settings.require('chroma_server_grpc_port')}"
    )
    system.start()
    server.start()
    server.wait_for_termination()

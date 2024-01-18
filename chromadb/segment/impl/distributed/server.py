from typing import Any, Dict, List, Sequence, Set
from uuid import UUID
from chromadb.config import Settings, System
from chromadb.ingest import CollectionAssignmentPolicy, Consumer
from chromadb.proto.chroma_pb2_grpc import (
    # SegmentServerServicer,
    # add_SegmentServerServicer_to_server,
    VectorReaderServicer,
    add_VectorReaderServicer_to_server,
)
import chromadb.proto.chroma_pb2 as proto
import grpc
from concurrent import futures
from chromadb.proto.convert import (
    to_proto_vector_embedding_record
)
from chromadb.segment import SegmentImplementation, SegmentType
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient
)
from chromadb.types import EmbeddingRecord
from chromadb.segment.distributed import MemberlistProvider, Memberlist
from chromadb.utils.rendezvous_hash import assign, murmur3hasher
from chromadb.ingest.impl.pulsar_admin import PulsarAdmin
import logging
import os

# This file is a prototype. It will be replaced with a real distributed segment server
# written in a different language. This is just a proof of concept to get the distributed
# segment type working end to end.

# Run this with python -m chromadb.segment.impl.distributed.server

SEGMENT_TYPE_IMPLS = {
    SegmentType.HNSW_DISTRIBUTED: "chromadb.segment.impl.vector.local_persistent_hnsw.PersistentLocalHnswSegment",
}


class SegmentServer(VectorReaderServicer):
    _segment_cache: Dict[UUID, SegmentImplementation] = {}
    _system: System
    _opentelemetry_client: OpenTelemetryClient
    _memberlist_provider: MemberlistProvider
    _curr_memberlist: Memberlist
    _assigned_topics: Set[str]
    _topic_to_subscription: Dict[str, UUID]
    _consumer: Consumer

    def __init__(self, system: System) -> None:
        super().__init__()
        self._system = system

        # Init dependency services
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        # TODO: add term and epoch to segment server
        self._memberlist_provider = system.require(MemberlistProvider)
        self._memberlist_provider.set_memberlist_name("worker-memberlist")
        self._assignment_policy = system.require(CollectionAssignmentPolicy)
        self._create_pulsar_topics()
        self._consumer = system.require(Consumer)

        # Init data
        self._topic_to_subscription = {}
        self._assigned_topics = set()
        self._curr_memberlist = self._memberlist_provider.get_memberlist()
        self._compute_assigned_topics()

        self._memberlist_provider.register_updated_memberlist_callback(
            self._on_memberlist_update
        )

    def _compute_assigned_topics(self) -> None:
        """Uses rendezvous hashing to compute the topics that this node is responsible for"""
        if not self._curr_memberlist:
            self._assigned_topics = set()
            return
        topics = self._assignment_policy.get_topics()
        my_ip = os.environ["MY_POD_IP"]
        new_assignments: List[str] = []
        for topic in topics:
            assigned = assign(topic, self._curr_memberlist, murmur3hasher)
            if assigned == my_ip:
                new_assignments.append(topic)
        new_assignments_set = set(new_assignments)
        # TODO: We need to lock around this assignment
        net_new_assignments = new_assignments_set - self._assigned_topics
        removed_assignments = self._assigned_topics - new_assignments_set

        for topic in removed_assignments:
            subscription = self._topic_to_subscription[topic]
            self._consumer.unsubscribe(subscription)
            del self._topic_to_subscription[topic]

        for topic in net_new_assignments:
            subscription = self._consumer.subscribe(topic, self._on_message)
            self._topic_to_subscription[topic] = subscription

        self._assigned_topics = new_assignments_set
        print(
            f"Topic assigment updated and now assigned to {len(self._assigned_topics)} topics"
        )

    def _on_memberlist_update(self, memberlist: Memberlist) -> None:
        """Called when the memberlist is updated"""
        self._curr_memberlist = memberlist
        if len(self._curr_memberlist) > 0:
            self._compute_assigned_topics()
        else:
            # In this case we'd want to warn that there are no members but
            # this is not an error, as it could be that the cluster is just starting up
            print("Memberlist is empty")

    def _on_message(self, embedding_records: Sequence[EmbeddingRecord]) -> None:
        """Called when a message is received from the consumer"""
        print(f"Received {len(embedding_records)} records")
        print(
            f"First record: {embedding_records[0]} is for collection {embedding_records[0]['collection_id']}"
        )
        return None

    def _create_pulsar_topics(self) -> None:
        """This creates the pulsar topics used by the system.
        HACK: THIS IS COMPLETELY A HACK AND WILL BE REPLACED
        BY A PROPER TOPIC MANAGEMENT SYSTEM IN THE COORDINATOR"""
        topics = self._assignment_policy.get_topics()
        admin = PulsarAdmin(self._system)
        for topic in topics:
            admin.create_topic(topic)

    def QueryVectors(
        self, request: proto.QueryVectorsRequest, context: Any
    ) -> proto.QueryVectorsResponse:
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Query segment not implemented yet")
        return proto.QueryVectorsResponse()

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
    # add_SegmentServerServicer_to_server(segment_server, server)  # type: ignore
    add_VectorReaderServicer_to_server(segment_server, server)  # type: ignore
    server.add_insecure_port(
        f"[::]:{system.settings.require('chroma_server_grpc_port')}"
    )
    system.start()
    server.start()
    server.wait_for_termination()

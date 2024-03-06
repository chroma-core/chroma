import sys

import grpc

from chromadb.ingest import (
    Producer,
    Consumer,
    ConsumerCallbackFn,
)
from chromadb.proto.convert import to_proto_submit
from chromadb.proto.logservice_pb2 import PushLogsRequest, PullLogsRequest, RecordLog
from chromadb.proto.logservice_pb2_grpc import LogServiceStub
from chromadb.types import (
    SubmitEmbeddingRecord,
    SeqId,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryClient,
    OpenTelemetryGranularity,
    trace_method,
)
from overrides import override
from typing import Sequence, Optional, Dict, cast
from uuid import UUID
import logging

logger = logging.getLogger(__name__)


class LogService(Producer, Consumer):
    """
    Distributed Chroma Log Service
    """

    _log_service_stub: LogServiceStub
    _channel: grpc.Channel
    _log_service_url: str
    _log_service_port: int

    def __init__(self, system: System):
        self._log_service_url = system.settings.require("chroma_logservice_host")
        self._log_service_port = system.settings.require("chroma_logservice_port")
        self._opentelemetry_client = system.require(OpenTelemetryClient)
        super().__init__(system)

    @trace_method("LogService.start", OpenTelemetryGranularity.ALL)
    @override
    def start(self) -> None:
        self._channel = grpc.insecure_channel(
            f"{self._log_service_url}:{self._log_service_port}"
        )
        self._log_service_stub = LogServiceStub(self._channel)  # type: ignore
        super().start()

    @trace_method("LogService.stop", OpenTelemetryGranularity.ALL)
    @override
    def stop(self) -> None:
        self._channel.close()
        super().stop()

    @trace_method("LogService.reset_state", OpenTelemetryGranularity.ALL)
    @override
    def reset_state(self) -> None:
        super().reset_state()

    @override
    def create_topic(self, topic_name: str) -> None:
        raise NotImplementedError("Not implemented")

    @trace_method("LogService.delete_topic", OpenTelemetryGranularity.ALL)
    @override
    def delete_topic(self, topic_name: str) -> None:
        raise NotImplementedError("Not implemented")

    @trace_method("LogService.submit_embedding", OpenTelemetryGranularity.ALL)
    @override
    def submit_embedding(
        self, topic_name: str, embedding: SubmitEmbeddingRecord
    ) -> SeqId:
        if not self._running:
            raise RuntimeError("Component not running")

        return self.submit_embeddings(topic_name, [embedding])[0]  # type: ignore

    @trace_method("LogService.submit_embeddings", OpenTelemetryGranularity.ALL)
    @override
    def submit_embeddings(
        self, topic_name: str, embeddings: Sequence[SubmitEmbeddingRecord]
    ) -> Sequence[SeqId]:
        logger.info(f"Submitting {len(embeddings)} embeddings to {topic_name}")

        if not self._running:
            raise RuntimeError("Component not running")

        if len(embeddings) == 0:
            return []

        # push records to the log service
        collection_id_to_embeddings: Dict[UUID, list[SubmitEmbeddingRecord]] = {}
        for embedding in embeddings:
            collection_id = cast(UUID, embedding.get("collection_id"))
            if collection_id is None:
                raise ValueError("collection_id is required")
            if collection_id not in collection_id_to_embeddings:
                collection_id_to_embeddings[collection_id] = []
            collection_id_to_embeddings[collection_id].append(embedding)

        counts = []
        for collection_id, records in collection_id_to_embeddings.items():
            protos_to_submit = [to_proto_submit(record) for record in records]
            counts.append(
                self.push_logs(
                    collection_id,
                    cast(Sequence[SubmitEmbeddingRecord], protos_to_submit),
                )
            )

        return counts

    @trace_method("LogService.subscribe", OpenTelemetryGranularity.ALL)
    @override
    def subscribe(
        self,
        topic_name: str,
        consume_fn: ConsumerCallbackFn,
        start: Optional[SeqId] = None,
        end: Optional[SeqId] = None,
        id: Optional[UUID] = None,
    ) -> UUID:
        logger.info(f"Subscribing to {topic_name}, noop for logservice")
        return UUID(int=0)

    @trace_method("LogService.unsubscribe", OpenTelemetryGranularity.ALL)
    @override
    def unsubscribe(self, subscription_id: UUID) -> None:
        logger.info(f"Unsubscribing from {subscription_id}, noop for logservice")

    @override
    def min_seqid(self) -> SeqId:
        return 0

    @override
    def max_seqid(self) -> SeqId:
        return sys.maxsize

    @property
    @override
    def max_batch_size(self) -> int:
        return sys.maxsize

    def push_logs(
        self, collection_id: UUID, records: Sequence[SubmitEmbeddingRecord]
    ) -> int:
        request = PushLogsRequest(collection_id=str(collection_id), records=records)
        response = self._log_service_stub.PushLogs(request)
        return response.record_count  # type: ignore

    def pull_logs(
        self, collection_id: UUID, start_id: int, batch_size: int
    ) -> Sequence[RecordLog]:
        request = PullLogsRequest(
            collection_id=str(collection_id),
            start_from_id=start_id,
            batch_size=batch_size,
        )
        response = self._log_service_stub.PullLogs(request)
        return response.records  # type: ignore

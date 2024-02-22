from typing import Sequence
from uuid import UUID
from chromadb.proto.chroma_pb2 import SubmitEmbeddingRecord
from chromadb.proto.logservice_pb2 import PushLogsRequest
from chromadb.proto.logservice_pb2_grpc import LogServiceStub
import grpc


class GrpcLogService:
    """A gRPC implementation of the SysDB. In the distributed system, the SysDB is also
    called the 'Coordinator'. This implementation is used by Chroma frontend servers
    to call a remote SysDB (Coordinator) service."""

    _log_service_stub: LogServiceStub
    _channel: grpc.Channel
    _log_service_url: str
    _log_service_port: int

    def __init__(self) -> None:
        # TODO: fix this to use config.py and implement System later
        #  for now, only support testing locally
        self._channel = grpc.insecure_channel("localhost:50052")
        self._log_service_stub = LogServiceStub(self._channel)  # type: ignore

    def push_logs(
        self, collection_id: UUID, records: Sequence[SubmitEmbeddingRecord]
    ) -> int:
        request = PushLogsRequest(collection_id=str(collection_id), records=records)
        response = self._log_service_stub.PushLogs(request)
        return response.record_count  # type: ignore

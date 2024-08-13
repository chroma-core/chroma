from concurrent import futures
from queue import Queue
from threading import Thread
from typing import Any, Generator, Tuple
import grpc
import pytest

from chromadb.proto.convert import to_proto_submit
from chromadb.proto.logservice_pb2 import PushLogsRequest, PushLogsResponse
from chromadb.proto.logservice_pb2_grpc import (
    LogServiceServicer,
    LogServiceStub,
    add_LogServiceServicer_to_server,
)
from chromadb.proto.utils import RetryOnRpcErrorClientInterceptor
from chromadb.types import Operation, OperationRecord


class FlakyLogServiceServicer(LogServiceServicer):
    num_requests_to_fail: int
    received_requests: Queue[Any]

    def __init__(
        self, num_requests_to_fail: int, received_requests: Queue[Any]
    ) -> None:
        super().__init__()
        self.num_requests_to_fail = num_requests_to_fail
        self.received_requests = received_requests

    def PushLogs(
        self, request: PushLogsRequest, context: grpc.ServicerContext
    ) -> PushLogsResponse:
        if self.num_requests_to_fail > 0:
            self.num_requests_to_fail -= 1
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Service unavailable")
            self.received_requests.put({"status": "failed", "request": request})
            return PushLogsResponse()

        self.received_requests.put({"status": "success", "request": request})
        return PushLogsResponse(record_count=1)


def start_server(
    num_requests_to_fail: int,
    received_requests: Queue[Any],
    started_queue: Queue[Any],
    stop_queue: Queue[Any],
) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_LogServiceServicer_to_server(  # type: ignore
        FlakyLogServiceServicer(num_requests_to_fail, received_requests), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    started_queue.put(1)
    # Block until server stop is requested
    stop_queue.get()
    server.stop(0)


class LogServiceRetryClient:
    stub: LogServiceStub

    def __init__(self, grpc_url: str) -> None:
        channel = grpc.insecure_channel(grpc_url)
        interceptors = [RetryOnRpcErrorClientInterceptor()]
        channel = grpc.intercept_channel(channel, *interceptors)
        self.stub = LogServiceStub(channel)  # type: ignore

    def push_log(self, collection_id: str, record: OperationRecord) -> None:
        proto_record = to_proto_submit(record)
        request = PushLogsRequest(collection_id=collection_id, records=[proto_record])
        self.stub.PushLogs(request)


@pytest.fixture()
def client_for_flaky_server_and_received_requests() -> (
    Generator[Tuple[LogServiceRetryClient, Queue[Any]], None, None]
):
    received_requests: Queue[Any] = Queue()
    started_queue: Queue[Any] = Queue()
    stop_queue: Queue[Any] = Queue()

    server_thread = Thread(
        target=start_server, args=(3, received_requests, started_queue, stop_queue)
    )
    server_thread.start()
    # Wait for server to be ready
    started_queue.get()

    client = LogServiceRetryClient("localhost:50051")

    yield client, received_requests

    stop_queue.put(1)
    server_thread.join()


def test_retry_interceptor(
    client_for_flaky_server_and_received_requests: Tuple[
        LogServiceRetryClient, Queue[Any]
    ]
) -> None:
    (client, received_requests) = client_for_flaky_server_and_received_requests
    client = LogServiceRetryClient("localhost:50051")
    client.push_log(
        "test",
        OperationRecord(
            id="1",
            embedding=None,
            encoding=None,
            metadata=None,
            operation=Operation.ADD,
        ),
    )

    requests = []
    while not received_requests.empty():
        requests.append(received_requests.get())

    # There should be 3 failed requests and 1 successful request
    assert len(requests) == 4
    assert all(r["status"] == "failed" for r in requests[:3])
    assert requests[3]["status"] == "success"

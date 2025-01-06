from typing import Dict, Optional

import grpc
from overrides import overrides

from chromadb.api.types import GetResult, Metadata, QueryResult
from chromadb.config import System
from chromadb.errors import VersionMismatchError
from chromadb.execution.executor.abstract import Executor
from chromadb.execution.expression.operator import Scan
from chromadb.execution.expression.plan import CountPlan, GetPlan, KNNPlan
from chromadb.proto import convert

from chromadb.proto.query_executor_pb2_grpc import QueryExecutorStub
from chromadb.proto.utils import RetryOnRpcErrorClientInterceptor
from chromadb.segment.impl.manager.distributed import DistributedSegmentManager
from chromadb.telemetry.opentelemetry.grpc import OtelInterceptor


def _clean_metadata(metadata: Optional[Metadata]) -> Optional[Metadata]:
    """Remove any chroma-specific metadata keys that the client shouldn't see from a metadata map."""
    if not metadata:
        return None
    result = {}
    for k, v in metadata.items():
        if not k.startswith("chroma:"):
            result[k] = v
    if len(result) == 0:
        return None
    return result


def _uri(metadata: Optional[Metadata]) -> Optional[str]:
    """Retrieve the uri (if any) from a Metadata map"""

    if metadata and "chroma:uri" in metadata:
        return str(metadata["chroma:uri"])
    return None


class DistributedExecutor(Executor):
    _grpc_stub_pool: Dict[str, QueryExecutorStub]
    _manager: DistributedSegmentManager
    _request_timeout_seconds: int

    def __init__(self, system: System):
        super().__init__(system)
        self._grpc_stub_pool = dict()
        self._manager = self.require(DistributedSegmentManager)
        self._request_timeout_seconds = system.settings.require(
            "chroma_query_request_timeout_seconds"
        )

    @overrides
    def count(self, plan: CountPlan) -> int:
        executor = self._grpc_executuor_stub(plan.scan)
        try:
            count_result = executor.Count(convert.to_proto_count_plan(plan))
        except grpc.RpcError as rpc_error:
            raise rpc_error
        return convert.from_proto_count_result(count_result)

    @overrides
    def get(self, plan: GetPlan) -> GetResult:
        executor = self._grpc_executuor_stub(plan.scan)
        try:
            get_result = executor.Get(convert.to_proto_get_plan(plan))
        except grpc.RpcError as rpc_error:
            raise rpc_error
        records = convert.from_proto_get_result(get_result)

        ids = [record["id"] for record in records]
        embeddings = (
            [record["embedding"] for record in records]
            if plan.projection.embedding
            else None
        )
        documents = (
            [record["document"] for record in records]
            if plan.projection.document
            else None
        )
        uris = (
            [_uri(record["metadata"]) for record in records]
            if plan.projection.uri
            else None
        )
        metadatas = (
            [_clean_metadata(record["metadata"]) for record in records]
            if plan.projection.metadata
            else None
        )

        # TODO: Fix typing
        return GetResult(
            ids=ids,
            embeddings=embeddings,  # type: ignore[typeddict-item]
            documents=documents,  # type: ignore[typeddict-item]
            uris=uris,  # type: ignore[typeddict-item]
            data=None,
            metadatas=metadatas,  # type: ignore[typeddict-item]
            included=plan.projection.included,
        )

    @overrides
    def knn(self, plan: KNNPlan) -> QueryResult:
        executor = self._grpc_executuor_stub(plan.scan)
        try:
            knn_result = executor.KNN(convert.to_proto_knn_plan(plan))
        except grpc.RpcError as rpc_error:
            raise rpc_error
        results = convert.from_proto_knn_batch_result(knn_result)

        ids = [[record["record"]["id"] for record in records] for records in results]
        embeddings = (
            [
                [record["record"]["embedding"] for record in records]
                for records in results
            ]
            if plan.projection.embedding
            else None
        )
        documents = (
            [
                [record["record"]["document"] for record in records]
                for records in results
            ]
            if plan.projection.document
            else None
        )
        uris = (
            [
                [_uri(record["record"]["metadata"]) for record in records]
                for records in results
            ]
            if plan.projection.uri
            else None
        )
        metadatas = (
            [
                [_clean_metadata(record["record"]["metadata"]) for record in records]
                for records in results
            ]
            if plan.projection.metadata
            else None
        )
        distances = (
            [[record["distance"] for record in records] for records in results]
            if plan.projection.rank
            else None
        )

        # TODO: Fix typing
        return QueryResult(
            ids=ids,
            embeddings=embeddings,  # type: ignore[typeddict-item]
            documents=documents,  # type: ignore[typeddict-item]
            uris=uris,  # type: ignore[typeddict-item]
            data=None,
            metadatas=metadatas,  # type: ignore[typeddict-item]
            distances=distances,  # type: ignore[typeddict-item]
            included=plan.projection.included,
        )

    def _grpc_executuor_stub(self, scan: Scan) -> QueryExecutorStub:
        # Since grpc endpoint is endpoint is determined by collection uuid,
        # the endpoint should be the same for all segments of the same collection
        grpc_url = self._manager.get_endpoint(scan.record)
        if grpc_url not in self._grpc_stub_pool:
            channel = grpc.insecure_channel(grpc_url)
            interceptors = [OtelInterceptor(), RetryOnRpcErrorClientInterceptor()]
            channel = grpc.intercept_channel(channel, *interceptors)
            self._grpc_stub_pool[grpc_url] = QueryExecutorStub(channel)  # type: ignore[no-untyped-call]

        return self._grpc_stub_pool[grpc_url]

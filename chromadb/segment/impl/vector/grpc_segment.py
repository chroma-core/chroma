from overrides import EnforceOverrides, override
from typing import List, Optional, Sequence
from chromadb.config import System
from chromadb.proto.convert import (
    from_proto_vector_embedding_record,
    from_proto_vector_query_result,
    to_proto_request_version_context,
    to_proto_vector,
)
from chromadb.proto.utils import RetryOnRpcErrorClientInterceptor
from chromadb.segment import VectorReader
from chromadb.segment.impl.vector.hnsw_params import PersistentHnswParams
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.opentelemetry.grpc import OtelInterceptor
from chromadb.types import (
    Metadata,
    RequestVersionContext,
    ScalarEncoding,
    Segment,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)
from chromadb.proto.chroma_pb2_grpc import VectorReaderStub
from chromadb.proto.chroma_pb2 import (
    GetVectorsRequest,
    GetVectorsResponse,
    QueryVectorsRequest,
    QueryVectorsResponse,
)
import grpc


class GrpcVectorSegment(VectorReader, EnforceOverrides):
    _vector_reader_stub: VectorReaderStub
    _segment: Segment
    _request_timeout_seconds: int

    def __init__(self, system: System, segment: Segment):
        # TODO: move to start() method
        # TODO: close channel in stop() method
        if segment["metadata"] is None or segment["metadata"]["grpc_url"] is None:
            raise Exception("Missing grpc_url in segment metadata")

        channel = grpc.insecure_channel(segment["metadata"]["grpc_url"])
        interceptors = [OtelInterceptor(), RetryOnRpcErrorClientInterceptor()]
        channel = grpc.intercept_channel(channel, *interceptors)
        self._vector_reader_stub = VectorReaderStub(channel)  # type: ignore
        self._segment = segment
        self._request_timeout_seconds = system.settings.require(
            "chroma_query_request_timeout_seconds"
        )

    @trace_method("GrpcVectorSegment.get_vectors", OpenTelemetryGranularity.ALL)
    @override
    def get_vectors(
        self,
        request_version_context: RequestVersionContext,
        ids: Optional[Sequence[str]] = None,
    ) -> Sequence[VectorEmbeddingRecord]:
        request = GetVectorsRequest(
            ids=ids,
            segment_id=self._segment["id"].hex,
            collection_id=self._segment["collection"].hex,
            version_context=to_proto_request_version_context(request_version_context),
        )
        response: GetVectorsResponse = self._vector_reader_stub.GetVectors(
            request,
            timeout=self._request_timeout_seconds,
        )
        results: List[VectorEmbeddingRecord] = []
        for vector in response.records:
            result = from_proto_vector_embedding_record(vector)
            results.append(result)
        return results

    @trace_method("GrpcVectorSegment.query_vectors", OpenTelemetryGranularity.ALL)
    @override
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        request = QueryVectorsRequest(
            vectors=[
                to_proto_vector(vector=v, encoding=ScalarEncoding.FLOAT32)
                for v in query["vectors"]
            ],
            k=query["k"],
            allowed_ids=query["allowed_ids"],
            include_embeddings=query["include_embeddings"],
            segment_id=self._segment["id"].hex,
            collection_id=self._segment["collection"].hex,
            version_context=to_proto_request_version_context(
                query["request_version_context"]
            ),
        )
        response: QueryVectorsResponse = self._vector_reader_stub.QueryVectors(
            request,
            timeout=self._request_timeout_seconds,
        )
        results: List[List[VectorQueryResult]] = []
        for result in response.results:
            curr_result: List[VectorQueryResult] = []
            for r in result.results:
                curr_result.append(from_proto_vector_query_result(r))
            results.append(curr_result)
        return results

    @override
    def count(self, request_version_context: RequestVersionContext) -> int:
        raise NotImplementedError()

    @override
    def max_seqid(self) -> int:
        return 0

    @staticmethod
    @override
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        # Great example of why language sharing is nice.
        segment_metadata = PersistentHnswParams.extract(metadata)
        return segment_metadata

    @override
    def delete(self) -> None:
        raise NotImplementedError()

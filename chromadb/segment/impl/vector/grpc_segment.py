from chromadb.api.configuration import (
    CollectionConfigurationInternal,
    HNSWConfigurationInternal,
)
from chromadb.proto.utils import get_default_grpc_options
from overrides import EnforceOverrides, override
from typing import List, Optional, Sequence, cast
from chromadb.config import System
from chromadb.proto.convert import (
    from_proto_vector_embedding_record,
    from_proto_vector_query_result,
    to_proto_vector,
)
from chromadb.segment import VectorReader
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.telemetry.opentelemetry.grpc import OtelInterceptor
from chromadb.types import (
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

    def __init__(self, system: System, segment: Segment):
        # TODO: move to start() method
        # TODO: close channel in stop() method
        if segment["metadata"] is None or segment["metadata"]["grpc_url"] is None:
            raise Exception("Missing grpc_url in segment metadata")

        channel = grpc.insecure_channel(
            segment["metadata"]["grpc_url"], options=get_default_grpc_options()
        )
        interceptors = [OtelInterceptor()]
        channel = grpc.intercept_channel(channel, *interceptors)
        self._vector_reader_stub = VectorReaderStub(channel)  # type: ignore
        self._segment = segment

    @trace_method("GrpcVectorSegment.get_vectors", OpenTelemetryGranularity.ALL)
    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        request = GetVectorsRequest(ids=ids, segment_id=self._segment["id"].hex)
        response: GetVectorsResponse = self._vector_reader_stub.GetVectors(request)
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
        )
        response: QueryVectorsResponse = self._vector_reader_stub.QueryVectors(request)
        results: List[List[VectorQueryResult]] = []
        for result in response.results:
            curr_result: List[VectorQueryResult] = []
            for r in result.results:
                curr_result.append(from_proto_vector_query_result(r))
            results.append(curr_result)
        return results

    @override
    def count(self) -> int:
        raise NotImplementedError()

    @override
    def max_seqid(self) -> int:
        return 0

    @staticmethod
    @override
    def configuration_from_collection_configuration(
        collection_configuration: CollectionConfigurationInternal,
    ) -> HNSWConfigurationInternal:
        return cast(
            HNSWConfigurationInternal,
            collection_configuration.get_parameter("hnsw_configuration").value,
        )

    @staticmethod
    @override
    def configuration_from_collection_configuration(
        collection_configuration: CollectionConfigurationInternal,
    ) -> HNSWConfigurationInternal:
        print(
            "HAMMAD DDEBUG IN GRPC_SEGMENT, configuration_from_collection_configuration"
        )
        return cast(
            HNSWConfigurationInternal,
            collection_configuration.get_parameter("hnsw_configuration").value,
        )

    @override
    def delete(self) -> None:
        raise NotImplementedError()

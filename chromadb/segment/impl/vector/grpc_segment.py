from overrides import EnforceOverrides, override
from typing import Optional, Sequence
from chromadb.config import System
from chromadb.segment import MetadataReader, VectorReader
from chromadb.types import (
    Segment,
    VectorEmbeddingRecord,
    VectorQuery,
    VectorQueryResult,
)
from chromadb.proto.chroma_pb2_grpc import VectorReaderStub
from chromadb.proto.chroma_pb2 import GetVectorsRequest, GetVectorsResponse
import grpc


class GrpcVectorSegment(VectorReader, EnforceOverrides):
    _vector_reader_stub: VectorReaderStub

    def __init__(self, system: System, Segment: Segment):
        # TODO: appropriately parameterize this - it should be passed in from the segment manager
        channel = grpc.insecure_channel("segment-server:50051")
        self._vector_reader_stub = VectorReaderStub(channel)

    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        request = GetVectorsRequest(ids=ids)
        response: GetVectorsResponse = self._vector_reader_stub.GetVectors(request)
        for vector in response.records:
            print(vector)
        return []

    @override
    def query_vectors(
        self, query: VectorQuery
    ) -> Sequence[Sequence[VectorQueryResult]]:
        return super().query_vectors(query)

    @override
    def count(self) -> int:
        return 0

    @override
    def max_seqid(self) -> int:
        return 0

    @override
    def propagate_collection_metadata(self, metadata: Metadata) -> Optional[Metadata]:
        return None

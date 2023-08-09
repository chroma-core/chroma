from overrides import EnforceOverrides, override
from typing import Optional, Sequence
from chromadb.config import System
from chromadb.proto.convert import from_proto_vector_embedding_record
from chromadb.segment import MetadataReader, VectorReader
from chromadb.segment.impl.vector.hnsw_params import PersistentHnswParams
from chromadb.types import (
    Metadata,
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
    _segment: Segment

    def __init__(self, system: System, segment: Segment):
        # TODO: appropriately parameterize this - it should be passed in from the segment manager
        # TODO: move to start() method
        # TODO: close channel in stop() method
        channel = grpc.insecure_channel("segment-server:50051")
        self._vector_reader_stub = VectorReaderStub(channel)
        self._segment = segment

    @override
    def get_vectors(
        self, ids: Optional[Sequence[str]] = None
    ) -> Sequence[VectorEmbeddingRecord]:
        request = GetVectorsRequest(ids=ids, segment_id=self._segment["id"].hex)
        response: GetVectorsResponse = self._vector_reader_stub.GetVectors(request)
        results: Sequence[VectorEmbeddingRecord] = []
        for vector in response.records:
            result = from_proto_vector_embedding_record(vector)
            results.append(result)
        return results

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

    @staticmethod
    @override
    def propagate_collection_metadata(metadata: Metadata) -> Optional[Metadata]:
        # TODO: should this be a rpc?
        # Great example of why language sharing is nice....but also strongly coupling
        segment_metadata = PersistentHnswParams.extract(metadata)
        return segment_metadata

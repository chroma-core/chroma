from typing import Dict, Generator, List, Optional, Sequence

from chromadb.config import Settings, System
from chromadb.segment.impl.metadata.grpc_segment import GrpcMetadataSegment
from chromadb.types import (
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
)
import chromadb.proto.chroma_pb2 as pb


# Note: trying to start() this segment will cause it to error since it doesn't
# have a remote server to talk to. This is only suitable for testing the
# python <-> proto translation logic.
def unstarted_grpc_metadata_segment() -> GrpcMetadataSegment:
    settings = Settings(
        allow_reset=True,
    )
    system = System(settings)
    grpc_metadata_segment = GrpcMetadataSegment(system)
    return grpc_metadata_segment


def test_basic_grpc_metadata_segment() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    assert md_segment is not None

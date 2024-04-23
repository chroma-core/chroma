from typing import Dict, Generator, List, Optional, Sequence
import uuid

from chromadb.config import Settings, System
from chromadb.segment.impl.metadata.grpc_segment import GrpcMetadataSegment
from chromadb.types import (
    Segment,
    SegmentScope,
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
    segment = Segment(
        id=uuid.uuid4(),
        type="test",
        scope=SegmentScope.METADATA,
        collection=None,
        metadata={
            "grpc_url": "test",
        }
    )
    grpc_metadata_segment = GrpcMetadataSegment(
        system=system,
        segment=segment,
    )
    return grpc_metadata_segment


def test_where_document_not_contains_to_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {"$not_contains": "test"}
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS


def test_where_document_contains_to_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {"$contains": "test"}
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.CONTAINS


def test_where_document_and_to_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {
        "$and": [
            {"$contains": "test"},
            {"$not_contains": "test"},
        ]
    }
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.AND
    assert len(children_pb.children) == 2

    children = children_pb.children
    for child in children:
        assert child.HasField("direct")
        assert child.direct.document == "test"
    # Protobuf retains the order of repeated fields so this is safe.
    assert children[0].direct.operator == pb.WhereDocumentOperator.CONTAINS
    assert children[1].direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS


def test_where_document_or_to_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {
        "$or": [
            {"$contains": "test"},
            {"$not_contains": "test"},
        ]
    }
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.OR
    assert len(children_pb.children) == 2

    children = children_pb.children
    for child in children:
        assert child.HasField("direct")
        assert child.direct.document == "test"
    # Protobuf retains the order of repeated fields so this is safe.
    assert children[0].direct.operator == pb.WhereDocumentOperator.CONTAINS
    assert children[1].direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS


def test_where_document_nested_boolean_operators() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {
        "$and": [
            {"$or": [
                {"$contains": "test"},
                {"$not_contains": "test"},
            ]},
            {"$or": [
                {"$contains": "test"},
                {"$not_contains": "test"},
            ]},
        ]
    }
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.AND
    assert len(children_pb.children) == 2

    children = children_pb.children
    for child in children:
        assert child.HasField("children")
        assert len(child.children.children) == 2

        nested_children = child.children.children
        for nested_child in nested_children:
            assert nested_child.HasField("direct")
            assert nested_child.direct.document == "test"
        # Protobuf retains the order of repeated fields so this is safe.
        assert nested_children[0].direct.operator == pb.WhereDocumentOperator.CONTAINS
        assert nested_children[1].direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS

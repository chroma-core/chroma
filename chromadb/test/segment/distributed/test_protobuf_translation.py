from typing import Dict, Generator, List, Optional, Sequence
import uuid

from chromadb.config import Settings, System
from chromadb.segment.impl.metadata.grpc_segment import GrpcMetadataSegment
from chromadb.types import (
    Segment,
    SegmentScope,
    UpdateMetadata,
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


def test_where_document_to_proto_not_contains() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {"$not_contains": "test"}
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS


def test_where_document_to_proto_contains_to_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where_document: WhereDocument = {"$contains": "test"}
    proto = md_segment._where_document_to_proto(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.CONTAINS


def test_where_document_to_proto_and() -> None:
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


def test_where_document_to_proto_or() -> None:
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


def test_where_document_to_proto_nested_boolean_operators() -> None:
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


def test_where_to_proto_string_value() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "test": "value",
    }
    proto: pb.Where = md_segment._where_to_proto(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_string_operand")
    assert d.single_string_operand.value == "value"


def test_where_to_proto_int_value() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "test": 1,
    }
    proto = md_segment._where_to_proto(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_int_operand")
    assert d.single_int_operand.value == 1


def test_where_to_proto_double_value() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "test": 1.0,
    }
    proto = md_segment._where_to_proto(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_double_operand")
    assert d.single_double_operand.value == 1.0


def test_where_to_proto_and() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "$and": [
            {"test": 1},
            {"test": "value"},
        ]
    }
    proto = md_segment._where_to_proto(where)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.AND

    children = children_pb.children
    assert len(children) == 2
    for child in children:
        assert child.HasField("direct_comparison")
        assert child.direct_comparison.key == "test"

    assert children[0].direct_comparison.HasField("single_int_operand")
    assert children[0].direct_comparison.single_int_operand.value == 1
    assert children[1].direct_comparison.HasField("single_string_operand")
    assert children[1].direct_comparison.single_string_operand.value == "value"


def test_where_to_proto_or() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "$or": [
            {"test": 1},
            {"test": "value"},
        ]
    }
    proto = md_segment._where_to_proto(where)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.OR

    children = children_pb.children
    assert len(children) == 2
    for child in children:
        assert child.HasField("direct_comparison")
        assert child.direct_comparison.key == "test"

    assert children[0].direct_comparison.HasField("single_int_operand")
    assert children[0].direct_comparison.single_int_operand.value == 1
    assert children[1].direct_comparison.HasField("single_string_operand")
    assert children[1].direct_comparison.single_string_operand.value == "value"


def test_where_to_proto_nested_boolean_operators() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    where: Where = {
        "$and": [
            {"$or": [
                {"test": 1},
                {"test": "value"},
            ]},
            {"$or": [
                {"test": 1},
                {"test": "value"},
            ]},
        ]
    }
    proto = md_segment._where_to_proto(where)
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
            assert nested_child.HasField("direct_comparison")
            assert nested_child.direct_comparison.key == "test"

        assert nested_children[0].direct_comparison.HasField("single_int_operand")
        assert nested_children[0].direct_comparison.single_int_operand.value == 1
        assert nested_children[1].direct_comparison.HasField("single_string_operand")
        assert nested_children[1].direct_comparison.single_string_operand.value == "value"


def test_metadata_embedding_record_string_from_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        string_value="test_value",
    )
    update: pb.UpdateMetadata = pb.UpdateMetadata(
        metadata={"test_key": val},
    )
    record: pb.MetadataEmbeddingRecord = pb.MetadataEmbeddingRecord(
        id="test_id",
        metadata=update,
    )

    mdr: MetadataEmbeddingRecord = md_segment._from_proto(record)
    assert mdr["id"] == "test_id"
    assert mdr["metadata"]
    assert mdr["metadata"]["test_key"] == "test_value"


def test_metadata_embedding_record_int_from_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        int_value=1,
    )
    update: pb.UpdateMetadata = pb.UpdateMetadata(
        metadata={"test_key": val},
    )
    record: pb.MetadataEmbeddingRecord = pb.MetadataEmbeddingRecord(
        id="test_id",
        metadata=update,
    )

    mdr: MetadataEmbeddingRecord = md_segment._from_proto(record)
    assert mdr["id"] == "test_id"
    assert mdr["metadata"]
    assert mdr["metadata"]["test_key"] == 1


def test_metadata_embedding_record_double_from_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        float_value=1.0,
    )
    update: pb.UpdateMetadata = pb.UpdateMetadata(
        metadata={"test_key": val},
    )
    record: pb.MetadataEmbeddingRecord = pb.MetadataEmbeddingRecord(
        id="test_id",
        metadata=update,
    )

    mdr: MetadataEmbeddingRecord = md_segment._from_proto(record)
    assert mdr["id"] == "test_id"
    assert mdr["metadata"]
    assert mdr["metadata"]["test_key"] == 1.0


def test_metadata_embedding_record_heterogeneous_from_proto() -> None:
    md_segment = unstarted_grpc_metadata_segment()
    val1: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        string_value="test_value",
    )
    val2: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        int_value=1,
    )
    val3: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        float_value=1.0,
    )
    update: pb.UpdateMetadata = pb.UpdateMetadata(
        metadata={
            "test_key1": val1,
            "test_key2": val2,
            "test_key3": val3,
        },
    )
    record: pb.MetadataEmbeddingRecord = pb.MetadataEmbeddingRecord(
        id="test_id",
        metadata=update,
    )

    mdr: MetadataEmbeddingRecord = md_segment._from_proto(record)
    assert mdr["id"] == "test_id"
    assert mdr["metadata"]
    assert mdr["metadata"]["test_key1"] == "test_value"
    assert mdr["metadata"]["test_key2"] == 1
    assert mdr["metadata"]["test_key3"] == 1.0

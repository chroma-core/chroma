import uuid
from chromadb.proto import convert
from chromadb.segment import SegmentType
from chromadb.types import (
    Collection,
    CollectionConfigurationInternal,
    Segment,
    SegmentScope,
    Where,
    WhereDocument,
)
import chromadb.proto.chroma_pb2 as pb
import chromadb.proto.query_executor_pb2 as query_pb

def test_collection_to_proto() -> None:
    collection = Collection(
        id=uuid.uuid4(),
        name="test_collection",
        configuration=CollectionConfigurationInternal(),
        metadata={"hnsw_m": 128},
        dimension=512,
        tenant="test_tenant",
        database="test_database",
        version=1,
        log_position=42,
    )

    assert convert.to_proto_collection(collection) == pb.Collection(
        id=collection.id.hex,
        name="test_collection",
        configuration_json_str=CollectionConfigurationInternal().to_json_str(),
        metadata=pb.UpdateMetadata(metadata={"hnsw_m": pb.UpdateMetadataValue(int_value=128)}),
        dimension=512,
        tenant="test_tenant",
        database="test_database",
        version=1,
        log_position=42,
    )

def test_collection_from_proto() -> None:
    proto = pb.Collection(
        id=uuid.uuid4().hex,
        name="test_collection",
        configuration_json_str=CollectionConfigurationInternal().to_json_str(),
        metadata=pb.UpdateMetadata(metadata={"hnsw_m": pb.UpdateMetadataValue(int_value=128)}),
        dimension=512,
        tenant="test_tenant",
        database="test_database",
        version=1,
        log_position=42,
    )
    assert convert.from_proto_collection(proto) == Collection(
        id=uuid.UUID(proto.id),
        name="test_collection",
        configuration=CollectionConfigurationInternal(),
        metadata={"hnsw_m": 128},
        dimension=512,
        tenant="test_tenant",
        database="test_database",
        version=1,
        log_position=42,
    )

def test_segment_to_proto() -> None:
    segment = Segment(
        id=uuid.uuid4(),
        type=SegmentType.HNSW_DISTRIBUTED.value,
        scope=SegmentScope.VECTOR,
        collection=uuid.uuid4(),
        metadata={"hnsw_m": 128},
        file_paths={"name": ["path_0", "path_1"]},
    )
    assert convert.to_proto_segment(segment) == pb.Segment(
        id=segment["id"].hex,
        type=SegmentType.HNSW_DISTRIBUTED.value,
        scope=pb.SegmentScope.VECTOR,
        collection=segment["collection"].hex,
        metadata=pb.UpdateMetadata(metadata={"hnsw_m": pb.UpdateMetadataValue(int_value=128)}),
        file_paths={"name": pb.FilePaths(paths=["path_0", "path_1"])},
    )

def test_segment_from_proto() -> None:
    proto = pb.Segment(
        id=uuid.uuid4().hex,
        type=SegmentType.HNSW_DISTRIBUTED.value,
        scope=pb.SegmentScope.VECTOR,
        collection=uuid.uuid4().hex,
        metadata=pb.UpdateMetadata(metadata={"hnsw_m": pb.UpdateMetadataValue(int_value=128)}),
        file_paths={"name": pb.FilePaths(paths=["path_0", "path_1"])},
    )
    assert convert.from_proto_segment(proto) == Segment(
        id=uuid.UUID(proto.id),
        type=SegmentType.HNSW_DISTRIBUTED.value,
        scope=SegmentScope.VECTOR,
        collection=uuid.UUID(proto.collection),
        metadata={"hnsw_m": 128},
        file_paths={"name": ["path_0", "path_1"]},
    )

def test_where_document_to_proto_not_contains() -> None:
    where_document: WhereDocument = {"$not_contains": "test"}
    proto = convert.to_proto_where_document(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS


def test_where_document_to_proto_contains_to_proto() -> None:
    where_document: WhereDocument = {"$contains": "test"}
    proto = convert.to_proto_where_document(where_document)
    assert proto.HasField("direct")
    assert proto.direct.document == "test"
    assert proto.direct.operator == pb.WhereDocumentOperator.CONTAINS


def test_where_document_to_proto_and() -> None:
    where_document: WhereDocument = {
        "$and": [
            {"$contains": "test"},
            {"$not_contains": "test"},
        ]
    }
    proto = convert.to_proto_where_document(where_document)
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
    where_document: WhereDocument = {
        "$or": [
            {"$contains": "test"},
            {"$not_contains": "test"},
        ]
    }
    proto = convert.to_proto_where_document(where_document)
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
    where_document: WhereDocument = {
        "$and": [
            {
                "$or": [
                    {"$contains": "test"},
                    {"$not_contains": "test"},
                ]
            },
            {
                "$or": [
                    {"$contains": "test"},
                    {"$not_contains": "test"},
                ]
            },
        ]
    }
    proto = convert.to_proto_where_document(where_document)
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
        assert (
            nested_children[1].direct.operator == pb.WhereDocumentOperator.NOT_CONTAINS
        )


def test_where_to_proto_string_value() -> None:
    where: Where = {
        "test": "value",
    }
    proto = convert.to_proto_where(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_string_operand")
    assert d.single_string_operand.value == "value"


def test_where_to_proto_int_value() -> None:
    where: Where = {
        "test": 1,
    }
    proto = convert.to_proto_where(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_int_operand")
    assert d.single_int_operand.value == 1


def test_where_to_proto_double_value() -> None:
    where: Where = {
        "test": 1.0,
    }
    proto = convert.to_proto_where(where)
    assert proto.HasField("direct_comparison")
    d = proto.direct_comparison
    assert d.key == "test"
    assert d.HasField("single_double_operand")
    assert d.single_double_operand.value == 1.0


def test_where_to_proto_and() -> None:
    where: Where = {
        "$and": [
            {"test": 1},
            {"test": "value"},
        ]
    }
    proto = convert.to_proto_where(where)
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
    where: Where = {
        "$or": [
            {"test": 1},
            {"test": "value"},
        ]
    }
    proto = convert.to_proto_where(where)
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
    where: Where = {
        "$and": [
            {
                "$or": [
                    {"test": 1},
                    {"test": "value"},
                ]
            },
            {
                "$or": [
                    {"test": 1},
                    {"test": "value"},
                ]
            },
        ]
    }
    proto = convert.to_proto_where(where)
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
        assert (
            nested_children[1].direct_comparison.single_string_operand.value == "value"
        )


def test_where_to_proto_float_operator() -> None:
    where: Where = {
        "$and": [
            {"test1": 1.0},
            {"test2": 2.0},
        ]
    }
    proto = convert.to_proto_where(where)
    assert proto.HasField("children")
    children_pb = proto.children
    assert children_pb.operator == pb.BooleanOperator.AND
    assert len(children_pb.children) == 2

    children = children_pb.children
    child_0 = children[0]
    assert child_0.HasField("direct_comparison")
    assert child_0.direct_comparison.key == "test1"
    assert child_0.direct_comparison.HasField("single_double_operand")
    assert child_0.direct_comparison.single_double_operand.value == 1.0

    child_1 = children[1]
    assert child_1.HasField("direct_comparison")
    assert child_1.direct_comparison.key == "test2"
    assert child_1.direct_comparison.HasField("single_double_operand")
    assert child_1.direct_comparison.single_double_operand.value == 2.0


def test_projection_record_from_proto() -> None:
    float_val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        float_value=1.0,
    )
    int_val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        int_value=2,
    )
    str_val: pb.UpdateMetadataValue = pb.UpdateMetadataValue(
        string_value="three",
    )
    update: pb.UpdateMetadata = pb.UpdateMetadata(
        metadata={"float_key": float_val, "int_key": int_val, "str_key": str_val},
    )
    record: query_pb.ProjectionRecord = query_pb.ProjectionRecord(
        id="test_id",
        document="document",
        metadata=update,
    )

    projection_record = convert.from_proto_projection_record(record)

    assert projection_record["id"] == "test_id"
    assert projection_record["metadata"]
    assert projection_record["metadata"]["float_key"] == 1.0
    assert projection_record["metadata"]["int_key"] == 2
    assert projection_record["metadata"]["str_key"] == "three"

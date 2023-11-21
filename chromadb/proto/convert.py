import array
from uuid import UUID
from typing import Dict, Optional, Tuple, Union, cast
from chromadb.api.types import Embedding
import chromadb.proto.chroma_pb2 as proto
from chromadb.utils.messageid import bytes_to_int, int_to_bytes
from chromadb.types import (
    Collection,
    EmbeddingRecord,
    Metadata,
    Operation,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
    SubmitEmbeddingRecord,
    UpdateMetadata,
    Vector,
    VectorEmbeddingRecord,
    VectorQueryResult,
)


# TODO: Unit tests for this file, handling optional states etc


def to_proto_vector(vector: Vector, encoding: ScalarEncoding) -> proto.Vector:
    if encoding == ScalarEncoding.FLOAT32:
        as_bytes = array.array("f", vector).tobytes()
        proto_encoding = proto.ScalarEncoding.FLOAT32
    elif encoding == ScalarEncoding.INT32:
        as_bytes = array.array("i", vector).tobytes()
        proto_encoding = proto.ScalarEncoding.INT32
    else:
        raise ValueError(
            f"Unknown encoding {encoding}, expected one of {ScalarEncoding.FLOAT32} \
            or {ScalarEncoding.INT32}"
        )

    return proto.Vector(dimension=len(vector), vector=as_bytes, encoding=proto_encoding)


def from_proto_vector(vector: proto.Vector) -> Tuple[Embedding, ScalarEncoding]:
    encoding = vector.encoding
    as_array: array.array[float] | array.array[int]
    if encoding == proto.ScalarEncoding.FLOAT32:
        as_array = array.array("f")
        out_encoding = ScalarEncoding.FLOAT32
    elif encoding == proto.ScalarEncoding.INT32:
        as_array = array.array("i")
        out_encoding = ScalarEncoding.INT32
    else:
        raise ValueError(
            f"Unknown encoding {encoding}, expected one of \
            {proto.ScalarEncoding.FLOAT32} or {proto.ScalarEncoding.INT32}"
        )

    as_array.frombytes(vector.vector)
    return (as_array.tolist(), out_encoding)


def from_proto_operation(operation: proto.Operation) -> Operation:
    if operation == proto.Operation.ADD:
        return Operation.ADD
    elif operation == proto.Operation.UPDATE:
        return Operation.UPDATE
    elif operation == proto.Operation.UPSERT:
        return Operation.UPSERT
    elif operation == proto.Operation.DELETE:
        return Operation.DELETE
    else:
        # TODO: full error
        raise RuntimeError(f"Unknown operation {operation}")


def from_proto_metadata(metadata: proto.UpdateMetadata) -> Optional[Metadata]:
    return cast(Optional[Metadata], _from_proto_metadata_handle_none(metadata, False))


def from_proto_update_metadata(
    metadata: proto.UpdateMetadata,
) -> Optional[UpdateMetadata]:
    return cast(
        Optional[UpdateMetadata], _from_proto_metadata_handle_none(metadata, True)
    )


def _from_proto_metadata_handle_none(
    metadata: proto.UpdateMetadata, is_update: bool
) -> Optional[Union[UpdateMetadata, Metadata]]:
    if not metadata.metadata:
        return None
    out_metadata: Dict[str, Union[str, int, float, None]] = {}
    for key, value in metadata.metadata.items():
        if value.HasField("string_value"):
            out_metadata[key] = value.string_value
        elif value.HasField("int_value"):
            out_metadata[key] = value.int_value
        elif value.HasField("float_value"):
            out_metadata[key] = value.float_value
        elif is_update:
            out_metadata[key] = None
        else:
            raise ValueError(f"Metadata key {key} value cannot be None")
    return out_metadata


def to_proto_update_metadata(metadata: UpdateMetadata) -> proto.UpdateMetadata:
    return proto.UpdateMetadata(
        metadata={k: to_proto_metadata_update_value(v) for k, v in metadata.items()}
    )


def from_proto_submit(
    submit_embedding_record: proto.SubmitEmbeddingRecord, seq_id: SeqId
) -> EmbeddingRecord:
    embedding, encoding = from_proto_vector(submit_embedding_record.vector)
    record = EmbeddingRecord(
        id=submit_embedding_record.id,
        seq_id=seq_id,
        embedding=embedding,
        encoding=encoding,
        metadata=from_proto_update_metadata(submit_embedding_record.metadata),
        operation=from_proto_operation(submit_embedding_record.operation),
        collection_id=UUID(hex=submit_embedding_record.collection_id),
    )
    return record


def from_proto_segment(segment: proto.Segment) -> Segment:
    return Segment(
        id=UUID(hex=segment.id),
        type=segment.type,
        scope=from_proto_segment_scope(segment.scope),
        topic=segment.topic if segment.HasField("topic") else None,
        collection=None
        if not segment.HasField("collection")
        else UUID(hex=segment.collection),
        metadata=from_proto_metadata(segment.metadata)
        if segment.HasField("metadata")
        else None,
    )


def to_proto_segment(segment: Segment) -> proto.Segment:
    return proto.Segment(
        id=segment["id"].hex,
        type=segment["type"],
        scope=to_proto_segment_scope(segment["scope"]),
        topic=segment["topic"],
        collection=None if segment["collection"] is None else segment["collection"].hex,
        metadata=None
        if segment["metadata"] is None
        else to_proto_update_metadata(segment["metadata"]),
    )


def from_proto_segment_scope(segment_scope: proto.SegmentScope) -> SegmentScope:
    if segment_scope == proto.SegmentScope.VECTOR:
        return SegmentScope.VECTOR
    elif segment_scope == proto.SegmentScope.METADATA:
        return SegmentScope.METADATA
    else:
        raise RuntimeError(f"Unknown segment scope {segment_scope}")


def to_proto_segment_scope(segment_scope: SegmentScope) -> proto.SegmentScope:
    if segment_scope == SegmentScope.VECTOR:
        return proto.SegmentScope.VECTOR
    elif segment_scope == SegmentScope.METADATA:
        return proto.SegmentScope.METADATA
    else:
        raise RuntimeError(f"Unknown segment scope {segment_scope}")


def to_proto_metadata_update_value(
    value: Union[str, int, float, None]
) -> proto.UpdateMetadataValue:
    if isinstance(value, str):
        return proto.UpdateMetadataValue(string_value=value)
    elif isinstance(value, int):
        return proto.UpdateMetadataValue(int_value=value)
    elif isinstance(value, float):
        return proto.UpdateMetadataValue(float_value=value)
    elif value is None:
        return proto.UpdateMetadataValue()
    else:
        raise ValueError(
            f"Unknown metadata value type {type(value)}, expected one of str, int, \
            float, or None"
        )


def from_proto_collection(collection: proto.Collection) -> Collection:
    return Collection(
        id=UUID(hex=collection.id),
        name=collection.name,
        topic=collection.topic,
        metadata=from_proto_metadata(collection.metadata)
        if collection.HasField("metadata")
        else None,
        dimension=collection.dimension
        if collection.HasField("dimension") and collection.dimension
        else None,
        database=collection.database,
        tenant=collection.tenant,
    )


def to_proto_collection(collection: Collection) -> proto.Collection:
    return proto.Collection(
        id=collection["id"].hex,
        name=collection["name"],
        topic=collection["topic"],
        metadata=None
        if collection["metadata"] is None
        else to_proto_update_metadata(collection["metadata"]),
        dimension=collection["dimension"],
        tenant=collection["tenant"],
        database=collection["database"],
    )


def to_proto_operation(operation: Operation) -> proto.Operation:
    if operation == Operation.ADD:
        return proto.Operation.ADD
    elif operation == Operation.UPDATE:
        return proto.Operation.UPDATE
    elif operation == Operation.UPSERT:
        return proto.Operation.UPSERT
    elif operation == Operation.DELETE:
        return proto.Operation.DELETE
    else:
        raise ValueError(
            f"Unknown operation {operation}, expected one of {Operation.ADD}, \
            {Operation.UPDATE}, {Operation.UPDATE}, or {Operation.DELETE}"
        )


def to_proto_submit(
    submit_record: SubmitEmbeddingRecord,
) -> proto.SubmitEmbeddingRecord:
    vector = None
    if submit_record["embedding"] is not None and submit_record["encoding"] is not None:
        vector = to_proto_vector(submit_record["embedding"], submit_record["encoding"])

    metadata = None
    if submit_record["metadata"] is not None:
        metadata = to_proto_update_metadata(submit_record["metadata"])

    return proto.SubmitEmbeddingRecord(
        id=submit_record["id"],
        vector=vector,
        metadata=metadata,
        operation=to_proto_operation(submit_record["operation"]),
        collection_id=submit_record["collection_id"].hex,
    )


def from_proto_vector_embedding_record(
    embedding_record: proto.VectorEmbeddingRecord,
) -> VectorEmbeddingRecord:
    return VectorEmbeddingRecord(
        id=embedding_record.id,
        seq_id=from_proto_seq_id(embedding_record.seq_id),
        embedding=from_proto_vector(embedding_record.vector)[0],
    )


def to_proto_vector_embedding_record(
    embedding_record: VectorEmbeddingRecord,
    encoding: ScalarEncoding,
) -> proto.VectorEmbeddingRecord:
    return proto.VectorEmbeddingRecord(
        id=embedding_record["id"],
        seq_id=to_proto_seq_id(embedding_record["seq_id"]),
        vector=to_proto_vector(embedding_record["embedding"], encoding),
    )


def from_proto_vector_query_result(
    vector_query_result: proto.VectorQueryResult,
) -> VectorQueryResult:
    return VectorQueryResult(
        id=vector_query_result.id,
        seq_id=from_proto_seq_id(vector_query_result.seq_id),
        distance=vector_query_result.distance,
        embedding=from_proto_vector(vector_query_result.vector)[0],
    )


def to_proto_seq_id(seq_id: SeqId) -> bytes:
    return int_to_bytes(seq_id)


def from_proto_seq_id(seq_id: bytes) -> SeqId:
    return bytes_to_int(seq_id)

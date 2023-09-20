import array
from typing import Optional, Tuple, Union
from chromadb.api.types import Embedding
import chromadb.proto.chroma_pb2 as proto
from chromadb.types import (
    EmbeddingRecord,
    Metadata,
    Operation,
    ScalarEncoding,
    SeqId,
    SubmitEmbeddingRecord,
    Vector,
)


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


def from_proto_operation(operation: proto.Operation.ValueType) -> Operation:
    if operation == proto.Operation.ADD:
        return Operation.ADD
    elif operation == proto.Operation.UPDATE:
        return Operation.UPDATE
    elif operation == proto.Operation.UPSERT:
        return Operation.UPSERT
    elif operation == proto.Operation.DELETE:
        return Operation.DELETE
    else:
        raise RuntimeError(f"Unknown operation {operation}")  # TODO: full error


def from_proto_metadata(metadata: proto.UpdateMetadata) -> Optional[Metadata]:
    if not metadata.metadata:
        return None
    out_metadata = {}
    for key, value in metadata.metadata.items():
        if value.HasField("string_value"):
            out_metadata[key] = value.string_value
        elif value.HasField("int_value"):
            out_metadata[key] = value.int_value
        elif value.HasField("float_value"):
            out_metadata[key] = value.float_value
        else:
            raise RuntimeError(f"Unknown metadata value type {value}")
    return out_metadata


def from_proto_submit(
    submit_embedding_record: proto.SubmitEmbeddingRecord, seq_id: SeqId
) -> EmbeddingRecord:
    embedding, encoding = from_proto_vector(submit_embedding_record.vector)
    record = EmbeddingRecord(
        id=submit_embedding_record.id,
        seq_id=seq_id,
        embedding=embedding,
        encoding=encoding,
        metadata=from_proto_metadata(submit_embedding_record.metadata),
        operation=from_proto_operation(submit_embedding_record.operation),
    )
    return record


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


def to_proto_operation(operation: Operation) -> proto.Operation.ValueType:
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
        metadata = {
            k: to_proto_metadata_update_value(v)
            for k, v in submit_record["metadata"].items()
        }

    return proto.SubmitEmbeddingRecord(
        id=submit_record["id"],
        vector=vector,
        metadata=proto.UpdateMetadata(metadata=metadata)
        if metadata is not None
        else None,
        operation=to_proto_operation(submit_record["operation"]),
    )

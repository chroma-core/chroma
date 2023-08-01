import array
from typing import Union
import chromadb.proto.chroma_pb2 as proto
from chromadb.types import Operation, ScalarEncoding, SubmitEmbeddingRecord, Vector


def to_proto_vector(vector: Vector, encoding: ScalarEncoding) -> proto.Vector:
    if encoding == ScalarEncoding.FLOAT32:
        as_bytes = array.array("f", vector).tobytes()
    elif encoding == ScalarEncoding.INT32:
        as_bytes = array.array("i", vector).tobytes()
    else:
        raise ValueError(
            f"Unknown encoding {encoding}, expected one of {ScalarEncoding.FLOAT32} \
            or {ScalarEncoding.INT32}"
        )

    return proto.Vector(dimension=len(vector), vector=as_bytes)


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
    if submit_record["embedding"] is not None:
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
        metadata=proto.UpdateMetadata(metadata=metadata),
        operation=to_proto_operation(submit_record["operation"]),
    )

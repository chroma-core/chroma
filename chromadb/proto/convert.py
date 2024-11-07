from uuid import UUID
from typing import Dict, Optional, Tuple, Union, cast
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.types import Embedding
import chromadb.proto.chroma_pb2 as proto
from chromadb.types import (
    Collection,
    LogRecord,
    Metadata,
    Operation,
    RequestVersionContext,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
    OperationRecord,
    UpdateMetadata,
    Vector,
    VectorEmbeddingRecord,
    VectorQueryResult,
)
from chromadb.errors import InvalidArgumentError
import numpy as np
from numpy.typing import NDArray


# TODO: Unit tests for this file, handling optional states etc
def to_proto_vector(vector: Vector, encoding: ScalarEncoding) -> proto.Vector:
    if encoding == ScalarEncoding.FLOAT32:
        as_bytes = np.array(vector, dtype=np.float32).tobytes()
        proto_encoding = proto.ScalarEncoding.FLOAT32
    elif encoding == ScalarEncoding.INT32:
        as_bytes = np.array(vector, dtype=np.int32).tobytes()
        proto_encoding = proto.ScalarEncoding.INT32
    else:
        raise InvalidArgumentError(
            f"Unknown encoding {encoding}, expected one of {ScalarEncoding.FLOAT32} \
            or {ScalarEncoding.INT32}"
        )

    return proto.Vector(dimension=vector.size, vector=as_bytes, encoding=proto_encoding)


def from_proto_vector(vector: proto.Vector) -> Tuple[Embedding, ScalarEncoding]:
    encoding = vector.encoding
    as_array: Union[NDArray[np.int32], NDArray[np.float32]]
    if encoding == proto.ScalarEncoding.FLOAT32:
        as_array = np.frombuffer(vector.vector, dtype=np.float32)
        out_encoding = ScalarEncoding.FLOAT32
    elif encoding == proto.ScalarEncoding.INT32:
        as_array = np.frombuffer(vector.vector, dtype=np.int32)
        out_encoding = ScalarEncoding.INT32
    else:
        raise InvalidArgumentError(
            f"Unknown encoding {encoding}, expected one of \
            {proto.ScalarEncoding.FLOAT32} or {proto.ScalarEncoding.INT32}"
        )

    return (as_array, out_encoding)


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
    out_metadata: Dict[str, Union[str, int, float, bool, None]] = {}
    for key, value in metadata.metadata.items():
        if value.HasField("bool_value"):
            out_metadata[key] = value.bool_value
        elif value.HasField("string_value"):
            out_metadata[key] = value.string_value
        elif value.HasField("int_value"):
            out_metadata[key] = value.int_value
        elif value.HasField("float_value"):
            out_metadata[key] = value.float_value
        elif is_update:
            out_metadata[key] = None
        else:
            raise InvalidArgumentError(f"Metadata key {key} value cannot be None")
    return out_metadata


def to_proto_update_metadata(metadata: UpdateMetadata) -> proto.UpdateMetadata:
    return proto.UpdateMetadata(
        metadata={k: to_proto_metadata_update_value(v) for k, v in metadata.items()}
    )


def from_proto_submit(
    operation_record: proto.OperationRecord, seq_id: SeqId
) -> LogRecord:
    embedding, encoding = from_proto_vector(operation_record.vector)
    record = LogRecord(
        log_offset=seq_id,
        record=OperationRecord(
            id=operation_record.id,
            embedding=embedding,
            encoding=encoding,
            metadata=from_proto_update_metadata(operation_record.metadata),
            operation=from_proto_operation(operation_record.operation),
        ),
    )
    return record


def from_proto_segment(segment: proto.Segment) -> Segment:
    return Segment(
        id=UUID(hex=segment.id),
        type=segment.type,
        scope=from_proto_segment_scope(segment.scope),
        collection=UUID(hex=segment.collection),
        metadata=from_proto_metadata(segment.metadata)
        if segment.HasField("metadata")
        else None,
    )


def to_proto_segment(segment: Segment) -> proto.Segment:
    return proto.Segment(
        id=segment["id"].hex,
        type=segment["type"],
        scope=to_proto_segment_scope(segment["scope"]),
        collection=segment["collection"].hex,
        metadata=None
        if segment["metadata"] is None
        else to_proto_update_metadata(segment["metadata"]),
    )


def from_proto_segment_scope(segment_scope: proto.SegmentScope) -> SegmentScope:
    if segment_scope == proto.SegmentScope.VECTOR:
        return SegmentScope.VECTOR
    elif segment_scope == proto.SegmentScope.METADATA:
        return SegmentScope.METADATA
    elif segment_scope == proto.SegmentScope.RECORD:
        return SegmentScope.RECORD
    else:
        raise RuntimeError(f"Unknown segment scope {segment_scope}")


def to_proto_segment_scope(segment_scope: SegmentScope) -> proto.SegmentScope:
    if segment_scope == SegmentScope.VECTOR:
        return proto.SegmentScope.VECTOR
    elif segment_scope == SegmentScope.METADATA:
        return proto.SegmentScope.METADATA
    elif segment_scope == SegmentScope.RECORD:
        return proto.SegmentScope.RECORD
    else:
        raise RuntimeError(f"Unknown segment scope {segment_scope}")


def to_proto_metadata_update_value(
    value: Union[str, int, float, bool, None]
) -> proto.UpdateMetadataValue:
    # Be careful with the order here. Since bools are a subtype of int in python,
    # isinstance(value, bool) and isinstance(value, int) both return true
    # for a value of bool type.
    if isinstance(value, bool):
        return proto.UpdateMetadataValue(bool_value=value)
    elif isinstance(value, str):
        return proto.UpdateMetadataValue(string_value=value)
    elif isinstance(value, int):
        return proto.UpdateMetadataValue(int_value=value)
    elif isinstance(value, float):
        return proto.UpdateMetadataValue(float_value=value)
    # None is used to delete the metadata key.
    elif value is None:
        return proto.UpdateMetadataValue()
    else:
        raise InvalidArgumentError(
            f"Unknown metadata value type {type(value)}, expected one of str, int, \
            float, or None"
        )


def from_proto_collection(collection: proto.Collection) -> Collection:
    return Collection(
        id=UUID(hex=collection.id),
        name=collection.name,
        configuration=CollectionConfigurationInternal.from_json_str(
            collection.configuration_json_str
        ),
        metadata=from_proto_metadata(collection.metadata)
        if collection.HasField("metadata")
        else None,
        dimension=collection.dimension
        if collection.HasField("dimension") and collection.dimension
        else None,
        database=collection.database,
        tenant=collection.tenant,
        version=collection.version,
        log_position=collection.log_position,
    )


def to_proto_collection(collection: Collection) -> proto.Collection:
    return proto.Collection(
        id=collection["id"].hex,
        name=collection["name"],
        configuration_json_str=collection.get_configuration().to_json_str(),
        metadata=None
        if collection["metadata"] is None
        else to_proto_update_metadata(collection["metadata"]),
        dimension=collection["dimension"],
        tenant=collection["tenant"],
        database=collection["database"],
        version=collection["version"],
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
        raise InvalidArgumentError(
            f"Unknown operation {operation}, expected one of {Operation.ADD}, \
            {Operation.UPDATE}, {Operation.UPDATE}, or {Operation.DELETE}"
        )


def to_proto_submit(
    submit_record: OperationRecord,
) -> proto.OperationRecord:
    vector = None
    if submit_record["embedding"] is not None and submit_record["encoding"] is not None:
        vector = to_proto_vector(submit_record["embedding"], submit_record["encoding"])

    metadata = None
    if submit_record["metadata"] is not None:
        metadata = to_proto_update_metadata(submit_record["metadata"])

    return proto.OperationRecord(
        id=submit_record["id"],
        vector=vector,
        metadata=metadata,
        operation=to_proto_operation(submit_record["operation"]),
    )


def from_proto_vector_embedding_record(
    embedding_record: proto.VectorEmbeddingRecord,
) -> VectorEmbeddingRecord:
    return VectorEmbeddingRecord(
        id=embedding_record.id,
        embedding=from_proto_vector(embedding_record.vector)[0],
    )


def to_proto_vector_embedding_record(
    embedding_record: VectorEmbeddingRecord,
    encoding: ScalarEncoding,
) -> proto.VectorEmbeddingRecord:
    return proto.VectorEmbeddingRecord(
        id=embedding_record["id"],
        vector=to_proto_vector(embedding_record["embedding"], encoding),
    )


def from_proto_vector_query_result(
    vector_query_result: proto.VectorQueryResult,
) -> VectorQueryResult:
    return VectorQueryResult(
        id=vector_query_result.id,
        distance=vector_query_result.distance,
        embedding=from_proto_vector(vector_query_result.vector)[0],
    )


def from_proto_request_version_context(
    request_version_context: proto.RequestVersionContext,
) -> RequestVersionContext:
    return RequestVersionContext(
        collection_version=request_version_context.collection_version,
        log_position=request_version_context.log_position,
    )


def to_proto_request_version_context(
    request_version_context: RequestVersionContext,
) -> proto.RequestVersionContext:
    return proto.RequestVersionContext(
        collection_version=request_version_context["collection_version"],
        log_position=request_version_context["log_position"],
    )

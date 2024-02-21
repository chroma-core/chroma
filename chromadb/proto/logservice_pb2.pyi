from chromadb.proto import chroma_pb2 as _chroma_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

DESCRIPTOR: _descriptor.FileDescriptor

class PushLogsRequest(_message.Message):
    __slots__ = ["collection_id", "records"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    records: _containers.RepeatedCompositeFieldContainer[
        _chroma_pb2.SubmitEmbeddingRecord
    ]
    def __init__(
        self,
        collection_id: _Optional[str] = ...,
        records: _Optional[
            _Iterable[_Union[_chroma_pb2.SubmitEmbeddingRecord, _Mapping]]
        ] = ...,
    ) -> None: ...

class PushLogsResponse(_message.Message):
    __slots__ = ["record_count"]
    RECORD_COUNT_FIELD_NUMBER: _ClassVar[int]
    record_count: int
    def __init__(self, record_count: _Optional[int] = ...) -> None: ...

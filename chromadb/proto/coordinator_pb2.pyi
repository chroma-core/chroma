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

class Status(_message.Message):
    __slots__ = ["reason", "code"]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    reason: str
    code: int
    def __init__(
        self, reason: _Optional[str] = ..., code: _Optional[int] = ...
    ) -> None: ...

class CreateCollectionRequest(_message.Message):
    __slots__ = ["collection", "get_or_create"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    GET_OR_CREATE_FIELD_NUMBER: _ClassVar[int]
    collection: Collection
    get_or_create: bool
    def __init__(
        self,
        collection: _Optional[_Union[Collection, _Mapping]] = ...,
        get_or_create: bool = ...,
    ) -> None: ...

class Collection(_message.Message):
    __slots__ = ["id", "name", "metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    metadata: _chroma_pb2.UpdateMetadata
    def __init__(
        self,
        id: _Optional[str] = ...,
        name: _Optional[str] = ...,
        metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ...,
    ) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ["collection", "status"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collection: Collection
    status: Status
    def __init__(
        self,
        collection: _Optional[_Union[Collection, _Mapping]] = ...,
        status: _Optional[_Union[Status, _Mapping]] = ...,
    ) -> None: ...

class GetCollectionsRequest(_message.Message):
    __slots__ = ["id", "name", "topic"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    topic: str
    def __init__(
        self,
        id: _Optional[str] = ...,
        name: _Optional[str] = ...,
        topic: _Optional[str] = ...,
    ) -> None: ...

class GetCollectionsResponse(_message.Message):
    __slots__ = ["collections", "status"]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[Collection]
    status: Status
    def __init__(
        self,
        collections: _Optional[_Iterable[_Union[Collection, _Mapping]]] = ...,
        status: _Optional[_Union[Status, _Mapping]] = ...,
    ) -> None: ...

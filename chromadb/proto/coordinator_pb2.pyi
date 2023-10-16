from chromadb.proto import chroma_pb2 as _chroma_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateSegmentRequest(_message.Message):
    __slots__ = ["segment"]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    segment: _chroma_pb2.Segment
    def __init__(self, segment: _Optional[_Union[_chroma_pb2.Segment, _Mapping]] = ...) -> None: ...

class DeleteSegmentRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetSegmentsRequest(_message.Message):
    __slots__ = ["id", "type", "scope", "topic", "collection"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    id: str
    type: str
    scope: _chroma_pb2.SegmentScope
    topic: str
    collection: str
    def __init__(self, id: _Optional[str] = ..., type: _Optional[str] = ..., scope: _Optional[_Union[_chroma_pb2.SegmentScope, str]] = ..., topic: _Optional[str] = ..., collection: _Optional[str] = ...) -> None: ...

class GetSegmentsResponse(_message.Message):
    __slots__ = ["segments", "status"]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    segments: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Segment]
    status: _chroma_pb2.Status
    def __init__(self, segments: _Optional[_Iterable[_Union[_chroma_pb2.Segment, _Mapping]]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class UpdateSegmentRequest(_message.Message):
    __slots__ = ["id", "topic", "reset_topic", "collection", "reset_collection", "metadata", "reset_metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    RESET_TOPIC_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    RESET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESET_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    topic: str
    reset_topic: bool
    collection: str
    reset_collection: bool
    metadata: _chroma_pb2.UpdateMetadata
    reset_metadata: bool
    def __init__(self, id: _Optional[str] = ..., topic: _Optional[str] = ..., reset_topic: bool = ..., collection: _Optional[str] = ..., reset_collection: bool = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., reset_metadata: bool = ...) -> None: ...

class CreateCollectionRequest(_message.Message):
    __slots__ = ["collection", "get_or_create"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    GET_OR_CREATE_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    get_or_create: bool
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., get_or_create: bool = ...) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ["collection", "status"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    status: _chroma_pb2.Status
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class DeleteCollectionRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetCollectionsRequest(_message.Message):
    __slots__ = ["id", "name", "topic"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    topic: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., topic: _Optional[str] = ...) -> None: ...

class GetCollectionsResponse(_message.Message):
    __slots__ = ["collections", "status"]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Collection]
    status: _chroma_pb2.Status
    def __init__(self, collections: _Optional[_Iterable[_Union[_chroma_pb2.Collection, _Mapping]]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class UpdateCollectionRequest(_message.Message):
    __slots__ = ["id", "topic", "name", "dimension", "metadata", "reset_metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESET_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    topic: str
    name: str
    dimension: int
    metadata: _chroma_pb2.UpdateMetadata
    reset_metadata: bool
    def __init__(self, id: _Optional[str] = ..., topic: _Optional[str] = ..., name: _Optional[str] = ..., dimension: _Optional[int] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., reset_metadata: bool = ...) -> None: ...

from chromadb.proto import chroma_pb2 as _chroma_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateDatabaseRequest(_message.Message):
    __slots__ = ["id", "name", "tenant"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    tenant: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class CreateDatabaseResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class GetDatabaseRequest(_message.Message):
    __slots__ = ["name", "tenant"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    name: str
    tenant: str
    def __init__(self, name: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class GetDatabaseResponse(_message.Message):
    __slots__ = ["database", "status"]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    database: _chroma_pb2.Database
    status: _chroma_pb2.Status
    def __init__(self, database: _Optional[_Union[_chroma_pb2.Database, _Mapping]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateTenantRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class CreateTenantResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class GetTenantRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class GetTenantResponse(_message.Message):
    __slots__ = ["tenant", "status"]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    tenant: _chroma_pb2.Tenant
    status: _chroma_pb2.Status
    def __init__(self, tenant: _Optional[_Union[_chroma_pb2.Tenant, _Mapping]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateSegmentRequest(_message.Message):
    __slots__ = ["segment"]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    segment: _chroma_pb2.Segment
    def __init__(self, segment: _Optional[_Union[_chroma_pb2.Segment, _Mapping]] = ...) -> None: ...

class CreateSegmentResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class DeleteSegmentRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeleteSegmentResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class GetSegmentsRequest(_message.Message):
    __slots__ = ["id", "type", "scope", "collection"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    id: str
    type: str
    scope: _chroma_pb2.SegmentScope
    collection: str
    def __init__(self, id: _Optional[str] = ..., type: _Optional[str] = ..., scope: _Optional[_Union[_chroma_pb2.SegmentScope, str]] = ..., collection: _Optional[str] = ...) -> None: ...

class GetSegmentsResponse(_message.Message):
    __slots__ = ["segments", "status"]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    segments: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Segment]
    status: _chroma_pb2.Status
    def __init__(self, segments: _Optional[_Iterable[_Union[_chroma_pb2.Segment, _Mapping]]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class UpdateSegmentRequest(_message.Message):
    __slots__ = ["id", "collection", "reset_collection", "metadata", "reset_metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    RESET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESET_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    collection: str
    reset_collection: bool
    metadata: _chroma_pb2.UpdateMetadata
    reset_metadata: bool
    def __init__(self, id: _Optional[str] = ..., collection: _Optional[str] = ..., reset_collection: bool = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., reset_metadata: bool = ...) -> None: ...

class UpdateSegmentResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class CreateCollectionRequest(_message.Message):
    __slots__ = ["id", "name", "metadata", "dimension", "get_or_create", "tenant", "database"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    GET_OR_CREATE_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    metadata: _chroma_pb2.UpdateMetadata
    dimension: int
    get_or_create: bool
    tenant: str
    database: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., dimension: _Optional[int] = ..., get_or_create: bool = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ...) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ["collection", "created", "status"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    created: bool
    status: _chroma_pb2.Status
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., created: bool = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class DeleteCollectionRequest(_message.Message):
    __slots__ = ["id", "tenant", "database"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant: str
    database: str
    def __init__(self, id: _Optional[str] = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ...) -> None: ...

class DeleteCollectionResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class GetCollectionsRequest(_message.Message):
    __slots__ = ["id", "name", "tenant", "database"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    tenant: str
    database: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ...) -> None: ...

class GetCollectionsResponse(_message.Message):
    __slots__ = ["collections", "status"]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Collection]
    status: _chroma_pb2.Status
    def __init__(self, collections: _Optional[_Iterable[_Union[_chroma_pb2.Collection, _Mapping]]] = ..., status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class UpdateCollectionRequest(_message.Message):
    __slots__ = ["id", "name", "dimension", "metadata", "reset_metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESET_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    dimension: int
    metadata: _chroma_pb2.UpdateMetadata
    reset_metadata: bool
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., dimension: _Optional[int] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., reset_metadata: bool = ...) -> None: ...

class UpdateCollectionResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class Notification(_message.Message):
    __slots__ = ["id", "collection_id", "type", "status"]
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: int
    collection_id: str
    type: str
    status: str
    def __init__(self, id: _Optional[int] = ..., collection_id: _Optional[str] = ..., type: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class ResetStateResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _chroma_pb2.Status
    def __init__(self, status: _Optional[_Union[_chroma_pb2.Status, _Mapping]] = ...) -> None: ...

class GetLastCompactionTimeForTenantRequest(_message.Message):
    __slots__ = ["tenant_id"]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, tenant_id: _Optional[_Iterable[str]] = ...) -> None: ...

class TenantLastCompactionTime(_message.Message):
    __slots__ = ["tenant_id", "last_compaction_time"]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_COMPACTION_TIME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    last_compaction_time: int
    def __init__(self, tenant_id: _Optional[str] = ..., last_compaction_time: _Optional[int] = ...) -> None: ...

class GetLastCompactionTimeForTenantResponse(_message.Message):
    __slots__ = ["tenant_last_compaction_time"]
    TENANT_LAST_COMPACTION_TIME_FIELD_NUMBER: _ClassVar[int]
    tenant_last_compaction_time: _containers.RepeatedCompositeFieldContainer[TenantLastCompactionTime]
    def __init__(self, tenant_last_compaction_time: _Optional[_Iterable[_Union[TenantLastCompactionTime, _Mapping]]] = ...) -> None: ...

class SetLastCompactionTimeForTenantRequest(_message.Message):
    __slots__ = ["tenant_last_compaction_time"]
    TENANT_LAST_COMPACTION_TIME_FIELD_NUMBER: _ClassVar[int]
    tenant_last_compaction_time: TenantLastCompactionTime
    def __init__(self, tenant_last_compaction_time: _Optional[_Union[TenantLastCompactionTime, _Mapping]] = ...) -> None: ...

class FlushSegmentCompactionInfo(_message.Message):
    __slots__ = ["segment_id", "file_paths"]
    class FilePathsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _chroma_pb2.FilePaths
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_chroma_pb2.FilePaths, _Mapping]] = ...) -> None: ...
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILE_PATHS_FIELD_NUMBER: _ClassVar[int]
    segment_id: str
    file_paths: _containers.MessageMap[str, _chroma_pb2.FilePaths]
    def __init__(self, segment_id: _Optional[str] = ..., file_paths: _Optional[_Mapping[str, _chroma_pb2.FilePaths]] = ...) -> None: ...

class FlushCollectionCompactionRequest(_message.Message):
    __slots__ = ["tenant_id", "collection_id", "log_position", "collection_version", "segment_compaction_info"]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_POSITION_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_COMPACTION_INFO_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    collection_id: str
    log_position: int
    collection_version: int
    segment_compaction_info: _containers.RepeatedCompositeFieldContainer[FlushSegmentCompactionInfo]
    def __init__(self, tenant_id: _Optional[str] = ..., collection_id: _Optional[str] = ..., log_position: _Optional[int] = ..., collection_version: _Optional[int] = ..., segment_compaction_info: _Optional[_Iterable[_Union[FlushSegmentCompactionInfo, _Mapping]]] = ...) -> None: ...

class FlushCollectionCompactionResponse(_message.Message):
    __slots__ = ["collection_id", "collection_version", "last_compaction_time"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_COMPACTION_TIME_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    collection_version: int
    last_compaction_time: int
    def __init__(self, collection_id: _Optional[str] = ..., collection_version: _Optional[int] = ..., last_compaction_time: _Optional[int] = ...) -> None: ...

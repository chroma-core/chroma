from chromadb.proto import chroma_pb2 as _chroma_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
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
    __slots__ = []
    def __init__(self) -> None: ...

class GetDatabaseRequest(_message.Message):
    __slots__ = ["name", "tenant"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    name: str
    tenant: str
    def __init__(self, name: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class GetDatabaseResponse(_message.Message):
    __slots__ = ["database"]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    database: _chroma_pb2.Database
    def __init__(self, database: _Optional[_Union[_chroma_pb2.Database, _Mapping]] = ...) -> None: ...

class ListDatabasesRequest(_message.Message):
    __slots__ = ["tenant", "limit", "offset"]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    limit: int
    offset: int
    def __init__(self, tenant: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class ListDatabasesResponse(_message.Message):
    __slots__ = ["databases"]
    DATABASES_FIELD_NUMBER: _ClassVar[int]
    databases: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Database]
    def __init__(self, databases: _Optional[_Iterable[_Union[_chroma_pb2.Database, _Mapping]]] = ...) -> None: ...

class CreateTenantRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class CreateTenantResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetTenantRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class GetTenantResponse(_message.Message):
    __slots__ = ["tenant"]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    tenant: _chroma_pb2.Tenant
    def __init__(self, tenant: _Optional[_Union[_chroma_pb2.Tenant, _Mapping]] = ...) -> None: ...

class CreateSegmentRequest(_message.Message):
    __slots__ = ["segment"]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    segment: _chroma_pb2.Segment
    def __init__(self, segment: _Optional[_Union[_chroma_pb2.Segment, _Mapping]] = ...) -> None: ...

class CreateSegmentResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class DeleteSegmentRequest(_message.Message):
    __slots__ = ["id", "collection"]
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    id: str
    collection: str
    def __init__(self, id: _Optional[str] = ..., collection: _Optional[str] = ...) -> None: ...

class DeleteSegmentResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

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
    __slots__ = ["segments"]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    segments: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Segment]
    def __init__(self, segments: _Optional[_Iterable[_Union[_chroma_pb2.Segment, _Mapping]]] = ...) -> None: ...

class UpdateSegmentRequest(_message.Message):
    __slots__ = ["id", "collection", "metadata", "reset_metadata"]
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESET_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    collection: str
    metadata: _chroma_pb2.UpdateMetadata
    reset_metadata: bool
    def __init__(self, id: _Optional[str] = ..., collection: _Optional[str] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., reset_metadata: bool = ...) -> None: ...

class UpdateSegmentResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class CreateCollectionRequest(_message.Message):
    __slots__ = ["id", "name", "configuration_json_str", "metadata", "dimension", "get_or_create", "tenant", "database", "segments"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_JSON_STR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    GET_OR_CREATE_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    configuration_json_str: str
    metadata: _chroma_pb2.UpdateMetadata
    dimension: int
    get_or_create: bool
    tenant: str
    database: str
    segments: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Segment]
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., configuration_json_str: _Optional[str] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ..., dimension: _Optional[int] = ..., get_or_create: bool = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ..., segments: _Optional[_Iterable[_Union[_chroma_pb2.Segment, _Mapping]]] = ...) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ["collection", "created"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    created: bool
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., created: bool = ...) -> None: ...

class DeleteCollectionRequest(_message.Message):
    __slots__ = ["id", "tenant", "database", "segment_ids"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant: str
    database: str
    segment_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ..., segment_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class DeleteCollectionResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetCollectionsRequest(_message.Message):
    __slots__ = ["id", "name", "tenant", "database", "limit", "offset"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    tenant: str
    database: str
    limit: int
    offset: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class GetCollectionsResponse(_message.Message):
    __slots__ = ["collections"]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Collection]
    def __init__(self, collections: _Optional[_Iterable[_Union[_chroma_pb2.Collection, _Mapping]]] = ...) -> None: ...

class GetCollectionWithSegmentsRequest(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetCollectionWithSegmentsResponse(_message.Message):
    __slots__ = ["collection", "segments"]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    segments: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Segment]
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., segments: _Optional[_Iterable[_Union[_chroma_pb2.Segment, _Mapping]]] = ...) -> None: ...

class CheckCollectionsRequest(_message.Message):
    __slots__ = ["collection_ids"]
    COLLECTION_IDS_FIELD_NUMBER: _ClassVar[int]
    collection_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, collection_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class CheckCollectionsResponse(_message.Message):
    __slots__ = ["deleted"]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, deleted: _Optional[_Iterable[bool]] = ...) -> None: ...

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
    __slots__ = []
    def __init__(self) -> None: ...

class ResetStateResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

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

class CollectionVersionFile(_message.Message):
    __slots__ = ["collection_info_immutable", "version_history"]
    COLLECTION_INFO_IMMUTABLE_FIELD_NUMBER: _ClassVar[int]
    VERSION_HISTORY_FIELD_NUMBER: _ClassVar[int]
    collection_info_immutable: CollectionInfoImmutable
    version_history: CollectionVersionHistory
    def __init__(self, collection_info_immutable: _Optional[_Union[CollectionInfoImmutable, _Mapping]] = ..., version_history: _Optional[_Union[CollectionVersionHistory, _Mapping]] = ...) -> None: ...

class CollectionInfoImmutable(_message.Message):
    __slots__ = ["tenant_id", "database_id", "database_name", "is_deleted", "dimension", "collection_id", "collection_name", "collection_creation_secs"]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DATABASE_ID_FIELD_NUMBER: _ClassVar[int]
    DATABASE_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_CREATION_SECS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    database_id: str
    database_name: str
    is_deleted: bool
    dimension: int
    collection_id: str
    collection_name: str
    collection_creation_secs: int
    def __init__(self, tenant_id: _Optional[str] = ..., database_id: _Optional[str] = ..., database_name: _Optional[str] = ..., is_deleted: bool = ..., dimension: _Optional[int] = ..., collection_id: _Optional[str] = ..., collection_name: _Optional[str] = ..., collection_creation_secs: _Optional[int] = ...) -> None: ...

class CollectionVersionHistory(_message.Message):
    __slots__ = ["versions"]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[CollectionVersionInfo]
    def __init__(self, versions: _Optional[_Iterable[_Union[CollectionVersionInfo, _Mapping]]] = ...) -> None: ...

class CollectionVersionInfo(_message.Message):
    __slots__ = ["version", "segment_info", "collection_info_mutable", "created_at_secs", "version_change_reason"]
    class VersionChangeReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        VERSION_CHANGE_REASON_DATA_COMPACTION: _ClassVar[CollectionVersionInfo.VersionChangeReason]
    VERSION_CHANGE_REASON_DATA_COMPACTION: CollectionVersionInfo.VersionChangeReason
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_INFO_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_INFO_MUTABLE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_SECS_FIELD_NUMBER: _ClassVar[int]
    VERSION_CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    version: int
    segment_info: CollectionSegmentInfo
    collection_info_mutable: CollectionInfoMutable
    created_at_secs: int
    version_change_reason: CollectionVersionInfo.VersionChangeReason
    def __init__(self, version: _Optional[int] = ..., segment_info: _Optional[_Union[CollectionSegmentInfo, _Mapping]] = ..., collection_info_mutable: _Optional[_Union[CollectionInfoMutable, _Mapping]] = ..., created_at_secs: _Optional[int] = ..., version_change_reason: _Optional[_Union[CollectionVersionInfo.VersionChangeReason, str]] = ...) -> None: ...

class CollectionInfoMutable(_message.Message):
    __slots__ = ["current_log_position", "current_collection_version", "updated_at_secs", "last_compaction_time_secs", "dimension"]
    CURRENT_LOG_POSITION_FIELD_NUMBER: _ClassVar[int]
    CURRENT_COLLECTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_SECS_FIELD_NUMBER: _ClassVar[int]
    LAST_COMPACTION_TIME_SECS_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    current_log_position: int
    current_collection_version: int
    updated_at_secs: int
    last_compaction_time_secs: int
    dimension: int
    def __init__(self, current_log_position: _Optional[int] = ..., current_collection_version: _Optional[int] = ..., updated_at_secs: _Optional[int] = ..., last_compaction_time_secs: _Optional[int] = ..., dimension: _Optional[int] = ...) -> None: ...

class CollectionSegmentInfo(_message.Message):
    __slots__ = ["segments"]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    segments: _containers.RepeatedCompositeFieldContainer[FlushSegmentCompactionInfo]
    def __init__(self, segments: _Optional[_Iterable[_Union[FlushSegmentCompactionInfo, _Mapping]]] = ...) -> None: ...

class CollectionVersionTuple(_message.Message):
    __slots__ = ["collection_id", "tenant_id", "version"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    tenant_id: str
    version: int
    def __init__(self, collection_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class CollectionLineageInfo(_message.Message):
    __slots__ = ["collection_id", "tenant_id", "is_root_collection", "parent_collections"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    IS_ROOT_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    PARENT_COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    tenant_id: str
    is_root_collection: bool
    parent_collections: _containers.RepeatedCompositeFieldContainer[CollectionVersionTuple]
    def __init__(self, collection_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., is_root_collection: bool = ..., parent_collections: _Optional[_Iterable[_Union[CollectionVersionTuple, _Mapping]]] = ...) -> None: ...

class ListCollectionVersionsRequest(_message.Message):
    __slots__ = ["collection_id", "tenant_id", "max_count", "versions_before", "versions_at_or_after"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_COUNT_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_BEFORE_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_AT_OR_AFTER_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    tenant_id: str
    max_count: int
    versions_before: int
    versions_at_or_after: int
    def __init__(self, collection_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., max_count: _Optional[int] = ..., versions_before: _Optional[int] = ..., versions_at_or_after: _Optional[int] = ...) -> None: ...

class ListCollectionVersionsResponse(_message.Message):
    __slots__ = ["versions", "list_is_truncated"]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    LIST_IS_TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[CollectionVersionInfo]
    list_is_truncated: bool
    def __init__(self, versions: _Optional[_Iterable[_Union[CollectionVersionInfo, _Mapping]]] = ..., list_is_truncated: bool = ...) -> None: ...

class RestoreCollectionRequest(_message.Message):
    __slots__ = ["collection_id", "tenant_id", "version_to_restore"]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_TO_RESTORE_FIELD_NUMBER: _ClassVar[int]
    collection_id: str
    tenant_id: str
    version_to_restore: int
    def __init__(self, collection_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., version_to_restore: _Optional[int] = ...) -> None: ...

class RestoreCollectionResponse(_message.Message):
    __slots__ = ["new_collection_version"]
    NEW_COLLECTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    new_collection_version: int
    def __init__(self, new_collection_version: _Optional[int] = ...) -> None: ...

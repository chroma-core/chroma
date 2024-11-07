from chromadb.proto import chroma_pb2 as _chroma_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScanOperator(_message.Message):
    __slots__ = ("collection", "knn_id", "metadata_id", "record_id")
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    KNN_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_ID_FIELD_NUMBER: _ClassVar[int]
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    collection: _chroma_pb2.Collection
    knn_id: str
    metadata_id: str
    record_id: str
    def __init__(self, collection: _Optional[_Union[_chroma_pb2.Collection, _Mapping]] = ..., knn_id: _Optional[str] = ..., metadata_id: _Optional[str] = ..., record_id: _Optional[str] = ...) -> None: ...

class FilterOperator(_message.Message):
    __slots__ = ("ids", "where", "where_document")
    IDS_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    WHERE_DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    ids: _chroma_pb2.UserIds
    where: _chroma_pb2.Where
    where_document: _chroma_pb2.WhereDocument
    def __init__(self, ids: _Optional[_Union[_chroma_pb2.UserIds, _Mapping]] = ..., where: _Optional[_Union[_chroma_pb2.Where, _Mapping]] = ..., where_document: _Optional[_Union[_chroma_pb2.WhereDocument, _Mapping]] = ...) -> None: ...

class KNNOperator(_message.Message):
    __slots__ = ("embeddings", "fetch")
    EMBEDDINGS_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    embeddings: _containers.RepeatedCompositeFieldContainer[_chroma_pb2.Vector]
    fetch: int
    def __init__(self, embeddings: _Optional[_Iterable[_Union[_chroma_pb2.Vector, _Mapping]]] = ..., fetch: _Optional[int] = ...) -> None: ...

class LimitOperator(_message.Message):
    __slots__ = ("skip", "fetch")
    SKIP_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    skip: int
    fetch: int
    def __init__(self, skip: _Optional[int] = ..., fetch: _Optional[int] = ...) -> None: ...

class ProjectionOperator(_message.Message):
    __slots__ = ("document", "embedding", "metadata")
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    document: bool
    embedding: bool
    metadata: bool
    def __init__(self, document: bool = ..., embedding: bool = ..., metadata: bool = ...) -> None: ...

class KNNProjectionOperator(_message.Message):
    __slots__ = ("projection", "distance")
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    projection: ProjectionOperator
    distance: bool
    def __init__(self, projection: _Optional[_Union[ProjectionOperator, _Mapping]] = ..., distance: bool = ...) -> None: ...

class CountPlan(_message.Message):
    __slots__ = ("scan",)
    SCAN_FIELD_NUMBER: _ClassVar[int]
    scan: ScanOperator
    def __init__(self, scan: _Optional[_Union[ScanOperator, _Mapping]] = ...) -> None: ...

class CountResult(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class GetPlan(_message.Message):
    __slots__ = ("scan", "filter", "limit", "projection")
    SCAN_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    scan: ScanOperator
    filter: FilterOperator
    limit: LimitOperator
    projection: ProjectionOperator
    def __init__(self, scan: _Optional[_Union[ScanOperator, _Mapping]] = ..., filter: _Optional[_Union[FilterOperator, _Mapping]] = ..., limit: _Optional[_Union[LimitOperator, _Mapping]] = ..., projection: _Optional[_Union[ProjectionOperator, _Mapping]] = ...) -> None: ...

class ProjectionRecord(_message.Message):
    __slots__ = ("id", "document", "embedding", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    document: str
    embedding: _chroma_pb2.Vector
    metadata: _chroma_pb2.UpdateMetadata
    def __init__(self, id: _Optional[str] = ..., document: _Optional[str] = ..., embedding: _Optional[_Union[_chroma_pb2.Vector, _Mapping]] = ..., metadata: _Optional[_Union[_chroma_pb2.UpdateMetadata, _Mapping]] = ...) -> None: ...

class GetResult(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[ProjectionRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[ProjectionRecord, _Mapping]]] = ...) -> None: ...

class KNNPlan(_message.Message):
    __slots__ = ("scan", "filter", "knn", "projection")
    SCAN_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    KNN_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    scan: ScanOperator
    filter: FilterOperator
    knn: KNNOperator
    projection: KNNProjectionOperator
    def __init__(self, scan: _Optional[_Union[ScanOperator, _Mapping]] = ..., filter: _Optional[_Union[FilterOperator, _Mapping]] = ..., knn: _Optional[_Union[KNNOperator, _Mapping]] = ..., projection: _Optional[_Union[KNNProjectionOperator, _Mapping]] = ...) -> None: ...

class KNNProjectionRecord(_message.Message):
    __slots__ = ("record", "distance")
    RECORD_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    record: ProjectionRecord
    distance: float
    def __init__(self, record: _Optional[_Union[ProjectionRecord, _Mapping]] = ..., distance: _Optional[float] = ...) -> None: ...

class KNNResult(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[KNNProjectionRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[KNNProjectionRecord, _Mapping]]] = ...) -> None: ...

class KNNBatchResult(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[KNNResult]
    def __init__(self, results: _Optional[_Iterable[_Union[KNNResult, _Mapping]]] = ...) -> None: ...

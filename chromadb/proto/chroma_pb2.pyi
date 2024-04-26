from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Operation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ADD: _ClassVar[Operation]
    UPDATE: _ClassVar[Operation]
    UPSERT: _ClassVar[Operation]
    DELETE: _ClassVar[Operation]

class ScalarEncoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FLOAT32: _ClassVar[ScalarEncoding]
    INT32: _ClassVar[ScalarEncoding]

class SegmentScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR: _ClassVar[SegmentScope]
    METADATA: _ClassVar[SegmentScope]
    RECORD: _ClassVar[SegmentScope]
    SQLITE: _ClassVar[SegmentScope]

class WhereDocumentOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTAINS: _ClassVar[WhereDocumentOperator]
    NOT_CONTAINS: _ClassVar[WhereDocumentOperator]

class BooleanOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AND: _ClassVar[BooleanOperator]
    OR: _ClassVar[BooleanOperator]

class ListOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    IN: _ClassVar[ListOperator]
    NIN: _ClassVar[ListOperator]

class GenericComparator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EQ: _ClassVar[GenericComparator]
    NE: _ClassVar[GenericComparator]

class NumberComparator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GT: _ClassVar[NumberComparator]
    GTE: _ClassVar[NumberComparator]
    LT: _ClassVar[NumberComparator]
    LTE: _ClassVar[NumberComparator]
ADD: Operation
UPDATE: Operation
UPSERT: Operation
DELETE: Operation
FLOAT32: ScalarEncoding
INT32: ScalarEncoding
VECTOR: SegmentScope
METADATA: SegmentScope
RECORD: SegmentScope
SQLITE: SegmentScope
CONTAINS: WhereDocumentOperator
NOT_CONTAINS: WhereDocumentOperator
AND: BooleanOperator
OR: BooleanOperator
IN: ListOperator
NIN: ListOperator
EQ: GenericComparator
NE: GenericComparator
GT: NumberComparator
GTE: NumberComparator
LT: NumberComparator
LTE: NumberComparator

class Status(_message.Message):
    __slots__ = ("reason", "code")
    REASON_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    reason: str
    code: int
    def __init__(self, reason: _Optional[str] = ..., code: _Optional[int] = ...) -> None: ...

class Vector(_message.Message):
    __slots__ = ("dimension", "vector", "encoding")
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    dimension: int
    vector: bytes
    encoding: ScalarEncoding
    def __init__(self, dimension: _Optional[int] = ..., vector: _Optional[bytes] = ..., encoding: _Optional[_Union[ScalarEncoding, str]] = ...) -> None: ...

class FilePaths(_message.Message):
    __slots__ = ("paths",)
    PATHS_FIELD_NUMBER: _ClassVar[int]
    paths: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, paths: _Optional[_Iterable[str]] = ...) -> None: ...

class Segment(_message.Message):
    __slots__ = ("id", "type", "scope", "collection", "metadata", "file_paths")
    class FilePathsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: FilePaths
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FilePaths, _Mapping]] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    FILE_PATHS_FIELD_NUMBER: _ClassVar[int]
    id: str
    type: str
    scope: SegmentScope
    collection: str
    metadata: UpdateMetadata
    file_paths: _containers.MessageMap[str, FilePaths]
    def __init__(self, id: _Optional[str] = ..., type: _Optional[str] = ..., scope: _Optional[_Union[SegmentScope, str]] = ..., collection: _Optional[str] = ..., metadata: _Optional[_Union[UpdateMetadata, _Mapping]] = ..., file_paths: _Optional[_Mapping[str, FilePaths]] = ...) -> None: ...

class Collection(_message.Message):
    __slots__ = ("id", "name", "configuration_json_str", "metadata", "dimension", "tenant", "database", "log_position", "version")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_JSON_STR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    LOG_POSITION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    configuration_json_str: str
    metadata: UpdateMetadata
    dimension: int
    tenant: str
    database: str
    log_position: int
    version: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., configuration_json_str: _Optional[str] = ..., metadata: _Optional[_Union[UpdateMetadata, _Mapping]] = ..., dimension: _Optional[int] = ..., tenant: _Optional[str] = ..., database: _Optional[str] = ..., log_position: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...

class Database(_message.Message):
    __slots__ = ("id", "name", "tenant")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    tenant: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class Tenant(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class UpdateMetadataValue(_message.Message):
    __slots__ = ("string_value", "int_value", "float_value", "bool_value")
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    string_value: str
    int_value: int
    float_value: float
    bool_value: bool
    def __init__(self, string_value: _Optional[str] = ..., int_value: _Optional[int] = ..., float_value: _Optional[float] = ..., bool_value: bool = ...) -> None: ...

class UpdateMetadata(_message.Message):
    __slots__ = ("metadata",)
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: UpdateMetadataValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[UpdateMetadataValue, _Mapping]] = ...) -> None: ...
    METADATA_FIELD_NUMBER: _ClassVar[int]
    metadata: _containers.MessageMap[str, UpdateMetadataValue]
    def __init__(self, metadata: _Optional[_Mapping[str, UpdateMetadataValue]] = ...) -> None: ...

class OperationRecord(_message.Message):
    __slots__ = ("id", "vector", "metadata", "operation")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: Vector
    metadata: UpdateMetadata
    operation: Operation
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Union[Vector, _Mapping]] = ..., metadata: _Optional[_Union[UpdateMetadata, _Mapping]] = ..., operation: _Optional[_Union[Operation, str]] = ...) -> None: ...

class CountRecordsRequest(_message.Message):
    __slots__ = ("segment_id", "collection_id")
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    segment_id: str
    collection_id: str
    def __init__(self, segment_id: _Optional[str] = ..., collection_id: _Optional[str] = ...) -> None: ...

class CountRecordsResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class QueryMetadataRequest(_message.Message):
    __slots__ = ("segment_id", "where", "where_document", "ids", "limit", "offset", "collection_id")
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    WHERE_FIELD_NUMBER: _ClassVar[int]
    WHERE_DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    segment_id: str
    where: Where
    where_document: WhereDocument
    ids: _containers.RepeatedScalarFieldContainer[str]
    limit: int
    offset: int
    collection_id: str
    def __init__(self, segment_id: _Optional[str] = ..., where: _Optional[_Union[Where, _Mapping]] = ..., where_document: _Optional[_Union[WhereDocument, _Mapping]] = ..., ids: _Optional[_Iterable[str]] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., collection_id: _Optional[str] = ...) -> None: ...

class QueryMetadataResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[MetadataEmbeddingRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[MetadataEmbeddingRecord, _Mapping]]] = ...) -> None: ...

class MetadataEmbeddingRecord(_message.Message):
    __slots__ = ("id", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    metadata: UpdateMetadata
    def __init__(self, id: _Optional[str] = ..., metadata: _Optional[_Union[UpdateMetadata, _Mapping]] = ...) -> None: ...

class WhereDocument(_message.Message):
    __slots__ = ("direct", "children")
    DIRECT_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    direct: DirectWhereDocument
    children: WhereDocumentChildren
    def __init__(self, direct: _Optional[_Union[DirectWhereDocument, _Mapping]] = ..., children: _Optional[_Union[WhereDocumentChildren, _Mapping]] = ...) -> None: ...

class DirectWhereDocument(_message.Message):
    __slots__ = ("document", "operator")
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    document: str
    operator: WhereDocumentOperator
    def __init__(self, document: _Optional[str] = ..., operator: _Optional[_Union[WhereDocumentOperator, str]] = ...) -> None: ...

class WhereDocumentChildren(_message.Message):
    __slots__ = ("children", "operator")
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    children: _containers.RepeatedCompositeFieldContainer[WhereDocument]
    operator: BooleanOperator
    def __init__(self, children: _Optional[_Iterable[_Union[WhereDocument, _Mapping]]] = ..., operator: _Optional[_Union[BooleanOperator, str]] = ...) -> None: ...

class Where(_message.Message):
    __slots__ = ("direct_comparison", "children")
    DIRECT_COMPARISON_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    direct_comparison: DirectComparison
    children: WhereChildren
    def __init__(self, direct_comparison: _Optional[_Union[DirectComparison, _Mapping]] = ..., children: _Optional[_Union[WhereChildren, _Mapping]] = ...) -> None: ...

class DirectComparison(_message.Message):
    __slots__ = ("key", "single_string_operand", "string_list_operand", "single_int_operand", "int_list_operand", "single_double_operand", "double_list_operand", "bool_list_operand", "single_bool_operand")
    KEY_FIELD_NUMBER: _ClassVar[int]
    SINGLE_STRING_OPERAND_FIELD_NUMBER: _ClassVar[int]
    STRING_LIST_OPERAND_FIELD_NUMBER: _ClassVar[int]
    SINGLE_INT_OPERAND_FIELD_NUMBER: _ClassVar[int]
    INT_LIST_OPERAND_FIELD_NUMBER: _ClassVar[int]
    SINGLE_DOUBLE_OPERAND_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_LIST_OPERAND_FIELD_NUMBER: _ClassVar[int]
    BOOL_LIST_OPERAND_FIELD_NUMBER: _ClassVar[int]
    SINGLE_BOOL_OPERAND_FIELD_NUMBER: _ClassVar[int]
    key: str
    single_string_operand: SingleStringComparison
    string_list_operand: StringListComparison
    single_int_operand: SingleIntComparison
    int_list_operand: IntListComparison
    single_double_operand: SingleDoubleComparison
    double_list_operand: DoubleListComparison
    bool_list_operand: BoolListComparison
    single_bool_operand: SingleBoolComparison
    def __init__(self, key: _Optional[str] = ..., single_string_operand: _Optional[_Union[SingleStringComparison, _Mapping]] = ..., string_list_operand: _Optional[_Union[StringListComparison, _Mapping]] = ..., single_int_operand: _Optional[_Union[SingleIntComparison, _Mapping]] = ..., int_list_operand: _Optional[_Union[IntListComparison, _Mapping]] = ..., single_double_operand: _Optional[_Union[SingleDoubleComparison, _Mapping]] = ..., double_list_operand: _Optional[_Union[DoubleListComparison, _Mapping]] = ..., bool_list_operand: _Optional[_Union[BoolListComparison, _Mapping]] = ..., single_bool_operand: _Optional[_Union[SingleBoolComparison, _Mapping]] = ...) -> None: ...

class WhereChildren(_message.Message):
    __slots__ = ("children", "operator")
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    children: _containers.RepeatedCompositeFieldContainer[Where]
    operator: BooleanOperator
    def __init__(self, children: _Optional[_Iterable[_Union[Where, _Mapping]]] = ..., operator: _Optional[_Union[BooleanOperator, str]] = ...) -> None: ...

class StringListComparison(_message.Message):
    __slots__ = ("values", "list_operator")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    LIST_OPERATOR_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    list_operator: ListOperator
    def __init__(self, values: _Optional[_Iterable[str]] = ..., list_operator: _Optional[_Union[ListOperator, str]] = ...) -> None: ...

class SingleStringComparison(_message.Message):
    __slots__ = ("value", "comparator")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    value: str
    comparator: GenericComparator
    def __init__(self, value: _Optional[str] = ..., comparator: _Optional[_Union[GenericComparator, str]] = ...) -> None: ...

class SingleBoolComparison(_message.Message):
    __slots__ = ("value", "comparator")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    value: bool
    comparator: GenericComparator
    def __init__(self, value: bool = ..., comparator: _Optional[_Union[GenericComparator, str]] = ...) -> None: ...

class IntListComparison(_message.Message):
    __slots__ = ("values", "list_operator")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    LIST_OPERATOR_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    list_operator: ListOperator
    def __init__(self, values: _Optional[_Iterable[int]] = ..., list_operator: _Optional[_Union[ListOperator, str]] = ...) -> None: ...

class SingleIntComparison(_message.Message):
    __slots__ = ("value", "generic_comparator", "number_comparator")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    GENERIC_COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    NUMBER_COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    value: int
    generic_comparator: GenericComparator
    number_comparator: NumberComparator
    def __init__(self, value: _Optional[int] = ..., generic_comparator: _Optional[_Union[GenericComparator, str]] = ..., number_comparator: _Optional[_Union[NumberComparator, str]] = ...) -> None: ...

class DoubleListComparison(_message.Message):
    __slots__ = ("values", "list_operator")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    LIST_OPERATOR_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    list_operator: ListOperator
    def __init__(self, values: _Optional[_Iterable[float]] = ..., list_operator: _Optional[_Union[ListOperator, str]] = ...) -> None: ...

class BoolListComparison(_message.Message):
    __slots__ = ("values", "list_operator")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    LIST_OPERATOR_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bool]
    list_operator: ListOperator
    def __init__(self, values: _Optional[_Iterable[bool]] = ..., list_operator: _Optional[_Union[ListOperator, str]] = ...) -> None: ...

class SingleDoubleComparison(_message.Message):
    __slots__ = ("value", "generic_comparator", "number_comparator")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    GENERIC_COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    NUMBER_COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    value: float
    generic_comparator: GenericComparator
    number_comparator: NumberComparator
    def __init__(self, value: _Optional[float] = ..., generic_comparator: _Optional[_Union[GenericComparator, str]] = ..., number_comparator: _Optional[_Union[NumberComparator, str]] = ...) -> None: ...

class RequestMetadata(_message.Message):
    __slots__ = ("collection_version", "log_position")
    COLLECTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    LOG_POSITION_FIELD_NUMBER: _ClassVar[int]
    collection_version: int
    log_position: int
    def __init__(self, collection_version: _Optional[int] = ..., log_position: _Optional[int] = ...) -> None: ...

class GetVectorsRequest(_message.Message):
    __slots__ = ("ids", "segment_id", "collection_id", "query_metadata")
    IDS_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_METADATA_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[str]
    segment_id: str
    collection_id: str
    query_metadata: RequestMetadata
    def __init__(self, ids: _Optional[_Iterable[str]] = ..., segment_id: _Optional[str] = ..., collection_id: _Optional[str] = ..., query_metadata: _Optional[_Union[RequestMetadata, _Mapping]] = ...) -> None: ...

class GetVectorsResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[VectorEmbeddingRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[VectorEmbeddingRecord, _Mapping]]] = ...) -> None: ...

class VectorEmbeddingRecord(_message.Message):
    __slots__ = ("id", "vector")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: Vector
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Union[Vector, _Mapping]] = ...) -> None: ...

class QueryVectorsRequest(_message.Message):
    __slots__ = ("vectors", "k", "allowed_ids", "include_embeddings", "segment_id", "collection_id", "query_metadata")
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_IDS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_EMBEDDINGS_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_METADATA_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[Vector]
    k: int
    allowed_ids: _containers.RepeatedScalarFieldContainer[str]
    include_embeddings: bool
    segment_id: str
    collection_id: str
    query_metadata: RequestMetadata
    def __init__(self, vectors: _Optional[_Iterable[_Union[Vector, _Mapping]]] = ..., k: _Optional[int] = ..., allowed_ids: _Optional[_Iterable[str]] = ..., include_embeddings: bool = ..., segment_id: _Optional[str] = ..., collection_id: _Optional[str] = ..., query_metadata: _Optional[_Union[RequestMetadata, _Mapping]] = ...) -> None: ...

class QueryVectorsResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[VectorQueryResults]
    def __init__(self, results: _Optional[_Iterable[_Union[VectorQueryResults, _Mapping]]] = ...) -> None: ...

class VectorQueryResults(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[VectorQueryResult]
    def __init__(self, results: _Optional[_Iterable[_Union[VectorQueryResult, _Mapping]]] = ...) -> None: ...

class VectorQueryResult(_message.Message):
    __slots__ = ("id", "distance", "vector")
    ID_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    id: str
    distance: float
    vector: Vector
    def __init__(self, id: _Optional[str] = ..., distance: _Optional[float] = ..., vector: _Optional[_Union[Vector, _Mapping]] = ...) -> None: ...

/// Well-known function IDs and names that are pre-populated in the database
///
/// GENERATED CODE - DO NOT EDIT MANUALLY
/// This file is auto-generated from go/pkg/sysdb/metastore/db/dbmodel/constants.go
/// by the build script in rust/types/build.rs
use uuid::Uuid;

/// UUID for the built-in record_counter function
pub const FUNCTION_RECORD_COUNTER_ID: Uuid = Uuid::from_bytes([
    0xcc, 0xf2, 0xe3, 0xba, 0x63, 0x3e, 0x43, 0xba, 0x93, 0x94, 0x46, 0xb0, 0xc5, 0x4c, 0x61, 0xe3,
]);
/// Name of the built-in record_counter function
pub const FUNCTION_RECORD_COUNTER_NAME: &str = "record_counter";

/// UUID for the built-in count_async function
pub const FUNCTION_COUNT_ASYNC_ID: Uuid = Uuid::from_bytes([
    0x4c, 0xad, 0xdd, 0x3b, 0xd4, 0x9d, 0x46, 0x25, 0xae, 0x1b, 0xd4, 0x18, 0x35, 0x30, 0x82, 0x4a,
]);
/// Name of the built-in count_async function
pub const FUNCTION_COUNT_ASYNC_NAME: &str = "count_async";

/// UUID for the built-in statistics function
pub const FUNCTION_STATISTICS_ID: Uuid = Uuid::from_bytes([
    0x30, 0x4b, 0x58, 0xad, 0xa5, 0xcb, 0x41, 0xdc, 0xb8, 0x8f, 0x36, 0xdd, 0x3b, 0xf1, 0xd4, 0x01,
]);
/// Name of the built-in statistics function
pub const FUNCTION_STATISTICS_NAME: &str = "statistics";

/// UUID for the built-in dummy_async function
pub const FUNCTION_DUMMY_ASYNC_ID: Uuid = Uuid::from_bytes([
    0x1d, 0xb3, 0xd1, 0x79, 0x37, 0xa7, 0x4c, 0x44, 0xa3, 0x01, 0x68, 0x7c, 0x1d, 0xa6, 0x9d, 0x7b,
]);
/// Name of the built-in dummy_async function
pub const FUNCTION_DUMMY_ASYNC_NAME: &str = "dummy_async";

/// UUID for the built-in http_generate function
pub const FUNCTION_HTTP_GENERATE_ID: Uuid = Uuid::from_bytes([
    0x9e, 0x3c, 0x75, 0x40, 0x4d, 0xdd, 0x40, 0xa2, 0xbb, 0xff, 0xad, 0x9c, 0xb3, 0xf0, 0x6e, 0xfc,
]);
/// Name of the built-in http_generate function
pub const FUNCTION_HTTP_GENERATE_NAME: &str = "http_generate";

/// UUID for the built-in revision_history function
pub const FUNCTION_REVISION_HISTORY_ID: Uuid = Uuid::from_bytes([
    0x2d, 0xf4, 0x34, 0x2c, 0x5b, 0x5a, 0x49, 0xaa, 0x83, 0x45, 0xc4, 0x65, 0x03, 0xe8, 0x55, 0x09,
]);
/// Name of the built-in revision_history function
pub const FUNCTION_REVISION_HISTORY_NAME: &str = "revision_history";

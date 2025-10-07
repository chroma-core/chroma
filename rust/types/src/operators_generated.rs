/// Well-known operator IDs and names that are pre-populated in the database
///
/// GENERATED CODE - DO NOT EDIT MANUALLY
/// This file is auto-generated from go/pkg/sysdb/metastore/db/dbmodel/constants.go
/// by the build script in rust/types/build.rs
use uuid::Uuid;

/// UUID for the built-in record_counter operator
pub const OPERATOR_RECORD_COUNTER_ID: Uuid = Uuid::from_bytes([
    0xcc, 0xf2, 0xe3, 0xba, 0x63, 0x3e, 0x43, 0xba, 0x93, 0x94, 0x46, 0xb0, 0xc5, 0x4c, 0x61, 0xe3,
]);
/// Name of the built-in record_counter operator
pub const OPERATOR_RECORD_COUNTER_NAME: &str = "record_counter";

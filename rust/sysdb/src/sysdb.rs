use super::test_sysdb::TestSysDb;
use crate::sqlite::SqliteSysDb;
use crate::{GetCollectionsOptions, GrpcSysDbConfig};
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes, TonicError, TonicMissingFieldError};
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::AdvanceAttachedFunctionRequest;
use chroma_types::chroma_proto::FinishAttachedFunctionRequest;
use chroma_types::chroma_proto::VersionListForCollection;
use chroma_types::{
    chroma_proto, chroma_proto::CollectionVersionInfo, CollectionAndSegments,
    CollectionMetadataUpdate, CountCollectionsError, CreateCollectionError, CreateDatabaseError,
    CreateDatabaseResponse, CreateTenantError, CreateTenantResponse, Database,
    DeleteCollectionError, DeleteDatabaseError, DeleteDatabaseResponse, GetCollectionByCrnError,
    GetCollectionSizeError, GetCollectionWithSegmentsError, GetCollectionsError, GetDatabaseError,
    GetDatabaseResponse, GetSegmentsError, GetTenantError, GetTenantResponse,
    InternalCollectionConfiguration, InternalUpdateCollectionConfiguration,
    ListAttachedFunctionsError, ListCollectionVersionsError, ListDatabasesError,
    ListDatabasesResponse, Metadata, ResetError, ResetResponse, ScheduleEntry,
    ScheduleEntryConversionError, SegmentFlushInfo, SegmentFlushInfoConversionError, SegmentUuid,
    UpdateCollectionError, UpdateTenantError, UpdateTenantResponse,
};
use chroma_types::{
    AdvanceAttachedFunctionError, AdvanceAttachedFunctionResponse, AttachedFunctionUpdateInfo,
    AttachedFunctionUuid, BatchGetCollectionSoftDeleteStatusError,
    BatchGetCollectionVersionFilePathsError, Collection, CollectionConversionError, CollectionUuid,
    CountForksError, DatabaseUuid, FinishAttachedFunctionError, FinishDatabaseDeletionError,
    FlushCompactionAndAttachedFunctionResponse, FlushCompactionResponse,
    FlushCompactionResponseConversionError, ForkCollectionError, Schema, SchemaError, Segment,
    SegmentConversionError, SegmentScope, Tenant,
};
use prost_types;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;
use tower::ServiceBuilder;
use uuid::{Error, Uuid};

pub const VERSION_FILE_S3_PREFIX: &str = "sysdb/version_files/";

// Helper function to convert serde_json::Value to prost_types::Value
pub(crate) fn json_to_prost_value(json: serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    let kind = match json {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(b),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Kind::NumberValue(f)
            } else {
                Kind::NullValue(0)
            }
        }
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(arr) => Kind::ListValue(prost_types::ListValue {
            values: arr.into_iter().map(json_to_prost_value).collect(),
        }),
        serde_json::Value::Object(map) => Kind::StructValue(prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, json_to_prost_value(v)))
                .collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

// Helper function to convert prost_types::Value to serde_json::Value
fn prost_value_to_json(value: prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind;
    match value.kind {
        Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(Kind::NumberValue(n)) => serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Kind::StringValue(s)) => serde_json::Value::String(s),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.into_iter().map(prost_value_to_json).collect())
        }
        Some(Kind::StructValue(s)) => prost_struct_to_json(s),
        None => serde_json::Value::Null,
    }
}

// Helper function to convert prost_types::Struct to serde_json::Value
fn prost_struct_to_json(s: prost_types::Struct) -> serde_json::Value {
    serde_json::Value::Object(
        s.fields
            .into_iter()
            .map(|(k, v)| (k, prost_value_to_json(v)))
            .collect(),
    )
}

#[derive(Debug, Clone)]
pub enum SysDb {
    Grpc(GrpcSysDb),
    Sqlite(SqliteSysDb),
    #[allow(dead_code)]
    Test(TestSysDb),
}

impl SysDb {
    pub async fn create_tenant(
        &mut self,
        tenant_name: String,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        match self {
            SysDb::Grpc(grpc) => grpc.create_tenant(tenant_name).await,
            SysDb::Sqlite(sqlite) => sqlite.create_tenant(tenant_name).await,
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn get_tenant(
        &mut self,
        tenant_name: String,
    ) -> Result<GetTenantResponse, GetTenantError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_tenant(tenant_name).await,
            SysDb::Sqlite(sqlite) => sqlite.get_tenant(&tenant_name).await,
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn update_tenant(
        &mut self,
        tenant_id: String,
        resource_name: String,
    ) -> Result<UpdateTenantResponse, UpdateTenantError> {
        match self {
            SysDb::Grpc(grpc) => grpc.update_tenant(tenant_id, resource_name).await,
            SysDb::Sqlite(sqlite) => sqlite.update_tenant(tenant_id, resource_name).await,
            SysDb::Test(test) => test.update_tenant(tenant_id, resource_name).await,
        }
    }

    pub async fn create_database(
        &mut self,
        database_id: Uuid,
        database_name: String,
        tenant: String,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.create_database(database_id, database_name, tenant)
                    .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .create_database(database_id, &database_name, &tenant)
                    .await
            }
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn list_databases(
        &mut self,
        tenant_id: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<ListDatabasesResponse, ListDatabasesError> {
        match self {
            SysDb::Grpc(grpc) => grpc.list_databases(tenant_id, limit, offset).await,
            SysDb::Sqlite(sqlite) => sqlite.list_databases(tenant_id, limit, offset).await,
            SysDb::Test(test) => test.list_databases(tenant_id, limit, offset).await,
        }
    }

    pub async fn get_database(
        &mut self,
        database_name: String,
        tenant: String,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_database(database_name, tenant).await,
            SysDb::Sqlite(sqlite) => sqlite.get_database(&database_name, &tenant).await,
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn delete_database(
        &mut self,
        database_name: String,
        tenant: String,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        match self {
            SysDb::Grpc(grpc) => grpc.delete_database(database_name, tenant).await,
            SysDb::Sqlite(sqlite) => sqlite.delete_database(database_name, tenant).await,
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn finish_database_deletion(
        &mut self,
        cutoff_time: SystemTime,
    ) -> Result<usize, FinishDatabaseDeletionError> {
        match self {
            SysDb::Grpc(grpc) => grpc.finish_database_deletion(cutoff_time).await,
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn get_collections(
        &mut self,
        options: GetCollectionsOptions,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_collections(options).await,
            SysDb::Sqlite(sqlite) => sqlite.get_collections(options).await,
            SysDb::Test(test) => test.get_collections(options).await,
        }
    }

    pub async fn get_collection_by_crn(
        &mut self,
        tenant_resource_name: String,
        database: String,
        name: String,
    ) -> Result<Collection, GetCollectionByCrnError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_collection_by_crn(tenant_resource_name, database, name)
                    .await
            }
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(test) => {
                test.get_collection_by_crn(tenant_resource_name, database, name)
                    .await
            }
        }
    }

    pub async fn count_collections(
        &mut self,
        tenant: String,
        database: Option<String>,
    ) -> Result<usize, CountCollectionsError> {
        // TODO(Sanket): optimize sqlite and test implementation.
        match self {
            SysDb::Grpc(grpc) => grpc.count_collections(tenant, database).await,
            SysDb::Sqlite(sqlite) => Ok(sqlite
                .get_collections(GetCollectionsOptions {
                    tenant: Some(tenant),
                    database,
                    ..Default::default()
                })
                .await
                .map_err(|_| CountCollectionsError::Internal)?
                .len()),
            SysDb::Test(test) => Ok(test
                .get_collections(GetCollectionsOptions {
                    tenant: Some(tenant),
                    database,
                    ..Default::default()
                })
                .await
                .map_err(|_| CountCollectionsError::Internal)?
                .len()),
        }
    }

    pub async fn get_collection_size(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<usize, GetCollectionSizeError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_collection_size(collection_id).await,
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(test) => test.get_collection_size(collection_id).await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_collection(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        name: String,
        segments: Vec<Segment>,
        configuration: Option<InternalCollectionConfiguration>,
        schema: Option<Schema>,
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<Collection, CreateCollectionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.create_collection(
                    tenant,
                    database,
                    collection_id,
                    name,
                    segments,
                    configuration,
                    schema,
                    metadata,
                    dimension,
                    get_or_create,
                )
                .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .create_collection(
                        tenant,
                        database,
                        collection_id,
                        name,
                        segments,
                        configuration,
                        schema.clone(),
                        metadata,
                        dimension,
                        get_or_create,
                    )
                    .await
            }
            SysDb::Test(test_sysdb) => {
                let collection = Collection {
                    collection_id,
                    name,
                    config: configuration
                        .unwrap_or(InternalCollectionConfiguration::default_hnsw()),
                    schema,
                    metadata,
                    dimension,
                    tenant: tenant.clone(),
                    database: database.clone(),
                    log_position: 0,
                    version: 0,
                    total_records_post_compaction: 0,
                    size_bytes_post_compaction: 0,
                    last_compaction_time_secs: 0,
                    version_file_path: None,
                    root_collection_id: None,
                    lineage_file_path: None,
                    updated_at: SystemTime::now(),
                    database_id: DatabaseUuid::new(),
                };

                test_sysdb.add_collection(collection.clone());
                for seg in segments {
                    test_sysdb.add_segment(seg);
                }
                Ok(collection)
            }
        }
    }

    pub async fn update_collection(
        &mut self,
        collection_id: CollectionUuid,
        name: Option<String>,
        metadata: Option<CollectionMetadataUpdate>,
        dimension: Option<u32>,
        configuration: Option<InternalUpdateCollectionConfiguration>,
    ) -> Result<(), UpdateCollectionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.update_collection(collection_id, name, metadata, dimension, configuration)
                    .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .update_collection(collection_id, name, metadata, dimension, configuration)
                    .await
            }
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn delete_collection(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        segment_ids: Vec<SegmentUuid>,
    ) -> Result<(), DeleteCollectionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.delete_collection(tenant, database, collection_id, segment_ids)
                    .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .delete_collection(tenant, database, collection_id, segment_ids)
                    .await
            }
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn finish_collection_deletion(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
    ) -> Result<(), DeleteCollectionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.finish_collection_deletion(tenant, database, collection_id)
                    .await
            }
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn fork_collection(
        &mut self,
        source_collection_id: CollectionUuid,
        source_collection_log_compaction_offset: u64,
        source_collection_log_enumeration_offset: u64,
        target_collection_id: CollectionUuid,
        target_collection_name: String,
    ) -> Result<CollectionAndSegments, ForkCollectionError> {
        match self {
            SysDb::Grpc(grpc_sys_db) => {
                grpc_sys_db
                    .fork_collection(
                        source_collection_id,
                        source_collection_log_compaction_offset,
                        source_collection_log_enumeration_offset,
                        target_collection_id,
                        target_collection_name,
                    )
                    .await
            }
            SysDb::Sqlite(_) => Err(ForkCollectionError::Local),
            SysDb::Test(_) => Err(ForkCollectionError::Local),
        }
    }

    pub async fn count_forks(
        &mut self,
        source_collection_id: CollectionUuid,
    ) -> Result<usize, CountForksError> {
        match self {
            SysDb::Grpc(grpc) => grpc.count_forks(source_collection_id).await,
            SysDb::Sqlite(_) => Err(CountForksError::Local),
            SysDb::Test(test) => test.count_forks(source_collection_id).await,
        }
    }

    pub async fn list_attached_functions(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<chroma_proto::AttachedFunction>, ListAttachedFunctionsError> {
        match self {
            SysDb::Grpc(grpc) => grpc.list_attached_functions(collection_id).await,
            SysDb::Sqlite(_) => Err(ListAttachedFunctionsError::NotImplemented),
            SysDb::Test(test) => test.list_attached_functions(collection_id).await,
        }
    }

    pub async fn get_collections_to_gc(
        &mut self,
        cutoff_time: Option<SystemTime>,
        limit: Option<u64>,
        tenant: Option<String>,
        min_versions_if_alive: Option<u64>,
    ) -> Result<Vec<CollectionToGcInfo>, GetCollectionsToGcError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_collections_to_gc(cutoff_time, limit, tenant, min_versions_if_alive)
                    .await
            }
            SysDb::Sqlite(_) => unimplemented!("Garbage collection does not work for local chroma"),
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn get_collection_to_gc(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionToGcInfo, GetCollectionsToGcError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_collection_to_gc(collection_id).await,
            SysDb::Sqlite(_) => unimplemented!("Garbage collection does not work for local chroma"),
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn get_segments(
        &mut self,
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_segments(id, r#type, scope, collection).await,
            SysDb::Sqlite(sqlite) => sqlite.get_segments(id, r#type, scope, collection).await,
            SysDb::Test(test) => test.get_segments(id, r#type, scope, collection).await,
        }
    }

    pub async fn get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, GetCollectionWithSegmentsError> {
        match self {
            SysDb::Grpc(grpc_sys_db) => {
                grpc_sys_db
                    .get_collection_with_segments(collection_id)
                    .await
            }
            SysDb::Sqlite(sqlite) => sqlite.get_collection_with_segments(collection_id).await,
            SysDb::Test(test_sys_db) => {
                test_sys_db
                    .get_collection_with_segments(collection_id)
                    .await
            }
        }
    }

    // Only meant for testing.
    pub async fn get_all_functions(
        &mut self,
    ) -> Result<Vec<(String, uuid::Uuid)>, Box<dyn std::error::Error>> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_all_functions().await,
            SysDb::Sqlite(_) => unimplemented!("get_all_functions not implemented for sqlite"),
            SysDb::Test(_) => unimplemented!("get_all_functions not implemented for test"),
        }
    }

    pub async fn batch_get_collection_version_file_paths(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, String>, BatchGetCollectionVersionFilePathsError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.batch_get_collection_version_file_paths(collection_ids)
                    .await
            }
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => {
                test.batch_get_collection_version_file_paths(collection_ids)
                    .await
            }
        }
    }

    pub async fn batch_get_collection_soft_delete_status(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, bool>, BatchGetCollectionSoftDeleteStatusError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.batch_get_collection_soft_delete_status(collection_ids)
                    .await
            }
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => {
                test.batch_get_collection_soft_delete_status(collection_ids)
                    .await
            }
        }
    }

    pub async fn get_last_compaction_time(
        &mut self,
        tenant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_last_compaction_time(tenant_ids).await,
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => test.get_last_compaction_time(tenant_ids).await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        total_records_post_compaction: u64,
        size_bytes_post_compaction: u64,
        schema: Option<Schema>,
    ) -> Result<FlushCompactionResponse, FlushCompactionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.flush_compaction(
                    tenant_id,
                    collection_id,
                    log_position,
                    collection_version,
                    segment_flush_info,
                    total_records_post_compaction,
                    size_bytes_post_compaction,
                    schema,
                )
                .await
            }
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => {
                test.flush_compaction(
                    tenant_id,
                    collection_id,
                    log_position,
                    collection_version,
                    segment_flush_info,
                    total_records_post_compaction,
                    size_bytes_post_compaction,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn flush_compaction_and_attached_function(
        &mut self,
        tenant_id: String,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        total_records_post_compaction: u64,
        size_bytes_post_compaction: u64,
        schema: Option<Schema>,
        attached_function_update: AttachedFunctionUpdateInfo,
    ) -> Result<FlushCompactionAndAttachedFunctionResponse, FlushCompactionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.flush_compaction_and_attached_function(
                    tenant_id,
                    collection_id,
                    log_position,
                    collection_version,
                    segment_flush_info,
                    total_records_post_compaction,
                    size_bytes_post_compaction,
                    schema,
                    attached_function_update,
                )
                .await
            }
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn list_collection_versions(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<CollectionVersionInfo>, ListCollectionVersionsError> {
        match self {
            SysDb::Grpc(_) => todo!(),
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => test.list_collection_versions(collection_id).await,
        }
    }

    pub async fn mark_version_for_deletion(
        &mut self,
        epoch_id: i64,
        versions: Vec<VersionListForCollection>,
    ) -> Result<HashMap<String, bool>, MarkVersionForDeletionError> {
        match self {
            SysDb::Grpc(grpc) => grpc.mark_version_for_deletion(epoch_id, versions).await,
            SysDb::Test(test) => {
                let versions_clone = versions.clone();
                test.mark_version_for_deletion(epoch_id, versions_clone)
                    .await
                    .map_err(|e| {
                        MarkVersionForDeletionError::FailedToMarkVersion(tonic::Status::internal(e))
                    })
                    .map(|_| {
                        let mut result = HashMap::new();
                        for version in versions {
                            result.insert(version.collection_id, true);
                        }
                        result
                    })
            }
            SysDb::Sqlite(_) => todo!(),
        }
    }

    pub async fn delete_collection_version(
        &mut self,
        versions: Vec<VersionListForCollection>,
    ) -> Result<HashMap<String, bool>, DeleteCollectionVersionError> {
        match self {
            SysDb::Grpc(client) => {
                let response = client.delete_collection_version(versions).await?;
                Ok(response)
            }
            SysDb::Test(client) => Ok(client.delete_collection_version(versions).await),
            SysDb::Sqlite(_) => todo!(),
        }
    }

    pub async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        match self {
            SysDb::Grpc(grpc) => grpc.reset().await,
            SysDb::Sqlite(sqlite) => sqlite.reset().await,
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn peek_schedule_by_collection_id(
        &mut self,
        collection_ids: &[CollectionUuid],
    ) -> Result<Vec<ScheduleEntry>, PeekScheduleError> {
        match self {
            SysDb::Grpc(grpc) => grpc.peek_schedule_by_collection_id(collection_ids).await,
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(test) => test.peek_schedule_by_collection_id(collection_ids).await,
        }
    }

    pub async fn finish_attached_function(
        &mut self,
        attached_function_id: AttachedFunctionUuid,
    ) -> Result<(), FinishAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => grpc.finish_attached_function(attached_function_id).await,
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(test) => test.finish_attached_function(attached_function_id).await,
        }
    }

    pub async fn advance_attached_function(
        &mut self,
        attached_function_id: AttachedFunctionUuid,
        attached_function_run_nonce: uuid::Uuid,
        completion_offset: u64,
        next_run_delay_secs: u64,
    ) -> Result<AdvanceAttachedFunctionResponse, AdvanceAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.advance_attached_function(
                    attached_function_id,
                    attached_function_run_nonce,
                    completion_offset,
                    next_run_delay_secs,
                )
                .await
            }
            SysDb::Sqlite(_) => unimplemented!(),
            SysDb::Test(_) => unimplemented!(),
        }
    }
}

#[derive(Clone, Debug)]
// Since this uses tonic transport channel, cloning is cheap. Each client only supports
// one inflight request at a time, so we need to clone the client for each requester.
pub struct GrpcSysDb {
    #[allow(clippy::type_complexity)]
    client: SysDbClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>,
}

#[derive(Error, Debug)]
pub(crate) enum GrpcSysDbError {
    #[error("Failed to connect to sysdb")]
    FailedToConnect(#[from] tonic::transport::Error),
}

impl ChromaError for GrpcSysDbError {
    fn code(&self) -> ErrorCodes {
        match self {
            GrpcSysDbError::FailedToConnect(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Configurable<GrpcSysDbConfig> for GrpcSysDb {
    async fn try_from_config(
        my_config: &GrpcSysDbConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let host = &my_config.host;
        let port = &my_config.port;
        tracing::info!("Connecting to sysdb at {}:{}", host, port);
        let connection_string = format!("http://{}:{}", host, port);
        let endpoint = match Endpoint::from_shared(connection_string) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                return Err(Box::new(GrpcSysDbError::FailedToConnect(e)));
            }
        };
        let endpoint = endpoint
            .connect_timeout(Duration::from_millis(my_config.connect_timeout_ms))
            .timeout(Duration::from_millis(my_config.request_timeout_ms));

        let channel = Channel::balance_list((0..my_config.num_channels).map(|_| endpoint.clone()));
        let channel = ServiceBuilder::new()
            .layer(chroma_tracing::GrpcClientTraceLayer)
            .service(channel);
        let client = SysDbClient::new(channel);
        Ok(GrpcSysDb { client })
    }
}

#[derive(Debug)]
pub struct CollectionToGcInfo {
    pub id: CollectionUuid,
    pub tenant: String,
    pub name: String,
    pub version_file_path: String,
    pub lineage_file_path: Option<String>,
}

#[derive(Debug, Error)]
pub enum GetCollectionsToGcError {
    #[error("No such collection")]
    NoSuchCollection,
    #[error("Failed to parse uuid")]
    ParsingError(#[from] Error),
    #[error("Grpc request failed")]
    RequestFailed(#[from] tonic::Status),
    #[error("Internal error: {0}")]
    Internal(#[from] Box<dyn ChromaError>),
}

impl ChromaError for GetCollectionsToGcError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsToGcError::NoSuchCollection => ErrorCodes::NotFound,
            GetCollectionsToGcError::ParsingError(_) => ErrorCodes::Internal,
            GetCollectionsToGcError::RequestFailed(_) => ErrorCodes::Internal,
            GetCollectionsToGcError::Internal(e) => e.code(),
        }
    }
}

impl TryFrom<chroma_proto::CollectionToGcInfo> for CollectionToGcInfo {
    type Error = GetCollectionsToGcError;

    fn try_from(value: chroma_proto::CollectionToGcInfo) -> Result<Self, Self::Error> {
        let collection_uuid = match Uuid::try_parse(&value.id) {
            Ok(uuid) => uuid,
            Err(e) => return Err(GetCollectionsToGcError::ParsingError(e)),
        };
        let collection_id = CollectionUuid(collection_uuid);
        Ok(CollectionToGcInfo {
            id: collection_id,
            tenant: value.tenant_id,
            name: value.name,
            version_file_path: value.version_file_path,
            lineage_file_path: value.lineage_file_path,
        })
    }
}

impl GrpcSysDb {
    pub async fn create_tenant(
        &mut self,
        tenant_name: String,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        let req = chroma_proto::CreateTenantRequest {
            name: tenant_name.clone(),
        };
        match self.client.create_tenant(req).await {
            Ok(_) => Ok(CreateTenantResponse {}),
            Err(err) if matches!(err.code(), Code::AlreadyExists) => {
                Err(CreateTenantError::AlreadyExists(tenant_name))
            }
            Err(err) => Err(CreateTenantError::Internal(err.into())),
        }
    }

    pub async fn get_tenant(
        &mut self,
        tenant_name: String,
    ) -> Result<GetTenantResponse, GetTenantError> {
        let req = chroma_proto::GetTenantRequest {
            name: tenant_name.clone(),
        };
        match self.client.get_tenant(req).await {
            Ok(resp) => {
                let tenant = resp
                    .into_inner()
                    .tenant
                    .ok_or(GetTenantError::NotFound(tenant_name))?;
                Ok(GetTenantResponse {
                    name: tenant.name,
                    resource_name: tenant.resource_name,
                })
            }
            Err(err) => Err(GetTenantError::Internal(err.into())),
        }
    }

    pub(crate) async fn create_database(
        &mut self,
        database_id: Uuid,
        database_name: String,
        tenant: String,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        let req = chroma_proto::CreateDatabaseRequest {
            id: database_id.to_string(),
            name: database_name.clone(),
            tenant,
        };
        let res = self.client.create_database(req).await;
        match res {
            Ok(_) => Ok(CreateDatabaseResponse {}),
            Err(e) => {
                tracing::error!("Failed to create database {:?}", e);
                let res = match e.code() {
                    Code::AlreadyExists => CreateDatabaseError::AlreadyExists(database_name),
                    _ => CreateDatabaseError::Internal(e.into()),
                };
                Err(res)
            }
        }
    }

    pub async fn list_databases(
        &mut self,
        tenant: String,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<ListDatabasesResponse, ListDatabasesError> {
        let req = chroma_proto::ListDatabasesRequest {
            tenant,
            limit: limit.map(|l| l as i32),
            offset: Some(offset as i32),
        };
        match self.client.list_databases(req).await {
            Ok(resp) => resp
                .into_inner()
                .databases
                .into_iter()
                .map(|db| {
                    Uuid::parse_str(&db.id)
                        .map_err(|err| ListDatabasesError::InvalidID(err.to_string()))
                        .map(|id| Database {
                            id,
                            name: db.name,
                            tenant: db.tenant,
                        })
                })
                .collect(),
            Err(err) => Err(ListDatabasesError::Internal(err.into())),
        }
    }

    pub async fn get_database(
        &mut self,
        database_name: String,
        tenant: String,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        let req = chroma_proto::GetDatabaseRequest {
            name: database_name.clone(),
            tenant,
        };
        let res = self.client.get_database(req).await;
        match res {
            Ok(res) => {
                let res = match res.into_inner().database {
                    Some(res) => res,
                    None => return Err(GetDatabaseError::NotFound(database_name)),
                };
                let db_id = match Uuid::parse_str(res.id.as_str()) {
                    Ok(uuid) => uuid,
                    Err(err) => return Err(GetDatabaseError::InvalidID(err.to_string())),
                };
                Ok(GetDatabaseResponse {
                    id: db_id,
                    name: res.name,
                    tenant: res.tenant,
                })
            }
            Err(e) => {
                tracing::error!("Failed to get database {:?}", e);
                let res = match e.code() {
                    Code::NotFound => GetDatabaseError::NotFound(database_name),
                    _ => GetDatabaseError::Internal(e.into()),
                };
                Err(res)
            }
        }
    }

    async fn delete_database(
        &mut self,
        database_name: String,
        tenant: String,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        let req = chroma_proto::DeleteDatabaseRequest {
            name: database_name.clone(),
            tenant,
        };
        match self.client.delete_database(req).await {
            Ok(_) => Ok(DeleteDatabaseResponse {}),
            Err(err) if matches!(err.code(), Code::NotFound) => {
                Err(DeleteDatabaseError::NotFound(database_name))
            }
            Err(err) => Err(DeleteDatabaseError::Internal(err.into())),
        }
    }

    async fn finish_database_deletion(
        &mut self,
        cutoff_time: SystemTime,
    ) -> Result<usize, FinishDatabaseDeletionError> {
        let req = chroma_proto::FinishDatabaseDeletionRequest {
            cutoff_time: Some(cutoff_time.into()),
        };

        let res = self
            .client
            .finish_database_deletion(req)
            .await
            .map_err(|e| TonicError(e).boxed())?;
        Ok(res.into_inner().num_deleted as usize)
    }

    async fn get_collections(
        &mut self,
        options: GetCollectionsOptions,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        let GetCollectionsOptions {
            collection_id,
            collection_ids,
            include_soft_deleted,
            name,
            tenant,
            database,
            limit,
            offset,
        } = options;

        // TODO: move off of status into our own error type
        let collection_id_str = collection_id.map(|id| String::from(id.0));
        let res = self
            .client
            .get_collections(chroma_proto::GetCollectionsRequest {
                id: collection_id_str,
                ids_filter: collection_ids.map(|ids| {
                    let ids = ids.into_iter().map(|id| id.0.to_string()).collect();
                    chroma_proto::CollectionIdsFilter { ids }
                }),
                name,
                include_soft_deleted: Some(include_soft_deleted),
                limit: limit.map(|l| l as i32),
                offset: Some(offset as i32),
                tenant: tenant.unwrap_or("".to_string()),
                database: database.unwrap_or("".to_string()),
            })
            .await;

        match res {
            Ok(res) => {
                let collections = res.into_inner().collections;

                let collections = collections
                    .into_iter()
                    .map(|proto_collection| proto_collection.try_into())
                    .collect::<Result<Vec<Collection>, CollectionConversionError>>();

                match collections {
                    Ok(collections) => Ok(collections),
                    Err(e) => Err(GetCollectionsError::Internal(e.boxed())),
                }
            }
            Err(e) => Err(GetCollectionsError::Internal(e.into())),
        }
    }

    async fn get_collection_by_crn(
        &mut self,
        tenant_resource_name: String,
        database: String,
        name: String,
    ) -> Result<Collection, GetCollectionByCrnError> {
        let req = chroma_proto::GetCollectionByResourceNameRequest {
            tenant_resource_name: tenant_resource_name.clone(),
            database: database.clone(),
            name: name.clone(),
        };
        let res = self.client.get_collection_by_resource_name(req).await;

        match res {
            Ok(res) => {
                let collection = match res.into_inner().collection {
                    Some(collection) => collection,
                    None => {
                        return Err(GetCollectionByCrnError::NotFound(format!(
                            "{}:{}:{}",
                            tenant_resource_name, database, name
                        )));
                    }
                };

                Ok(collection
                    .try_into()
                    .map_err(|e: CollectionConversionError| {
                        GetCollectionByCrnError::Internal(e.boxed())
                    })?)
            }
            Err(e) => Err(GetCollectionByCrnError::Internal(e.into())),
        }
    }

    async fn count_collections(
        &mut self,
        tenant: String,
        database: Option<String>,
    ) -> Result<usize, CountCollectionsError> {
        let request = chroma_proto::CountCollectionsRequest { tenant, database };
        let res = self.client.count_collections(request).await;
        match res {
            Ok(res) => Ok(res.into_inner().count as usize),
            Err(_) => Err(CountCollectionsError::Internal),
        }
    }

    async fn get_collection_size(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<usize, GetCollectionSizeError> {
        let request = chroma_proto::GetCollectionSizeRequest {
            id: collection_id.0.to_string(),
        };
        let res = self.client.get_collection_size(request).await;
        match res {
            Ok(res) => Ok(res.into_inner().total_records_post_compaction as usize),
            Err(e) => match e.code() {
                Code::NotFound => Err(GetCollectionSizeError::NotFound(collection_id.to_string())),
                _ => Err(GetCollectionSizeError::Internal(e.into())),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_collection(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        name: String,
        segments: Vec<Segment>,
        configuration: Option<InternalCollectionConfiguration>,
        schema: Option<Schema>,
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<Collection, CreateCollectionError> {
        let configuration_json_str = match configuration {
            Some(configuration) => serde_json::to_string(&configuration)
                .map_err(CreateCollectionError::Configuration)?,
            None => "{}".to_string(),
        };
        let res = self
            .client
            .create_collection(chroma_proto::CreateCollectionRequest {
                id: collection_id.0.to_string(),
                tenant,
                database,
                name: name.clone(),
                segments: segments
                    .into_iter()
                    .map(chroma_proto::Segment::from)
                    .collect(),
                configuration_json_str,
                metadata: metadata.map(|metadata| metadata.into()),
                dimension,
                get_or_create: Some(get_or_create),
                schema_str: schema
                    .map(|s| serde_json::to_string(&s))
                    .transpose()
                    .map_err(|e| {
                        CreateCollectionError::Schema(SchemaError::InvalidSchema {
                            reason: e.to_string(),
                        })
                    })?,
            })
            .await
            .map_err(|err| match err.code() {
                Code::AlreadyExists => CreateCollectionError::AlreadyExists(name),
                Code::Aborted => CreateCollectionError::Aborted(err.message().to_string()),
                _ => CreateCollectionError::Internal(err.into()),
            })?;

        let collection = res
            .into_inner()
            .collection
            .ok_or(CreateCollectionError::Internal(
                TonicMissingFieldError("collection").boxed(),
            ))?
            .try_into()
            .map_err(|e: CollectionConversionError| CreateCollectionError::Internal(e.boxed()))?;
        Ok(collection)
    }

    async fn update_collection(
        &mut self,
        collection_id: CollectionUuid,
        name: Option<String>,
        metadata: Option<CollectionMetadataUpdate>,
        dimension: Option<u32>,
        configuration: Option<InternalUpdateCollectionConfiguration>,
    ) -> Result<(), UpdateCollectionError> {
        let mut configuration_json_str = None;
        if let Some(configuration) = configuration {
            configuration_json_str = Some(serde_json::to_string(&configuration).unwrap());
        }
        let req = chroma_proto::UpdateCollectionRequest {
            id: collection_id.0.to_string(),
            name: name.clone(),
            metadata_update: metadata.map(|metadata| match metadata {
                CollectionMetadataUpdate::UpdateMetadata(metadata) => {
                    chroma_proto::update_collection_request::MetadataUpdate::Metadata(
                        metadata.into(),
                    )
                }
                CollectionMetadataUpdate::ResetMetadata => {
                    chroma_proto::update_collection_request::MetadataUpdate::ResetMetadata(true)
                }
            }),
            dimension: dimension.map(|dim| dim as i32),
            configuration_json_str,
        };

        self.client.update_collection(req).await.map_err(|e| {
            if e.code() == Code::NotFound {
                UpdateCollectionError::NotFound(collection_id.to_string())
            } else {
                UpdateCollectionError::Internal(e.into())
            }
        })?;

        Ok(())
    }

    async fn delete_collection(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
        segment_ids: Vec<SegmentUuid>,
    ) -> Result<(), DeleteCollectionError> {
        self.client
            .delete_collection(chroma_proto::DeleteCollectionRequest {
                tenant,
                database,
                id: collection_id.0.to_string(),
                segment_ids: segment_ids.into_iter().map(|id| id.0.to_string()).collect(),
            })
            .await
            .map_err(|e| {
                if e.code() == Code::NotFound {
                    DeleteCollectionError::NotFound(collection_id.to_string())
                } else {
                    DeleteCollectionError::Internal(e.into())
                }
            })?;
        Ok(())
    }

    async fn finish_collection_deletion(
        &mut self,
        tenant: String,
        database: String,
        collection_id: CollectionUuid,
    ) -> Result<(), DeleteCollectionError> {
        self.client
            .finish_collection_deletion(chroma_proto::FinishCollectionDeletionRequest {
                tenant,
                database,
                id: collection_id.0.to_string(),
            })
            .await
            .map_err(|e| {
                if e.code() == Code::NotFound {
                    DeleteCollectionError::NotFound(collection_id.to_string())
                } else {
                    DeleteCollectionError::Internal(e.into())
                }
            })?;
        Ok(())
    }

    pub async fn fork_collection(
        &mut self,
        source_collection_id: CollectionUuid,
        source_collection_log_compaction_offset: u64,
        source_collection_log_enumeration_offset: u64,
        target_collection_id: CollectionUuid,
        target_collection_name: String,
    ) -> Result<CollectionAndSegments, ForkCollectionError> {
        let res = self
            .client
            .fork_collection(chroma_proto::ForkCollectionRequest {
                source_collection_id: source_collection_id.0.to_string(),
                source_collection_log_compaction_offset,
                source_collection_log_enumeration_offset,
                target_collection_id: target_collection_id.0.to_string(),
                target_collection_name: target_collection_name.clone(),
            })
            .await
            .map_err(|err| match err.code() {
                Code::AlreadyExists => ForkCollectionError::AlreadyExists(target_collection_name),
                Code::NotFound => ForkCollectionError::NotFound(source_collection_id.0.to_string()),
                _ => ForkCollectionError::Internal(err.into()),
            })?
            .into_inner();
        let raw_segment_counts = res.segments.len();
        let mut segment_map: HashMap<_, _> = res
            .segments
            .into_iter()
            .map(|seg| (seg.scope(), seg))
            .collect();
        if segment_map.len() < raw_segment_counts {
            return Err(ForkCollectionError::DuplicateSegment);
        }
        Ok(CollectionAndSegments {
            collection: res
                .collection
                .ok_or(ForkCollectionError::Field("collection".to_string()))?
                .try_into()?,
            metadata_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Metadata)
                .ok_or(ForkCollectionError::Field("metadata".to_string()))?
                .try_into()?,
            record_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Record)
                .ok_or(ForkCollectionError::Field("record".to_string()))?
                .try_into()?,
            vector_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Vector)
                .ok_or(ForkCollectionError::Field("vector".to_string()))?
                .try_into()?,
        })
    }

    pub async fn count_forks(
        &mut self,
        source_collection_id: CollectionUuid,
    ) -> Result<usize, CountForksError> {
        let res = self
            .client
            .count_forks(chroma_proto::CountForksRequest {
                source_collection_id: source_collection_id.0.to_string(),
            })
            .await
            .map_err(|err| match err.code() {
                Code::NotFound => CountForksError::NotFound(source_collection_id.0.to_string()),
                _ => CountForksError::Internal(err.into()),
            })?
            .into_inner();

        Ok(res.count as usize)
    }

    pub async fn list_attached_functions(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Vec<chroma_proto::AttachedFunction>, ListAttachedFunctionsError> {
        let res = self
            .client
            .list_attached_functions(chroma_proto::ListAttachedFunctionsRequest {
                input_collection_id: collection_id.0.to_string(),
            })
            .await
            .map_err(|err| match err.code() {
                Code::NotFound => ListAttachedFunctionsError::NotFound(collection_id.0.to_string()),
                _ => ListAttachedFunctionsError::Internal(err.into()),
            })?
            .into_inner();

        Ok(res.attached_functions)
    }

    pub async fn get_collections_to_gc(
        &mut self,
        cutoff_time: Option<SystemTime>,
        limit: Option<u64>,
        tenant: Option<String>,
        min_versions_if_alive: Option<u64>,
    ) -> Result<Vec<CollectionToGcInfo>, GetCollectionsToGcError> {
        let res = self
            .client
            .list_collections_to_gc(chroma_proto::ListCollectionsToGcRequest {
                cutoff_time: cutoff_time.map(|t| t.into()),
                limit,
                tenant_id: tenant,
                min_versions_if_alive,
            })
            .await;

        match res {
            Ok(collections) => collections
                .into_inner()
                .collections
                .into_iter()
                .map(|collection| collection.try_into())
                .collect::<Result<Vec<CollectionToGcInfo>, GetCollectionsToGcError>>(),
            Err(e) => Err(GetCollectionsToGcError::RequestFailed(e)),
        }
    }

    pub async fn get_collection_to_gc(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionToGcInfo, GetCollectionsToGcError> {
        let mut collections = self
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                if e.code() == ErrorCodes::NotFound {
                    GetCollectionsToGcError::NoSuchCollection
                } else {
                    GetCollectionsToGcError::Internal(e.boxed())
                }
            })?;

        if collections.is_empty() {
            return Err(GetCollectionsToGcError::NoSuchCollection);
        }
        if collections.len() > 1 {
            tracing::error!(
                "Multiple collections returned when querying for ID: {}",
                collection_id
            );
            return Err(GetCollectionsToGcError::NoSuchCollection);
        }

        let collection = collections.remove(0);

        Ok(CollectionToGcInfo {
            id: collection.collection_id,
            tenant: collection.tenant,
            name: collection.name,
            version_file_path: collection.version_file_path.unwrap_or_default(),
            lineage_file_path: collection.lineage_file_path,
        })
    }

    async fn get_segments(
        &mut self,
        id: Option<SegmentUuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: CollectionUuid,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        let res = self
            .client
            .get_segments(chroma_proto::GetSegmentsRequest {
                // TODO: modularize
                id: id.as_ref().map(ToString::to_string),
                r#type,
                scope: scope.map(|x| x as i32),
                collection: collection.to_string(),
            })
            .await;
        match res {
            Ok(res) => {
                let segments = res.into_inner().segments;
                let converted_segments = segments
                    .into_iter()
                    .map(|proto_segment| proto_segment.try_into())
                    .collect::<Result<Vec<Segment>, SegmentConversionError>>();

                match converted_segments {
                    Ok(segments) => Ok(segments),
                    Err(e) => Err(GetSegmentsError::Internal(e.boxed())),
                }
            }
            Err(e) => Err(GetSegmentsError::Internal(e.into())),
        }
    }

    async fn get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, GetCollectionWithSegmentsError> {
        let res = self
            .client
            .get_collection_with_segments(chroma_proto::GetCollectionWithSegmentsRequest {
                id: collection_id.to_string(),
            })
            .await?
            .into_inner();
        let raw_segment_counts = res.segments.len();
        let mut segment_map: HashMap<_, _> = res
            .segments
            .into_iter()
            .map(|seg| (seg.scope(), seg))
            .collect();
        if segment_map.len() < raw_segment_counts {
            return Err(GetCollectionWithSegmentsError::DuplicateSegment);
        }
        Ok(CollectionAndSegments {
            collection: res
                .collection
                .ok_or(GetCollectionWithSegmentsError::Field(
                    "collection".to_string(),
                ))?
                .try_into()?,
            metadata_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Metadata)
                .ok_or(GetCollectionWithSegmentsError::Field(
                    "metadata".to_string(),
                ))?
                .try_into()?,
            record_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Record)
                .ok_or(GetCollectionWithSegmentsError::Field("record".to_string()))?
                .try_into()?,
            vector_segment: segment_map
                .remove(&chroma_proto::SegmentScope::Vector)
                .ok_or(GetCollectionWithSegmentsError::Field("vector".to_string()))?
                .try_into()?,
        })
    }

    async fn get_all_functions(
        &mut self,
    ) -> Result<Vec<(String, uuid::Uuid)>, Box<dyn std::error::Error>> {
        let res = self
            .client
            .get_functions(chroma_proto::GetFunctionsRequest {})
            .await?;

        let operators = res.into_inner().functions;
        let mut result = Vec::new();
        for op in operators {
            let id = uuid::Uuid::parse_str(&op.id)?;
            result.push((op.name, id));
        }
        Ok(result)
    }

    async fn batch_get_collection_version_file_paths(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, String>, BatchGetCollectionVersionFilePathsError> {
        let res = self
            .client
            .batch_get_collection_version_file_paths(
                chroma_proto::BatchGetCollectionVersionFilePathsRequest {
                    collection_ids: collection_ids
                        .into_iter()
                        .map(|id| id.0.to_string())
                        .collect(),
                },
            )
            .await?;
        let collection_id_to_path = res.into_inner().collection_id_to_version_file_path;
        let mut result = HashMap::new();
        for (key, value) in collection_id_to_path {
            let collection_id = CollectionUuid(
                Uuid::try_parse(&key)
                    .map_err(|err| BatchGetCollectionVersionFilePathsError::Uuid(err, key))?,
            );
            result.insert(collection_id, value);
        }
        Ok(result)
    }

    async fn batch_get_collection_soft_delete_status(
        &mut self,
        collection_ids: Vec<CollectionUuid>,
    ) -> Result<HashMap<CollectionUuid, bool>, BatchGetCollectionSoftDeleteStatusError> {
        let res = self
            .client
            .batch_get_collection_soft_delete_status(
                chroma_proto::BatchGetCollectionSoftDeleteStatusRequest {
                    collection_ids: collection_ids
                        .into_iter()
                        .map(|id| id.0.to_string())
                        .collect(),
                },
            )
            .await?;
        let collection_id_to_status = res.into_inner().collection_id_to_is_soft_deleted;
        let mut result = HashMap::new();
        for (key, value) in collection_id_to_status {
            let collection_id = CollectionUuid(
                Uuid::try_parse(&key)
                    .map_err(|err| BatchGetCollectionSoftDeleteStatusError::Uuid(err, key))?,
            );
            result.insert(collection_id, value);
        }
        Ok(result)
    }

    async fn get_last_compaction_time(
        &mut self,
        tenant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError> {
        let res = self
            .client
            .get_last_compaction_time_for_tenant(
                chroma_proto::GetLastCompactionTimeForTenantRequest {
                    tenant_id: tenant_ids,
                },
            )
            .await;
        match res {
            Ok(res) => {
                let last_compaction_times = res.into_inner().tenant_last_compaction_time;
                let last_compaction_times = last_compaction_times
                    .into_iter()
                    .map(|proto_tenant| proto_tenant.try_into())
                    .collect::<Result<Vec<Tenant>, ()>>();
                Ok(last_compaction_times.unwrap())
            }
            Err(e) => Err(GetLastCompactionTimeError::FailedToGetLastCompactionTime(e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        total_records_post_compaction: u64,
        size_bytes_post_compaction: u64,
        schema: Option<Schema>,
    ) -> Result<FlushCompactionResponse, FlushCompactionError> {
        let segment_compaction_info =
            segment_flush_info
                .iter()
                .map(|segment_flush_info| segment_flush_info.try_into())
                .collect::<Result<
                    Vec<chroma_proto::FlushSegmentCompactionInfo>,
                    SegmentFlushInfoConversionError,
                >>();

        let segment_compaction_info = match segment_compaction_info {
            Ok(segment_compaction_info) => segment_compaction_info,
            Err(e) => {
                return Err(FlushCompactionError::SegmentFlushInfoConversionError(e));
            }
        };

        let schema_str = schema
            .map(|s| serde_json::to_string(&s))
            .transpose()
            .map_err(|e| {
                FlushCompactionError::Schema(SchemaError::InvalidSchema {
                    reason: e.to_string(),
                })
            })?;
        let req = chroma_proto::FlushCollectionCompactionRequest {
            tenant_id,
            collection_id: collection_id.0.to_string(),
            log_position,
            collection_version,
            segment_compaction_info,
            total_records_post_compaction,
            size_bytes_post_compaction,
            schema_str,
        };

        let res = self.client.flush_collection_compaction(req).await;
        match res {
            Ok(res) => {
                let res = res.into_inner();
                // Convert proto response to our type
                let collection_id =
                    CollectionUuid(uuid::Uuid::parse_str(&res.collection_id).map_err(|_| {
                        FlushCompactionError::FlushCompactionResponseConversionError(
                            FlushCompactionResponseConversionError::InvalidUuid,
                        )
                    })?);
                Ok(FlushCompactionResponse {
                    collection_id,
                    collection_version: res.collection_version,
                    last_compaction_time: res.last_compaction_time,
                })
            }
            Err(e) => Err(FlushCompactionError::FailedToFlushCompaction(e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_compaction_and_attached_function(
        &mut self,
        tenant_id: String,
        collection_id: CollectionUuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        total_records_post_compaction: u64,
        size_bytes_post_compaction: u64,
        schema: Option<Schema>,
        attached_function_update: AttachedFunctionUpdateInfo,
    ) -> Result<FlushCompactionAndAttachedFunctionResponse, FlushCompactionError> {
        let segment_compaction_info =
            segment_flush_info
                .iter()
                .map(|segment_flush_info| segment_flush_info.try_into())
                .collect::<Result<
                    Vec<chroma_proto::FlushSegmentCompactionInfo>,
                    SegmentFlushInfoConversionError,
                >>();

        let segment_compaction_info = match segment_compaction_info {
            Ok(segment_compaction_info) => segment_compaction_info,
            Err(e) => {
                return Err(FlushCompactionError::SegmentFlushInfoConversionError(e));
            }
        };

        let schema_str = schema.and_then(|s| {
            serde_json::to_string(&s).ok().or_else(|| {
                tracing::error!(
                    "Failed to serialize schema for flush_compaction_and_attached_function"
                );
                None
            })
        });

        let flush_compaction = Some(chroma_proto::FlushCollectionCompactionRequest {
            tenant_id,
            collection_id: collection_id.0.to_string(),
            log_position,
            collection_version,
            segment_compaction_info,
            total_records_post_compaction,
            size_bytes_post_compaction,
            schema_str,
        });

        let attached_function_update_proto = Some(chroma_proto::AttachedFunctionUpdateInfo {
            id: attached_function_update.attached_function_id.0.to_string(),
            run_nonce: attached_function_update
                .attached_function_run_nonce
                .to_string(),
            completion_offset: attached_function_update.completion_offset,
        });

        let req = chroma_proto::FlushCollectionCompactionAndAttachedFunctionRequest {
            flush_compaction,
            attached_function_update: attached_function_update_proto,
        };

        let res = self
            .client
            .flush_collection_compaction_and_attached_function(req)
            .await;
        match res {
            Ok(res) => {
                let res = res.into_inner();
                let res = match res.try_into() {
                    Ok(res) => res,
                    Err(e) => {
                        return Err(
                            FlushCompactionError::FlushCompactionResponseConversionError(e),
                        );
                    }
                };
                Ok(res)
            }
            Err(e) => {
                if e.code() == Code::FailedPrecondition {
                    return Err(FlushCompactionError::FailedToFlushCompaction(e));
                }
                Err(FlushCompactionError::FailedToFlushCompaction(e))
            }
        }
    }

    async fn mark_version_for_deletion(
        &mut self,
        epoch_id: i64,
        versions: Vec<chroma_proto::VersionListForCollection>,
    ) -> Result<HashMap<String, bool>, MarkVersionForDeletionError> {
        let req = chroma_proto::MarkVersionForDeletionRequest { epoch_id, versions };

        let res = self.client.mark_version_for_deletion(req).await?;
        Ok(res.into_inner().collection_id_to_success)
    }

    async fn delete_collection_version(
        &mut self,
        versions: Vec<chroma_proto::VersionListForCollection>,
    ) -> Result<HashMap<String, bool>, DeleteCollectionVersionError> {
        let req = chroma_proto::DeleteCollectionVersionRequest {
            epoch_id: 0, // TODO: Pass this through
            versions,
        };

        let res = self.client.delete_collection_version(req).await?;
        Ok(res.into_inner().collection_id_to_success)
    }

    async fn update_tenant(
        &mut self,
        tenant_id: String,
        resource_name: String,
    ) -> Result<UpdateTenantResponse, UpdateTenantError> {
        let req = chroma_proto::SetTenantResourceNameRequest {
            id: tenant_id,
            resource_name,
        };

        self.client.set_tenant_resource_name(req).await?;
        Ok(UpdateTenantResponse {})
    }

    async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        self.client
            .reset_state(())
            .await
            .map_err(|e| TonicError(e).boxed())?;
        Ok(ResetResponse {})
    }

    async fn finish_attached_function(
        &mut self,
        attached_function_id: AttachedFunctionUuid,
    ) -> Result<(), FinishAttachedFunctionError> {
        let req = FinishAttachedFunctionRequest {
            id: attached_function_id.0.to_string(),
        };
        self.client
            .finish_attached_function(req)
            .await
            .map_err(|e| {
                if e.code() == Code::NotFound {
                    FinishAttachedFunctionError::AttachedFunctionNotFound
                } else {
                    FinishAttachedFunctionError::FailedToFinishAttachedFunction(e)
                }
            })?;
        Ok(())
    }

    async fn advance_attached_function(
        &mut self,
        attached_function_id: AttachedFunctionUuid,
        attached_function_run_nonce: uuid::Uuid,
        completion_offset: u64,
        next_run_delay_secs: u64,
    ) -> Result<AdvanceAttachedFunctionResponse, AdvanceAttachedFunctionError> {
        let req = AdvanceAttachedFunctionRequest {
            collection_id: None, // Not used by coordinator
            id: Some(attached_function_id.0.to_string()),
            run_nonce: Some(attached_function_run_nonce.to_string()),
            completion_offset: Some(completion_offset),
            next_run_delay_secs: Some(next_run_delay_secs),
        };

        let response = self
            .client
            .advance_attached_function(req)
            .await
            .map_err(|e| {
                if e.code() == Code::NotFound {
                    AdvanceAttachedFunctionError::AttachedFunctionNotFound
                } else {
                    AdvanceAttachedFunctionError::FailedToAdvanceAttachedFunction(e)
                }
            })?;

        let response = response.into_inner();

        // Parse next_nonce
        let next_nonce = uuid::Uuid::parse_str(&response.next_run_nonce).map_err(|e| {
            tracing::error!(
                next_nonce = %response.next_run_nonce,
                error = %e,
                "Server returned invalid next_nonce UUID"
            );
            AdvanceAttachedFunctionError::FailedToAdvanceAttachedFunction(tonic::Status::internal(
                "Invalid next_nonce in response",
            ))
        })?;

        // Parse next_run timestamp
        let next_run =
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(response.next_run_at);

        Ok(AdvanceAttachedFunctionResponse {
            next_nonce,
            next_run,
            completion_offset: response.completion_offset,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_attached_function(
        &mut self,
        name: String,
        operator_name: String,
        input_collection_id: chroma_types::CollectionUuid,
        output_collection_name: String,
        params: serde_json::Value,
        tenant_name: String,
        database_name: String,
        min_records_for_invocation: u64,
    ) -> Result<chroma_types::AttachedFunctionUuid, AttachFunctionError> {
        // Convert serde_json::Value to prost_types::Struct for gRPC
        let params_struct = match params {
            serde_json::Value::Object(map) => Some(prost_types::Struct {
                fields: map
                    .into_iter()
                    .map(|(k, v)| (k, json_to_prost_value(v)))
                    .collect(),
            }),
            _ => None, // Non-object params omitted from proto
        };
        let req = chroma_proto::AttachFunctionRequest {
            name: name.clone(),
            function_name: operator_name.clone(),
            input_collection_id: input_collection_id.to_string(),
            output_collection_name: output_collection_name.clone(),
            params: params_struct,
            tenant_id: tenant_name.clone(),
            database: database_name.clone(),
            min_records_for_invocation,
        };
        let response = self.client.attach_function(req).await?.into_inner();
        // Parse the returned attached_function_id - this should always succeed since the server generated it
        // If this fails, it indicates a serious server bug or protocol corruption
        let attached_function_id = chroma_types::AttachedFunctionUuid(
            uuid::Uuid::parse_str(&response.id).map_err(|e| {
                tracing::error!(
                    attached_function_id = %response.id,
                    error = %e,
                    "Server returned invalid attached_function_id UUID - attached function was created but response is corrupt"
                );
                AttachFunctionError::ServerReturnedInvalidData
            })?,
        );
        Ok(attached_function_id)
    }

    /// Helper function to convert a proto AttachedFunction to a chroma_types::AttachedFunction
    fn attached_function_from_proto(
        attached_function: chroma_proto::AttachedFunction,
    ) -> Result<chroma_types::AttachedFunction, GetAttachedFunctionError> {
        // Parse attached_function_id
        let attached_function_id = chroma_types::AttachedFunctionUuid(
            uuid::Uuid::parse_str(&attached_function.id).map_err(|e| {
                tracing::error!(
                    attached_function_id = %attached_function.id,
                    error = %e,
                    "Server returned invalid attached_function_id UUID"
                );
                GetAttachedFunctionError::ServerReturnedInvalidData
            })?,
        );

        // Parse input_collection_id
        let parsed_input_collection_id = chroma_types::CollectionUuid(
            uuid::Uuid::parse_str(&attached_function.input_collection_id).map_err(|e| {
                tracing::error!(
                    input_collection_id = %attached_function.input_collection_id,
                    error = %e,
                    "Server returned invalid input_collection_id UUID"
                );
                GetAttachedFunctionError::ServerReturnedInvalidData
            })?,
        );

        // Parse next_run timestamp from microseconds
        let next_run = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_micros(attached_function.next_run_at);

        // Parse nonces
        let lowest_live_nonce = match &attached_function.lowest_live_nonce {
            Some(nonce_str) if !nonce_str.is_empty() => Some(
                uuid::Uuid::parse_str(nonce_str)
                    .map(chroma_types::NonceUuid)
                    .map_err(|e| {
                        tracing::error!(
                            lowest_live_nonce = %nonce_str,
                            error = %e,
                            "Server returned invalid lowest_live_nonce UUID"
                        );
                        GetAttachedFunctionError::ServerReturnedInvalidData
                    })?,
            ),
            _ => None,
        };

        let next_nonce = uuid::Uuid::parse_str(&attached_function.next_nonce)
            .map(chroma_types::NonceUuid)
            .map_err(|e| {
                tracing::error!(
                    next_nonce = %attached_function.next_nonce,
                    error = %e,
                    "Server returned invalid next_nonce UUID"
                );
                GetAttachedFunctionError::ServerReturnedInvalidData
            })?;

        // Convert params from Struct to JSON string
        let params_str = attached_function.params.map(|s| {
            let json_value = prost_struct_to_json(s);
            serde_json::to_string(&json_value).unwrap_or_else(|_| "{}".to_string())
        });

        // Parse output_collection_id if present
        let parsed_output_collection_id =
            if let Some(id_str) = attached_function.output_collection_id.as_ref() {
                if id_str.is_empty() {
                    None
                } else {
                    Some(chroma_types::CollectionUuid(
                        uuid::Uuid::parse_str(id_str).map_err(|e| {
                            tracing::error!(
                                output_collection_id = %id_str,
                                error = %e,
                                "Server returned invalid output_collection_id UUID"
                            );
                            GetAttachedFunctionError::ServerReturnedInvalidData
                        })?,
                    ))
                }
            } else {
                None
            };

        // Parse function_id from the dedicated UUID field
        let function_id = uuid::Uuid::parse_str(&attached_function.function_id).map_err(|e| {
            tracing::error!(
                function_id = %attached_function.function_id,
                error = %e,
                "Server returned invalid function_id UUID"
            );
            GetAttachedFunctionError::ServerReturnedInvalidData
        })?;

        Ok(chroma_types::AttachedFunction {
            id: attached_function_id,
            name: attached_function.name,
            function_id,
            input_collection_id: parsed_input_collection_id,
            output_collection_name: attached_function.output_collection_name,
            output_collection_id: parsed_output_collection_id,
            params: params_str,
            tenant_id: attached_function.tenant_id,
            database_id: attached_function.database_id,
            last_run: None,
            next_run,
            lowest_live_nonce,
            next_nonce,
            completion_offset: attached_function.completion_offset,
            min_records_for_invocation: attached_function.min_records_for_invocation,
            is_deleted: false,
            created_at: std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_micros(attached_function.created_at),
            updated_at: std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_micros(attached_function.updated_at),
        })
    }

    pub async fn get_attached_function_by_name(
        &mut self,
        input_collection_id: chroma_types::CollectionUuid,
        attached_function_name: String,
    ) -> Result<chroma_types::AttachedFunction, GetAttachedFunctionError> {
        let req = chroma_proto::GetAttachedFunctionByNameRequest {
            input_collection_id: input_collection_id.to_string(),
            name: attached_function_name.clone(),
        };

        let response = match self.client.get_attached_function_by_name(req).await {
            Ok(resp) => resp,
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    return Err(GetAttachedFunctionError::NotFound);
                }
                return Err(GetAttachedFunctionError::FailedToGetAttachedFunction(
                    status,
                ));
            }
        };
        let response = response.into_inner();

        // Extract the nested attached function from response
        let attached_function = response.attached_function.ok_or_else(|| {
            GetAttachedFunctionError::FailedToGetAttachedFunction(tonic::Status::internal(
                "Missing attached function in response",
            ))
        })?;

        Self::attached_function_from_proto(attached_function)
    }

    pub async fn get_attached_function_by_uuid(
        &mut self,
        attached_function_uuid: chroma_types::AttachedFunctionUuid,
    ) -> Result<chroma_types::AttachedFunction, GetAttachedFunctionError> {
        let req = chroma_proto::GetAttachedFunctionByUuidRequest {
            id: attached_function_uuid.0.to_string(),
        };

        let response = match self.client.get_attached_function_by_uuid(req).await {
            Ok(resp) => resp,
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    return Err(GetAttachedFunctionError::NotFound);
                }
                return Err(GetAttachedFunctionError::FailedToGetAttachedFunction(
                    status,
                ));
            }
        };
        let response = response.into_inner();

        // Extract the nested attached function from response
        let attached_function = response.attached_function.ok_or_else(|| {
            GetAttachedFunctionError::FailedToGetAttachedFunction(tonic::Status::internal(
                "Missing attached function in response",
            ))
        })?;

        Self::attached_function_from_proto(attached_function)
    }

    pub async fn create_output_collection_for_attached_function(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
        collection_name: String,
        tenant_id: String,
        database_id: String,
    ) -> Result<CollectionUuid, CreateOutputCollectionForAttachedFunctionError> {
        let req = chroma_proto::CreateOutputCollectionForAttachedFunctionRequest {
            attached_function_id: attached_function_id.0.to_string(),
            collection_name,
            tenant_id,
            database_id,
        };

        let response = self
            .client
            .create_output_collection_for_attached_function(req)
            .await
            .map_err(|e| {
                if e.code() == tonic::Code::NotFound {
                    return CreateOutputCollectionForAttachedFunctionError::AttachedFunctionNotFound;
                }
                if e.code() == tonic::Code::AlreadyExists {
                    return CreateOutputCollectionForAttachedFunctionError::OutputCollectionAlreadyExists;
                }
                CreateOutputCollectionForAttachedFunctionError::FailedToCreateOutputCollectionForAttachedFunction(e)
            })?;

        let response = response.into_inner();

        // Parse the returned collection_id
        let collection_id = uuid::Uuid::parse_str(&response.collection_id).map_err(|e| {
            tracing::error!(
                collection_id = %response.collection_id,
                error = %e,
                "Server returned invalid collection_id UUID"
            );
            CreateOutputCollectionForAttachedFunctionError::ServerReturnedInvalidData
        })?;

        Ok(CollectionUuid(collection_id))
    }

    pub async fn soft_delete_attached_function(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
        delete_output: bool,
    ) -> Result<(), DeleteAttachedFunctionError> {
        let req = chroma_proto::DetachFunctionRequest {
            attached_function_id: attached_function_id.to_string(),
            delete_output,
        };

        match self.client.detach_function(req).await {
            Ok(_) => Ok(()),
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    Err(DeleteAttachedFunctionError::NotFound)
                } else {
                    Err(DeleteAttachedFunctionError::FailedToDeleteAttachedFunction(
                        status,
                    ))
                }
            }
        }
    }

    async fn peek_schedule_by_collection_id(
        &mut self,
        collection_ids: &[CollectionUuid],
    ) -> Result<Vec<ScheduleEntry>, PeekScheduleError> {
        let req = chroma_proto::PeekScheduleByCollectionIdRequest {
            collection_id: collection_ids.iter().map(|id| id.0.to_string()).collect(),
        };
        let res = self
            .client
            .peek_schedule_by_collection_id(req)
            .await
            .map_err(|e| TonicError(e).boxed())?;
        res.into_inner()
            .schedule
            .into_iter()
            .map(|entry| entry.try_into())
            .collect::<Result<Vec<ScheduleEntry>, ScheduleEntryConversionError>>()
            .map_err(PeekScheduleError::Conversion)
    }

    async fn get_soft_deleted_attached_functions(
        &mut self,
        cutoff_time: SystemTime,
        limit: i32,
    ) -> Result<Vec<chroma_types::AttachedFunctionUuid>, GetSoftDeletedAttachedFunctionsError> {
        let cutoff_timestamp = prost_types::Timestamp {
            seconds: cutoff_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            nanos: 0,
        };

        let req = chroma_proto::GetSoftDeletedAttachedFunctionsRequest {
            cutoff_time: Some(cutoff_timestamp),
            limit,
        };

        let res = self
            .client
            .get_soft_deleted_attached_functions(req)
            .await
            .map_err(|e| {
                GetSoftDeletedAttachedFunctionsError::FailedToGetSoftDeletedAttachedFunctions(e)
            })?;

        let attached_function_ids: Result<Vec<chroma_types::AttachedFunctionUuid>, _> = res
            .into_inner()
            .attached_functions
            .into_iter()
            .map(|af| {
                uuid::Uuid::parse_str(&af.id)
                    .map(chroma_types::AttachedFunctionUuid)
                    .map_err(|e| {
                        tracing::error!(
                            attached_function_id = %af.id,
                            error = %e,
                            "Server returned invalid attached_function_id UUID"
                        );
                        GetSoftDeletedAttachedFunctionsError::ServerReturnedInvalidData
                    })
            })
            .collect();

        attached_function_ids
    }

    async fn finish_attached_function_deletion(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
    ) -> Result<(), FinishAttachedFunctionDeletionError> {
        let req = chroma_proto::FinishAttachedFunctionDeletionRequest {
            attached_function_id: attached_function_id.to_string(),
        };

        self.client
            .finish_attached_function_deletion(req)
            .await
            .map_err(FinishAttachedFunctionDeletionError::FailedToFinishDeletion)?;

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum PeekScheduleError {
    #[error("Failed to peek schedule")]
    Internal(#[from] Box<dyn ChromaError>),
    #[error("Failed to convert schedule entry")]
    Conversion(#[from] ScheduleEntryConversionError),
}

impl ChromaError for PeekScheduleError {
    fn code(&self) -> ErrorCodes {
        match self {
            PeekScheduleError::Internal(e) => e.code(),
            PeekScheduleError::Conversion(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GetSoftDeletedAttachedFunctionsError {
    #[error("Failed to get soft deleted attached functions: {0}")]
    FailedToGetSoftDeletedAttachedFunctions(#[from] tonic::Status),
    #[error("Server returned invalid data - response contains corrupt attached function IDs")]
    ServerReturnedInvalidData,
    #[error("Not implemented for this SysDb backend")]
    NotImplemented,
}

impl ChromaError for GetSoftDeletedAttachedFunctionsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum FinishAttachedFunctionDeletionError {
    #[error("Failed to finish attached function deletion: {0}")]
    FailedToFinishDeletion(#[from] tonic::Status),
    #[error("Not implemented for this SysDb backend")]
    NotImplemented,
}

impl ChromaError for FinishAttachedFunctionDeletionError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum GetLastCompactionTimeError {
    #[error("Failed to fetch")]
    FailedToGetLastCompactionTime(#[from] tonic::Status),

    #[error("Tenant not found in sysdb")]
    TenantNotFound,
}

impl ChromaError for GetLastCompactionTimeError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetLastCompactionTimeError::FailedToGetLastCompactionTime(_) => ErrorCodes::Internal,
            GetLastCompactionTimeError::TenantNotFound => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum FlushCompactionError {
    #[error("Failed to flush compaction")]
    FailedToFlushCompaction(#[from] tonic::Status),
    #[error("Failed to convert segment flush info")]
    SegmentFlushInfoConversionError(#[from] SegmentFlushInfoConversionError),
    #[error("Failed to convert flush compaction response")]
    FlushCompactionResponseConversionError(#[from] FlushCompactionResponseConversionError),
    #[error("Collection not found in sysdb")]
    CollectionNotFound,
    #[error("Segment not found in sysdb")]
    SegmentNotFound,
    #[error("Failed to serialize schema")]
    Schema(#[from] SchemaError),
}

impl ChromaError for FlushCompactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushCompactionError::FailedToFlushCompaction(status) => {
                if status.code() == Code::FailedPrecondition {
                    ErrorCodes::FailedPrecondition
                } else {
                    ErrorCodes::Internal
                }
            }
            FlushCompactionError::SegmentFlushInfoConversionError(_) => ErrorCodes::Internal,
            FlushCompactionError::FlushCompactionResponseConversionError(_) => ErrorCodes::Internal,
            FlushCompactionError::CollectionNotFound => ErrorCodes::Internal,
            FlushCompactionError::SegmentNotFound => ErrorCodes::Internal,
            FlushCompactionError::Schema(e) => e.code(),
        }
    }

    fn should_trace_error(&self) -> bool {
        self.code() == ErrorCodes::Internal
    }
}

#[derive(Error, Debug)]
pub enum MarkVersionForDeletionError {
    #[error("Failed to mark version for deletion")]
    FailedToMarkVersion(#[from] tonic::Status),
}

impl ChromaError for MarkVersionForDeletionError {
    fn code(&self) -> ErrorCodes {
        match self {
            MarkVersionForDeletionError::FailedToMarkVersion(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum DeleteCollectionVersionError {
    #[error("Failed to delete version: {0}")]
    FailedToDeleteVersion(#[from] tonic::Status),
}

impl ChromaError for DeleteCollectionVersionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteCollectionVersionError::FailedToDeleteVersion(e) => e.code().into(),
        }
    }
}

//////////////////////////  Attached Function Operations //////////////////////////

impl SysDb {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_attached_function(
        &mut self,
        name: String,
        operator_name: String,
        input_collection_id: chroma_types::CollectionUuid,
        output_collection_name: String,
        params: serde_json::Value,
        tenant_name: String,
        database_name: String,
        min_records_for_invocation: u64,
    ) -> Result<chroma_types::AttachedFunctionUuid, AttachFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.create_attached_function(
                    name,
                    operator_name,
                    input_collection_id,
                    output_collection_name,
                    params,
                    tenant_name,
                    database_name,
                    min_records_for_invocation,
                )
                .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .create_attached_function(
                        name,
                        operator_name,
                        input_collection_id,
                        output_collection_name,
                        params,
                        tenant_name,
                        database_name,
                        min_records_for_invocation,
                    )
                    .await
            }
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn get_attached_function_by_name(
        &mut self,
        input_collection_id: chroma_types::CollectionUuid,
        attached_function_name: String,
    ) -> Result<chroma_types::AttachedFunction, GetAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_attached_function_by_name(input_collection_id, attached_function_name)
                    .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .get_attached_function_by_name(input_collection_id, attached_function_name)
                    .await
            }
            SysDb::Test(_) => {
                todo!()
            }
        }
    }

    pub async fn get_attached_function_by_uuid(
        &mut self,
        attached_function_uuid: chroma_types::AttachedFunctionUuid,
    ) -> Result<chroma_types::AttachedFunction, GetAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_attached_function_by_uuid(attached_function_uuid)
                    .await
            }
            SysDb::Sqlite(_) => {
                // TODO: Implement for Sqlite
                Err(GetAttachedFunctionError::NotFound)
            }
            SysDb::Test(_) => {
                // TODO: Implement for TestSysDb
                Err(GetAttachedFunctionError::NotFound)
            }
        }
    }

    pub async fn create_output_collection_for_attached_function(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
        collection_name: String,
        tenant_id: String,
        database_id: String,
    ) -> Result<CollectionUuid, CreateOutputCollectionForAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.create_output_collection_for_attached_function(
                    attached_function_id,
                    collection_name,
                    tenant_id,
                    database_id,
                )
                .await
            }
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(_) => todo!(),
        }
    }

    pub async fn soft_delete_attached_function(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
        delete_output: bool,
    ) -> Result<(), DeleteAttachedFunctionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.soft_delete_attached_function(attached_function_id, delete_output)
                    .await
            }
            SysDb::Sqlite(_) => Err(DeleteAttachedFunctionError::NotImplemented),
            SysDb::Test(_) => Err(DeleteAttachedFunctionError::NotImplemented),
        }
    }

    pub async fn get_soft_deleted_attached_functions(
        &mut self,
        cutoff_time: SystemTime,
        limit: i32,
    ) -> Result<Vec<chroma_types::AttachedFunctionUuid>, GetSoftDeletedAttachedFunctionsError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_soft_deleted_attached_functions(cutoff_time, limit)
                    .await
            }
            SysDb::Sqlite(_) => Err(GetSoftDeletedAttachedFunctionsError::NotImplemented),
            SysDb::Test(_) => Err(GetSoftDeletedAttachedFunctionsError::NotImplemented),
        }
    }

    pub async fn finish_attached_function_deletion(
        &mut self,
        attached_function_id: chroma_types::AttachedFunctionUuid,
    ) -> Result<(), FinishAttachedFunctionDeletionError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.finish_attached_function_deletion(attached_function_id)
                    .await
            }
            SysDb::Sqlite(_) => Err(FinishAttachedFunctionDeletionError::NotImplemented),
            SysDb::Test(_) => Err(FinishAttachedFunctionDeletionError::NotImplemented),
        }
    }
}

#[derive(Error, Debug)]
pub enum AttachFunctionError {
    #[error("Attached function already exists")]
    AlreadyExists,
    #[error("Failed to create attached function: {0}")]
    FailedToCreateAttachedFunction(#[from] tonic::Status),
    #[error(
        "Server returned invalid data - attached function was created but response is corrupt"
    )]
    ServerReturnedInvalidData,
}

impl ChromaError for AttachFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            AttachFunctionError::AlreadyExists => ErrorCodes::AlreadyExists,
            AttachFunctionError::FailedToCreateAttachedFunction(e) => e.code().into(),
            AttachFunctionError::ServerReturnedInvalidData => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum GetAttachedFunctionError {
    #[error("Attached function not found")]
    NotFound,
    #[error("Attached function not ready - still initializing")]
    NotReady,
    #[error("Failed to get attached function: {0}")]
    FailedToGetAttachedFunction(tonic::Status),
    #[error("Server returned invalid data")]
    ServerReturnedInvalidData,
}

impl ChromaError for GetAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetAttachedFunctionError::NotFound => ErrorCodes::NotFound,
            GetAttachedFunctionError::NotReady => ErrorCodes::FailedPrecondition,
            GetAttachedFunctionError::FailedToGetAttachedFunction(e) => e.code().into(),
            GetAttachedFunctionError::ServerReturnedInvalidData => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum CreateOutputCollectionForAttachedFunctionError {
    #[error("Attached function not found")]
    AttachedFunctionNotFound,
    #[error("Output collection already exists")]
    OutputCollectionAlreadyExists,
    #[error("Failed to create output collection for attached function: {0}")]
    FailedToCreateOutputCollectionForAttachedFunction(#[from] tonic::Status),
    #[error("Server returned invalid data")]
    ServerReturnedInvalidData,
}

impl ChromaError for CreateOutputCollectionForAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CreateOutputCollectionForAttachedFunctionError::AttachedFunctionNotFound => ErrorCodes::NotFound,
            CreateOutputCollectionForAttachedFunctionError::OutputCollectionAlreadyExists => {
                ErrorCodes::AlreadyExists
            }
            CreateOutputCollectionForAttachedFunctionError::FailedToCreateOutputCollectionForAttachedFunction(e) => {
                e.code().into()
            }
            CreateOutputCollectionForAttachedFunctionError::ServerReturnedInvalidData => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum DeleteAttachedFunctionError {
    #[error("Attached function not found")]
    NotFound,
    #[error("Failed to delete attached function: {0}")]
    FailedToDeleteAttachedFunction(#[from] tonic::Status),
    #[error("Not implemented for this SysDb backend")]
    NotImplemented,
}

impl ChromaError for DeleteAttachedFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DeleteAttachedFunctionError::NotFound => ErrorCodes::NotFound,
            DeleteAttachedFunctionError::FailedToDeleteAttachedFunction(e) => e.code().into(),
            DeleteAttachedFunctionError::NotImplemented => ErrorCodes::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use tonic::Status;

    use super::*;

    #[test]
    fn flush_compaction_error() {
        let fce = FlushCompactionError::FailedToFlushCompaction(Status::failed_precondition(
            "collection soft deleted",
        ));
        assert!(!fce.should_trace_error());
    }

    #[test]
    fn get_collections_to_gc_error_internal_propagation() {
        // Test that Internal errors are properly propagated with their original error code
        let internal_error = GetCollectionsToGcError::Internal(Box::new(chroma_error::TonicError(
            Status::internal("database error"),
        )));
        assert_eq!(internal_error.code(), ErrorCodes::Internal);

        // Test that NoSuchCollection returns NotFound
        let not_found_error = GetCollectionsToGcError::NoSuchCollection;
        assert_eq!(not_found_error.code(), ErrorCodes::NotFound);
    }
}

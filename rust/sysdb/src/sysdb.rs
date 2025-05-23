use super::test_sysdb::TestSysDb;
use crate::sqlite::SqliteSysDb;
use crate::GrpcSysDbConfig;
use async_trait::async_trait;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes, TonicError, TonicMissingFieldError};
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::VersionListForCollection;
use chroma_types::{
    chroma_proto, chroma_proto::CollectionVersionInfo, CollectionAndSegments,
    CollectionMetadataUpdate, CountCollectionsError, CreateCollectionError, CreateDatabaseError,
    CreateDatabaseResponse, CreateTenantError, CreateTenantResponse, Database,
    DeleteCollectionError, DeleteDatabaseError, DeleteDatabaseResponse, GetCollectionSizeError,
    GetCollectionWithSegmentsError, GetCollectionsError, GetDatabaseError, GetDatabaseResponse,
    GetSegmentsError, GetTenantError, GetTenantResponse, InternalCollectionConfiguration,
    ListCollectionVersionsError, ListDatabasesError, ListDatabasesResponse, Metadata, ResetError,
    ResetResponse, SegmentFlushInfo, SegmentFlushInfoConversionError, SegmentUuid,
    UpdateCollectionConfiguration, UpdateCollectionError, VectorIndexConfiguration,
};
use chroma_types::{
    BatchGetCollectionSoftDeleteStatusError, BatchGetCollectionVersionFilePathsError, Collection,
    CollectionConversionError, CollectionUuid, CountForksError, FinishDatabaseDeletionError,
    FlushCompactionResponse, FlushCompactionResponseConversionError, ForkCollectionError, Segment,
    SegmentConversionError, SegmentScope, Tenant,
};
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
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        match self {
            SysDb::Grpc(grpc) => {
                grpc.get_collections(collection_id, name, tenant, database, limit, offset)
                    .await
            }
            SysDb::Sqlite(sqlite) => {
                sqlite
                    .get_collections(collection_id, name, tenant, database, limit, offset)
                    .await
            }
            SysDb::Test(test) => {
                test.get_collections(collection_id, name, tenant, database)
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
                .get_collections(None, None, Some(tenant), database, None, 0)
                .await
                .map_err(|_| CountCollectionsError::Internal)?
                .len()),
            SysDb::Test(test) => Ok(test
                .get_collections(None, None, Some(tenant), database)
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
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<Collection, CreateCollectionError> {
        let configuration = match configuration {
            Some(mut config) => {
                let hnsw_params = config.get_hnsw_config_from_legacy_metadata(&metadata)?;
                if let Some(hnsw_params) = hnsw_params {
                    config.vector_index = VectorIndexConfiguration::Hnsw(hnsw_params);
                }
                config
            }
            None => metadata
                .clone()
                .map(|m| {
                    InternalCollectionConfiguration::from_legacy_metadata(m).map_err(|e| e.boxed())
                })
                .transpose()?
                .unwrap_or(InternalCollectionConfiguration::default_hnsw()),
        };

        match self {
            SysDb::Grpc(grpc) => {
                grpc.create_collection(
                    tenant,
                    database,
                    collection_id,
                    name,
                    segments,
                    configuration,
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
                    config: configuration,
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
        configuration: Option<UpdateCollectionConfiguration>,
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

    pub async fn get_collections_to_gc(
        &mut self,
        cutoff_time: Option<SystemTime>,
        limit: Option<u64>,
        tenant: Option<String>,
    ) -> Result<Vec<CollectionToGcInfo>, GetCollectionsToGcError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_collections_to_gc(cutoff_time, limit, tenant).await,
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
        tanant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError> {
        match self {
            SysDb::Grpc(grpc) => grpc.get_last_compaction_time(tanant_ids).await,
            SysDb::Sqlite(_) => todo!(),
            SysDb::Test(test) => test.get_last_compaction_time(tanant_ids).await,
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
}

#[derive(Clone, Debug)]
// Since this uses tonic transport channel, cloning is cheap. Each client only supports
// one inflight request at a time, so we need to clone the client for each requester.
pub struct GrpcSysDb {
    #[allow(clippy::type_complexity)]
    client: SysDbClient<chroma_tracing::GrpcTraceService<tonic::transport::Channel>>,
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
            .layer(chroma_tracing::GrpcTraceLayer)
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
    #[error("Failed to parse uuid")]
    ParsingError(#[from] Error),
    #[error("Grpc request failed")]
    RequestFailed(#[from] tonic::Status),
}

impl ChromaError for GetCollectionsToGcError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsToGcError::ParsingError(_) => ErrorCodes::Internal,
            GetCollectionsToGcError::RequestFailed(_) => ErrorCodes::Internal,
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
            Ok(resp) => Ok(GetTenantResponse {
                name: resp
                    .into_inner()
                    .tenant
                    .ok_or(GetTenantError::NotFound(tenant_name))?
                    .name,
            }),
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
        collection_id: Option<CollectionUuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
        limit: Option<u32>,
        offset: u32,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        // TODO: move off of status into our own error type
        let collection_id_str = collection_id.map(|id| String::from(id.0));
        let res = self
            .client
            .get_collections(chroma_proto::GetCollectionsRequest {
                id: collection_id_str,
                name,
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
        configuration: InternalCollectionConfiguration,
        metadata: Option<Metadata>,
        dimension: Option<i32>,
        get_or_create: bool,
    ) -> Result<Collection, CreateCollectionError> {
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
                configuration_json_str: serde_json::to_string(&configuration)
                    .map_err(CreateCollectionError::Configuration)?,
                metadata: metadata.map(|metadata| metadata.into()),
                dimension,
                get_or_create: Some(get_or_create),
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
        configuration: Option<UpdateCollectionConfiguration>,
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

    pub async fn get_collections_to_gc(
        &mut self,
        cutoff_time: Option<SystemTime>,
        limit: Option<u64>,
        tenant: Option<String>,
    ) -> Result<Vec<CollectionToGcInfo>, GetCollectionsToGcError> {
        let res = self
            .client
            .list_collections_to_gc(chroma_proto::ListCollectionsToGcRequest {
                cutoff_time: cutoff_time.map(|t| t.into()),
                limit,
                tenant_id: tenant,
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

        let req = chroma_proto::FlushCollectionCompactionRequest {
            tenant_id,
            collection_id: collection_id.0.to_string(),
            log_position,
            collection_version,
            segment_compaction_info,
            total_records_post_compaction,
            size_bytes_post_compaction,
        };

        let res = self.client.flush_collection_compaction(req).await;
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
            Err(e) => Err(FlushCompactionError::FailedToFlushCompaction(e)),
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

    async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        self.client
            .reset_state(())
            .await
            .map_err(|e| TonicError(e).boxed())?;
        Ok(ResetResponse {})
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
}

impl ChromaError for FlushCompactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            FlushCompactionError::FailedToFlushCompaction(_) => ErrorCodes::Internal,
            FlushCompactionError::SegmentFlushInfoConversionError(_) => ErrorCodes::Internal,
            FlushCompactionError::FlushCompactionResponseConversionError(_) => ErrorCodes::Internal,
            FlushCompactionError::CollectionNotFound => ErrorCodes::Internal,
            FlushCompactionError::SegmentNotFound => ErrorCodes::Internal,
        }
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

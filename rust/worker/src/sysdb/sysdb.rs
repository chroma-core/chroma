use super::config::SysDbConfig;
use crate::chroma_proto;
use crate::chroma_proto::sys_db_client;
use crate::config::Configurable;
use crate::errors::ChromaError;
use crate::errors::ErrorCodes;
use crate::types::Collection;
use crate::types::CollectionConversionError;
use crate::types::FlushCompactionResponse;
use crate::types::FlushCompactionResponseConversionError;
use crate::types::Segment;
use crate::types::SegmentConversionError;
use crate::types::SegmentFlushInfo;
use crate::types::SegmentFlushInfoConversionError;
use crate::types::SegmentScope;
use crate::types::Tenant;
use async_trait::async_trait;
use std::sync::Arc;

use std::fmt::Debug;
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_DATBASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[async_trait]
pub(crate) trait SysDb: Send + Sync + SysDbClone + Debug {
    async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, GetCollectionsError>;

    async fn get_segments(
        &mut self,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, GetSegmentsError>;

    async fn get_last_compaction_time(
        &mut self,
        tanant_ids: Vec<String>,
    ) -> Result<Vec<Tenant>, GetLastCompactionTimeError>;

    async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: Uuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
    ) -> Result<FlushCompactionResponse, FlushCompactionError>;
}

// We'd like to be able to clone the trait object, so we need to use the
// "clone box" pattern. See https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object#comment48814207_30353928
// https://chat.openai.com/share/b3eae92f-0b80-446f-b79d-6287762a2420
pub(crate) trait SysDbClone {
    fn clone_box(&self) -> Box<dyn SysDb>;
}

impl<T> SysDbClone for T
where
    T: 'static + SysDb + Clone,
{
    fn clone_box(&self) -> Box<dyn SysDb> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SysDb> {
    fn clone(&self) -> Box<dyn SysDb> {
        self.clone_box()
    }
}

#[derive(Clone, Debug)]
// Since this uses tonic transport channel, cloning is cheap. Each client only supports
// one inflight request at a time, so we need to clone the client for each requester.
pub(crate) struct GrpcSysDb {
    client: sys_db_client::SysDbClient<tonic::transport::Channel>,
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
impl Configurable<SysDbConfig> for GrpcSysDb {
    async fn try_from_config(config: &SysDbConfig) -> Result<Self, Box<dyn ChromaError>> {
        match &config {
            SysDbConfig::Grpc(my_config) => {
                let host = &my_config.host;
                let port = &my_config.port;
                println!("Connecting to sysdb at {}:{}", host, port);
                let connection_string = format!("http://{}:{}", host, port);
                let client = sys_db_client::SysDbClient::connect(connection_string).await;
                match client {
                    Ok(client) => {
                        return Ok(GrpcSysDb { client: client });
                    }
                    Err(e) => {
                        return Err(Box::new(GrpcSysDbError::FailedToConnect(e)));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SysDb for GrpcSysDb {
    async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, GetCollectionsError> {
        // TODO: move off of status into our own error type
        let collection_id_str;
        match collection_id {
            Some(id) => {
                collection_id_str = Some(id.to_string());
            }
            None => {
                collection_id_str = None;
            }
        }

        let res = self
            .client
            .get_collections(chroma_proto::GetCollectionsRequest {
                id: collection_id_str,
                name: name,
                limit: None,
                offset: None,
                tenant: if tenant.is_some() {
                    tenant.unwrap()
                } else {
                    DEFAULT_TENANT.to_string()
                },
                database: if database.is_some() {
                    database.unwrap()
                } else {
                    DEFAULT_DATBASE.to_string()
                },
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
                    Ok(collections) => {
                        return Ok(collections);
                    }
                    Err(e) => {
                        return Err(GetCollectionsError::ConversionError(e));
                    }
                }
            }
            Err(e) => {
                return Err(GetCollectionsError::FailedToGetCollections(e));
            }
        }
    }

    async fn get_segments(
        &mut self,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, GetSegmentsError> {
        let res = self
            .client
            .get_segments(chroma_proto::GetSegmentsRequest {
                // TODO: modularize
                id: if id.is_some() {
                    Some(id.unwrap().to_string())
                } else {
                    None
                },
                r#type: r#type,
                scope: if scope.is_some() {
                    Some(scope.unwrap() as i32)
                } else {
                    None
                },
                collection: if collection.is_some() {
                    Some(collection.unwrap().to_string())
                } else {
                    None
                },
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
                    Ok(segments) => {
                        return Ok(segments);
                    }
                    Err(e) => {
                        return Err(GetSegmentsError::ConversionError(e));
                    }
                }
            }
            Err(e) => {
                return Err(GetSegmentsError::FailedToGetSegments(e));
            }
        }
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
                return Ok(last_compaction_times.unwrap());
            }
            Err(e) => {
                return Err(GetLastCompactionTimeError::FailedToGetLastCompactionTime(e));
            }
        }
    }

    async fn flush_compaction(
        &mut self,
        tenant_id: String,
        collection_id: Uuid,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
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
            collection_id: collection_id.to_string(),
            log_position,
            collection_version,
            segment_compaction_info,
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
                return Ok(res);
            }
            Err(e) => {
                return Err(FlushCompactionError::FailedToFlushCompaction(e));
            }
        }
    }
}

#[derive(Error, Debug)]
// TODO: This should use our sysdb errors from the proto definition
// We will have to do an error uniformization pass at some point
pub(crate) enum GetCollectionsError {
    #[error("Failed to fetch")]
    FailedToGetCollections(#[from] tonic::Status),
    #[error("Failed to convert proto collection")]
    ConversionError(#[from] CollectionConversionError),
}

impl ChromaError for GetCollectionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetCollectionsError::FailedToGetCollections(_) => ErrorCodes::Internal,
            GetCollectionsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
// TODO: This should use our sysdb errors from the proto definition
// We will have to do an error uniformization pass at some point
pub(crate) enum GetSegmentsError {
    #[error("Failed to fetch")]
    FailedToGetSegments(#[from] tonic::Status),
    #[error("Failed to convert proto segment")]
    ConversionError(#[from] SegmentConversionError),
}

impl ChromaError for GetSegmentsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetSegmentsError::FailedToGetSegments(_) => ErrorCodes::Internal,
            GetSegmentsError::ConversionError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum GetLastCompactionTimeError {
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
pub(crate) enum FlushCompactionError {
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

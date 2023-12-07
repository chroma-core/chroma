use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::chroma_proto::{
    sys_db_client, Collection, GetCollectionsRequest, GetSegmentsRequest, Segment, SegmentScope,
}; // TODO: should we use the proto generated structs or our own structs?

const DEFAULT_DATBASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[async_trait]
pub(crate) trait SysDb: Send + Sync {
    async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        topic: Option<String>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, tonic::Status>;

    async fn get_segments(
        &mut self,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        topic: Option<String>,
        collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, tonic::Status>;
}

#[derive(Clone)]
// Since this uses tonic transport channel, cloning is cheap. Each client only supports
// one inflight request at a time, so we need to clone the client for each requester.
pub(crate) struct GrpcSysDb {
    client: sys_db_client::SysDbClient<tonic::transport::Channel>,
}

impl GrpcSysDb {
    pub(crate) async fn new() -> Self {
        let client = sys_db_client::SysDbClient::connect("http://[::1]:50051").await;
        match client {
            Ok(client) => {
                return GrpcSysDb { client: client };
            }
            Err(e) => {
                // TODO: probably don't want to panic here
                panic!("Failed to connect to sysdb: {}", e);
            }
        }
    }
}

#[async_trait]
impl SysDb for GrpcSysDb {
    async fn get_collections(
        &mut self,
        collection_id: Option<Uuid>,
        topic: Option<String>,
        name: Option<String>,
        tenant: Option<String>,
        database: Option<String>,
    ) -> Result<Vec<Collection>, tonic::Status> {
        // TODO; move off of status into our own error type
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
            .get_collections(GetCollectionsRequest {
                id: collection_id_str,
                topic: topic,
                name: name,
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
                return Ok(collections);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    async fn get_segments(
        &mut self,
        id: Option<Uuid>,
        r#type: Option<String>,
        scope: Option<SegmentScope>,
        topic: Option<String>,
        collection: Option<Uuid>,
    ) -> Result<Vec<Segment>, tonic::Status> {
        let res = self
            .client
            .get_segments(GetSegmentsRequest {
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
                topic: topic,
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
                return Ok(segments);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

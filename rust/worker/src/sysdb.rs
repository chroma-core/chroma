use std::collections::HashMap;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    chroma_proto::{sys_db_client, GetCollectionsRequest, GetSegmentsRequest},
    convert::{from_proto_collection, from_proto_segment},
    types::{Collection, Segment, SegmentScope},
}; // TODO: should we use the proto generated structs or our own structs?

const DEFAULT_DATBASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

#[async_trait]
pub(crate) trait SysDb: Send + Sync + SysDbClone {
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

// We'd like to be able to clone the trait object, so we need to use the
// "clone box" pattern. See https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object#comment48814207_30353928
// https://chat.openai.com/share/b3eae92f-0b80-446f-b79d-6287762a2420
trait SysDbClone {
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

                // map from_proto_collections over collections and if any of them fail, return an error
                let collections = collections
                    .into_iter()
                    .map(|proto_collection| from_proto_collection(proto_collection))
                    .collect::<Result<Vec<Collection>, &'static str>>();

                match collections {
                    Ok(collections) => {
                        return Ok(collections);
                    }
                    Err(e) => {
                        return Err(tonic::Status::new(
                            tonic::Code::Internal,
                            format!("Failed to convert proto collection: {}", e),
                        ));
                    }
                }
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
        println!("get_segments: {:?}", res);
        match res {
            Ok(res) => {
                let segments = res.into_inner().segments;
                let converted_segments = segments
                    .into_iter()
                    .map(|proto_segment| from_proto_segment(proto_segment))
                    .collect::<Result<Vec<Segment>, &'static str>>();

                match converted_segments {
                    Ok(segments) => {
                        println!("returning segments");
                        return Ok(segments);
                    }
                    Err(e) => {
                        println!("failed to convert segments: {}", e);
                        return Err(tonic::Status::new(
                            tonic::Code::Internal,
                            format!("Failed to convert proto segment: {}", e),
                        ));
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

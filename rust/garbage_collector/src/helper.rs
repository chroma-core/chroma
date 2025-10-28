use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::{
    CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest,
    ListCollectionVersionsRequest, ListCollectionVersionsResponse, OperationRecord,
    PushLogsRequest, Segment, SegmentScope, Vector,
};
use chroma_types::InternalCollectionConfiguration;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Clone)]
pub struct ChromaGrpcClients {
    pub sysdb: SysDbClient<Channel>,
    pub log_service: LogServiceClient<Channel>,
}

impl ChromaGrpcClients {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let sysdb_channel = Channel::from_static("http://localhost:50051")
            .connect()
            .await?;
        let logservice_channel = Channel::from_static("http://localhost:50054")
            .connect()
            .await?;

        Ok(Self {
            sysdb: SysDbClient::new(sysdb_channel),
            log_service: LogServiceClient::new(logservice_channel),
        })
    }

    pub async fn create_database_and_collection(
        &mut self,
        tenant_id: &str,
        database_name: &str,
        collection_name: &str,
        enable_spann: bool,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Create tenant first
        let tenant_req = CreateTenantRequest {
            name: tenant_id.to_string(),
        };
        self.sysdb.create_tenant(tenant_req).await?;

        // Create database
        let db_req = CreateDatabaseRequest {
            id: uuid::Uuid::new_v4().to_string(),
            name: database_name.to_string(),
            tenant: tenant_id.to_string(),
        };
        self.sysdb.create_database(db_req).await?;

        // Create segments for the collection
        let collection_id = Uuid::new_v4().to_string();
        let segments = vec![
            // Vector segment
            Segment {
                id: Uuid::new_v4().to_string(),
                r#type: if enable_spann {
                    "urn:chroma:segment/vector/spann".to_string()
                } else {
                    "urn:chroma:segment/vector/hnsw-distributed".to_string()
                },
                scope: SegmentScope::Vector as i32,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: std::collections::HashMap::new(),
            },
            // Metadata segment
            Segment {
                id: Uuid::new_v4().to_string(),
                r#type: "urn:chroma:segment/metadata/blockfile".to_string(),
                scope: SegmentScope::Metadata as i32,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: std::collections::HashMap::new(),
            },
            // Record segment
            Segment {
                id: Uuid::new_v4().to_string(),
                r#type: "urn:chroma:segment/record/blockfile".to_string(),
                scope: SegmentScope::Record as i32,
                collection: collection_id.clone(),
                metadata: None,
                file_paths: std::collections::HashMap::new(),
            },
        ];

        let config_str = if enable_spann {
            serde_json::to_string(&InternalCollectionConfiguration::default_spann())?
        } else {
            "{}".to_string()
        };

        // Create collection with segments
        let coll_req = CreateCollectionRequest {
            id: collection_id.clone(),
            name: collection_name.to_string(),
            tenant: tenant_id.to_string(),
            database: database_name.to_string(),
            dimension: Some(3),
            configuration_json_str: config_str,
            schema_str: None,
            get_or_create: Some(true),
            metadata: None,
            segments,
        };
        let response = self.sysdb.create_collection(coll_req).await?;
        Ok(response.into_inner().collection.unwrap().id)
    }

    pub async fn add_embeddings(
        &mut self,
        collection_id: &str,
        embeddings: Vec<Vec<f32>>,
        ids: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let records = ids
            .into_iter()
            .zip(embeddings)
            .map(|(id, embedding)| {
                // Convert f32 vector to bytes
                let vector_bytes: Vec<u8> = embedding
                    .iter()
                    .flat_map(|&x| x.to_le_bytes().to_vec())
                    .collect();

                OperationRecord {
                    id,
                    vector: Some(Vector {
                        dimension: embedding.len() as i32,
                        vector: vector_bytes,
                        encoding: 0,
                    }),
                    operation: 0,
                    metadata: None,
                }
            })
            .collect();

        let push_req = PushLogsRequest {
            collection_id: collection_id.to_string(),
            records,
        };

        let response = self.log_service.push_logs(push_req).await?;
        let response_inner = response.into_inner();

        // Check if any records were actually added
        if response_inner.record_count > 0 {
            Ok(())
        } else {
            Err("No records were added to the log service".into())
        }
    }

    #[allow(dead_code)]
    pub fn sysdb_client(&mut self) -> &mut SysDbClient<Channel> {
        &mut self.sysdb
    }

    pub async fn list_collection_versions(
        &mut self,
        collection_id: String,
        tenant_id: String,
        max_count: Option<i64>,
        versions_before: Option<i64>,
        versions_at_or_after: Option<i64>,
        include_marked_for_deletion: Option<bool>,
    ) -> Result<ListCollectionVersionsResponse, Box<dyn std::error::Error>> {
        let request = ListCollectionVersionsRequest {
            collection_id,
            tenant_id,
            max_count,
            versions_before,
            versions_at_or_after,
            include_marked_for_deletion,
        };

        let response = self.sysdb.list_collection_versions(request).await?;
        Ok(response.into_inner())
    }
}

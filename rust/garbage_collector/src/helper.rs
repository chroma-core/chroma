use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::{
    Collection, CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest,
    FilterOperator, GetCollectionWithSegmentsRequest, GetPlan, KnnOperator, KnnPlan,
    KnnProjectionOperator, LimitOperator, ListCollectionVersionsRequest,
    ListCollectionVersionsResponse, OperationRecord, ProjectionOperator, PushLogsRequest,
    ScanOperator, Segment, SegmentScope, Vector,
};
use chroma_types::InternalCollectionConfiguration;
use std::collections::HashMap;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Clone)]
pub struct ChromaGrpcClients {
    pub sysdb: SysDbClient<Channel>,
    pub log_service: LogServiceClient<Channel>,
    pub query_executor: QueryExecutorClient<Channel>,
}

impl ChromaGrpcClients {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let sysdb_channel = Channel::from_static("http://localhost:50051")
            .connect()
            .await?;
        let logservice_channel = Channel::from_static("http://localhost:50054")
            .connect()
            .await?;
        let queryservice_channel = Channel::from_static("http://localhost:50053")
            .connect()
            .await?;

        Ok(Self {
            sysdb: SysDbClient::new(sysdb_channel),
            log_service: LogServiceClient::new(logservice_channel),
            query_executor: QueryExecutorClient::new(queryservice_channel),
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
    pub async fn query_collection(
        &mut self,
        collection_id: &str,
        query_embedding: Vec<f32>,
    ) -> Result<Vec<(String, f32)>, Box<dyn std::error::Error>> {
        // Convert f32 vector to bytes
        let vector_bytes: Vec<u8> = query_embedding
            .iter()
            .flat_map(|&x| x.to_le_bytes().to_vec())
            .collect();

        let knn_plan = KnnPlan {
            scan: Some(ScanOperator {
                collection: Some(Collection {
                    id: collection_id.to_string(),
                    name: String::new(),
                    database: String::new(),
                    tenant: String::new(),
                    dimension: Some(query_embedding.len() as i32),
                    configuration_json_str: String::new(),
                    metadata: None,
                    log_position: 0,
                    version: 0,
                    total_records_post_compaction: 0,
                    size_bytes_post_compaction: 0,
                    last_compaction_time_secs: 0,
                    version_file_path: None,
                    root_collection_id: None,
                    lineage_file_path: None,
                }),
                knn: None,
                metadata: None,
                record: None,
            }),
            filter: None,
            knn: Some(KnnOperator {
                embeddings: vec![Vector {
                    dimension: query_embedding.len() as i32,
                    vector: vector_bytes,
                    encoding: 0,
                }],
                fetch: 2,
            }),
            projection: Some(KnnProjectionOperator {
                projection: Some(ProjectionOperator {
                    document: false,
                    embedding: false,
                    metadata: false,
                }),
                distance: true,
            }),
        };

        let response = self.query_executor.knn(knn_plan).await?;
        let results = response.into_inner().results;

        let mut id_distances = Vec::new();
        for result in results {
            for record in result.records {
                id_distances.push((record.record.unwrap().id, record.distance.unwrap_or(0.0)));
            }
        }

        Ok(id_distances)
    }

    pub async fn get_records(
        &mut self,
        collection_id: String,
        _ids: Option<Vec<String>>,
        include_embeddings: bool,
        include_metadatas: bool,
        include_documents: bool,
    ) -> Result<GetResult, Box<dyn std::error::Error>> {
        // First get collection and its segments
        let collection_segments = self
            .sysdb
            .get_collection_with_segments(GetCollectionWithSegmentsRequest { id: collection_id })
            .await?
            .into_inner();

        // Map segments to their scopes
        let mut scope_to_segment: HashMap<i32, Segment> = collection_segments
            .segments
            .into_iter()
            .map(|s| (s.scope, s))
            .collect();

        // Create the scan operator with collection info and segments
        let scan = ScanOperator {
            collection: collection_segments.collection,
            knn: scope_to_segment.remove(&(SegmentScope::Vector as i32)),
            metadata: scope_to_segment.remove(&(SegmentScope::Metadata as i32)),
            record: scope_to_segment.remove(&(SegmentScope::Record as i32)),
        };

        // Create the get plan
        let get_plan = GetPlan {
            scan: Some(scan),
            filter: Some(FilterOperator {
                ids: None, // ids.map(|ids| UserIds { ids }),
                r#where: None,
                where_document: None,
            }),
            limit: Some(LimitOperator {
                skip: 0,
                fetch: None,
            }),
            projection: Some(ProjectionOperator {
                document: false, // include_documents,
                embedding: true, // include_embeddings,
                metadata: false, // include_metadatas,
            }),
        };

        // Execute the get query
        let response = self.query_executor.get(get_plan).await?;
        let response_inner = response.into_inner();

        // Convert the response into a GetResult struct
        let mut result = GetResult {
            ids: Vec::new(),
            embeddings: if include_embeddings {
                Some(Vec::new())
            } else {
                None
            },
            metadatas: if include_metadatas {
                Some(Vec::new())
            } else {
                None
            },
            documents: if include_documents {
                Some(Vec::new())
            } else {
                None
            },
        };

        // Process each record
        for record in response_inner.records {
            result.ids.push(record.id);

            if include_embeddings {
                if let Some(embedding) = record.embedding {
                    // Convert bytes back to f32 vector
                    let mut float_vec = Vec::new();
                    for chunk in embedding.vector.chunks(4) {
                        if chunk.len() == 4 {
                            float_vec
                                .push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                        }
                    }
                    result.embeddings.as_mut().unwrap().push(float_vec);
                }
            }

            if include_metadatas {
                if let Some(ref metadata) = record.metadata {
                    let mut metadata_map = std::collections::HashMap::new();
                    for (key, value) in &metadata.metadata {
                        // Convert UpdateMetadataValue to String based on its value variant
                        let string_value = match &value.value {
                            Some(chroma_types::chroma_proto::update_metadata_value::Value::StringValue(s)) => s.clone(),
                            Some(chroma_types::chroma_proto::update_metadata_value::Value::IntValue(i)) => i.to_string(),
                            Some(chroma_types::chroma_proto::update_metadata_value::Value::FloatValue(f)) => f.to_string(),
                            Some(chroma_types::chroma_proto::update_metadata_value::Value::BoolValue(b)) => b.to_string(),
                            None => String::new(),
                        };
                        metadata_map.insert(key.clone(), string_value);
                    }
                    result.metadatas.as_mut().unwrap().push(metadata_map);
                }
            }

            if include_documents {
                if let Some(ref metadata) = record.metadata {
                    if let Some(doc_value) = metadata.metadata.get("chroma:document") {
                        // Convert document UpdateMetadataValue to String
                        if let Some(
                            chroma_types::chroma_proto::update_metadata_value::Value::StringValue(
                                doc_str,
                            ),
                        ) = &doc_value.value
                        {
                            result.documents.as_mut().unwrap().push(doc_str.clone());
                        }
                    }
                }
            }
        }

        Ok(result)
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

// Add this struct to hold the get results
#[derive(Debug)]
pub struct GetResult {
    pub ids: Vec<String>,
    pub embeddings: Option<Vec<Vec<f32>>>,
    pub metadatas: Option<Vec<std::collections::HashMap<String, String>>>,
    pub documents: Option<Vec<String>>,
}

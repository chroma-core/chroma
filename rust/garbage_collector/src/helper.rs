use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::query_executor_client::QueryExecutorClient;
use chroma_types::chroma_proto::sys_db_client::SysDbClient;
use chroma_types::chroma_proto::{
    Collection, CreateCollectionRequest, CreateDatabaseRequest, KnnOperator, KnnPlan,
    KnnProjectionOperator, OperationRecord, ProjectionOperator, PushLogsRequest, ScanOperator,
    Vector,
};
use tonic::transport::Channel;

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
        let logservice_channel = Channel::from_static("http://localhost:50052")
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
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Create database
        let db_req = CreateDatabaseRequest {
            id: uuid::Uuid::new_v4().to_string(),
            name: database_name.to_string(),
            tenant: tenant_id.to_string(),
        };
        self.sysdb.create_database(db_req).await?;

        // Create collection
        let coll_req = CreateCollectionRequest {
            id: uuid::Uuid::new_v4().to_string(),
            name: collection_name.to_string(),
            tenant: tenant_id.to_string(),
            database: database_name.to_string(),
            dimension: Some(3),
            configuration_json_str: "{}".to_string(),
            get_or_create: Some(true),
            metadata: None,
            segments: vec![],
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
                    operation: 1,
                    metadata: None,
                }
            })
            .collect();

        let push_req = PushLogsRequest {
            collection_id: collection_id.to_string(),
            records,
        };
        self.log_service.push_logs(push_req).await?;
        Ok(())
    }

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
}

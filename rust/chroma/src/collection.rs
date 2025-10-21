use std::sync::Arc;

use chroma_api_types::ForkCollectionPayload;
use chroma_types::{
    plan::SearchPayload, AddCollectionRecordsRequest, AddCollectionRecordsResponse, Collection,
    CollectionUuid, DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, GetRequest,
    GetResponse, IncludeList, Metadata, QueryRequest, QueryResponse, Schema, SearchRequest,
    SearchResponse, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateMetadata, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse, Where,
};
use reqwest::Method;
use serde::{de::DeserializeOwned, Serialize};

use crate::{client::ChromaHttpClientError, ChromaHttpClient};

#[derive(Clone)]
pub struct ChromaCollection {
    pub(crate) client: ChromaHttpClient,
    pub(crate) collection: Arc<Collection>,
}

impl std::fmt::Debug for ChromaCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChromaCollection")
            .field("database", &self.collection.database)
            .field("tenant", &self.collection.tenant)
            .field("name", &self.collection.name)
            .field("collection_id", &self.collection.collection_id)
            .field("version", &self.collection.version)
            .finish()
    }
}

impl ChromaCollection {
    pub fn database(&self) -> &str {
        &self.collection.database
    }

    pub fn metadata(&self) -> &Option<Metadata> {
        &self.collection.metadata
    }

    pub fn schema(&self) -> &Option<Schema> {
        &self.collection.schema
    }

    pub fn tenant(&self) -> &str {
        &self.collection.tenant
    }

    pub fn name(&self) -> &str {
        &self.collection.name
    }

    pub fn id(&self) -> CollectionUuid {
        self.collection.collection_id
    }

    pub fn version(&self) -> i32 {
        self.collection.version
    }

    pub async fn count(&self) -> Result<u32, ChromaHttpClientError> {
        self.send::<(), u32>("count", Method::GET, None).await
    }

    pub async fn modify(
        &mut self,
        new_name: Option<impl AsRef<str>>,
        new_metadata: Option<Metadata>,
    ) -> Result<(), ChromaClientError> {
        self.send::<_, ()>(
            "modify",
            Method::PUT,
            Some(serde_json::json!({
                "new_name": new_name.as_ref().map(|s| s.as_ref()),
                "new_metadata": new_metadata,
            })),
        )
        .await?;

        let mut updated_collection = (*self.collection).clone();
        if let Some(name) = new_name {
            updated_collection.name = name.as_ref().to_string();
        }
        if let Some(metadata) = new_metadata {
            updated_collection.metadata = Some(metadata);
        }

        self.collection = Arc::new(updated_collection);

        Ok(())
    }

    pub async fn get(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
        offset: Option<u32>,
        include: Option<IncludeList>,
    ) -> Result<GetResponse, ChromaHttpClientError> {
        let request = GetRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
            limit,
            offset.unwrap_or_default(),
            include.unwrap_or_else(IncludeList::default_get),
        )?;
        let request = request.into_payload()?;
        self.send("get", Method::POST, Some(request)).await
    }

    pub async fn query(
        &self,
        query_embeddings: Vec<Vec<f32>>,
        n_results: Option<u32>,
        r#where: Option<Where>,
        ids: Option<Vec<String>>,
        include: Option<IncludeList>,
    ) -> Result<QueryResponse, ChromaHttpClientError> {
        let request = QueryRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
            query_embeddings,
            n_results.unwrap_or(10),
            include.unwrap_or_else(IncludeList::default_query),
        )?;
        let request = request.into_payload()?;
        self.send("query", Method::POST, Some(request)).await
    }

    pub async fn search(
        &self,
        searches: Vec<SearchPayload>,
    ) -> Result<SearchResponse, ChromaHttpClientError> {
        let request = SearchRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            searches,
        )?;
        let request = request.into_payload();
        self.send("search", Method::POST, Some(request)).await
    }

    pub async fn add(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<AddCollectionRecordsResponse, ChromaHttpClientError> {
        let request = AddCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send("add", Method::POST, Some(request)).await
    }

    pub async fn update(
        &self,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpdateCollectionRecordsResponse, ChromaHttpClientError> {
        let request = UpdateCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send("update", Method::POST, Some(request)).await
    }

    pub async fn upsert(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpsertCollectionRecordsResponse, ChromaHttpClientError> {
        let request = UpsertCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send("upsert", Method::POST, Some(request)).await
    }

    pub async fn delete(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
    ) -> Result<DeleteCollectionRecordsResponse, ChromaHttpClientError> {
        let request = DeleteCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
        )?;
        let request = request.into_payload()?;
        self.send("delete", Method::POST, Some(request)).await
    }

    pub async fn fork(
        &self,
        new_name: impl Into<String>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        let request = ForkCollectionPayload {
            new_name: new_name.into(),
        };
        let collection: Collection = self.send("fork", Method::POST, Some(request)).await?;
        Ok(ChromaCollection {
            client: self.client.clone(),
            collection: Arc::new(collection),
        })
    }

    async fn send<Body: Serialize, Response: DeserializeOwned>(
        &self,
        operation: &str,
        method: Method,
        body: Option<Body>,
    ) -> Result<Response, ChromaHttpClientError> {
        let operation_name = format!("collection_{operation}");
        let path = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/{}",
            self.collection.tenant,
            self.collection.database,
            self.collection.collection_id,
            operation
        );
        self.client
            .send(&operation_name, method, path, body, None::<()>)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::with_client;
    use chroma_types::{
        Include, IncludeList, Metadata, MetadataComparison, MetadataExpression, MetadataValue,
        PrimitiveOperator, UpdateMetadata, UpdateMetadataValue, Where,
    };

    async fn create_test_collection(
        client: &ChromaHttpClient,
        name: &str,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        client.create_collection(name, None, None).await
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_accessor_methods() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_accessors")
                .await
                .unwrap();

            assert!(!collection.database().is_empty());
            assert_eq!(collection.metadata(), &None);
            assert!(collection.schema().is_some());
            assert!(!collection.tenant().is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_count_empty_collection() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_count_empty")
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Empty collection count: {}", count);
            assert_eq!(count, 0);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_add_single_record() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_single")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document1".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Collection count after add: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_add_multiple_records() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_multiple")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    Some(vec![
                        Some("first document".to_string()),
                        Some("second document".to_string()),
                        Some("third document".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Collection count after adding multiple: {}", count);
            assert_eq!(count, 3);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_add_with_metadata() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_metadata")
                .await
                .unwrap();

            let mut metadata = Metadata::new();
            metadata.insert("category".to_string(), "test".into());
            metadata.insert("version".to_string(), 1.into());

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document with metadata".to_string())]),
                    None,
                    Some(vec![Some(metadata)]),
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_add_with_uris() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_uris")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document with uri".to_string())]),
                    Some(vec![Some("https://example.com/doc1".to_string())]),
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_all_records() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_all")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("first".to_string()), Some("second".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection.get(None, None, None, None, None).await.unwrap();
            println!("Get all response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(response.ids.contains(&"id1".to_string()));
            assert!(response.ids.contains(&"id2".to_string()));
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_by_ids() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_by_ids")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .get(
                    Some(vec!["id1".to_string(), "id3".to_string()]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Get by ids response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(response.ids.contains(&"id1".to_string()));
            assert!(response.ids.contains(&"id3".to_string()));
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_with_limit_and_offset() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_limit_offset")
                .await
                .unwrap();

            collection
                .add(
                    vec![
                        "id1".to_string(),
                        "id2".to_string(),
                        "id3".to_string(),
                        "id4".to_string(),
                    ],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                        vec![10.0, 11.0, 12.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .get(None, None, Some(2), Some(1), None)
                .await
                .unwrap();
            println!("Get with limit and offset response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(!response.ids.is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_with_where_clause() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_where")
                .await
                .unwrap();

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            let response = collection
                .get(None, Some(where_clause), None, None, None)
                .await
                .unwrap();
            println!("Get with where clause response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_with_include_list() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_include")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("test document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let include = IncludeList(vec![
                Include::Document,
                Include::Embedding,
                Include::Metadata,
            ]);
            let response = collection
                .get(None, None, None, None, Some(include))
                .await
                .unwrap();
            println!("Get with include list response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0], "id1");
            assert!(response.documents.is_some());
            assert_eq!(
                response.documents.as_ref().unwrap()[0],
                Some("test document".to_string())
            );
            assert!(response.embeddings.is_some());
            assert_eq!(
                response.embeddings.as_ref().unwrap()[0],
                vec![1.0, 2.0, 3.0]
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_query_basic() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_basic")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![10.0, 20.0, 30.0],
                    ],
                    Some(vec![
                        Some("first".to_string()),
                        Some("second".to_string()),
                        Some("third".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(vec![vec![1.0, 2.0, 3.0]], None, None, None, None)
                .await
                .unwrap();
            println!("Query basic response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert!(!response.ids[0].is_empty());
            assert!(response.ids[0].contains(&"id1".to_string()));
            assert!(response.distances.is_some());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_query_with_n_results() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_n_results")
                .await
                .unwrap();

            collection
                .add(
                    vec![
                        "id1".to_string(),
                        "id2".to_string(),
                        "id3".to_string(),
                        "id4".to_string(),
                        "id5".to_string(),
                    ],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![1.2, 2.2, 3.2],
                        vec![1.3, 2.3, 3.3],
                        vec![1.4, 2.4, 3.4],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(vec![vec![1.0, 2.0, 3.0]], Some(3), None, None, None)
                .await
                .unwrap();
            println!("Query with n_results response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0].len(), 3);
            assert!(response.distances.is_some());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_query_with_where_clause() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_where")
                .await
                .unwrap();

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![1.1, 2.1, 3.1]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            let response = collection
                .query(
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    Some(where_clause),
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Query with where clause response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0].len(), 1);
            assert_eq!(response.ids[0][0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_query_multiple_embeddings() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_multiple")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(
                    vec![vec![1.0, 2.0, 3.0], vec![7.0, 8.0, 9.0]],
                    Some(1),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Query multiple embeddings response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert_eq!(response.ids[0].len(), 1);
            assert_eq!(response.ids[1].len(), 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_update_embeddings() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_embeddings")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .update(
                    vec!["id1".to_string()],
                    Some(vec![Some(vec![4.0, 5.0, 6.0])]),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Embedding])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.embeddings.is_some());
            assert_eq!(
                get_response.embeddings.as_ref().unwrap()[0],
                vec![4.0, 5.0, 6.0]
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_update_documents() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_documents")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .update(
                    vec!["id1".to_string()],
                    None,
                    Some(vec![Some("updated document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Document])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.documents.is_some());
            assert_eq!(
                get_response.documents.as_ref().unwrap()[0],
                Some("updated document".to_string())
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_update_metadata() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_metadata")
                .await
                .unwrap();

            let mut original_metadata = Metadata::new();
            original_metadata.insert("version".to_string(), 1.into());

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    None,
                    Some(vec![Some(original_metadata)]),
                )
                .await
                .unwrap();

            let mut updated_metadata = UpdateMetadata::new();
            updated_metadata.insert("version".to_string(), UpdateMetadataValue::Int(2));
            updated_metadata.insert(
                "new_field".to_string(),
                UpdateMetadataValue::Str("test".to_string()),
            );

            collection
                .update(
                    vec!["id1".to_string()],
                    None,
                    None,
                    None,
                    Some(vec![Some(updated_metadata)]),
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Metadata])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.metadatas.is_some());
            let metadata = get_response.metadatas.as_ref().unwrap()[0]
                .as_ref()
                .unwrap();
            assert_eq!(metadata.get("version"), Some(&MetadataValue::Int(2)));
            assert_eq!(
                metadata.get("new_field"),
                Some(&MetadataValue::Str("test".to_string()))
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_upsert_insert_new() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_insert")
                .await
                .unwrap();

            collection
                .upsert(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("new document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert insert: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_upsert_update_existing() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_update")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .upsert(
                    vec!["id1".to_string()],
                    vec![vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("updated via upsert".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert update: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_upsert_mixed() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_mixed")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("existing".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .upsert(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![4.0, 5.0, 6.0], vec![7.0, 8.0, 9.0]],
                    Some(vec![Some("updated".to_string()), Some("new".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert mixed: {}", count);
            assert_eq!(count, 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_delete_by_ids() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_delete_by_ids")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .delete(Some(vec!["id1".to_string(), "id3".to_string()]), None)
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after delete: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_delete_by_where() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_delete_by_where")
                .await
                .unwrap();

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            collection.delete(None, Some(where_clause)).await.unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after delete: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_fork_basic() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_source")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("first".to_string()), Some("second".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let forked = collection.fork("test_fork_target").await.unwrap();
            println!("Forked collection: {:?}", forked);

            assert_eq!(forked.collection.name, "test_fork_target");
            assert_ne!(
                forked.collection.collection_id,
                collection.collection.collection_id
            );

            let forked_count = forked.count().await.unwrap();
            println!("Forked collection count: {}", forked_count);
            assert_eq!(forked_count, 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_fork_preserves_data() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_preserves_source")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("test document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let forked = collection.fork("test_fork_preserves_target").await.unwrap();

            let forked_get_response = forked
                .get(
                    None,
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Document])),
                )
                .await
                .unwrap();
            println!("Forked collection get response: {:?}", forked_get_response);
            assert_eq!(forked_get_response.ids.len(), 1);
            assert_eq!(forked_get_response.ids[0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_fork_independence() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_independence_source")
                .await
                .unwrap();

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let forked = collection
                .fork("test_fork_independence_target")
                .await
                .unwrap();

            forked
                .add(
                    vec!["id2".to_string()],
                    vec![vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let original_count = collection.count().await.unwrap();
            let forked_count = forked.count().await.unwrap();
            println!(
                "Original count: {}, Forked count: {}",
                original_count, forked_count
            );
            assert_eq!(original_count, 1);
            assert_eq!(forked_count, 2);
        })
        .await;
    }
}

use std::sync::Arc;

use chroma_api_types::ForkCollectionPayload;
use chroma_types::{
    plan::SearchPayload, AddCollectionRecordsRequest, AddCollectionRecordsResponse, Collection,
    DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, GetRequest, GetResponse,
    IncludeList, InternalSchema, Metadata, QueryRequest, QueryResponse, SearchRequest,
    SearchResponse, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateMetadata, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse, Where,
};
use reqwest::Method;
use serde::{de::DeserializeOwned, Serialize};

use crate::{client::ChromaClientError, ChromaClient};

#[derive(Clone, Debug)]
pub struct ChromaCollection {
    pub(crate) client: ChromaClient,
    pub(crate) collection: Arc<Collection>,
}

impl ChromaCollection {
    pub fn database(&self) -> &str {
        &self.collection.database
    }

    pub fn metadata(&self) -> &Option<Metadata> {
        &self.collection.metadata
    }

    pub fn schema(&self) -> &Option<InternalSchema> {
        &self.collection.schema
    }

    pub fn tenant(&self) -> &str {
        &self.collection.tenant
    }

    pub async fn count(&self) -> Result<u32, ChromaClientError> {
        self.send::<(), u32>("count", Method::GET, None).await
    }

    pub async fn get(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
        offset: Option<u32>,
        include: Option<IncludeList>,
    ) -> Result<GetResponse, ChromaClientError> {
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

        self.send("get", Method::POST, Some(request)).await
    }

    pub async fn query(
        &self,
        query_embeddings: Vec<Vec<f32>>,
        n_results: Option<u32>,
        r#where: Option<Where>,
        ids: Option<Vec<String>>,
        include: Option<IncludeList>,
    ) -> Result<QueryResponse, ChromaClientError> {
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

        self.send("query", Method::POST, Some(request)).await
    }

    pub async fn search(
        &self,
        searches: Vec<SearchPayload>,
    ) -> Result<SearchResponse, ChromaClientError> {
        let request = SearchRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            searches,
        )?;

        self.send("search", Method::POST, Some(request)).await
    }

    pub async fn add(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<AddCollectionRecordsResponse, ChromaClientError> {
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

        self.send("add", Method::POST, Some(request)).await
    }

    pub async fn update(
        &self,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpdateCollectionRecordsResponse, ChromaClientError> {
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

        self.send("update", Method::POST, Some(request)).await
    }

    pub async fn upsert(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpsertCollectionRecordsResponse, ChromaClientError> {
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

        self.send("upsert", Method::POST, Some(request)).await
    }

    pub async fn delete(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
    ) -> Result<DeleteCollectionRecordsResponse, ChromaClientError> {
        let request = DeleteCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
        )?;

        self.send("delete", Method::POST, Some(request)).await
    }

    pub async fn fork(
        &self,
        new_name: impl Into<String>,
    ) -> Result<ChromaCollection, ChromaClientError> {
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
    ) -> Result<Response, ChromaClientError> {
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

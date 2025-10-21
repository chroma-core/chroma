use crate::client::chroma_client::ChromaClient;
use crate::client::prelude::CollectionModel;
use crate::client::utils::send_request;
use chroma_types::{
    AddCollectionRecordsPayload, AddCollectionRecordsResponse, CountResponse, GetResponse,
    IncludeList, Metadata,
};
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::error::Error;
use std::ops::Deref;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CollectionAPIError {
    #[error("Failed to count records from collection {0}")]
    Count(String),
    #[error("Failed to get records from collection {0}")]
    Get(String),
    #[error("Failed to add records to collection {0}")]
    Add(String),
}

#[derive(Debug, Clone, Default)]
pub struct Collection {
    chroma_client: ChromaClient,
    collection: CollectionModel,
}

impl Deref for Collection {
    type Target = CollectionModel;

    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}

impl Collection {
    #[allow(dead_code)]
    pub fn new(chroma_client: ChromaClient, collection: CollectionModel) -> Self {
        Self {
            chroma_client,
            collection,
        }
    }

    #[allow(dead_code)]
    pub async fn get(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<&str>,
        where_document: Option<&str>,
        include: Option<IncludeList>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<GetResponse, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/get",
            self.chroma_client.tenant_id, self.chroma_client.db, self.collection.collection_id
        );

        let mut payload = Map::new();

        if let Some(ids) = ids {
            payload.insert("ids".to_string(), json!(ids));
        }

        if let Some(r#where) = r#where {
            let parsed: Value = serde_json::from_str(r#where)?;
            payload.insert("where".to_string(), parsed);
        }

        if let Some(where_document) = where_document {
            let parsed: Value = serde_json::from_str(where_document)?;
            payload.insert("where_document".to_string(), parsed);
        }

        if let Some(include) = include {
            payload.insert("include".to_string(), json!(include));
        }

        if let Some(limit) = limit {
            payload.insert("limit".to_string(), json!(limit));
        }

        if let Some(offset) = offset {
            payload.insert("offset".to_string(), json!(offset));
        }

        let response = send_request::<Map<String, Value>, GetResponse>(
            &self.chroma_client.host,
            Method::POST,
            &route,
            self.chroma_client.headers()?,
            Some(&payload),
        )
        .await
        .map_err(|_| CollectionAPIError::Get(self.collection.name.clone()))?;
        Ok(response)
    }

    #[allow(dead_code)]
    pub async fn count(&self) -> Result<CountResponse, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/count",
            self.chroma_client.tenant_id, self.chroma_client.db, self.collection.collection_id
        );
        let response = send_request::<(), CountResponse>(
            &self.chroma_client.host,
            Method::GET,
            &route,
            self.chroma_client.headers()?,
            None,
        )
        .await
        .map_err(|_| CollectionAPIError::Count(self.collection.name.clone()))?;
        Ok(response)
    }

    pub async fn add(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<(), Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/add",
            self.chroma_client.tenant_id, self.chroma_client.db, self.collection_id
        );

        let payload = AddCollectionRecordsPayload::new(ids, embeddings, documents, uris, metadatas);

        let _response = send_request::<AddCollectionRecordsPayload, AddCollectionRecordsResponse>(
            &self.chroma_client.host,
            Method::POST,
            &route,
            self.chroma_client.headers()?,
            Some(&payload),
        )
        .await?;

        Ok(())
    }
}

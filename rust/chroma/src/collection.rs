use std::sync::Arc;

use chroma_types::{Collection, InternalSchema, Metadata};
use reqwest::Method;
use serde::{de::DeserializeOwned, Serialize};

use crate::{client::ChromaClientError, ChromaClient};

#[derive(Clone, Debug)]
pub struct ChromaCollection {
    client: ChromaClient,
    collection: Arc<Collection>,
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

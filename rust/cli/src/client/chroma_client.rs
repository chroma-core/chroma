use crate::client::admin_client::AdminClient;
use crate::client::collection::Collection;
use crate::client::prelude::CollectionModel;
use crate::client::utils::send_request;
use crate::utils::Profile;
use axum::http::Method;
use chroma_types::{CollectionConfiguration, CreateCollectionPayload, Metadata, Schema};
use std::error::Error;
use std::ops::Deref;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChromaClientError {
    #[error("Failed to get collection {0}")]
    CollectionGet(String),
    #[error("Failed to create collection {0}")]
    CreateCollection(String),
    #[error("Failed to list collections")]
    ListCollections,
}

#[derive(Debug, Clone, Default)]
pub struct ChromaClient {
    pub admin_client: AdminClient,
    pub db: String,
}

impl ChromaClient {
    #[allow(dead_code)]
    pub fn new(host: String, tenant_id: String, db: String, api_key: Option<String>) -> Self {
        let admin_client = AdminClient::new(host, tenant_id, api_key);
        Self { admin_client, db }
    }

    #[allow(dead_code)]
    pub fn with_admin_client(admin_client: AdminClient, db: String) -> Self {
        Self { admin_client, db }
    }

    #[allow(dead_code)]
    pub fn from_profile(host: String, profile: &Profile, db: String) -> Self {
        let admin_client = AdminClient::from_profile(host, profile);
        Self { admin_client, db }
    }

    #[allow(dead_code)]
    pub fn local_default() -> Self {
        let admin_client = AdminClient::local_default();
        Self {
            admin_client,
            db: "default_database".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn local(host: String) -> Self {
        let admin_client = AdminClient::local(host);
        Self {
            admin_client,
            db: "default_database".to_string(),
        }
    }

    #[allow(dead_code)]
    pub async fn get_collection(&self, name: String) -> Result<Collection, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}",
            self.tenant_id, self.db, name
        );
        let response = send_request::<(), CollectionModel>(
            &self.host,
            Method::GET,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| ChromaClientError::CollectionGet(name))?;
        Ok(Collection::new(self.clone(), response))
    }

    pub async fn list_collections(&self) -> Result<Vec<Collection>, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections",
            self.tenant_id, self.db
        );
        let response = send_request::<(), Vec<CollectionModel>>(
            &self.host,
            Method::GET,
            &route,
            self.headers()?,
            None,
        )
        .await
        .map_err(|_| ChromaClientError::ListCollections)?;
        Ok(response
            .iter()
            .map(|c| Collection::new(self.clone(), c.clone()))
            .collect())
    }

    pub async fn create_collection(
        &self,
        name: String,
        metadata: Option<Metadata>,
        configuration: Option<CollectionConfiguration>,
        schema: Option<Schema>,
    ) -> Result<Collection, Box<dyn Error>> {
        let route = format!(
            "/api/v2/tenants/{}/databases/{}/collections",
            self.tenant_id, self.db
        );

        let payload = CreateCollectionPayload {
            name,
            configuration,
            metadata,
            get_or_create: false,
            schema,
        };
        let response = send_request::<CreateCollectionPayload, CollectionModel>(
            &self.host,
            Method::POST,
            &route,
            self.headers()?,
            Some(&payload),
        )
        .await?;
        Ok(Collection::new(self.clone(), response))
    }
}

impl Deref for ChromaClient {
    type Target = AdminClient;
    fn deref(&self) -> &Self::Target {
        &self.admin_client
    }
}

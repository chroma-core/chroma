use reqwest::Method;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    client::ChromaClientOptions,
    types::{GetUserIdentityResponse, HeartbeatResponse, ListCollectionsRequest},
};

#[derive(Error, Debug)]
pub enum ChromaClientError {
    #[error("Request error: {0:?}")]
    RequestError(#[from] reqwest::Error),
    #[error("Could not resolve database ID: {0}")]
    CouldNotResolveDatabaseId(String),
    #[error("Serialization/Deserialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct ChromaClient {
    base_url: String,
    client: reqwest::Client,
    tenant_id: Arc<Mutex<Option<String>>>,
    default_database_id: Arc<Mutex<Option<String>>>,
    #[cfg(feature = "opentelemetry")]
    metrics: crate::client::metrics::Metrics,
}

// TODO: remove and replace with actual Database struct
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Database {
    id: String,
    name: String,
}

impl ChromaClient {
    pub fn new(options: ChromaClientOptions) -> Self {
        // todo: add user-agent
        let client = reqwest::Client::builder()
            .default_headers(options.headers())
            .build()
            .expect("Failed to initialize TLS backend");

        ChromaClient {
            base_url: options.base_url.clone(),
            client,
            tenant_id: Arc::new(Mutex::new(options.tenant_id)),
            default_database_id: Arc::new(Mutex::new(options.default_database_id)),
            #[cfg(feature = "opentelemetry")]
            metrics: crate::client::metrics::Metrics::new(),
        }
    }

    pub async fn set_default_database_id(&self, database_id: String) {
        let mut lock = self.default_database_id.lock().await;
        *lock = Some(database_id);
    }

    pub async fn create_database(&self, name: String) -> Result<(), ChromaClientError> {
        // Returns empty map ({})
        self.send::<_, (), serde_json::Value>(
            "create_database",
            Method::POST,
            format!("/api/v2/tenants/{}/databases", self.get_tenant_id().await?),
            Some(serde_json::json!({ "name": name })),
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn list_databases(&self) -> Result<Vec<Database>, ChromaClientError> {
        let tenant_id = self.get_tenant_id().await?;

        self.send::<(), (), _>(
            "list_databases",
            Method::GET,
            format!("/api/v2/tenants/{}/databases", tenant_id),
            None,
            None,
        )
        .await
    }

    pub async fn delete_database(&self, database_name: String) -> Result<(), ChromaClientError> {
        // Returns empty map ({})
        self.send::<(), (), serde_json::Value>(
            "delete_database",
            Method::DELETE,
            format!(
                "/api/v2/tenants/{}/databases/{}",
                self.get_tenant_id().await?,
                database_name
            ),
            None,
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn get_auth_identity(&self) -> Result<GetUserIdentityResponse, ChromaClientError> {
        self.send::<(), (), _>(
            "get_auth_identity",
            Method::GET,
            "/api/v2/auth/identity".to_string(),
            None,
            None,
        )
        .await
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, ChromaClientError> {
        self.send::<(), (), _>(
            "heartbeat",
            Method::GET,
            "/api/v2/heartbeat".to_string(),
            None,
            None,
        )
        .await
    }

    pub async fn list_collections(
        &self,
        params: ListCollectionsRequest,
    ) -> Result<Vec<String>, ChromaClientError> {
        let tenant_id = self.get_tenant_id().await?;
        let database_id = self.get_database_id(params.database_id).await?;

        #[derive(Serialize)]
        struct QueryParams {
            limit: usize,
            offset: Option<usize>,
        }

        self.send::<(), _, _>(
            "list_collections",
            Method::GET,
            format!(
                "/api/v2/tenants/{}/databases/{}/collections",
                tenant_id, database_id
            ),
            None,
            Some(QueryParams {
                limit: params.limit,
                offset: params.offset,
            }),
        )
        .await
    }

    async fn get_database_id(
        &self,
        id_override: Option<String>,
    ) -> Result<String, ChromaClientError> {
        if let Some(id) = id_override {
            return Ok(id);
        }

        let mut database_id_lock = self.default_database_id.lock().await;
        if let Some(database_id) = &*database_id_lock {
            return Ok(database_id.clone());
        }

        let identity = self.get_auth_identity().await?;

        if identity.databases.len() > 1 {
            return Err(ChromaClientError::CouldNotResolveDatabaseId(
                "Client has access to multiple databases; please provide a database_id".to_string(),
            ));
        }

        let database_id = identity.databases.first().ok_or_else(|| {
            ChromaClientError::CouldNotResolveDatabaseId(
                "Client has access to no databases".to_string(),
            )
        })?;

        *database_id_lock = Some(database_id.clone());
        Ok(database_id.clone())
    }

    async fn get_tenant_id(&self) -> Result<String, ChromaClientError> {
        let mut tenant_id_lock = self.tenant_id.lock().await;
        if let Some(tenant_id) = &*tenant_id_lock {
            return Ok(tenant_id.clone());
        }

        let identity = self.get_auth_identity().await?;
        let tenant_id = identity.tenant;
        *tenant_id_lock = Some(tenant_id.clone());
        Ok(tenant_id)
    }

    async fn send<Body: Serialize, QueryParams: Serialize, Response: DeserializeOwned>(
        &self,
        operation_name: &str,
        method: Method,
        path: String,
        body: Option<Body>,
        query_params: Option<QueryParams>,
    ) -> Result<Response, ChromaClientError> {
        // todo: / normalization
        let url = format!("{}{}", self.base_url, path);

        let mut request = self.client.request(method.clone(), &url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        if let Some(query_params) = query_params {
            request = request.query(&query_params);
        }

        tracing::trace!(url = url, method =? method, "Sending request");

        #[cfg(feature = "opentelemetry")]
        let started_at = std::time::Instant::now();

        let response = request.send().await?;

        #[cfg(feature = "opentelemetry")]
        {
            self.metrics.record_request(
                operation_name,
                response.status().as_u16(),
                started_at.elapsed().as_secs_f64() * 1000.0,
            );
        }
        #[cfg(not(feature = "opentelemetry"))]
        {
            let _ = operation_name;
        }

        response.error_for_status_ref()?;
        let json = response.json::<serde_json::Value>().await?;

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                url = url,
                method =? method,
                "Received response: {}",
                serde_json::to_string_pretty(&json).unwrap_or_else(|_| "<failed to serialize>".to_string())
            );
        }

        let json = serde_json::from_value::<Response>(json)?;

        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ChromaAuthMethod;
    use futures_util::FutureExt;
    use std::sync::LazyLock;

    static CHROMA_CLIENT_OPTIONS: LazyLock<ChromaClientOptions> = LazyLock::new(|| {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(err) => {
                if err.not_found() {
                    tracing::warn!("No .env file found");
                } else {
                    panic!("Error loading .env file: {}", err);
                }
            }
        };

        ChromaClientOptions {
            base_url: std::env::var("CHROMA_ENDPOINT")
                .unwrap_or_else(|_| "https://api.trychroma.com".to_string()),
            auth_method: ChromaAuthMethod::cloud_api_key(
                &std::env::var("CHROMA_CLOUD_API_KEY").unwrap(),
            )
            .unwrap(),
            ..Default::default()
        }
    });

    async fn with_client<F, Fut>(callback: F)
    where
        F: FnOnce(ChromaClient) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let client = ChromaClient::new(CHROMA_CLIENT_OPTIONS.clone());

        // Create isolated database for test
        let database_name = format!("test_db_{}", uuid::Uuid::new_v4());
        client.create_database(database_name.clone()).await.unwrap();
        let databases = client.list_databases().await.unwrap();
        let database_id = databases
            .iter()
            .find(|db| db.name == database_name)
            .unwrap()
            .id
            .clone();
        client.set_default_database_id(database_id.clone()).await;

        let result = std::panic::AssertUnwindSafe(callback(client.clone()))
            .catch_unwind()
            .await;

        // Delete test database
        if let Err(err) = client.delete_database(database_name).await {
            tracing::error!("Failed to delete test database {}: {}", database_id, err);
        }

        result.unwrap();
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_heartbeat() {
        with_client(|client| async move {
            let heartbeat = client.heartbeat().await.unwrap();
            assert!(heartbeat.nanosecond_heartbeat > 0);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_auth_identity() {
        with_client(|client| async move {
            let identity = client.get_auth_identity().await.unwrap();
            assert!(!identity.tenant.is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_list_collections() {
        with_client(|client| async move {
            let collections = client
                .list_collections(ListCollectionsRequest::builder().build())
                .await
                .unwrap();
            assert!(collections.is_empty());

            // todo: create collection and assert it's returned, test limit/offset
        })
        .await;
    }
}

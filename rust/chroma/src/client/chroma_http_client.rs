use backon::ExponentialBuilder;
use backon::Retryable;
use chroma_api_types::ErrorResponse;
use chroma_error::ChromaValidationError;
use chroma_types::Collection;
use chroma_types::CollectionConfiguration;
use chroma_types::Metadata;
use chroma_types::WhereError;
use parking_lot::Mutex;
use reqwest::Method;
use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use thiserror::Error;

use chroma_api_types::{GetUserIdentityResponse, HeartbeatResponse};

use crate::client::ChromaHttpClientOptions;
use crate::client::ChromaHttpClientOptionsError;
use crate::collection::ChromaCollection;

const USER_AGENT: &str = concat!(
    "Chroma Rust Client v",
    env!("CARGO_PKG_VERSION"),
    " (https://github.com/chroma-core/chroma)"
);

#[derive(Error, Debug)]
pub enum ChromaHttpClientError {
    #[error("Request error: {0:?}")]
    RequestError(#[from] reqwest::Error),
    #[error("API error: {0:?} ({1})")]
    ApiError(String, reqwest::StatusCode),
    #[error("Could not resolve database ID: {0}")]
    CouldNotResolveDatabaseId(String),
    #[error("Serialization/Deserialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Validation error: {0}")]
    ValidationError(#[from] ChromaValidationError),
    // NOTE(rescrv):  The where validation drops the ChromaValidationError.  Bigger refactor.
    // TODO(rescrv):  Address the above note.
    #[error("Invalid where clause")]
    InvalidWhere,
}

impl From<WhereError> for ChromaHttpClientError {
    fn from(err: WhereError) -> Self {
        match err {
            WhereError::Serialization(json) => Self::SerdeError(json),
            WhereError::Validation(_) => Self::InvalidWhere,
        }
    }
}

#[derive(Debug)]
pub struct ChromaHttpClient {
    base_url: reqwest::Url,
    client: reqwest::Client,
    retry_policy: ExponentialBuilder,
    tenant_id: Arc<Mutex<Option<String>>>,
    database_name: Arc<Mutex<Option<String>>>,
    resolve_tenant_or_database_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(feature = "opentelemetry")]
    metrics: crate::client::metrics::Metrics,
}

impl Default for ChromaHttpClient {
    fn default() -> Self {
        Self::new(ChromaHttpClientOptions::default())
    }
}

impl Clone for ChromaHttpClient {
    fn clone(&self) -> Self {
        ChromaHttpClient {
            base_url: self.base_url.clone(),
            client: self.client.clone(),
            retry_policy: self.retry_policy,
            tenant_id: Arc::new(Mutex::new(self.tenant_id.lock().clone())),
            database_name: Arc::new(Mutex::new(self.database_name.lock().clone())),
            resolve_tenant_or_database_lock: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(feature = "opentelemetry")]
            metrics: self.metrics.clone(),
        }
    }
}

// TODO: remove and replace with actual Database struct
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Database {
    id: String,
    name: String,
}

impl ChromaHttpClient {
    pub fn new(options: ChromaHttpClientOptions) -> Self {
        let mut headers = options.headers();
        headers.append("user-agent", USER_AGENT.try_into().unwrap());

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("Failed to initialize TLS backend");

        ChromaHttpClient {
            base_url: options.endpoint.clone(),
            client,
            retry_policy: options.retry_options.into(),
            tenant_id: Arc::new(Mutex::new(options.tenant_id)),
            database_name: Arc::new(Mutex::new(options.database_name)),
            resolve_tenant_or_database_lock: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(feature = "opentelemetry")]
            metrics: crate::client::metrics::Metrics::new(),
        }
    }

    pub fn from_env() -> Result<Self, ChromaHttpClientOptionsError> {
        Ok(Self::new(ChromaHttpClientOptions::from_env()?))
    }

    pub fn cloud() -> Result<Self, ChromaHttpClientOptionsError> {
        Ok(Self::new(ChromaHttpClientOptions::from_cloud_env()?))
    }

    pub fn set_database_name(&self, database_name: impl AsRef<str>) {
        let mut lock = self.database_name.lock();
        *lock = Some(database_name.as_ref().to_string());
    }

    pub async fn create_database(
        &self,
        name: impl AsRef<str>,
    ) -> Result<(), ChromaHttpClientError> {
        // Returns empty map ({})
        self.send::<_, (), serde_json::Value>(
            "create_database",
            Method::POST,
            format!("/api/v2/tenants/{}/databases", self.get_tenant_id().await?),
            Some(serde_json::json!({ "name": name.as_ref() })),
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn list_databases(&self) -> Result<Vec<Database>, ChromaHttpClientError> {
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

    pub async fn delete_database(
        &self,
        database_name: impl AsRef<str>,
    ) -> Result<(), ChromaHttpClientError> {
        // Returns empty map ({})
        self.send::<(), (), serde_json::Value>(
            "delete_database",
            Method::DELETE,
            format!(
                "/api/v2/tenants/{}/databases/{}",
                self.get_tenant_id().await?,
                database_name.as_ref()
            ),
            None,
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn get_auth_identity(
        &self,
    ) -> Result<GetUserIdentityResponse, ChromaHttpClientError> {
        self.send::<(), (), _>(
            "get_auth_identity",
            Method::GET,
            "/api/v2/auth/identity".to_string(),
            None,
            None,
        )
        .await
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, ChromaHttpClientError> {
        self.send::<(), (), _>(
            "heartbeat",
            Method::GET,
            "/api/v2/heartbeat".to_string(),
            None,
            None,
        )
        .await
    }

    pub async fn get_or_create_collection(
        &self,
        name: impl AsRef<str>,
        configuration: Option<CollectionConfiguration>,
        metadata: Option<Metadata>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        self.common_create_collection(name, configuration, metadata, true)
            .await
    }

    pub async fn create_collection(
        &self,
        name: impl AsRef<str>,
        configuration: Option<CollectionConfiguration>,
        metadata: Option<Metadata>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        self.common_create_collection(name, configuration, metadata, false)
            .await
    }

    pub async fn get_collection(
        &self,
        name: impl AsRef<str>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        let tenant_id = self.get_tenant_id().await?;
        let database_name = self.get_database_name().await?;

        let collection: chroma_types::Collection = self
            .send::<(), _, chroma_types::Collection>(
                "get_collection",
                Method::GET,
                format!(
                    "/api/v2/tenants/{}/databases/{}/collections/{}",
                    tenant_id,
                    database_name,
                    name.as_ref()
                ),
                None,
                None::<()>,
            )
            .await?;

        Ok(ChromaCollection {
            client: self.clone(),
            collection: Arc::new(collection),
        })
    }

    pub async fn delete_collection(
        &self,
        name: impl AsRef<str>,
    ) -> Result<(), ChromaHttpClientError> {
        let tenant_id = self.get_tenant_id().await?;
        let database_name = self.get_database_name().await?;

        self.send::<(), (), serde_json::Value>(
            "delete_collection",
            Method::DELETE,
            format!(
                "/api/v2/tenants/{}/databases/{}/collections/{}",
                tenant_id,
                database_name,
                name.as_ref()
            ),
            None,
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn list_collections(
        &self,
        limit: usize,
        offset: Option<usize>,
    ) -> Result<Vec<ChromaCollection>, ChromaHttpClientError> {
        let tenant_id = self.get_tenant_id().await?;
        let database_name = self.get_database_name().await?;

        #[derive(Serialize)]
        struct QueryParams {
            limit: usize,
            offset: Option<usize>,
        }

        let collections = self
            .send::<(), _, Vec<Collection>>(
                "list_collections",
                Method::GET,
                format!(
                    "/api/v2/tenants/{}/databases/{}/collections",
                    tenant_id, database_name
                ),
                None,
                Some(QueryParams { limit, offset }),
            )
            .await?;

        Ok(collections
            .into_iter()
            .map(|collection| ChromaCollection {
                client: self.clone(),
                collection: Arc::new(collection),
            })
            .collect())
    }

    async fn common_create_collection(
        &self,
        name: impl AsRef<str>,
        configuration: Option<CollectionConfiguration>,
        metadata: Option<Metadata>,
        get_or_create: bool,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        let tenant_id = self.get_tenant_id().await?;
        let database_name = self.get_database_name().await?;

        let collection: chroma_types::Collection = self
            .send(
                "create_collection",
                Method::POST,
                format!(
                    "/api/v2/tenants/{}/databases/{}/collections",
                    tenant_id, database_name
                ),
                Some(serde_json::json!({
                    "name": name.as_ref(),
                    "configuration": configuration,
                    "metadata": metadata,
                    "get_or_create": get_or_create,
                })),
                None::<()>,
            )
            .await?;

        Ok(ChromaCollection {
            client: self.clone(),
            collection: Arc::new(collection),
        })
    }

    async fn get_database_name(&self) -> Result<String, ChromaHttpClientError> {
        {
            let database_name_lock = self.database_name.lock();
            if let Some(database_name) = &*database_name_lock {
                return Ok(database_name.clone());
            }
        }

        let _guard = self.resolve_tenant_or_database_lock.lock().await;

        {
            let database_name_lock = self.database_name.lock();
            if let Some(database_name) = &*database_name_lock {
                return Ok(database_name.clone());
            }
        }

        let identity = self.get_auth_identity().await?;

        if identity.databases.len() > 1 {
            return Err(ChromaHttpClientError::CouldNotResolveDatabaseId(
                "Client has access to multiple databases; please provide a database_name"
                    .to_string(),
            ));
        }

        let database_name = identity.databases.into_iter().next().ok_or_else(|| {
            ChromaHttpClientError::CouldNotResolveDatabaseId(
                "Client has access to no databases".to_string(),
            )
        })?;

        {
            let mut database_name_lock = self.database_name.lock();
            *database_name_lock = Some(database_name.clone());
        }

        Ok(database_name.clone())
    }

    async fn get_tenant_id(&self) -> Result<String, ChromaHttpClientError> {
        {
            let tenant_id_lock = self.tenant_id.lock();
            if let Some(tenant_id) = &*tenant_id_lock {
                return Ok(tenant_id.clone());
            }
        }

        let _guard = self.resolve_tenant_or_database_lock.lock().await;
        {
            let tenant_id_lock = self.tenant_id.lock();
            if let Some(tenant_id) = &*tenant_id_lock {
                return Ok(tenant_id.clone());
            }
        }

        let identity = self.get_auth_identity().await?;
        let tenant_id = identity.tenant;

        {
            let mut tenant_id_lock = self.tenant_id.lock();
            *tenant_id_lock = Some(tenant_id.clone());
        }

        Ok(tenant_id)
    }

    pub(crate) async fn send<
        Body: Serialize,
        QueryParams: Serialize,
        Response: DeserializeOwned,
    >(
        &self,
        operation_name: &str,
        method: Method,
        path: String,
        body: Option<Body>,
        query_params: Option<QueryParams>,
    ) -> Result<Response, ChromaHttpClientError> {
        let url = self.base_url.join(&path).expect(
            "The base URL is valid and we control all path construction, so this should never fail",
        );

        let attempt = || async {
            let mut request = self.client.request(method.clone(), url.clone());
            if let Some(body) = &body {
                request = request.json(body);
            }
            if let Some(query_params) = &query_params {
                request = request.query(query_params);
            }

            tracing::trace!(url = %url, method =? method, "Sending request");

            #[cfg(feature = "opentelemetry")]
            let started_at = std::time::Instant::now();

            let response = request.send().await.map_err(|err| (err, None))?;

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

            if let Err(err) = response.error_for_status_ref() {
                return Err((err, Some(response)));
            }

            Ok::<reqwest::Response, (reqwest::Error, Option<reqwest::Response>)>(response)
        };

        let response = attempt
            .retry(&self.retry_policy)
            .notify(|(err, _), _| {
                tracing::warn!(
                    url = %url,
                    method =? method,
                    status =? err.status(),
                    "Request failed with retryable error. Retrying...",
                );

                #[cfg(feature = "opentelemetry")]
                self.metrics.increment_retry(operation_name);
            })
            .when(|(err, _)| {
                err.status()
                    .map(|status| status == StatusCode::TOO_MANY_REQUESTS)
                    .unwrap_or_default()
                    || (method == Method::GET
                        && err.status().map(|s| s.is_server_error()).unwrap_or(true))
            })
            .await;

        let response = match response {
            Ok(response) => response,
            Err((err, maybe_response)) => {
                if let Some(response) = maybe_response {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    let json = match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(json) => json,
                        Err(_) => {
                            tracing::trace!(
                                url = %url,
                                method =? method,
                                "Received non-JSON error response: {}",
                                text
                            );

                            return Err(ChromaHttpClientError::ApiError(
                                format!("Non-JSON error response: {}", text),
                                status,
                            ));
                        }
                    };

                    if tracing::enabled!(tracing::Level::TRACE) {
                        tracing::trace!(
                            url = %url,
                            method =? method,
                            "Received response: {}",
                            serde_json::to_string_pretty(&json).unwrap_or_else(|_| "<failed to serialize>".to_string())
                        );
                    }

                    if let Ok(api_error) = serde_json::from_value::<ErrorResponse>(json) {
                        return Err(ChromaHttpClientError::ApiError(
                            format!("{}: {}", api_error.error, api_error.message),
                            status,
                        ));
                    }
                }

                return Err(ChromaHttpClientError::RequestError(err));
            }
        };

        let json = response.json::<serde_json::Value>().await?;

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                url = %url,
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
    use crate::client::ChromaRetryOptions;
    use crate::tests::with_client;
    use httpmock::{HttpMockResponse, MockServer};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

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
    async fn test_retries_get_requests() {
        let server = MockServer::start_async().await;

        let was_called = Arc::new(AtomicBool::new(false));
        let mock = server
            .mock_async(|when, then| {
                when.method("GET").path("/retry-get");
                // then.status(500);

                let was_called = was_called.clone();
                then.respond_with(move |_| {
                    if was_called.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        return HttpMockResponse::builder()
                            .status(200)
                            .body(r#"{"value": "ok"}"#)
                            .build();
                    }

                    HttpMockResponse::builder()
                        .status(500)
                        .body("Internal Server Error")
                        .build()
                });
            })
            .await;

        let client = ChromaHttpClient::new(ChromaHttpClientOptions {
            endpoint: server.base_url().parse().unwrap(),
            retry_options: ChromaRetryOptions {
                max_retries: 3,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                jitter: false,
            },
            ..Default::default()
        });

        let response: serde_json::Value = client
            .send::<(), (), serde_json::Value>(
                "retry_get",
                Method::GET,
                "/retry-get".into(),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(response, serde_json::json!({"value": "ok"}));
        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_retries_non_get_on_429() {
        let server = MockServer::start_async().await;

        let was_called = Arc::new(AtomicBool::new(false));
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/retry-post");

                let was_called = was_called.clone();

                then.respond_with(move |_| {
                    if was_called.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        return HttpMockResponse::builder()
                            .status(200)
                            .body(r#"{"status": "ok"}"#)
                            .build();
                    }

                    HttpMockResponse::builder()
                        .status(429)
                        .body("Too Many Requests")
                        .build()
                });
            })
            .await;

        let client = ChromaHttpClient::new(ChromaHttpClientOptions {
            endpoint: server.base_url().parse().unwrap(),
            retry_options: ChromaRetryOptions {
                max_retries: 2,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                jitter: false,
            },
            ..Default::default()
        });

        let response: serde_json::Value = client
            .send::<serde_json::Value, (), serde_json::Value>(
                "retry_post",
                Method::POST,
                "/retry-post".into(),
                Some(serde_json::json!({"request": "body"})),
                None::<()>,
            )
            .await
            .unwrap();

        assert_eq!(response, serde_json::json!({"status": "ok"}));
        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_parses_error() {
        with_client(|client| async move {
            client.create_collection("foo", None, None).await.unwrap();

            let err = client
                .create_collection("foo", None, None)
                .await
                .unwrap_err();

            match err {
                ChromaHttpClientError::ApiError(msg, status) => {
                    assert_eq!(status, StatusCode::CONFLICT);
                    assert!(msg.contains("already exists"));
                }
                _ => panic!("Expected ApiError"),
            };
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_list_collections() {
        with_client(|client| async move {
            let collections = client.list_collections(100, None).await.unwrap();
            assert!(collections.is_empty());

            client.create_collection("first", None, None).await.unwrap();

            client
                .create_collection("second", None, None)
                .await
                .unwrap();

            let collections = client.list_collections(100, None).await.unwrap();
            assert_eq!(collections.len(), 2);

            let collections = client.list_collections(1, Some(1)).await.unwrap();
            assert_eq!(collections.len(), 1);
            assert_eq!(collections[0].collection.name, "second");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_create_collection() {
        with_client(|client| async move {
            let collection = client.create_collection("foo", None, None).await.unwrap();
            assert_eq!(collection.collection.name, "foo");

            client
                .get_or_create_collection("foo", None, None)
                .await
                .unwrap();
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_collection() {
        with_client(|client| async move {
            client
                .create_collection("my_collection".to_string(), None, None)
                .await
                .unwrap();

            let collection = client
                .get_collection("my_collection".to_string())
                .await
                .unwrap();

            assert_eq!(collection.collection.name, "my_collection");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_delete_collection() {
        with_client(|client| async move {
            client
                .create_collection("to_be_deleted".to_string(), None, None)
                .await
                .unwrap();

            client
                .delete_collection("to_be_deleted".to_string())
                .await
                .unwrap();

            let err = client
                .get_collection("to_be_deleted".to_string())
                .await
                .unwrap_err();

            match err {
                ChromaHttpClientError::ApiError(msg, status) => {
                    assert_eq!(status, StatusCode::NOT_FOUND);
                    assert!(msg.contains("does not exist"));
                }
                _ => panic!("Expected ApiError"),
            };
        })
        .await;
    }
}

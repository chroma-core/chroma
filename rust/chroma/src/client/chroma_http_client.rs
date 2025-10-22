use backon::ExponentialBuilder;
use backon::Retryable;
use chroma_api_types::ErrorResponse;
use chroma_error::ChromaValidationError;
use chroma_types::Collection;
use chroma_types::Metadata;
use chroma_types::Schema;
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

/// Errors that originate from the Chroma client during request execution.
#[derive(Error, Debug)]
pub enum ChromaHttpClientError {
    /// Network-level HTTP request failed.
    #[error("Request error: {0:?}")]
    RequestError(#[from] reqwest::Error),
    /// Chroma API returned an error status with a structured error message.
    ///
    /// Contains the error message from the server and the HTTP status code that triggered the error.
    #[error("API error: {0:?} ({1})")]
    ApiError(String, reqwest::StatusCode),
    /// Client lacks access to a unique database or cannot determine which database to use.
    #[error("Could not resolve database ID: {0}")]
    CouldNotResolveDatabaseId(String),
    /// JSON serialization or deserialization of request/response bodies failed.
    #[error("Serialization/Deserialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    /// Request parameters failed validation checks before transmission.
    #[error("Validation error: {0}")]
    ValidationError(#[from] ChromaValidationError),
    // NOTE(rescrv):  The where validation drops the ChromaValidationError.  Bigger refactor.
    // TODO(rescrv):  Address the above note.
    /// Where clause failed validation checks.
    ///
    /// This error is returned when a where clause provided to a query operation contains
    /// invalid syntax or semantics. It represents a simplified version of the underlying
    /// validation error from the where clause parser.
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

#[cfg(feature = "opentelemetry")]
static METRICS: std::sync::LazyLock<crate::client::metrics::Metrics> =
    std::sync::LazyLock::new(crate::client::metrics::Metrics::new);

/// Client handle for interacting with a Chroma AI-native database deployment.
///
/// This is the primary entry point for all database-level operations. A `ChromaClient` manages
/// connection state, authentication, automatic retries, and tenant/database resolution.
/// Operations include database lifecycle management, collection enumeration, and system health checks.
///
/// # Architecture
///
/// Each client maintains:
/// - An HTTP client pool for concurrent requests
/// - Cached tenant and database IDs resolved from authentication
/// - A retry policy with exponential backoff
/// - Optional OpenTelemetry metrics when the `opentelemetry` feature is enabled
///
/// # Cloning
///
/// `ChromaClient` implements `Clone` with shared connection pooling but independent cached state.
/// This enables spawning concurrent operations while maintaining efficient resource usage.
///
/// # Examples
///
/// ```
/// use chroma::{ChromaHttpClient, client::ChromaHttpClientOptions, client::ChromaAuthMethod};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let options = ChromaHttpClientOptions {
///     endpoint: "https://api.trychroma.com".parse()?,
///     auth_method: ChromaAuthMethod::cloud_api_key("my-key")?,
///     ..Default::default()
/// };
/// let client = ChromaHttpClient::new(options);
///
/// let heartbeat = client.heartbeat().await?;
/// assert!(heartbeat.nanosecond_heartbeat > 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ChromaHttpClient {
    base_url: reqwest::Url,
    client: reqwest::Client,
    retry_policy: ExponentialBuilder,
    tenant_id: Arc<Mutex<Option<String>>>,
    database_name: Arc<Mutex<Option<String>>>,
    resolve_tenant_or_database_lock: Arc<tokio::sync::Mutex<()>>,
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
        }
    }
}

/// Represents a database within a Chroma tenant.
///
/// A database is a logical namespace for organizing collections. Each database has a unique
/// identifier and a user-assigned name. This struct is returned by [`ChromaHttpClient::list_databases`].
// TODO: remove and replace with actual Database struct
#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct Database {
    /// The unique identifier for this database.
    pub id: String,
    /// The user-assigned name for this database.
    pub name: String,
}

impl ChromaHttpClient {
    /// Constructs a client from explicit configuration options.
    ///
    /// Initializes the HTTP client with the specified endpoint, authentication, and retry behavior.
    /// The client immediately becomes ready to make API calls without requiring additional setup.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma::{ChromaHttpClient, client::ChromaHttpClientOptions, client::ChromaAuthMethod};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = ChromaHttpClientOptions {
    ///     endpoint: "https://api.trychroma.com".parse()?,
    ///     auth_method: ChromaAuthMethod::cloud_api_key("my-key")?,
    ///     ..Default::default()
    /// };
    /// let client = ChromaHttpClient::new(options);
    /// # Ok(())
    /// # }
    /// ```
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
        }
    }

    /// Constructs a client from environment variables.
    ///
    /// Reads configuration from `CHROMA_ENDPOINT`, `CHROMA_TENANT`, and `CHROMA_DATABASE`.
    /// Falls back to default local endpoint if `CHROMA_ENDPOINT` is not set.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is malformed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chroma::ChromaHttpClient;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ChromaHttpClient::from_env()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_env() -> Result<Self, ChromaHttpClientOptionsError> {
        Ok(Self::new(ChromaHttpClientOptions::from_env()?))
    }

    /// Constructs a client configured for Chroma Cloud from environment variables.
    ///
    /// Reads `CHROMA_API_KEY` (required), `CHROMA_ENDPOINT` (defaults to Chroma Cloud),
    /// `CHROMA_TENANT`, and `CHROMA_DATABASE` from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if `CHROMA_API_KEY` is not set or the endpoint URL is malformed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chroma::ChromaHttpClient;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ChromaHttpClient::cloud()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cloud() -> Result<Self, ChromaHttpClientOptionsError> {
        Ok(Self::new(ChromaHttpClientOptions::from_cloud_env()?))
    }

    /// Assigns the database to use for subsequent collection operations.
    ///
    /// Overrides any previously cached or configured database name. Operations after this call
    /// will target the specified database until changed again.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # fn example(client: ChromaHttpClient) {
    /// client.set_database_name("production");
    /// # }
    /// ```
    pub fn set_database_name(&self, database_name: impl AsRef<str>) {
        let mut lock = self.database_name.lock();
        *lock = Some(database_name.as_ref().to_string());
    }

    /// Resolves the database name for collection operations.
    ///
    /// Returns the cached database name if available, otherwise fetches and caches the user's
    /// identity information. Uses a lock to prevent concurrent resolution attempts.
    pub async fn get_database_name(&self) -> Result<String, ChromaHttpClientError> {
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

    /// Resolves the tenant ID for the authenticated user.
    ///
    /// Returns the cached tenant ID if available, otherwise fetches and caches the user's
    /// identity information. Uses a lock to prevent concurrent resolution attempts.
    pub async fn get_tenant_id(&self) -> Result<String, ChromaHttpClientError> {
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

    /// Creates a new database within the authenticated tenant.
    ///
    /// The database becomes immediately available for collection operations after creation.
    /// Database names must be unique within a tenant.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A database with the same name already exists
    /// - Network communication fails
    /// - The tenant ID cannot be resolved
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// client.create_database("analytics").await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Enumerates all databases accessible to this client within the authenticated tenant.
    ///
    /// # Errors
    ///
    /// Returns an error if network communication fails or tenant ID cannot be resolved.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let databases = client.list_databases().await?;
    /// for db in databases {
    ///     println!("Database: {} (ID: {})", db.name, db.id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
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

    /// Deletes a database from the current tenant.
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

    /// Retrieves identity information for the authenticated user.
    ///
    /// Returns the tenant and database access details for the current authentication credentials.
    /// This is used internally to resolve tenant and database IDs but can also be called directly
    /// to verify authentication status.
    ///
    /// # Errors
    ///
    /// Returns an error if authentication fails or network communication fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let identity = client.get_auth_identity().await?;
    /// println!("Tenant: {}", identity.tenant);
    /// println!("Databases: {}", identity.databases.len());
    /// # Ok(())
    /// # }
    /// ```
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

    /// Performs a health check against the Chroma server.
    ///
    /// Sends a lightweight request to verify server availability and responsiveness.
    /// The response contains a nanosecond-precision timestamp from the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the server is unreachable or returns an error status.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let heartbeat = client.heartbeat().await?;
    /// assert!(heartbeat.nanosecond_heartbeat > 0);
    /// # Ok(())
    /// # }
    /// ```
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

    /// Retrieves an existing collection or creates it if it doesn't exist.
    ///
    /// Idempotent collection access that always succeeds if the name is valid. If a collection
    /// with the given name already exists, returns a handle to it. Otherwise, creates a new
    /// collection with the specified configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network communication fails
    /// - The database name cannot be resolved
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let collection = client.get_or_create_collection(
    ///     "my_vectors",
    ///     None,
    ///     None
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create_collection(
        &self,
        name: impl AsRef<str>,
        schema: Option<Schema>,
        metadata: Option<Metadata>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        self.common_create_collection(name, schema, metadata, true)
            .await
    }

    /// Creates a new collection with the specified parameters.
    ///
    /// Fails if a collection with the same name already exists in the database.
    /// To get an existing collection or create it if missing, use [`get_or_create_collection`](Self::get_or_create_collection).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A collection with the same name already exists
    /// - Network communication fails
    /// - The database name cannot be resolved
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let collection = client.create_collection(
    ///     "embeddings",
    ///     None,
    ///     None
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_collection(
        &self,
        name: impl AsRef<str>,
        schema: Option<Schema>,
        metadata: Option<Metadata>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        self.common_create_collection(name, schema, metadata, false)
            .await
    }

    /// Retrieves an existing collection by name.
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

    /// Removes a collection and all its records from the database.
    ///
    /// Permanently deletes the collection and all contained embeddings, metadata, and documents.
    /// This operation cannot be undone.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collection does not exist
    /// - Network communication fails
    /// - The database or tenant cannot be resolved
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// client.delete_collection("old_embeddings").await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Enumerates collections in the specified database with pagination support.
    ///
    /// Returns collection handles that can be used to perform read and write operations.
    /// Results are ordered consistently but the specific ordering is implementation-defined.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network communication fails
    /// - The database name cannot be resolved
    /// - The authenticated user lacks read permissions
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaHttpClient;
    /// # async fn example(client: ChromaHttpClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let collections = client.list_collections(
    ///     10,
    ///     Some(0)
    /// ).await?;
    /// for collection in collections {
    ///     println!("Collection: {}", collection.name());
    /// }
    /// # Ok(())
    /// # }
    /// ```
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
        schema: Option<Schema>,
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
                    "schema": schema,
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

    /// Executes an HTTP request with automatic retry logic and OpenTelemetry metrics.
    ///
    /// This is the core transport method used by all higher-level operations. It handles:
    /// - Request serialization and query parameter encoding
    /// - Exponential backoff retry for GET requests and 429 responses
    /// - Response deserialization with detailed tracing
    /// - Metrics recording when the `opentelemetry` feature is enabled
    ///
    /// # Retry Behavior
    ///
    /// Retries automatically for:
    /// - Any GET request that fails with a retryable error
    /// - Any request (GET/POST/DELETE) that receives a 429 (Too Many Requests) response
    ///
    /// Non-GET requests with other error statuses fail immediately without retry.
    pub(crate) async fn send<
        Body: Serialize,
        QueryParams: Serialize,
        Response: DeserializeOwned,
    >(
        &self,
        operation_name: &str,
        method: Method,
        path: impl AsRef<str>,
        body: Option<Body>,
        query_params: Option<QueryParams>,
    ) -> Result<Response, ChromaHttpClientError> {
        let url = self.base_url.join(path.as_ref()).expect(
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
                METRICS.record_request(
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
                METRICS.increment_retry(operation_name);
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
    use crate::tests::{unique_collection_name, with_client};
    use chroma_types::{EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration};
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
            .send::<(), (), serde_json::Value>("retry_get", Method::GET, "/retry-get", None, None)
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
                "/retry-post",
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
        with_client(|mut client| async move {
            let collection = client.new_collection("foo").await;
            let err = client
                .create_collection(collection.name(), None, None)
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
        with_client(|mut client| async move {
            let first = client.new_collection("first").await;
            let second = client.new_collection("second").await;
            let first = first.name();
            let second = second.name();

            let collections = client.list_collections(1000, None).await.unwrap();
            let names: std::collections::HashSet<_> = collections
                .iter()
                .map(|collection| collection.name().to_string())
                .collect();

            assert!(names.contains(first));
            assert!(names.contains(second));
            let positions = collections
                .iter()
                .enumerate()
                .filter(|(_, collection)| collection.name() == first || collection.name() == second)
                .collect::<Vec<_>>();
            assert_eq!(positions.len(), 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_create_collection() {
        with_client(|mut client| async move {
            let schema = Schema::default_with_embedding_function(
                EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                    name: "bar".to_string(),
                    config: serde_json::json!({}),
                }),
            );
            let collection1 = client.new_collection("foo").await;
            let collection2 = client
                .get_or_create_collection(collection1.name(), Some(schema), None)
                .await
                .unwrap();
            assert_eq!(collection1.name(), collection2.name());
            assert_eq!(collection1.schema(), collection2.schema());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_get_collection() {
        with_client(|mut client| async move {
            let collection = client.new_collection("my_collection").await;
            let name = collection.name().to_string();
            let collection = client.get_collection(collection.name()).await.unwrap();
            assert_eq!(collection.collection.name, name);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_delete_collection() {
        with_client(|client| async move {
            let name = unique_collection_name("to_be_deleted");

            client
                .create_collection(name.clone(), None, None)
                .await
                .unwrap();

            client.delete_collection(name.clone()).await.unwrap();

            let err = client.get_collection(name.clone()).await.unwrap_err();

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

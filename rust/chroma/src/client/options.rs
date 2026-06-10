//! Configuration types for Chroma client initialization and behavior.
//!
//! This module defines options that control authentication, retry behavior, and connection
//! parameters for the Chroma client. The primary type is [`ChromaClientOptions`], which bundles
//! all configuration needed to construct a [`ChromaClient`](crate::ChromaClient).

use std::time::Duration;

use backon::ExponentialBuilder;
use reqwest::header::{HeaderName, HeaderValue, InvalidHeaderValue};

/// Configuration for automatic retry behavior when requests fail.
///
/// Implements exponential backoff with optional jitter to prevent thundering herd problems
/// when multiple clients retry simultaneously.
#[derive(Clone, Debug)]
pub struct ChromaRetryOptions {
    /// Maximum number of retry attempts before giving up.
    pub max_retries: usize,
    /// Initial delay before the first retry.
    pub min_delay: Duration,
    /// Maximum delay between retries (backoff is capped at this value).
    pub max_delay: Duration,
    /// Whether to add random jitter to retry delays to avoid synchronized retries.
    pub jitter: bool,
}

impl Default for ChromaRetryOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
            jitter: true,
        }
    }
}

impl From<ChromaRetryOptions> for ExponentialBuilder {
    fn from(options: ChromaRetryOptions) -> Self {
        let mut builder = ExponentialBuilder::new()
            .with_max_times(options.max_retries)
            .with_min_delay(options.min_delay)
            .with_max_delay(options.max_delay);
        if options.jitter {
            builder = builder.with_jitter();
        }
        builder
    }
}

/// Authentication method for Chroma API requests.
///
/// Supports multiple authentication strategies depending on deployment configuration.
#[derive(Debug, Clone)]
pub enum ChromaAuthMethod {
    /// No authentication (for local development or unsecured instances).
    None,
    /// Custom header-based authentication.
    HeaderAuth {
        /// The HTTP header name to use for authentication.
        header: HeaderName,
        /// The authentication token or credential value.
        value: HeaderValue,
    },
}

impl ChromaAuthMethod {
    /// Creates authentication for Chroma Cloud using an API key.
    ///
    /// The API key is transmitted via the `x-chroma-token` header and marked as sensitive
    /// to prevent it from appearing in logs.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key contains invalid HTTP header characters.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma::client::ChromaAuthMethod;
    ///
    /// let auth = ChromaAuthMethod::cloud_api_key("my-secret-key").unwrap();
    /// ```
    pub fn cloud_api_key(key: &str) -> Result<Self, InvalidHeaderValue> {
        let mut value = HeaderValue::from_str(key)?;
        value.set_sensitive(true);

        Ok(ChromaAuthMethod::HeaderAuth {
            header: HeaderName::from_static("x-chroma-token"),
            value,
        })
    }

    pub(crate) fn chroma_cloud_api_key(&self) -> Option<&str> {
        match self {
            ChromaAuthMethod::HeaderAuth { header, value }
                if header.as_str().eq_ignore_ascii_case("x-chroma-token") =>
            {
                value.to_str().ok()
            }
            ChromaAuthMethod::HeaderAuth { .. } | ChromaAuthMethod::None => None,
        }
    }

    /// Applies this authentication method to an outgoing request.
    pub(crate) fn apply(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            ChromaAuthMethod::HeaderAuth { header, value } => {
                request.header(header.clone(), value.clone())
            }
            ChromaAuthMethod::None => request,
        }
    }
}

/// Errors that occur during client configuration construction.
#[derive(Debug, thiserror::Error)]
pub enum ChromaHttpClientOptionsError {
    /// An authentication credential contains invalid characters for HTTP headers.
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    /// The provided endpoint URL is malformed or cannot be parsed.
    #[error("Invalid endpoint URL: {0}")]
    InvalidEndpoint(String),
    /// A required configuration parameter is missing from the environment or input.
    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),
}

const DEFAULT_LOCAL_ENDPOINT: &str = "http://localhost:8000";
const DEFAULT_CLOUD_ENDPOINT: &str = "https://api.trychroma.com";

fn endpoint_from_env(default_endpoint: &str) -> Result<reqwest::Url, ChromaHttpClientOptionsError> {
    let default_endpoint = default_endpoint.parse().expect("valid URL");

    if let Ok(endpoint) = std::env::var("CHROMA_ENDPOINT") {
        return endpoint
            .parse::<reqwest::Url>()
            .map_err(|err| ChromaHttpClientOptionsError::InvalidEndpoint(err.to_string()));
    }

    if let Ok(host) = std::env::var("CHROMA_HOST") {
        return endpoint_from_host(&host, &default_endpoint);
    }

    Ok(default_endpoint)
}

fn endpoint_from_host(
    host: &str,
    default_endpoint: &reqwest::Url,
) -> Result<reqwest::Url, ChromaHttpClientOptionsError> {
    let endpoint = if host.contains("://") {
        host.to_string()
    } else {
        format!("{}://{}", default_endpoint.scheme(), host)
    };

    endpoint
        .parse::<reqwest::Url>()
        .map_err(|err| ChromaHttpClientOptionsError::InvalidEndpoint(err.to_string()))
}

/// Configuration bundle for initializing a Chroma client.
///
/// Aggregates connection parameters, authentication credentials, and operational policies
/// into a single structure. Used to construct [`ChromaHttpClient`](crate::ChromaHttpClient) instances.
#[derive(Debug, Clone)]
pub struct ChromaHttpClientOptions {
    /// The base URL of the Chroma server (e.g., `https://api.trychroma.com`).
    pub endpoint: reqwest::Url,
    /// Additional Chroma endpoints to use for failover after the primary `endpoint`.
    ///
    /// When empty, the client talks only to `endpoint`. When populated, the client will
    /// try `endpoint` first and then each additional endpoint in order for read-only requests.
    pub endpoints: Vec<reqwest::Url>,
    /// Authentication strategy to use for API requests.
    pub auth_method: ChromaAuthMethod,
    /// Retry configuration for failed requests.
    pub retry_options: ChromaRetryOptions,
    /// Explicit tenant ID. If None, will be automatically resolved from authentication.
    pub tenant_id: Option<String>,
    /// Will be automatically resolved at request time if not provided. It can only be resolved automatically if this client has access to exactly one database.
    pub database_name: Option<String>,
}

impl Default for ChromaHttpClientOptions {
    fn default() -> Self {
        ChromaHttpClientOptions {
            endpoint: DEFAULT_LOCAL_ENDPOINT.parse().expect("valid URL"),
            endpoints: Vec::new(),
            auth_method: ChromaAuthMethod::None,
            retry_options: ChromaRetryOptions::default(),
            tenant_id: None,
            database_name: None,
        }
    }
}

impl ChromaHttpClientOptions {
    /// Constructs client options from environment variables for local or self-hosted deployments.
    ///
    /// Reads:
    /// - `CHROMA_ENDPOINT` (optional, defaults to `http://localhost:8000`)
    /// - `CHROMA_HOST` (optional URL or bare host fallback when `CHROMA_ENDPOINT` is unset)
    /// - `CHROMA_TENANT` (optional, defaults to `"default_tenant"`)
    /// - `CHROMA_DATABASE` (optional, defaults to `"default_database"`)
    ///
    /// Uses no authentication, suitable for local development.
    ///
    /// # Errors
    ///
    /// Returns an error if `CHROMA_ENDPOINT` or `CHROMA_HOST` is set but cannot be parsed as a URL.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chroma::client::ChromaHttpClientOptions;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = ChromaHttpClientOptions::from_env()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_env() -> Result<Self, ChromaHttpClientOptionsError> {
        let endpoint = endpoint_from_env(DEFAULT_LOCAL_ENDPOINT)?;

        let tenant_id = std::env::var("CHROMA_TENANT").unwrap_or("default_tenant".to_string());
        let database_name =
            std::env::var("CHROMA_DATABASE").unwrap_or("default_database".to_string());

        Ok(ChromaHttpClientOptions {
            database_name: Some(database_name),
            tenant_id: Some(tenant_id),
            endpoint,
            ..Default::default()
        })
    }

    /// Constructs client options from environment variables for Chroma Cloud.
    ///
    /// Reads:
    /// - `CHROMA_API_KEY` (required)
    /// - `CHROMA_ENDPOINT` (optional, defaults to `https://api.trychroma.com`)
    /// - `CHROMA_HOST` (optional URL or bare host fallback when `CHROMA_ENDPOINT` is unset)
    /// - `CHROMA_TENANT` (optional, will be auto-resolved if not provided)
    /// - `CHROMA_DATABASE` (optional, will be auto-resolved if not provided)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `CHROMA_API_KEY` is not set
    /// - `CHROMA_ENDPOINT` or `CHROMA_HOST` is set but cannot be parsed as a URL
    /// - The API key contains invalid header characters
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chroma::client::ChromaHttpClientOptions;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = ChromaHttpClientOptions::from_cloud_env()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_cloud_env() -> Result<Self, ChromaHttpClientOptionsError> {
        let endpoint = endpoint_from_env(DEFAULT_CLOUD_ENDPOINT)?;

        let api_key = std::env::var("CHROMA_API_KEY").map_err(|_| {
            ChromaHttpClientOptionsError::MissingConfiguration("CHROMA_API_KEY".to_string())
        })?;

        let tenant_id = std::env::var("CHROMA_TENANT").ok();
        let database_name = std::env::var("CHROMA_DATABASE").ok();

        Ok(ChromaHttpClientOptions {
            database_name,
            tenant_id,
            endpoint,
            auth_method: ChromaAuthMethod::cloud_api_key(&api_key)?,
            ..Default::default()
        })
    }

    /// Constructs client options for Chroma Cloud with explicit credentials.
    ///
    /// Configures the client to connect to `https://api.trychroma.com` with the provided
    /// API key and database name. The tenant ID will be automatically resolved from authentication.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key contains invalid HTTP header characters.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma::client::ChromaHttpClientOptions;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = ChromaHttpClientOptions::cloud("my-api-key", "production-db")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cloud(
        api_key: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Result<Self, ChromaHttpClientOptionsError> {
        let api_key = api_key.into();
        let database_name = database_name.into();
        Ok(ChromaHttpClientOptions {
            database_name: Some(database_name),
            auth_method: ChromaAuthMethod::cloud_api_key(&api_key)?,
            endpoint: DEFAULT_CLOUD_ENDPOINT.parse().expect("valid URL"),
            ..Default::default()
        })
    }

    /// Constructs client options for Chroma Cloud admin operations.
    ///
    /// Configures the client to connect to `https://api.trychroma.com` with the provided
    /// API key. No database is selected, making this suitable for admin operations like
    /// listing, creating, or deleting databases.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key contains invalid HTTP header characters.
    ///
    /// # Examples
    ///
    /// ```
    /// use chroma::client::ChromaHttpClientOptions;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = ChromaHttpClientOptions::cloud_admin("my-api-key")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cloud_admin(api_key: impl Into<String>) -> Result<Self, ChromaHttpClientOptionsError> {
        let api_key = api_key.into();
        Ok(ChromaHttpClientOptions {
            auth_method: ChromaAuthMethod::cloud_api_key(&api_key)?,
            endpoint: DEFAULT_CLOUD_ENDPOINT.parse().expect("valid URL"),
            endpoints: Vec::new(),
            retry_options: ChromaRetryOptions::default(),
            tenant_id: None,
            database_name: None,
        })
    }

    pub(crate) fn all_endpoints(&self) -> Vec<reqwest::Url> {
        let mut endpoints = Vec::with_capacity(1 + self.endpoints.len());
        endpoints.push(self.endpoint.clone());
        for endpoint in &self.endpoints {
            if !endpoints.iter().any(|existing| existing == endpoint) {
                endpoints.push(endpoint.clone());
            }
        }
        endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const ENV_KEYS: [&str; 5] = [
        "CHROMA_API_KEY",
        "CHROMA_DATABASE",
        "CHROMA_ENDPOINT",
        "CHROMA_HOST",
        "CHROMA_TENANT",
    ];

    struct EnvSnapshot {
        values: Vec<(&'static str, Option<String>)>,
    }

    impl EnvSnapshot {
        fn capture() -> Self {
            Self {
                values: ENV_KEYS
                    .into_iter()
                    .map(|key| (key, std::env::var(key).ok()))
                    .collect(),
            }
        }

        fn clear() {
            for key in ENV_KEYS {
                std::env::remove_var(key);
            }
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            for (key, value) in &self.values {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn with_chroma_env<T>(vars: &[(&'static str, &'static str)], test: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap();
        let _snapshot = EnvSnapshot::capture();
        EnvSnapshot::clear();
        for (key, value) in vars {
            std::env::set_var(key, value);
        }
        test()
    }

    #[test]
    fn from_env_uses_chroma_host_when_endpoint_is_unset() {
        with_chroma_env(&[("CHROMA_HOST", "http://example.com:9000")], || {
            let options = ChromaHttpClientOptions::from_env().unwrap();

            assert_eq!(options.endpoint.as_str(), "http://example.com:9000/");
        });
    }

    #[test]
    fn from_env_uses_bare_chroma_host_with_local_scheme() {
        with_chroma_env(&[("CHROMA_HOST", "localhost:9000")], || {
            let options = ChromaHttpClientOptions::from_env().unwrap();

            assert_eq!(options.endpoint.as_str(), "http://localhost:9000/");
        });
    }

    #[test]
    fn from_env_prefers_chroma_endpoint_over_chroma_host() {
        with_chroma_env(
            &[
                ("CHROMA_ENDPOINT", "http://endpoint.example.com:9000"),
                ("CHROMA_HOST", "http://host.example.com:9000"),
            ],
            || {
                let options = ChromaHttpClientOptions::from_env().unwrap();

                assert_eq!(
                    options.endpoint.as_str(),
                    "http://endpoint.example.com:9000/"
                );
            },
        );
    }

    #[test]
    fn from_cloud_env_uses_chroma_host_when_endpoint_is_unset() {
        with_chroma_env(
            &[
                ("CHROMA_API_KEY", "test-key"),
                ("CHROMA_HOST", "https://cloud.example.com"),
            ],
            || {
                let options = ChromaHttpClientOptions::from_cloud_env().unwrap();

                assert_eq!(options.endpoint.as_str(), "https://cloud.example.com/");
            },
        );
    }

    #[test]
    fn from_cloud_env_uses_bare_chroma_host_with_cloud_scheme() {
        with_chroma_env(
            &[
                ("CHROMA_API_KEY", "test-key"),
                ("CHROMA_HOST", "api.devchroma.com"),
            ],
            || {
                let options = ChromaHttpClientOptions::from_cloud_env().unwrap();

                assert_eq!(options.endpoint.as_str(), "https://api.devchroma.com/");
            },
        );
    }
}

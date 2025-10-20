use std::time::Duration;

use backon::ExponentialBuilder;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, InvalidHeaderValue};

#[derive(Clone, Debug)]
pub struct ChromaRetryOptions {
    pub max_retries: usize,
    pub min_delay: Duration,
    pub max_delay: Duration,
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

#[derive(Debug, Clone)]
pub enum ChromaAuthMethod {
    None,
    HeaderAuth {
        header: HeaderName,
        value: HeaderValue,
    },
}

impl ChromaAuthMethod {
    pub fn cloud_api_key(key: &str) -> Result<Self, InvalidHeaderValue> {
        let mut value = HeaderValue::from_str(key)?;
        value.set_sensitive(true);

        Ok(ChromaAuthMethod::HeaderAuth {
            header: HeaderName::from_static("x-chroma-token"),
            value,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChromaClientOptionsError {
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Invalid endpoint URL: {0}")]
    InvalidEndpoint(String),
    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),
}

const DEFAULT_LOCAL_ENDPOINT: &str = "http://localhost:8000";
const DEFAULT_CLOUD_ENDPOINT: &str = "https://api.trychroma.com";

#[derive(Debug, Clone)]
pub struct ChromaClientOptions {
    pub endpoint: reqwest::Url,
    pub auth_method: ChromaAuthMethod,
    pub retry_options: ChromaRetryOptions,
    /// Will be automatically resolved at request time if not provided
    pub tenant_id: Option<String>,
    /// Will be automatically resolved at request time if not provided. It can only be resolved automatically if this client has access to exactly one database.
    pub default_database_name: Option<String>,
}

impl Default for ChromaClientOptions {
    fn default() -> Self {
        ChromaClientOptions {
            endpoint: DEFAULT_LOCAL_ENDPOINT.parse().expect("valid URL"),
            auth_method: ChromaAuthMethod::None,
            retry_options: ChromaRetryOptions::default(),
            tenant_id: None,
            default_database_name: None,
        }
    }
}

impl ChromaClientOptions {
    pub fn from_env() -> Result<Self, ChromaClientOptionsError> {
        let endpoint = std::env::var("CHROMA_ENDPOINT")
            .map(|s| s.parse())
            .unwrap_or(Ok(ChromaClientOptions::default().endpoint))
            .map_err(|err| ChromaClientOptionsError::InvalidEndpoint(err.to_string()))?;

        let tenant_id = std::env::var("CHROMA_TENANT").unwrap_or("default_tenant".to_string());
        let database_name =
            std::env::var("CHROMA_DATABASE").unwrap_or("default_database".to_string());

        Ok(ChromaClientOptions {
            default_database_name: Some(database_name),
            tenant_id: Some(tenant_id),
            endpoint,
            ..Default::default()
        })
    }

    pub fn from_cloud_env() -> Result<Self, ChromaClientOptionsError> {
        let endpoint = std::env::var("CHROMA_ENDPOINT")
            .map(|s| s.parse::<reqwest::Url>())
            .unwrap_or(Ok(DEFAULT_CLOUD_ENDPOINT.parse().expect("valid URL")))
            .map_err(|err| ChromaClientOptionsError::InvalidEndpoint(err.to_string()))?;

        let api_key = std::env::var("CHROMA_API_KEY").map_err(|_| {
            ChromaClientOptionsError::MissingConfiguration("CHROMA_API_KEY".to_string())
        })?;

        let tenant_id = std::env::var("CHROMA_TENANT").ok();
        let database_name = std::env::var("CHROMA_DATABASE").ok();

        Ok(ChromaClientOptions {
            default_database_name: database_name,
            tenant_id,
            endpoint,
            auth_method: ChromaAuthMethod::cloud_api_key(&api_key)?,
            ..Default::default()
        })
    }

    pub fn cloud(
        api_key: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Result<Self, ChromaClientOptionsError> {
        let api_key = api_key.into();
        let database_name = database_name.into();
        Ok(ChromaClientOptions {
            default_database_name: Some(database_name),
            auth_method: ChromaAuthMethod::cloud_api_key(&api_key)?,
            endpoint: DEFAULT_CLOUD_ENDPOINT.parse().expect("valid URL"),
            ..Default::default()
        })
    }

    pub(crate) fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        match &self.auth_method {
            ChromaAuthMethod::HeaderAuth { header, value } => {
                headers.insert(header.clone(), value.clone());
            }
            ChromaAuthMethod::None => {}
        }
        headers
    }
}

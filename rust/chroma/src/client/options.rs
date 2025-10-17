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

#[derive(Debug, Clone)]
pub struct ChromaClientOptions {
    pub base_url: reqwest::Url,
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
            base_url: "https://api.trychroma.com".parse().unwrap(),
            auth_method: ChromaAuthMethod::None,
            retry_options: ChromaRetryOptions::default(),
            tenant_id: None,
            default_database_name: None,
        }
    }
}

impl ChromaClientOptions {
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

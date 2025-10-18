use reqwest::header::{HeaderMap, HeaderName, HeaderValue, InvalidHeaderValue};

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
    pub base_url: String,
    pub auth_method: ChromaAuthMethod,
    /// Will be automatically resolved at request time if not provided
    pub tenant_id: Option<String>,
    /// Will be automatically resolved at request time if not provided. It can only be resolved automatically if this client has access to exactly one database.
    pub default_database_id: Option<String>,
}

impl Default for ChromaClientOptions {
    fn default() -> Self {
        ChromaClientOptions {
            base_url: "https://api.trychroma.com".to_string(),
            auth_method: ChromaAuthMethod::None,
            tenant_id: None,
            default_database_id: None,
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

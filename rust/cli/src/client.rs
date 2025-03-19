use crate::utils::Profile;
use chroma_frontend::server::CreateDatabasePayload;
use chroma_types::{Database, ListDatabasesResponse};
use reqwest::{
    blocking::{Client, Response},
    header::{HeaderMap, HeaderValue},
    StatusCode,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;

#[derive(Debug)]
pub enum ChromaCliClientError {
    /// Network-related errors
    Network(reqwest::Error),
    /// Error during serialization or deserialization
    Serialization(serde_json::Error),
    /// API returned an error status code
    ApiError(StatusCode, String),
    /// Response body was expected but not present
    MissingResponseBody,
    /// Invalid API key format
    InvalidApiKey(String),
    /// Other unexpected errors
    Other(String),
}

impl fmt::Display for ChromaCliClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChromaCliClientError::Network(err) => write!(f, "Network error: {}", err),
            ChromaCliClientError::Serialization(err) => write!(f, "Serialization error: {}", err),
            ChromaCliClientError::ApiError(status, message) => {
                write!(f, "API error ({}): {}", status, message)
            }
            ChromaCliClientError::MissingResponseBody => {
                write!(f, "Expected response body but none found")
            }
            ChromaCliClientError::InvalidApiKey(msg) => write!(f, "Invalid API key: {}", msg),
            ChromaCliClientError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ChromaCliClientError {}

impl From<reqwest::Error> for ChromaCliClientError {
    fn from(err: reqwest::Error) -> Self {
        ChromaCliClientError::Network(err)
    }
}

impl From<serde_json::Error> for ChromaCliClientError {
    fn from(err: serde_json::Error) -> Self {
        ChromaCliClientError::Serialization(err)
    }
}

impl From<&str> for ChromaCliClientError {
    fn from(msg: &str) -> Self {
        ChromaCliClientError::Other(msg.to_string())
    }
}

impl From<String> for ChromaCliClientError {
    fn from(msg: String) -> Self {
        ChromaCliClientError::Other(msg)
    }
}

#[derive(Deserialize, Debug)]
struct EmptyResponse {}

#[derive(Debug, Clone, Copy)]
enum RequestMethod {
    Get,
    Post,
    Delete,
}

pub struct ChromaClient {
    client: Client,
    pub api_url: String,
    pub tenant_id: String,
    headers: HeaderMap,
}

pub trait ChromaClientTrait {
    fn list_databases(&self) -> Result<Vec<Database>, ChromaCliClientError>;
    fn create_database(&self, name: String) -> Result<(), ChromaCliClientError>;
    fn delete_database(&self, name: String) -> Result<(), ChromaCliClientError>;
}

impl ChromaClient {
    pub fn new(
        api_url: String,
        api_key: String,
        tenant_id: String,
    ) -> Result<Self, ChromaCliClientError> {
        let client = Client::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Chroma-Token",
            HeaderValue::from_str(&api_key)
                .map_err(|e| ChromaCliClientError::InvalidApiKey(e.to_string()))?,
        );

        Ok(Self {
            client,
            api_url,
            tenant_id,
            headers,
        })
    }

    pub fn from_profile(api_url: String, profile: &Profile) -> Result<Self, ChromaCliClientError> {
        Self::new(api_url, profile.api_key.clone(), profile.team_id.clone())
    }

    fn request<T: DeserializeOwned, U: Serialize + ?Sized>(
        &self,
        method: RequestMethod,
        route: &str,
        body: Option<&U>,
    ) -> Result<Option<T>, ChromaCliClientError> {
        let url = format!("{}{}", self.api_url, route);
        let mut headers = self.headers.clone();

        if let RequestMethod::Post = method {
            headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        }

        let response = match method {
            RequestMethod::Get => self.client.get(&url).headers(headers).send()?,
            RequestMethod::Post => {
                if let Some(data) = body {
                    self.client.post(&url).headers(headers).json(data).send()?
                } else {
                    return Err(ChromaCliClientError::Other(
                        "POST request requires a body".into(),
                    ));
                }
            }
            RequestMethod::Delete => {
                self.client.delete(&url).headers(headers).send()?;
                return Ok(None);
            }
        };

        self.process_response::<T>(response, method)
    }

    fn process_response<T: DeserializeOwned>(
        &self,
        response: Response,
        method: RequestMethod,
    ) -> Result<Option<T>, ChromaCliClientError> {
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .unwrap_or_else(|_| String::from("No error message"));
            return Err(ChromaCliClientError::ApiError(status, error_text));
        }

        match method {
            RequestMethod::Delete => Ok(None),
            _ => {
                // For GET and POST, we expect a response body
                let result: T = response.json()?;
                Ok(Some(result))
            }
        }
    }

    pub fn get<T: DeserializeOwned>(&self, route: &str) -> Result<T, ChromaCliClientError> {
        let result = self.request::<T, ()>(RequestMethod::Get, route, None)?;
        result.ok_or_else(|| ChromaCliClientError::MissingResponseBody)
    }

    pub fn post<T: DeserializeOwned, U: Serialize>(
        &self,
        route: &str,
        body: &U,
    ) -> Result<T, ChromaCliClientError> {
        let result = self.request::<T, U>(RequestMethod::Post, route, Some(body))?;
        result.ok_or_else(|| ChromaCliClientError::MissingResponseBody)
    }

    pub fn delete(&self, route: &str) -> Result<(), ChromaCliClientError> {
        self.request::<(), ()>(RequestMethod::Delete, route, None)?;
        Ok(())
    }
}

impl ChromaClientTrait for ChromaClient {
    fn list_databases(&self) -> Result<Vec<Database>, ChromaCliClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        let response = self.get::<ListDatabasesResponse>(&route)?;
        Ok(response)
    }

    fn create_database(&self, name: String) -> Result<(), ChromaCliClientError> {
        let route = format!("/api/v2/tenants/{}/databases", self.tenant_id);
        self.post::<EmptyResponse, _>(&route, &CreateDatabasePayload { name })?;
        Ok(())
    }

    fn delete_database(&self, name: String) -> Result<(), ChromaCliClientError> {
        let route = format!("/api/v2/tenants/{}/databases/{}", self.tenant_id, name);
        self.delete(&route)?;
        Ok(())
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_types::{AttachedFunction, Chunk, LogRecord};
use serde::Serialize;
use thiserror::Error;

use crate::execution::operators::execute_task::{AttachedFunctionExecutor, HydratedInputBatch};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const DEFAULT_DATABASE_NAME: &str = "FOUNDATION";

#[derive(Debug, Serialize)]
struct CurrentsRequest {
    tenant_id: String,
    database_id: String,
    database_name: String,
    wiki_collection: String,
    currents_collection: String,
    wiki_write_offset: u64,
}

#[derive(Debug)]
pub struct HttpCurrentsExecutor {
    endpoint_url: String,
    output_collection: String,
    database_name: String,
    modal_key: String,
    modal_secret: String,
    client: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum HttpCurrentsError {
    #[error("Missing required param: {0}")]
    MissingParam(String),
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),
    #[error("Invalid params JSON: {0}")]
    InvalidParamsJson(String),
    #[error("http_currents expects exactly one input batch, got {0}")]
    InvalidInputBatchCount(usize),
    #[error("HTTP error: {0}")]
    Http(String),
}

impl ChromaError for HttpCurrentsError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            HttpCurrentsError::MissingParam(_)
            | HttpCurrentsError::MissingEnvVar(_)
            | HttpCurrentsError::InvalidParamsJson(_)
            | HttpCurrentsError::InvalidInputBatchCount(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            HttpCurrentsError::Http(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

impl HttpCurrentsExecutor {
    fn validate_input_batch_count(batch_count: usize) -> Result<(), HttpCurrentsError> {
        if batch_count != 1 {
            return Err(HttpCurrentsError::InvalidInputBatchCount(batch_count));
        }

        Ok(())
    }

    pub fn from_attached_function(af: &AttachedFunction) -> Result<Self, Box<dyn ChromaError>> {
        let params_json = af.params.as_deref().unwrap_or("{}");
        let params: serde_json::Value = serde_json::from_str(params_json).map_err(|e| {
            Box::new(HttpCurrentsError::InvalidParamsJson(e.to_string())) as Box<dyn ChromaError>
        })?;

        let get_str = |key: &str| -> Result<String, Box<dyn ChromaError>> {
            params
                .get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    Box::new(HttpCurrentsError::MissingParam(key.into())) as Box<dyn ChromaError>
                })
        };

        let endpoint_url = get_str("endpoint_url")?;
        let database_name = params
            .get("database_name")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DATABASE_NAME)
            .to_string();

        let modal_key = std::env::var("MODAL_KEY").map_err(|_| {
            Box::new(HttpCurrentsError::MissingEnvVar("MODAL_KEY".into())) as Box<dyn ChromaError>
        })?;
        let modal_secret = std::env::var("MODAL_SECRET").map_err(|_| {
            Box::new(HttpCurrentsError::MissingEnvVar("MODAL_SECRET".into()))
                as Box<dyn ChromaError>
        })?;

        Ok(Self {
            endpoint_url,
            output_collection: af.output_collection_name.clone(),
            database_name,
            modal_key,
            modal_secret,
            client: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|e| {
                    Box::new(HttpCurrentsError::Http(format!(
                        "failed to build reqwest client: {e}"
                    ))) as Box<dyn ChromaError>
                })?,
        })
    }

    async fn refresh_currents(
        &self,
        request_body: &CurrentsRequest,
    ) -> Result<(), Box<dyn ChromaError>> {
        let currents_url = format!("{}/currents", self.endpoint_url.trim_end_matches('/'));

        let response = self
            .client
            .post(&currents_url)
            .header("Modal-Key", &self.modal_key)
            .header("Modal-Secret", &self.modal_secret)
            .json(request_body)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                Box::new(HttpCurrentsError::Http(format!(
                    "POST /currents failed: {e}"
                ))) as Box<dyn ChromaError>
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let truncated: String = body.chars().take(256).collect();
            tracing::error!(
                "[HttpCurrentsExecutor] POST /currents returned {}: {}",
                status,
                truncated,
            );
            return Err(Box::new(HttpCurrentsError::Http(format!(
                "POST /currents returned {status}"
            ))));
        }

        Ok(())
    }
}

#[async_trait]
impl AttachedFunctionExecutor for HttpCurrentsExecutor {
    async fn execute(
        &self,
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        _output_reader: Option<&chroma_segment::blockfile_record::RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        Self::validate_input_batch_count(input_batches.len())
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        let batch = &input_batches[0];
        let request_body = CurrentsRequest {
            tenant_id: batch.tenant_id.clone(),
            database_id: batch.database_id.clone(),
            database_name: self.database_name.clone(),
            wiki_collection: batch.input_collection_name.clone(),
            currents_collection: self.output_collection.clone(),
            wiki_write_offset: batch.completion_offset,
        };

        tracing::info!(
            "[HttpCurrentsExecutor] Refreshing currents for wiki={} currents={} offset={} via {}",
            request_body.wiki_collection,
            request_body.currents_collection,
            request_body.wiki_write_offset,
            self.endpoint_url,
        );

        self.refresh_currents(&request_body).await?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

#[cfg(test)]
mod tests {
    use super::{CurrentsRequest, HttpCurrentsError, HttpCurrentsExecutor, DEFAULT_DATABASE_NAME};

    #[test]
    fn currents_request_uses_foundation_default_database_name() {
        let request = CurrentsRequest {
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            database_name: DEFAULT_DATABASE_NAME.to_string(),
            wiki_collection: "wiki".to_string(),
            currents_collection: "currents".to_string(),
            wiki_write_offset: 42,
        };

        assert_eq!(request.database_name, "FOUNDATION");
        assert_eq!(request.wiki_write_offset, 42);
    }

    #[test]
    fn validate_input_batch_count_rejects_zero_or_many_batches() {
        let err = HttpCurrentsExecutor::validate_input_batch_count(0).unwrap_err();
        assert!(matches!(err, HttpCurrentsError::InvalidInputBatchCount(0)));

        let err = HttpCurrentsExecutor::validate_input_batch_count(2).unwrap_err();
        assert!(matches!(err, HttpCurrentsError::InvalidInputBatchCount(2)));

        HttpCurrentsExecutor::validate_input_batch_count(1).unwrap();
    }
}

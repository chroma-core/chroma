use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_types::{AttachedFunction, Chunk, LogRecord};
use serde::Serialize;
use thiserror::Error;

use crate::execution::operators::execute_task::{AttachedFunctionExecutor, HydratedInputBatch};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Serialize)]
struct CurrentsRequest {
    tenant_id: String,
    database_id: String,
    wiki_collection: String,
    currents_collection: String,
    wiki_write_offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_refresh_offset_delta: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tuning_params: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct HttpCurrentsExecutor {
    endpoint_url: String,
    wiki_collection: String,
    currents_collection: String,
    modal_key: String,
    modal_secret: String,
    min_refresh_offset_delta: Option<u64>,
    tuning_params: Option<serde_json::Value>,
    client: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum HttpCurrentsError {
    #[error("Missing required param: {0}")]
    MissingParam(String),
    #[error("Invalid param: {0}")]
    InvalidParam(String),
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("No input batches available for currents refresh")]
    MissingInput,
}

impl ChromaError for HttpCurrentsError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            HttpCurrentsError::MissingParam(_)
            | HttpCurrentsError::InvalidParam(_)
            | HttpCurrentsError::MissingEnvVar(_)
            | HttpCurrentsError::MissingInput => chroma_error::ErrorCodes::InvalidArgument,
            HttpCurrentsError::Http(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

impl HttpCurrentsExecutor {
    pub fn from_attached_function(af: &AttachedFunction) -> Result<Self, Box<dyn ChromaError>> {
        let params_json = af.params.as_deref().unwrap_or("{}");
        let params: serde_json::Value = serde_json::from_str(params_json).map_err(|e| {
            Box::new(HttpCurrentsError::Http(format!("invalid params JSON: {e}")))
                as Box<dyn ChromaError>
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

        let get_u64_opt = |key: &str| -> Result<Option<u64>, Box<dyn ChromaError>> {
            match params.get(key) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(value) => value.as_u64().map(Some).ok_or_else(|| {
                    Box::new(HttpCurrentsError::InvalidParam(format!(
                        "{key} must be an unsigned integer"
                    ))) as Box<dyn ChromaError>
                }),
            }
        };

        let endpoint_url = get_str("endpoint_url")?;
        let wiki_collection = get_str("wiki_collection")?;
        let min_refresh_offset_delta = get_u64_opt("min_refresh_offset_delta")?;
        let tuning_params = params.get("tuning_params").cloned();

        let modal_key = std::env::var("MODAL_KEY").map_err(|_| {
            Box::new(HttpCurrentsError::MissingEnvVar("MODAL_KEY".into())) as Box<dyn ChromaError>
        })?;
        let modal_secret = std::env::var("MODAL_SECRET").map_err(|_| {
            Box::new(HttpCurrentsError::MissingEnvVar("MODAL_SECRET".into()))
                as Box<dyn ChromaError>
        })?;

        Ok(Self {
            endpoint_url,
            wiki_collection,
            currents_collection: af.output_collection_name.clone(),
            modal_key,
            modal_secret,
            min_refresh_offset_delta,
            tuning_params,
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
        let total_records = input_batches
            .iter()
            .map(|batch| batch.records.len())
            .sum::<usize>();
        if total_records == 0 {
            tracing::info!("[HttpCurrentsExecutor] No input records to process");
            return Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())));
        }

        let first_batch = input_batches
            .first()
            .ok_or_else(|| Box::new(HttpCurrentsError::MissingInput) as Box<dyn ChromaError>)?;
        let wiki_write_offset = input_batches
            .iter()
            .map(|batch| batch.completion_offset)
            .max()
            .ok_or_else(|| Box::new(HttpCurrentsError::MissingInput) as Box<dyn ChromaError>)?;

        let request_body = CurrentsRequest {
            tenant_id: first_batch.tenant_id.clone(),
            database_id: first_batch.database_id.clone(),
            wiki_collection: self.wiki_collection.clone(),
            currents_collection: self.currents_collection.clone(),
            wiki_write_offset,
            min_refresh_offset_delta: self.min_refresh_offset_delta,
            tuning_params: self.tuning_params.clone(),
        };

        tracing::info!(
            "[HttpCurrentsExecutor] Refreshing currents for wiki={} into {} at offset {}",
            self.wiki_collection,
            self.currents_collection,
            wiki_write_offset,
        );

        self.refresh_currents(&request_body).await?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

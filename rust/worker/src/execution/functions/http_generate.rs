use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_types::{AttachedFunction, Chunk, LogRecord, MaterializedLogOperation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution::operators::execute_task::AttachedFunctionExecutor;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_INITIAL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const POLL_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

#[derive(Debug, Serialize)]
struct GenerateRecord {
    id: String,
    document: String,
    metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct GenerateRecordSet {
    source_collection: String,
    source_kind: String,
    output_collection: String,
    base_collection: Option<String>,
    records: Vec<GenerateRecord>,
    completion_offset: u64,
}

#[derive(Debug, Serialize)]
struct GenerateRequest {
    record_set: GenerateRecordSet,
}

#[derive(Debug, Deserialize)]
struct SpawnResponse {
    call_id: String,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    status: String,
    error: Option<String>,
}

#[derive(Debug)]
pub struct HttpGenerateExecutor {
    endpoint_url: String,
    source_collection: String,
    source_kind: String,
    output_collection: String,
    modal_key: String,
    modal_secret: String,
    client: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum HttpGenerateError {
    #[error("Missing required param: {0}")]
    MissingParam(String),
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("Generation failed: {0}")]
    GenerationFailed(String),
    #[error("Poll timeout after {0:?}")]
    PollTimeout(std::time::Duration),
}

impl ChromaError for HttpGenerateError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            HttpGenerateError::MissingParam(_) | HttpGenerateError::MissingEnvVar(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            _ => chroma_error::ErrorCodes::Internal,
        }
    }
}

impl HttpGenerateExecutor {
    /// Build from an `AttachedFunction`.
    ///
    /// Reads `endpoint_url`, `source_collection`, and `source_kind` from
    /// params JSON.  Modal proxy-auth tokens come from env vars
    /// `MODAL_KEY` and `MODAL_SECRET`.
    pub fn from_attached_function(af: &AttachedFunction) -> Result<Self, Box<dyn ChromaError>> {
        let params_json = af.params.as_deref().unwrap_or("{}");
        let params: serde_json::Value = serde_json::from_str(params_json).map_err(|e| {
            Box::new(HttpGenerateError::Http(format!("invalid params JSON: {e}")))
                as Box<dyn ChromaError>
        })?;

        let get_str = |key: &str| -> Result<String, Box<dyn ChromaError>> {
            params
                .get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    Box::new(HttpGenerateError::MissingParam(key.into())) as Box<dyn ChromaError>
                })
        };

        let endpoint_url = get_str("endpoint_url")?;
        let source_collection = get_str("source_collection")?;
        let source_kind = get_str("source_kind")?;

        let modal_key = std::env::var("MODAL_KEY").map_err(|_| {
            Box::new(HttpGenerateError::MissingEnvVar("MODAL_KEY".into())) as Box<dyn ChromaError>
        })?;
        let modal_secret = std::env::var("MODAL_SECRET").map_err(|_| {
            Box::new(HttpGenerateError::MissingEnvVar("MODAL_SECRET".into()))
                as Box<dyn ChromaError>
        })?;

        Ok(Self {
            endpoint_url,
            source_collection,
            source_kind,
            output_collection: af.output_collection_name.clone(),
            modal_key,
            modal_secret,
            client: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .unwrap_or_default(),
        })
    }

    /// POST /generate → get call_id back.
    async fn spawn_generation(
        &self,
        request_body: &GenerateRequest,
    ) -> Result<String, Box<dyn ChromaError>> {
        let generate_url = format!("{}/generate", self.endpoint_url.trim_end_matches('/'));

        let response = self
            .client
            .post(&generate_url)
            .header("Modal-Key", &self.modal_key)
            .header("Modal-Secret", &self.modal_secret)
            .json(request_body)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| {
                Box::new(HttpGenerateError::Http(format!(
                    "POST /generate failed: {e}"
                ))) as Box<dyn ChromaError>
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let truncated: String = body.chars().take(256).collect();
            tracing::error!(
                "[HttpGenerateExecutor] POST /generate returned {}: {}",
                status,
                truncated,
            );
            return Err(Box::new(HttpGenerateError::Http(format!(
                "POST /generate returned {status}"
            ))));
        }

        let spawn_resp: SpawnResponse = response.json().await.map_err(|e| {
            Box::new(HttpGenerateError::Http(format!(
                "failed to parse spawn response: {e}"
            ))) as Box<dyn ChromaError>
        })?;

        Ok(spawn_resp.call_id)
    }

    /// GET /status/{call_id} in a loop with exponential backoff until
    /// the job completes or fails.
    async fn poll_until_done(&self, call_id: &str) -> Result<(), Box<dyn ChromaError>> {
        let status_url = format!(
            "{}/status/{}",
            self.endpoint_url.trim_end_matches('/'),
            call_id
        );
        let start = std::time::Instant::now();
        let mut interval = POLL_INITIAL_INTERVAL;

        loop {
            if start.elapsed() > POLL_TIMEOUT {
                return Err(Box::new(HttpGenerateError::PollTimeout(POLL_TIMEOUT)));
            }

            tokio::time::sleep(interval).await;
            interval = std::cmp::min(interval * 2, POLL_MAX_INTERVAL);

            let response = self
                .client
                .get(&status_url)
                .header("Modal-Key", &self.modal_key)
                .header("Modal-Secret", &self.modal_secret)
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await;

            let response = match response {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("[HttpGenerateExecutor] Poll request failed (will retry): {e}");
                    continue;
                }
            };

            if !response.status().is_success() {
                tracing::warn!(
                    "[HttpGenerateExecutor] Poll returned HTTP {} (will retry)",
                    response.status()
                );
                continue;
            }

            let status_resp: StatusResponse = match response.json().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        "[HttpGenerateExecutor] Failed to parse poll response (will retry): {e}"
                    );
                    continue;
                }
            };

            match status_resp.status.as_str() {
                "pending" => {
                    tracing::debug!(
                        "[HttpGenerateExecutor] Job {} still pending ({:.0}s elapsed)",
                        call_id,
                        start.elapsed().as_secs_f64(),
                    );
                }
                "complete" => {
                    tracing::info!(
                        "[HttpGenerateExecutor] Job {} completed after {:.0}s",
                        call_id,
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(());
                }
                "failed" => {
                    let msg = status_resp.error.unwrap_or_else(|| "unknown error".into());
                    return Err(Box::new(HttpGenerateError::GenerationFailed(msg)));
                }
                other => {
                    tracing::warn!(
                        "[HttpGenerateExecutor] Unexpected status {other:?}, treating as pending"
                    );
                }
            }
        }
    }
}

fn metadata_value_to_json(value: &chroma_types::MetadataValue) -> serde_json::Value {
    match value {
        chroma_types::MetadataValue::Bool(b) => serde_json::Value::Bool(*b),
        chroma_types::MetadataValue::Int(i) => serde_json::json!(*i),
        chroma_types::MetadataValue::Float(f) => serde_json::json!(*f),
        chroma_types::MetadataValue::Str(s) => serde_json::Value::String(s.clone()),
        _ => serde_json::Value::Null,
    }
}

#[async_trait]
impl AttachedFunctionExecutor for HttpGenerateExecutor {
    async fn execute(
        &self,
        input_records: Chunk<HydratedMaterializedLogRecord<'_, '_>>,
        _output_reader: Option<&chroma_segment::blockfile_record::RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let mut records = Vec::new();

        for (record, _) in input_records.iter() {
            if record.get_operation() == MaterializedLogOperation::DeleteExisting {
                continue;
            }

            let id = record.get_user_id().to_string();
            let document = record.merged_document_ref().unwrap_or("").to_string();
            let metadata: HashMap<String, serde_json::Value> = record
                .merged_metadata()
                .into_iter()
                .map(|(k, v)| (k, metadata_value_to_json(&v)))
                .collect();

            records.push(GenerateRecord {
                id,
                document,
                metadata,
            });
        }

        if records.is_empty() {
            tracing::info!("[HttpGenerateExecutor] No non-delete records to process");
            return Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())));
        }

        let num_records = records.len();
        let request_body = GenerateRequest {
            record_set: GenerateRecordSet {
                source_collection: self.source_collection.clone(),
                source_kind: self.source_kind.clone(),
                output_collection: self.output_collection.clone(),
                base_collection: None,
                records,
                // TODO: Remove completion offset from the schema of this request
                completion_offset: 0,
            },
        };

        tracing::info!(
            "[HttpGenerateExecutor] Spawning generation for {} records via {}",
            num_records,
            self.endpoint_url,
        );

        // 1. POST /generate → get call_id
        let call_id = self.spawn_generation(&request_body).await?;
        tracing::info!(
            "[HttpGenerateExecutor] Job spawned with call_id={call_id}, polling for completion"
        );

        // 2. Poll GET /status/{call_id} until done
        self.poll_until_done(&call_id).await?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

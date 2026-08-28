use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_types::{AttachedFunction, Chunk, LogRecord, MaterializedLogOperation};
use frontend_core::foundation::source_kind_for_collection_name;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution::functions::trace_headers::current_trace_headers;
use crate::execution::operators::execute_task::{AttachedFunctionExecutor, HydratedInputBatch};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_INITIAL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const POLL_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);
/// Keep individual JSON requests well below endpoint and proxy body limits.
pub(crate) const MAX_GENERATE_REQUEST_BYTES: usize = 16 * 1024 * 1024;
/// Bound the work handed to a single generation job unless the attached
/// function overrides it with its `batch_size` parameter.
const DEFAULT_GENERATE_BATCH_SIZE: usize = 500_000;

#[derive(Clone, Debug, Serialize)]
struct GenerateRecord {
    id: String,
    document: String,
    metadata: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize)]
struct GenerateRecordSet {
    tenant_id: String,
    database_id: String,
    source_collection: String,
    source_kind: String,
    output_collection: String,
    base_collection: Option<String>,
    records: Vec<GenerateRecord>,
    completion_offset: u64,
}

#[derive(Clone, Debug, Serialize)]
struct GenerateRequest {
    record_sets: Vec<GenerateRecordSet>,
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
    output_collection: String,
    modal_key: String,
    modal_secret: String,
    batch_size: usize,
    client: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum HttpGenerateError {
    #[error("Missing required param: {0}")]
    MissingParam(String),
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),
    #[error("Invalid batch_size: must be a positive integer")]
    InvalidBatchSize,
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
            HttpGenerateError::MissingParam(_)
            | HttpGenerateError::MissingEnvVar(_)
            | HttpGenerateError::InvalidBatchSize => chroma_error::ErrorCodes::InvalidArgument,
            _ => chroma_error::ErrorCodes::Internal,
        }
    }
}

impl HttpGenerateExecutor {
    /// Build from an `AttachedFunction`.
    ///
    /// Reads `endpoint_url` from params JSON. Modal proxy-auth tokens come from env vars
    /// `MODAL_KEY` and `MODAL_SECRET`.
    pub fn from_attached_function(af: &AttachedFunction) -> Result<Self, Box<dyn ChromaError>> {
        let params = Self::params_from_attached_function(af)
            .map_err(|error| Box::new(error) as Box<dyn ChromaError>)?;

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
        let batch_size = Self::batch_size_from_params(&params)
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
        tracing::info!(
            attached_function_id = %af.id,
            batch_size,
            "[HttpGenerateExecutor] Using configured batch size"
        );

        let modal_key = std::env::var("MODAL_KEY").map_err(|_| {
            Box::new(HttpGenerateError::MissingEnvVar("MODAL_KEY".into())) as Box<dyn ChromaError>
        })?;
        let modal_secret = std::env::var("MODAL_SECRET").map_err(|_| {
            Box::new(HttpGenerateError::MissingEnvVar("MODAL_SECRET".into()))
                as Box<dyn ChromaError>
        })?;

        Ok(Self {
            endpoint_url,
            output_collection: af.output_collection_name.clone(),
            modal_key,
            modal_secret,
            batch_size,
            client: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|e| {
                    Box::new(HttpGenerateError::Http(format!(
                        "failed to build reqwest client: {e}"
                    ))) as Box<dyn ChromaError>
                })?,
        })
    }

    pub(crate) fn configured_batch_size(af: &AttachedFunction) -> Result<usize, HttpGenerateError> {
        Self::batch_size_from_params(&Self::params_from_attached_function(af)?)
    }

    fn params_from_attached_function(
        af: &AttachedFunction,
    ) -> Result<serde_json::Value, HttpGenerateError> {
        serde_json::from_str(af.params.as_deref().unwrap_or("{}"))
            .map_err(|error| HttpGenerateError::Http(format!("invalid params JSON: {error}")))
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
            .headers(current_trace_headers())
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

    fn serialized_json_size<T: Serialize>(value: &T) -> Result<usize, HttpGenerateError> {
        serde_json::to_vec(value)
            .map(|json| json.len())
            .map_err(|e| {
                HttpGenerateError::Http(format!("failed to serialize generate request: {e}"))
            })
    }

    fn batch_size_from_params(params: &serde_json::Value) -> Result<usize, HttpGenerateError> {
        let Some(batch_size) = params.get("batch_size") else {
            return Ok(DEFAULT_GENERATE_BATCH_SIZE);
        };

        batch_size
            .as_u64()
            .and_then(|batch_size| usize::try_from(batch_size).ok())
            .filter(|batch_size| *batch_size > 0)
            .ok_or(HttpGenerateError::InvalidBatchSize)
    }

    /// Builds the next request without retaining earlier or later requests.
    fn next_request<I>(
        records: &mut I,
        pending_record: &mut Option<GenerateRecord>,
        template: &GenerateRecordSet,
        max_records_per_request: usize,
    ) -> Result<Option<GenerateRequest>, HttpGenerateError>
    where
        I: Iterator<Item = GenerateRecord>,
    {
        let empty_request = GenerateRequest {
            record_sets: vec![template.clone()],
        };
        let mut batch_size = Self::serialized_json_size(&empty_request)?;
        let mut batch = template.clone();

        loop {
            let Some(record) = pending_record.take().or_else(|| records.next()) else {
                return Ok((!batch.records.is_empty()).then_some(GenerateRequest {
                    record_sets: vec![batch],
                }));
            };
            let record_size = Self::serialized_json_size(&record)?;
            let separator_size = usize::from(!batch.records.is_empty());

            if !batch.records.is_empty()
                && (batch.records.len() >= max_records_per_request
                    || batch_size
                        .saturating_add(separator_size)
                        .saturating_add(record_size)
                        > MAX_GENERATE_REQUEST_BYTES)
            {
                *pending_record = Some(record);
                return Ok(Some(GenerateRequest {
                    record_sets: vec![batch],
                }));
            }

            if batch.records.is_empty()
                && batch_size.saturating_add(record_size) > MAX_GENERATE_REQUEST_BYTES
            {
                tracing::warn!(
                    record_size,
                    max_request_bytes = MAX_GENERATE_REQUEST_BYTES,
                    "A single generate record exceeds the request size limit; sending it alone"
                );
            }

            batch_size = batch_size
                .saturating_add(separator_size)
                .saturating_add(record_size);
            batch.records.push(record);
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
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        _output_reader: Option<&chroma_segment::blockfile_record::RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let mut request_count = 0usize;
        let mut total_records = 0usize;
        for batch in &input_batches {
            let template = GenerateRecordSet {
                tenant_id: batch.tenant_id.clone(),
                database_id: batch.database_id.clone(),
                source_collection: batch.input_collection_name.clone(),
                source_kind: source_kind_for_collection_name(&batch.input_collection_name)
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?
                    .to_string(),
                output_collection: self.output_collection.clone(),
                base_collection: None,
                records: Vec::new(),
                completion_offset: batch.pulled_log_offset,
            };
            let mut records = batch
                .records
                .iter()
                .filter(|(record, _)| {
                    record.get_operation() != MaterializedLogOperation::DeleteExisting
                })
                .map(|(record, _)| GenerateRecord {
                    id: record.get_user_id().to_string(),
                    document: record.merged_document_ref().unwrap_or("").to_string(),
                    metadata: record
                        .merged_metadata()
                        .into_iter()
                        .map(|(key, value)| (key, metadata_value_to_json(&value)))
                        .collect(),
                });
            let mut pending_record = None;

            while let Some(request_body) = Self::next_request(
                &mut records,
                &mut pending_record,
                &template,
                self.batch_size,
            )
            .map_err(|error| Box::new(error) as Box<dyn ChromaError>)?
            {
                let batch_records = request_body.record_sets[0].records.len();
                total_records = total_records.saturating_add(batch_records);
                tracing::info!(
                    request_index = request_count,
                    batch_records,
                    "[HttpGenerateExecutor] Spawning generation batch"
                );

                let call_id = self.spawn_generation(&request_body).await?;
                drop(request_body);
                self.poll_until_done(&call_id).await?;
                request_count = request_count.saturating_add(1);
            }
        }

        tracing::info!(
            total_records,
            request_count,
            endpoint = %self.endpoint_url,
            "[HttpGenerateExecutor] Finished generation requests"
        );

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GenerateRecord, GenerateRecordSet, HttpGenerateError, HttpGenerateExecutor,
        DEFAULT_GENERATE_BATCH_SIZE, MAX_GENERATE_REQUEST_BYTES,
    };
    use frontend_core::foundation::source_kind_for_collection_name;
    use std::collections::HashMap;

    #[test]
    fn generate_record_set_carries_canonical_source_kind() {
        let record_set = GenerateRecordSet {
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            source_collection: "slack_master".to_string(),
            source_kind: source_kind_for_collection_name("slack_master")
                .unwrap()
                .to_string(),
            output_collection: "wiki".to_string(),
            base_collection: None,
            records: Vec::new(),
            completion_offset: 0,
        };

        assert_eq!(record_set.source_kind, "slack");
        assert_eq!(record_set.source_collection, "slack_master");
    }

    fn record_set_with_documents(documents: Vec<String>) -> GenerateRecordSet {
        GenerateRecordSet {
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            source_collection: "slack_master".to_string(),
            source_kind: "slack".to_string(),
            output_collection: "wiki".to_string(),
            base_collection: None,
            records: documents
                .into_iter()
                .enumerate()
                .map(|(index, document)| GenerateRecord {
                    id: index.to_string(),
                    document,
                    metadata: HashMap::new(),
                })
                .collect(),
            completion_offset: 42,
        }
    }

    #[test]
    fn generate_requests_are_batched_by_serialized_json_size() {
        let document = "x".repeat(MAX_GENERATE_REQUEST_BYTES / 2);
        let record_set = record_set_with_documents(vec![document.clone(), document]);
        let template = GenerateRecordSet {
            records: Vec::new(),
            ..record_set.clone()
        };
        let mut records = record_set.records.into_iter();
        let mut pending = None;
        let mut requests = Vec::new();
        while let Some(request) = HttpGenerateExecutor::next_request(
            &mut records,
            &mut pending,
            &template,
            DEFAULT_GENERATE_BATCH_SIZE,
        )
        .unwrap()
        {
            requests.push(request);
        }

        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| {
            HttpGenerateExecutor::serialized_json_size(request).unwrap()
                <= MAX_GENERATE_REQUEST_BYTES
        }));
    }

    #[test]
    fn generate_requests_are_batched_by_record_count() {
        let record_set = record_set_with_documents(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
        ]);
        let template = GenerateRecordSet {
            records: Vec::new(),
            ..record_set.clone()
        };
        let mut records = record_set.records.into_iter();
        let mut pending = None;
        let mut requests = Vec::new();
        while let Some(request) =
            HttpGenerateExecutor::next_request(&mut records, &mut pending, &template, 2).unwrap()
        {
            requests.push(request);
        }

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].record_sets[0].records.len(), 2);
        assert_eq!(requests[1].record_sets[0].records.len(), 1);
    }

    #[test]
    fn a_single_oversized_record_is_still_sent_alone() {
        let record_set = record_set_with_documents(vec!["x".repeat(MAX_GENERATE_REQUEST_BYTES)]);
        let template = GenerateRecordSet {
            records: Vec::new(),
            ..record_set.clone()
        };
        let mut records = record_set.records.into_iter();
        let mut pending = None;

        let request = HttpGenerateExecutor::next_request(
            &mut records,
            &mut pending,
            &template,
            DEFAULT_GENERATE_BATCH_SIZE,
        )
        .unwrap()
        .unwrap();

        assert_eq!(request.record_sets[0].records.len(), 1);
        assert!(
            HttpGenerateExecutor::serialized_json_size(&request).unwrap()
                > MAX_GENERATE_REQUEST_BYTES
        );
        assert!(HttpGenerateExecutor::next_request(
            &mut records,
            &mut pending,
            &template,
            DEFAULT_GENERATE_BATCH_SIZE,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn generate_batch_size_defaults_and_rejects_invalid_values() {
        assert_eq!(
            HttpGenerateExecutor::batch_size_from_params(&serde_json::json!({})).unwrap(),
            DEFAULT_GENERATE_BATCH_SIZE
        );
        assert_eq!(
            HttpGenerateExecutor::batch_size_from_params(&serde_json::json!({"batch_size": 42}))
                .unwrap(),
            42
        );
        assert!(matches!(
            HttpGenerateExecutor::batch_size_from_params(&serde_json::json!({"batch_size": 42.0})),
            Err(HttpGenerateError::InvalidBatchSize)
        ));
        assert!(matches!(
            HttpGenerateExecutor::batch_size_from_params(&serde_json::json!({"batch_size": 0})),
            Err(HttpGenerateError::InvalidBatchSize)
        ));
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_types::{AttachedFunction, Chunk, LogRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution::functions::trace_headers::current_trace_headers;
use crate::execution::operators::execute_task::{AttachedFunctionExecutor, HydratedInputBatch};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_INITIAL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const POLL_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

#[derive(Debug, Serialize)]
struct CurrentsRequest {
    tenant_id: String,
    database_id: String,
    database_name: String,
    wiki_collection: String,
    currents_collection: String,
    wiki_write_offset: u64,
}

#[derive(Debug, Deserialize)]
struct CurrentsResponse {
    call_id: String,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    status: String,
    error: Option<String>,
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
    #[error("Currents refresh failed: {0}")]
    CurrentsFailed(String),
    #[error("Poll timeout after {0:?}")]
    PollTimeout(std::time::Duration),
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
            HttpCurrentsError::Http(_)
            | HttpCurrentsError::CurrentsFailed(_)
            | HttpCurrentsError::PollTimeout(_) => chroma_error::ErrorCodes::Internal,
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
        let params_json = af.params.as_deref().ok_or_else(|| {
            Box::new(HttpCurrentsError::MissingParam("params".into())) as Box<dyn ChromaError>
        })?;
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
        let database_name = get_str("database_name")?;

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
    ) -> Result<String, Box<dyn ChromaError>> {
        let currents_url = format!("{}/currents", self.endpoint_url.trim_end_matches('/'));

        let response = self
            .client
            .post(&currents_url)
            .headers(current_trace_headers())
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

        let body = response.text().await.map_err(|e| {
            Box::new(HttpCurrentsError::Http(format!(
                "failed to read currents response: {e}"
            ))) as Box<dyn ChromaError>
        })?;
        let currents_resp: CurrentsResponse = serde_json::from_str(&body).map_err(|e| {
            Box::new(HttpCurrentsError::Http(format!(
                "failed to parse currents response: {e}"
            ))) as Box<dyn ChromaError>
        })?;

        Ok(currents_resp.call_id)
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
                return Err(Box::new(HttpCurrentsError::PollTimeout(POLL_TIMEOUT)));
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
                    tracing::warn!("[HttpCurrentsExecutor] Poll request failed (will retry): {e}");
                    continue;
                }
            };

            if !response.status().is_success() {
                tracing::warn!(
                    "[HttpCurrentsExecutor] Poll returned HTTP {} (will retry)",
                    response.status()
                );
                continue;
            }

            let status_resp: StatusResponse = match response.json().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        "[HttpCurrentsExecutor] Failed to parse poll response (will retry): {e}"
                    );
                    continue;
                }
            };

            match status_resp.status.as_str() {
                "pending" => {
                    tracing::debug!(
                        "[HttpCurrentsExecutor] Job {} still pending ({:.0}s elapsed)",
                        call_id,
                        start.elapsed().as_secs_f64(),
                    );
                }
                "complete" => {
                    tracing::info!(
                        "[HttpCurrentsExecutor] Job {} completed after {:.0}s",
                        call_id,
                        start.elapsed().as_secs_f64(),
                    );
                    return Ok(());
                }
                "failed" => {
                    let msg = status_resp.error.unwrap_or_else(|| "unknown error".into());
                    return Err(Box::new(HttpCurrentsError::CurrentsFailed(msg)));
                }
                other => {
                    tracing::warn!(
                        "[HttpCurrentsExecutor] Unexpected status {other:?}, treating as pending"
                    );
                }
            }
        }
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

        let call_id = self.refresh_currents(&request_body).await?;
        tracing::info!(
            "[HttpCurrentsExecutor] Job spawned with call_id={call_id}, polling for completion"
        );
        self.poll_until_done(&call_id).await?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

#[cfg(test)]
mod tests {
    use super::{CurrentsRequest, CurrentsResponse, HttpCurrentsError, HttpCurrentsExecutor};
    use chroma_types::{AttachedFunction, AttachedFunctionUuid, CollectionUuid};
    use std::time::SystemTime;

    fn test_attached_function(params: Option<String>) -> AttachedFunction {
        AttachedFunction {
            id: AttachedFunctionUuid::new(),
            name: "test-currents".to_string(),
            function_id: uuid::Uuid::new_v4(),
            input_collection_id: CollectionUuid::new(),
            output_collection_name: "currents".to_string(),
            output_collection_id: None,
            params,
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            last_run: None,
            completion_offset: 0,
            min_records_for_invocation: 1,
            is_deleted: false,
            is_async: true,
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn validate_input_batch_count_rejects_zero_or_many_batches() {
        let err = HttpCurrentsExecutor::validate_input_batch_count(0).unwrap_err();
        assert!(matches!(err, HttpCurrentsError::InvalidInputBatchCount(0)));

        let err = HttpCurrentsExecutor::validate_input_batch_count(2).unwrap_err();
        assert!(matches!(err, HttpCurrentsError::InvalidInputBatchCount(2)));

        HttpCurrentsExecutor::validate_input_batch_count(1).unwrap();
    }

    #[test]
    fn from_attached_function_requires_params() {
        let err = HttpCurrentsExecutor::from_attached_function(&test_attached_function(None))
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            HttpCurrentsError::MissingParam("params".to_string()).to_string()
        );
    }

    #[test]
    fn from_attached_function_requires_database_name_param() {
        let params = Some(r#"{"endpoint_url":"https://example.test"}"#.to_string());
        let err = HttpCurrentsExecutor::from_attached_function(&test_attached_function(params))
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            HttpCurrentsError::MissingParam("database_name".to_string()).to_string()
        );
    }

    #[test]
    fn currents_request_carries_database_name_from_params() {
        let request = CurrentsRequest {
            tenant_id: "tenant".to_string(),
            database_id: "database".to_string(),
            database_name: "FOUNDATION".to_string(),
            wiki_collection: "wiki".to_string(),
            currents_collection: "currents".to_string(),
            wiki_write_offset: 42,
        };

        assert_eq!(request.database_name, "FOUNDATION");
        assert_eq!(request.wiki_write_offset, 42);
    }

    #[test]
    fn currents_response_requires_call_id() {
        let response: CurrentsResponse = serde_json::from_str(r#"{"call_id":"currents-123"}"#)
            .expect("call_id response should parse");
        assert_eq!(response.call_id, "currents-123");

        let err = serde_json::from_str::<CurrentsResponse>(r#"{}"#).unwrap_err();
        assert!(err.to_string().contains("missing field `call_id`"));
    }
}

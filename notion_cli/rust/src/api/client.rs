//! `NotionInternal`: tiny client over `https://www.notion.so/api/v3`.
//!
//! Mirrors the methods used by the daemon path of the Python script:
//! `loadUserContent`, `getSpaces`, `syncRecordValues`, `search`,
//! `enqueueExportBlock`, `getTasks`. Adds the rate-limit gate + token
//! bucket on every call.

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, COOKIE, USER_AGENT};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

use crate::token::Tokens;
use super::rate_limit::{
    default_initial_backoff_s, RateLimitGate, RateLimitedError, TokenBucket,
};

const API_BASE: &str = "https://www.notion.so/api/v3";
pub const USER_AGENT_STR: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

#[derive(Debug, Clone)]
pub struct NotionInternal {
    pub http: Client,
    pub tokens: Arc<Tokens>,
    pub bucket: TokenBucket,
    pub gate: RateLimitGate,
}

impl NotionInternal {
    pub fn new(tokens: Tokens, bucket: TokenBucket, gate: RateLimitGate) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(USER_AGENT_STR)
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            http,
            tokens: Arc::new(tokens),
            bucket,
            gate,
        })
    }

    fn api_headers(&self) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        h.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_STR));
        h.insert("Content-Type", HeaderValue::from_static("application/json"));
        h.insert("Notion-Audit-Log-Platform", HeaderValue::from_static("web"));
        h.insert("Notion-Client-Version", HeaderValue::from_static("23.13.0.1714"));
        let cookie = self.tokens.cookie_header_for_api();
        h.insert(COOKIE, HeaderValue::from_str(&cookie).context("cookie header")?);
        Ok(h)
    }

    /// Core POST `/api/v3/<endpoint>` with JSON body. Handles 429s by
    /// raising `RateLimitedError` (which the gate also captures).
    pub async fn post(&self, endpoint: &str, body: Value) -> Result<Value> {
        self.gate.wait_if_open().await;
        self.bucket.take().await;
        let url = format!("{API_BASE}/{endpoint}");
        let resp = self
            .http
            .post(&url)
            .headers(self.api_headers()?)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let body_text = resp.text().await.unwrap_or_default();
            let err = RateLimitedError::from_response_parts(
                retry_after.as_deref(),
                body_text,
                default_initial_backoff_s(),
            );
            self.gate.trip(err.retry_after);
            return Err(err.into());
        }
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("HTTP {status} on {endpoint}: {body_text}");
        }
        let v: Value = resp.json().await.with_context(|| format!("decoding {endpoint}"))?;
        Ok(v)
    }

    #[allow(dead_code)]
    pub async fn load_user_content(&self) -> Result<Value> {
        self.post("loadUserContent", json!({})).await
    }

    #[allow(dead_code)]
    pub async fn get_spaces(&self) -> Result<Value> {
        self.post("getSpaces", json!({})).await
    }

    pub async fn sync_record_values(&self, requests: Vec<Value>) -> Result<Value> {
        self.post("syncRecordValues", json!({ "requests": requests })).await
    }

    /// Mirror of Python `client.search(space_id, query, limit, variant)`.
    /// `variant = "minimal"` keeps the response small (only the bits we
    /// actually walk) and is much faster than the legacy variant.
    pub async fn search(
        &self,
        space_id: &str,
        query: &str,
        limit: u32,
        variant: &str,
    ) -> Result<Value> {
        let body = match variant {
            "minimal" => json!({
                "type": "BlocksInSpace",
                "query": query,
                "spaceId": space_id,
                "limit": limit,
                "filters": {
                    "isDeletedOnly": false,
                    "excludeTemplates": false,
                    "navigableBlockContentOnly": true,
                    "requireEditPermissions": false,
                    "ancestors": [],
                    "createdBy": [],
                    "editedBy": [],
                    "lastEditedTime": {},
                    "createdTime": {},
                    "inTeams": []
                },
                "sort": {
                    "field": "relevance"
                },
                "source": "quick_find"
            }),
            _ => json!({
                "query": query,
                "spaceId": space_id,
                "limit": limit,
                "filters": {
                    "isDeletedOnly": false,
                    "excludeTemplates": false,
                    "isNavigableOnly": false,
                    "requireEditPermissions": false
                },
                "source": "quick_find_input_change"
            }),
        };
        self.post("search", body).await
    }

    pub async fn enqueue_export_block(
        &self,
        block_id: &str,
        space_id: &str,
        include_files: &str,
    ) -> Result<String> {
        let body = json!({
            "task": {
                "eventName": "exportBlock",
                "request": {
                    "block": { "id": block_id, "spaceId": space_id },
                    "recursive": true,
                    "exportOptions": {
                        "exportType": "markdown",
                        "timeZone": "UTC",
                        "locale": "en",
                        "includeContents": include_files,
                        "collectionViewExportType": "currentView",
                        "flattenExportFiletree": false,
                    }
                }
            }
        });
        let v = self.post("enqueueTask", body).await?;
        let id = v
            .get("taskId")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("enqueueTask returned no taskId: {v}"))?;
        Ok(id.to_string())
    }

    pub async fn get_tasks(&self, task_ids: &[String]) -> Result<Vec<Value>> {
        if task_ids.is_empty() {
            return Ok(vec![]);
        }
        let v = self
            .post("getTasks", json!({ "taskIds": task_ids }))
            .await?;
        let results = v
            .get("results")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(results)
    }
}

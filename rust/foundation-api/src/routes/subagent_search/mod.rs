//! `POST /api/subagent_search` — runs a query against the external "context-1"
//! deep-research API and streams the result back to the caller.
//!
//! This endpoint owns its wire contract; it is not a transparent proxy. We
//! parse every event from the dependency into a typed [`AgentEvent`] and
//! re-emit our own [`SubagentSearchEvent`]s:
//!
//! - `action` / `observation` — forwarded as typed progress events.
//! - `result` — the agent's final answer parsed into structured
//!   [`RankedDocument`]s (so callers don't re-parse the `<Document>` block),
//!   emitted just before the terminal `done`. An answer with no documents is a
//!   valid empty `result`, not an error.
//! - `done` — terminates the stream.
//!
//! An upstream `error` event ends the stream with a [`SubagentStreamError`];
//! unknown event types are dropped.
//!
//! The deep-research dependency is reached at `POST {url}/search` with a JSON
//! body of `{query, model, collection_name, chroma_api_key, chroma_tenant,
//! chroma_database}` and an `Accept: text/event-stream` response of
//! `data: {"type": action|observation|done|error, "data": {...}}` lines.
//!
//! Credentials are taken from the request: the caller's `x-chroma-token` is the
//! Chroma API key, the tenant comes from the resolved identity, and the
//! database/collection come from foundation config.

mod events;
use crate::routes::links::page_url;
use crate::routes::{caller_token, to_sse_event, whoami::whoami_and_authorize};
use crate::wiki::chunking::ChunkRecordId;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ErrorCodes};
pub(crate) use events::RankedDocument;
use events::{parse_ranked_documents, AgentEvent, SubagentResultError, SubagentSearchEvent};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};

/// Deep-research model. Fixed to `scout`; deliberately not user-configurable.
const MODEL: &str = "scout";
/// The SSE field prefix the upstream emits each event under.
const SSE_DATA_PREFIX: &str = "data: ";

/// Request body for `POST /api/subagent_search`.
#[derive(Debug, Deserialize)]
pub struct SubagentSearchRequest {
    /// The research query.
    pub query: String,
}

/// Chroma credentials + target collection forwarded to the deep-research API.
#[derive(Debug, Clone)]
pub struct SubagentSearchCreds {
    pub chroma_api_key: String,
    pub chroma_tenant: String,
    pub chroma_database: String,
    pub collection_name: String,
}

impl SubagentSearchCreds {
    /// Builds the deep-research credentials from Foundation config, the resolved
    /// tenant, and the caller's Chroma token. The single place that maps config
    /// onto the forwarded creds, shared by the REST route, the agent tool, and
    /// the MCP tool so the three entry points can't send divergent payloads.
    pub fn from_config(
        config: &crate::config::FoundationConfig,
        tenant: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            chroma_api_key: token.into(),
            chroma_tenant: tenant.into(),
            chroma_database: config.database_name.clone(),
            collection_name: config.wiki_collection.clone(),
        }
    }
}

/// Errors raised before the SSE stream starts. Once streaming begins,
/// mid-stream failures are surfaced as a [`SubagentStreamError`] instead.
#[derive(Debug, thiserror::Error)]
pub enum SubagentSearchError {
    /// `deep_research_api_url` is unset, so the route is disabled.
    #[error("deep research is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
}

impl ChromaError for SubagentSearchError {
    fn code(&self) -> ErrorCodes {
        match self {
            SubagentSearchError::RouteDisabled => ErrorCodes::Internal,
            SubagentSearchError::MissingToken => ErrorCodes::InvalidArgument,
        }
    }
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

/// `POST /api/subagent_search` handler.
pub async fn foundation_subagent_search(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<SubagentSearchRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, SubagentStreamError>>>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;

    let _guard = server
        .scorecard_request(&["op:foundation_subagent_search", &format!("tenant:{tenant}")])?;

    let url = server
        .config
        .foundation
        .deep_research_api_url
        .clone()
        .ok_or(SubagentSearchError::RouteDisabled)?;
    let token = caller_token(&headers)
        .ok_or(SubagentSearchError::MissingToken)?
        .to_string();

    let creds = SubagentSearchCreds::from_config(&server.config.foundation, tenant, token);

    let stream =
        stream_subagent_search(server.shared_http_client.clone(), url, creds, request.query);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

// ---------------------------------------------------------------------------
// SSE stream: typed progress events, then a parsed `result`, then `done`
// ---------------------------------------------------------------------------

/// A transport/upstream failure encountered mid-stream. Because the SSE
/// response has already returned `200 OK`, axum can only end the body when one
/// of these is yielded — the client sees the stream terminate.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SubagentStreamError(String);

/// Serializes one of our owned events into an SSE frame, surfacing a
/// serialization failure as a stream error rather than panicking.
fn sse_event(event: &SubagentSearchEvent) -> Result<Event, SubagentStreamError> {
    to_sse_event(event, |err| {
        SubagentStreamError(format!("failed to serialize event: {err}"))
    })
}

/// Runs the deep-research agent and streams typed [`SubagentSearchEvent`]s to
/// the caller: the agent's `action`/`observation` steps, then a `result` event
/// carrying the final answer parsed into structured [`RankedDocument`]s, then
/// the terminal `done`.
///
/// Ends the stream with a [`SubagentStreamError`] on a transport/upstream
/// failure, on an upstream `error` event, or if the upstream closes without a
/// terminal `done`. An answer with zero documents is a valid empty `result`.
fn stream_subagent_search(
    http: reqwest::Client,
    url: String,
    creds: SubagentSearchCreds,
    query: String,
) -> impl Stream<Item = Result<Event, SubagentStreamError>> {
    async_stream::stream! {
        let data = subagent_search_data_stream(http, url, creds, query);
        futures::pin_mut!(data);

        // The terminal answer is the last `action`'s `user_text`; track it as
        // events stream by so we can emit it as a structured `result`.
        let mut final_answer: Option<String> = None;
        // Whether we saw a terminal `done`. If the upstream byte stream ends
        // without one (and without an `error`), we synthesize an error below so
        // the caller never sees a silent, terminator-less close.
        let mut saw_done = false;
        while let Some(item) = data.next().await {
            let raw = match item {
                Ok(raw) => raw,
                Err(message) => {
                    yield Err(SubagentStreamError(message));
                    return;
                }
            };

            // Map the upstream event to the events we emit (often one, none
            // for suppressed/unknown events, two for the terminal pair).
            let outgoing: Vec<SubagentSearchEvent> = match AgentEvent::parse(&raw) {
                AgentEvent::Action(action) => {
                    if let Some(text) = action.user_text() {
                        final_answer = Some(text.to_string());
                    }
                    // The raw `<Document>` answer is surfaced only as the
                    // structured `result`, so don't also emit it as progress.
                    if action.is_answer_only() {
                        Vec::new()
                    } else {
                        vec![SubagentSearchEvent::Action(action)]
                    }
                }
                AgentEvent::Observation(observation) => {
                    vec![SubagentSearchEvent::Observation(observation)]
                }
                // An upstream agent error terminates our stream.
                AgentEvent::Error(error) => {
                    yield Err(SubagentStreamError(error.message));
                    return;
                }
                // Emit the structured `result` then `done` as the terminator.
                AgentEvent::Done => {
                    saw_done = true;
                    // An answer that parses to zero documents is a legitimate
                    // "no hits" result, not a failure — emit an empty `result`
                    // so it stays distinguishable from a broken stream.
                    let documents = final_answer
                        .as_deref()
                        .map(parse_ranked_documents)
                        .unwrap_or_default();
                    vec![
                        SubagentSearchEvent::Result { documents },
                        SubagentSearchEvent::Done,
                    ]
                }
                // Unknown / unparseable events carry nothing we can model.
                AgentEvent::Unknown => Vec::new(),
            };

            for event in &outgoing {
                match sse_event(event) {
                    Ok(frame) => yield Ok(frame),
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
            }
        }

        // The byte stream ended without a `done` (or `error`, which would have
        // returned above): surface a terminal error so the close is never silent.
        if !saw_done {
            yield Err(SubagentStreamError(
                "deep research stream ended without a terminal event".to_string(),
            ));
        }
    }
}

/// POSTs to the deep-research API and yields the raw JSON payload of each SSE
/// `data:` line. Transport or non-2xx failures yield a single `Err` and end
/// the stream.
fn subagent_search_data_stream(
    http: reqwest::Client,
    url: String,
    creds: SubagentSearchCreds,
    query: String,
) -> impl Stream<Item = Result<String, String>> {
    async_stream::stream! {
        let response = match send_search_request(&http, &url, &creds, &query).await {
            Ok(response) => response,
            Err(err) => {
                yield Err(err);
                return;
            }
        };

        // Reassemble `data:` lines from the byte stream. We buffer *bytes* (not
        // a `String`) and decode only complete lines: a UTF-8 codepoint can
        // straddle a chunk boundary, but never a `\n` (0x0A), so per-line
        // decoding is lossless. The upstream emits exactly one `data: {json}`
        // line per event, so each decoded data line is a complete JSON value.
        let mut bytes = response.bytes_stream();
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    yield Err(format!("deep research stream failed: {err}"));
                    return;
                }
            };
            buffer.extend_from_slice(&chunk);
            while let Some(newline) = buffer.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = buffer.drain(..=newline).collect();
                if let Some(data) = parse_sse_data_line(&line) {
                    yield Ok(data);
                }
            }
        }
        // Emit any trailing event the upstream left unterminated by a newline.
        if let Some(data) = parse_sse_data_line(&buffer) {
            yield Ok(data);
        }
    }
}

/// Sends the `POST {url}/search` request, returning the streaming response or a
/// human-readable error for transport failures and non-2xx statuses.
async fn send_search_request(
    http: &reqwest::Client,
    url: &str,
    creds: &SubagentSearchCreds,
    query: &str,
) -> Result<reqwest::Response, String> {
    let endpoint = format!("{}/search", url.trim_end_matches('/'));
    http.post(&endpoint)
        .header("accept", "text/event-stream")
        .json(&subagent_search_payload(creds, query))
        .send()
        .await
        .and_then(|resp| resp.error_for_status())
        .map_err(|err| format!("deep research request failed: {err}"))
}

/// Builds the deep-research `/search` request body. The model is fixed to
/// [`MODEL`].
fn subagent_search_payload(creds: &SubagentSearchCreds, query: &str) -> Value {
    json!({
        "query": query,
        "model": MODEL,
        "collection_name": creds.collection_name,
        "chroma_api_key": creds.chroma_api_key,
        "chroma_tenant": creds.chroma_tenant,
        "chroma_database": creds.chroma_database,
    })
}

/// Returns the JSON payload of an SSE `data:` line. The line is split on `\n`,
/// so its bytes form complete UTF-8 codepoints; the trailing CR/LF is trimmed.
/// Blank lines, comments (`:`), and other SSE fields (`event:`, `id:`, …) yield
/// `None`.
fn parse_sse_data_line(line: &[u8]) -> Option<String> {
    String::from_utf8_lossy(line)
        .trim_end_matches(['\r', '\n'])
        .strip_prefix(SSE_DATA_PREFIX)
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// Result collection: distill a finished stream into structured documents
// ---------------------------------------------------------------------------

/// Runs the deep-research agent to completion and renders its ranked documents
/// into a plain-text block suitable for an LLM tool result.
///
/// This is the entry point the `subagent_search` agent tool calls: it owns the
/// streaming + parsing so the tool stays a thin formatter. Errors propagate the
/// same [`SubagentResultError`]s as [`collect_subagent_search_final`].
pub(crate) async fn subagent_search_text(
    http: reqwest::Client,
    url: String,
    creds: SubagentSearchCreds,
    query: String,
    ui_origin: Option<&str>,
) -> Result<String, SubagentResultError> {
    let tenant = creds.chroma_tenant.clone();
    let documents = collect_subagent_search_final(http, url, creds, query).await?;
    Ok(format_ranked_documents(&documents, ui_origin, &tenant))
}

/// Renders ranked documents (most-relevant first) into a numbered text block
/// for the model to read. Each entry carries the record `id`, the page `slug=`
/// it resolves to, and a `url=` page link when a UI origin is configured —
/// matching the `search` tool's output so the agent cites results from both
/// tools the same way. A document whose id is not a chunk id gets neither
/// slug nor url.
fn format_ranked_documents(
    documents: &[events::RankedDocument],
    ui_origin: Option<&str>,
    tenant: &str,
) -> String {
    documents
        .iter()
        .enumerate()
        .map(|(i, doc)| {
            let slug = ChunkRecordId::slug_from_id(&doc.id);
            let url = slug
                .and_then(|slug| page_url(ui_origin, tenant, slug))
                .map(|url| format!(" url={url}"))
                .unwrap_or_default();
            let slug = slug.map(|slug| format!(" slug={slug}")).unwrap_or_default();
            format!(
                "{}. {}{}{}\n   {}",
                i + 1,
                doc.id,
                slug,
                url,
                doc.justification
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Consumes the deep-research stream to a terminal `done`, extracts the agent's
/// final answer (the `user_text` from the last `action` event), and parses its
/// `<Document>/<Justification>` blocks into structured [`RankedDocument`]s.
///
/// Stream failures, upstream `error` events, and streams that end without a
/// terminal event are errors. A completed stream whose answer parses to zero
/// documents yields `Ok(vec![])`. Used by the `subagent_search` agent tool and
/// by the MCP `subagent_search` tool.
pub(crate) async fn collect_subagent_search_final(
    http: reqwest::Client,
    url: String,
    creds: SubagentSearchCreds,
    query: String,
) -> Result<Vec<events::RankedDocument>, SubagentResultError> {
    let stream = subagent_search_data_stream(http, url, creds, query);
    futures::pin_mut!(stream);

    // Keep the last action's `user_text` — the agent's final answer.
    let mut final_answer: Option<String> = None;
    let mut saw_done = false;
    while let Some(item) = stream.next().await {
        let raw = item.map_err(SubagentResultError::Stream)?;
        match AgentEvent::parse(&raw) {
            AgentEvent::Action(action) => {
                if let Some(text) = action.user_text() {
                    final_answer = Some(text.to_string());
                }
            }
            AgentEvent::Error(error) => {
                return Err(SubagentResultError::Upstream(error.message));
            }
            AgentEvent::Done => {
                saw_done = true;
                break;
            }
            AgentEvent::Observation(_) | AgentEvent::Unknown => {}
        }
    }

    if !saw_done {
        return Err(SubagentResultError::MissingTerminalEvent);
    }

    // A completed stream whose answer parses to zero documents is a valid "no
    // hits" result.
    Ok(final_answer
        .as_deref()
        .map(parse_ranked_documents)
        .unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Tests live in `tests/`, split by type and numbered for readability. The
// `tests` module reaches this module's private items via `super::super::` (and
// the event types via `super::super::events::`).
#[cfg(test)]
mod tests;

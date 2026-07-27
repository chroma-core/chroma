//! `POST /api/agent` — runs the `chroma-agent` loop over the foundation
//! retrieval tools and streams each step back to the caller as SSE.
//!
//! The agent is driven manually (reset -> observe -> infer -> act -> observe)
//! inside an [`async_stream`] so every inference step and tool observation can
//! be emitted as it happens, mirroring the Python reference's
//! `action`/`observation`/`done` schema. Inference is non-streaming
//! (Anthropic), so events are step-level, not token-level.
//!
//! The tools (`search`, `read_page`, `subagent_search`) reuse the same cores as
//! the standalone `/api/search`, `/api/read-page`, and `/api/subagent_search`
//! routes; per-request state (collection, token, deep-research creds) is
//! resolved once in the handler and captured by the tools. The shared
//! `reqwest::Client` is cloned into both the Anthropic model and the
//! deep-research tool so connection pools are reused rather than rebuilt per
//! request.
//!
//! Clients may seed the agent's system prompt via the request body (`system`);
//! when omitted, a built-in default steers the agent to answer from the
//! knowledge base using the tools.
//!
//! The outbound SSE event schema and its projections from the agent's
//! trajectory types live in [`events`]; this module owns the route, request
//! handling, agent assembly, and the stream driver.

mod events;

use std::collections::HashMap;

use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use chroma_metering::{MeterEvent, SearchAgentUsageContext};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use validator::Validate;

use chroma_agent::{
    Agent, AnthropicAgentInferenceModel, AnthropicModel, InferenceUsage, Observation,
    ObservationBuilder, ObservationItem, ToolSet,
};
use events::{action_event, action_text, observation_event, AgentSseEvent};

use crate::agent_tools::{ReadPageTool, SearchTool, SubagentSearchTool};
use crate::routes::subagent_search::SubagentSearchCreds;
use crate::routes::{caller_token, to_sse_event, whoami::whoami_and_authorize};
use crate::wiki::embed::WikiEmbedder;
use crate::wiki::WikiClientError;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};

/// Default system prompt when the caller does not supply one. Steers the agent
/// to ground its answer in the knowledge base via the available tools.
const DEFAULT_SYSTEM_PROMPT: &str = "You are a research assistant for an internal \
knowledge base. Use the `search` tool for targeted lookups and the \
`subagent_search` tool for broad, multi-part research questions. When a search \
hit looks relevant, use the `read_page` tool with its `slug` to read the full \
page before relying on it. Ground every claim in retrieved documents and cite \
each source page inline where you use it — as a Markdown link (titled by the \
page, targeting the result's `url=`/`URL:` when reported, otherwise its slug) \
— rather than appending a list of sources at the end. If the tools surface \
nothing relevant, say so plainly rather than guessing.";

/// Request body for `POST /api/agent`.
#[derive(Debug, Deserialize, Validate)]
pub struct AgentRequest {
    /// The user's first message, seeded as the agent's initial observation.
    #[validate(length(min = 1, message = "input must not be empty"))]
    pub input: String,
    /// Which Anthropic model to drive the loop with, given as a full wire id
    /// (e.g. `claude-sonnet-4-5-20250929`). Defaults to the latest Sonnet.
    #[serde(default = "default_model")]
    pub model: String,
    /// System prompt
    #[serde(default = "default_system_prompt")]
    pub system: String,
}

/// Default model when the caller omits `model`.
pub(crate) fn default_model() -> String {
    AnthropicModel::Sonnet4_5.id().to_string()
}

/// Default `system` prompt when the caller omits it (see
/// [`DEFAULT_SYSTEM_PROMPT`]).
pub(crate) fn default_system_prompt() -> String {
    DEFAULT_SYSTEM_PROMPT.to_string()
}

/// Errors raised before the SSE stream starts. Once streaming begins, run
/// failures are surfaced as an in-band [`AgentSseEvent::Error`] instead.
#[derive(Debug, thiserror::Error)]
pub enum AgentRouteError {
    /// `frontend_ingress_url` is unset, so the wiki client (and thus the
    /// `search` tool) is unavailable.
    #[error("agent is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// The requested model string did not map to a known Anthropic model.
    #[error("unknown model '{0}' (expected a full model id, e.g. 'claude-sonnet-4-5-20250929')")]
    UnknownModel(String),
    /// Resolving the wiki collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] WikiClientError),
    /// The Anthropic inference model could not be constructed (e.g.
    /// `ANTHROPIC_API_KEY` is unset).
    #[error("inference model unavailable: {0}")]
    Inference(String),
}

impl ChromaError for AgentRouteError {
    fn code(&self) -> ErrorCodes {
        match self {
            AgentRouteError::RouteDisabled => ErrorCodes::Internal,
            AgentRouteError::MissingToken => ErrorCodes::InvalidArgument,
            AgentRouteError::UnknownModel(_) => ErrorCodes::InvalidArgument,
            // A 404 resolving the wiki collection means Foundation isn't
            // provisioned for this tenant — surface it as NotFound (404) so
            // callers can tell "not set up" apart from a transient failure,
            // rather than collapsing it into a generic 500.
            AgentRouteError::Resolve(err) if err.is_not_found() => ErrorCodes::NotFound,
            AgentRouteError::Resolve(err) => err.code(),
            AgentRouteError::Inference(_) => ErrorCodes::Internal,
        }
    }
}

/// A serialization failure encountered while framing an SSE event. Because the
/// SSE response has already returned `200 OK`, axum can only end the body when
/// one of these is yielded.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AgentSseError(String);

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

/// `POST /api/agent` handler.
pub async fn foundation_agent(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<AgentRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, AgentSseError>>>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;

    let _guard = server.scorecard_request(&["op:foundation_agent", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;

    let model = request
        .model
        .parse::<AnthropicModel>()
        .map_err(|_| AgentRouteError::UnknownModel(request.model.clone()))?;

    let (agent, collection_id) = build_agent(&server, &headers, &tenant, &request, model).await?;
    let stream = drive_agent(
        agent,
        request.input,
        tenant,
        server.config.foundation.database_name.clone(),
        collection_id,
    )
    .map(|event| sse_event(&event));
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Resolves per-request state and assembles the [`Agent`]. The wiki collection
/// and embedder back the `search` tool; the deep-research creds back the
/// `subagent_search` tool, which is registered only when the dependency is
/// configured. The Anthropic model reuses the shared HTTP pool, and the system
/// prompt is taken from the request (which defaults to [`DEFAULT_SYSTEM_PROMPT`]
/// when the caller omits it). The configured `foundation_ui_origin` is handed
/// to each tool so retrieved documents carry resolvable page URLs the agent
/// can cite (mirroring the MCP tools' deterministic link stamping).
async fn build_agent(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    request: &AgentRequest,
    model: AnthropicModel,
) -> Result<(Agent, String), AgentRouteError> {
    let wiki_client = server
        .foundation_chroma_client
        .as_ref()
        .ok_or(AgentRouteError::RouteDisabled)?;
    let token = caller_token(headers)
        .ok_or(AgentRouteError::MissingToken)?
        .to_string();
    let collection = wiki_client.wiki_collection(tenant, &token).await?;
    let collection_id = collection.id().to_string();

    let ui_origin = server.config.foundation.foundation_ui_origin.clone();

    let mut toolset = ToolSet::new();
    toolset.add(SearchTool::new(
        collection.clone(),
        WikiEmbedder::new(None),
        token.clone(),
        tenant.to_string(),
        ui_origin.clone(),
    ));
    toolset.add(ReadPageTool::new(
        collection,
        tenant.to_string(),
        ui_origin.clone(),
    ));

    // The deep-research tool is optional: register it only when the dependency
    // is configured, so the agent still runs (search-only) without it.
    if let Some(url) = server.config.foundation.deep_research_api_url.clone() {
        let creds = SubagentSearchCreds::from_config(&server.config.foundation, tenant, token);
        toolset.add(SubagentSearchTool::new(
            server.shared_http_client.clone(),
            url,
            creds,
            ui_origin,
        ));
    }

    let inference = AnthropicAgentInferenceModel::from_env(model)
        .map_err(|err| AgentRouteError::Inference(err.to_string()))?
        .with_client(server.shared_http_client.clone());

    Ok((
        Agent::new(toolset, Box::new(inference)).with_system_prompt(request.system.clone()),
        collection_id,
    ))
}

// ---------------------------------------------------------------------------
// SSE stream: drive the agent loop, emitting action/observation/done events
// ---------------------------------------------------------------------------

/// Serializes one event into an SSE frame, surfacing a serialization failure as
/// a stream error rather than panicking.
fn sse_event(event: &AgentSseEvent) -> Result<Event, AgentSseError> {
    to_sse_event(event, |err| {
        AgentSseError(format!("failed to serialize agent event: {err}"))
    })
}

/// Drives the agent from `input` to completion, yielding an `action` event per
/// inference step, an `observation` event per tool round, and a terminal `done`
/// carrying the final answer. A run failure (e.g. an inference error) ends the
/// stream with an in-band `error` event.
///
/// This is the pure loop driver, kept separate from SSE framing so it can be
/// unit-tested by collecting the typed [`AgentSseEvent`]s; the handler maps the
/// result through [`sse_event`] to frame each event.
fn drive_agent(
    mut agent: Agent,
    input: String,
    tenant: String,
    database: String,
    collection_id: String,
) -> impl Stream<Item = AgentSseEvent> {
    async_stream::stream! {
        agent.reset();
        let mut initial = ObservationBuilder::new();
        initial.push_user(input);
        agent.observe(initial.build());

        // The terminal answer is the last action's user-facing text.
        let mut final_text = String::new();
        let mut usage_by_model = HashMap::new();

        loop {
            let step = match agent.infer_with_usage().await {
                Ok(step) => step,
                // Nothing actionable: end with whatever answer we have.
                Err(err) => {
                    yield AgentSseEvent::Error { message: err.to_string() };
                    return;
                }
            };
            if let Some(usage) = step.usage.as_ref() {
                record_search_agent_usage(&mut usage_by_model, usage);
            }
            let Some(action) = step.action else {
                break;
            };

            let text = action_text(&action);
            if !text.is_empty() {
                final_text = text;
            }

            yield action_event(&action);

            // Execute the action's tool calls. With the default
            // `ReportToModel` policy this only returns `Err` if the policy is
            // changed to `Terminate`; tool failures otherwise come back as an
            // observation with `is_error` set.
            match agent.act(action).await {
                Ok(Some(observation)) => {
                    for usage in extract_subagent_usages(&observation) {
                        record_search_agent_usage(&mut usage_by_model, &usage);
                    }
                    yield observation_event(&observation);
                    agent.observe(observation);
                }
                // Terminal action (no tool calls): the run is done.
                Ok(None) => break,
                Err(err) => {
                    yield AgentSseEvent::Error { message: err.to_string() };
                    return;
                }
            }

            if agent.is_done() {
                break;
            }
        }

        submit_search_agent_usage_events(&usage_by_model, &database, &tenant, &collection_id).await;
        yield AgentSseEvent::Done { final_text };
    }
}

fn record_search_agent_usage(
    usage_by_model: &mut HashMap<String, InferenceUsage>,
    usage: &InferenceUsage,
) {
    usage_by_model
        .entry(usage.model.clone())
        .and_modify(|total| {
            total.input_tokens += usage.input_tokens;
            total.output_tokens += usage.output_tokens;
            total.cache_read_tokens += usage.cache_read_tokens;
            total.cache_write_tokens += usage.cache_write_tokens;
        })
        .or_insert_with(|| usage.clone());
}

fn extract_subagent_usages(observation: &Observation) -> Vec<InferenceUsage> {
    observation
        .items
        .iter()
        .filter_map(|item| {
            let ObservationItem::ToolResult {
                metadata:
                    Some(chroma_agent::ToolCallMetadata::SubagentUsage {
                        model,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_write_tokens,
                    }),
                ..
            } = item
            else {
                return None;
            };

            Some(InferenceUsage {
                model: model.clone(),
                input_tokens: *input_tokens,
                output_tokens: *output_tokens,
                cache_read_tokens: *cache_read_tokens,
                cache_write_tokens: *cache_write_tokens,
            })
        })
        .collect()
}

async fn submit_search_agent_usage_events(
    usage_by_model: &HashMap<String, InferenceUsage>,
    database: &str,
    tenant: &str,
    collection_id: &str,
) {
    for usage in usage_by_model.values() {
        if let Err(error) = MeterEvent::SearchAgentUsage(SearchAgentUsageContext {
            tenant: tenant.to_string(),
            database: database.to_string(),
            collection_id: collection_id.to_string(),
            model: usage.model.clone(),
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_write_tokens,
        })
        .submit()
        .await
        {
            tracing::warn!(
                error = %error,
                tenant,
                database,
                model = usage.model,
                input_tokens = usage.input_tokens,
                output_tokens = usage.output_tokens,
                cache_read_tokens = usage.cache_read_tokens,
                cache_write_tokens = usage.cache_write_tokens,
                "failed to submit search agent usage meter event"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Tests live in `tests/`, split by type and numbered for readability. They
// reach this module's private items via `super::super::` and the event types /
// projections via `super::super::events::`.
#[cfg(test)]
mod tests;

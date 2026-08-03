use crate::server::FoundationApiServer;
use axum::response::sse::Event;
use axum::{
    http::HeaderMap,
    routing::{get, post},
    Router,
};
use serde::Serialize;

/// HTTP header carrying the caller's Chroma Cloud token, forwarded to the FE
/// and the embedding service so authz/quota/billing key off the user.
pub(crate) const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

/// Returns the caller's Chroma token from the `x-chroma-token` header, or
/// `None` when it is absent, non-ASCII, or empty. Routes map `None` to their
/// own missing-token error.
///
/// The header-name lookup is case-insensitive: `HeaderMap` normalizes names to
/// lowercase, so `X-Chroma-Token`, `x-chroma-token`, etc. all match. The token
/// value is returned verbatim (it is case-sensitive).
pub(crate) fn caller_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(CHROMA_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|token| !token.is_empty())
}

/// Serializes `event` into an SSE `data:` frame, mapping a serialization
/// failure into a caller-supplied stream error.
///
/// Shared by the SSE routes (`/api/agent`, `/api/subagent_search`): they each
/// own a distinct stream-error type and message but frame events identically,
/// so they pass a closure that builds their own error from the serde failure.
pub(crate) fn to_sse_event<T, E>(
    event: &T,
    on_error: impl FnOnce(serde_json::Error) -> E,
) -> Result<Event, E>
where
    T: Serialize,
{
    serde_json::to_string(event)
        .map(|json| Event::default().data(json))
        .map_err(on_error)
}

pub(crate) mod agent;
pub(crate) mod apply_patch;
pub(crate) mod init;
pub(crate) mod init_schema;
pub(crate) mod links;
pub(crate) mod mcp;
pub(crate) mod read_page;
pub(crate) mod search;
pub(crate) mod subagent_search;
pub(crate) mod trajectories;
pub(crate) mod upsert_page;
pub(super) mod whoami;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
        .route("/api/init", post(init::foundation_init))
        .route(
            "/api/upsert-page",
            post(upsert_page::foundation_upsert_page),
        )
        .route(
            "/api/apply-patch",
            post(apply_patch::foundation_apply_patch),
        )
        .route("/api/search", post(search::foundation_search))
        .route("/api/read-page", post(read_page::foundation_read_page))
        .route(
            "/api/trajectories/save",
            post(trajectories::foundation_save_trajectory),
        )
        .route(
            "/api/trajectories/open",
            post(trajectories::foundation_open_trajectory),
        )
        .route(
            "/api/trajectories/{id}/entries",
            post(trajectories::foundation_append_trajectory_entries),
        )
        .route(
            "/api/trajectories/{id}/finalize",
            post(trajectories::foundation_finalize_trajectory),
        )
        .route(
            "/api/trajectories/{id}/reasoning",
            get(trajectories::foundation_get_trajectory_reasoning),
        )
        .route(
            "/api/trajectories/{id}",
            get(trajectories::foundation_get_trajectory),
        )
        .route(
            "/api/subagent_search",
            post(subagent_search::foundation_subagent_search),
        )
        .route("/api/agent", post(agent::foundation_agent))
}

use crate::server::FoundationApiServer;
use axum::{http::HeaderMap, routing::post, Router};

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

pub(crate) mod init;
pub(crate) mod search;
pub(crate) mod subagent_search;
pub(crate) mod upsert_page;
pub(super) mod whoami;

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
        .route("/api/init", post(init::foundation_init))
        .route(
            "/api/upsert-page",
            post(upsert_page::foundation_upsert_page),
        )
        .route("/api/search", post(search::foundation_search))
        .route(
            "/api/subagent_search",
            post(subagent_search::foundation_subagent_search),
        )
}

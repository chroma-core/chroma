use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chroma_error::{ChromaError, ErrorCodes};
use serde_json::Value;

use crate::{
    auth::{AuthzAction, AuthzResource},
    errors::ServerError,
    server::FoundationApiServer,
};

/// `POST /api/ask` — reverse-proxy to mullet's `/api/ask`.
///
/// Authenticates the caller via the Chroma auth layer, then forwards the
/// JSON body with `user` set to the caller's `user_id` (the team-membership
/// id from `GetUserIdentityResponse`). The auth-resolved `user` overrides
/// any caller-supplied `user` so a client can't impersonate.
///
/// Mullet's status, `content-type`, and body bytes are relayed verbatim;
/// mullet's own 4xx/5xx pass through unchanged. An upstream/network
/// failure surfaces as 503 (the Rust `ChromaError` ladder has no 502,
/// which is dashboard-api's choice).
pub(crate) async fn ask(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    body: Bytes,
) -> Result<Response, ServerError> {
    // Two-step auth: resolve identity, then authorize against that tenant.
    // The Cloud `authenticate_and_authorize` impl enforces
    // `resource.tenant == identity.tenant` (403 on mismatch, including
    // `tenant: None`). `init.rs::whoami_and_authorize` documents the same
    // gotcha; inlined here because the proxy needs both `tenant` (for the
    // scorecard tag) and `user_id` (to inject into the upstream body),
    // and the existing helper only returns the tenant.
    let identity = server.auth.get_user_identity(&headers).await?;
    let tenant = identity.tenant.clone();
    server
        .auth
        .authenticate_and_authorize(
            &headers,
            AuthzAction::ViewFoundation,
            AuthzResource {
                tenant: Some(tenant.clone()),
                database: None,
                collection: None,
            },
        )
        .await?;
    let user_id = identity.user_id;

    let _guard = server.scorecard_request(&["op:foundation_ask", &format!("tenant:{}", tenant)])?;

    let body_json = merge_user(&body, user_id)?;

    let upstream_url = format!(
        "{}/api/ask",
        server.config.foundation.mullet_url.trim_end_matches('/')
    );

    let upstream = server
        .http_client
        .post(&upstream_url)
        .header(header::ACCEPT, "application/json")
        .json(&body_json)
        .send()
        .await
        .map_err(|e| {
            tracing::warn!(
                upstream = %upstream_url,
                tenant = %tenant,
                error = %e,
                "mullet upstream request failed",
            );
            MulletProxyError::Upstream
        })?;

    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let response_bytes = upstream.bytes().await.map_err(|e| {
        tracing::warn!(
            upstream = %upstream_url,
            tenant = %tenant,
            error = %e,
            "failed to read mullet response body",
        );
        MulletProxyError::Upstream
    })?;

    tracing::info!(
        upstream = %upstream_url,
        tenant = %tenant,
        op = "foundation_ask",
        status = status.as_u16(),
        bytes = response_bytes.len(),
        "mullet upstream response relayed",
    );

    // `reqwest::StatusCode` and `axum::http::StatusCode` are the same
    // `http` crate type, so round-tripping via `from_u16` is lossless.
    let axum_status =
        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut response = (axum_status, response_bytes).into_response();
    if let Some(ct) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    Ok(response)
}

/// Inject the auth-resolved `user` into the caller's JSON body. Overwrites
/// any caller-supplied `user` so a client can't impersonate. An empty
/// body is treated as `{}` so a body-less POST still reaches mullet with
/// just `{"user": "<id>"}` (mullet's own zod schema rejects missing
/// required fields with 400, which the proxy relays verbatim).
fn merge_user(body: &Bytes, user_id: String) -> Result<Value, MulletProxyError> {
    let mut value: Value = if body.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(body).map_err(|e| MulletProxyError::InvalidBody(e.to_string()))?
    };
    let Value::Object(map) = &mut value else {
        return Err(MulletProxyError::InvalidBody(
            "request body must be a JSON object".to_string(),
        ));
    };
    map.insert("user".to_string(), Value::String(user_id));
    Ok(value)
}

#[derive(Debug, thiserror::Error)]
enum MulletProxyError {
    #[error("invalid JSON body: {0}")]
    InvalidBody(String),
    #[error("mullet upstream unavailable")]
    Upstream,
}

impl ChromaError for MulletProxyError {
    fn code(&self) -> ErrorCodes {
        match self {
            MulletProxyError::InvalidBody(_) => ErrorCodes::InvalidArgument,
            MulletProxyError::Upstream => ErrorCodes::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests;

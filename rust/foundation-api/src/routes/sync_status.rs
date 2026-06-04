use crate::{errors::ServerError, server::FoundationApiServer};
use axum::{
    body::Body,
    extract::State,
    http::{
        header::{CONTENT_TYPE, COOKIE},
        HeaderMap, HeaderValue, Response, StatusCode,
    },
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::Deserialize;

const X_CHROMA_TOKEN: &str = "x-chroma-token";

/// `GET /api/sync-status` — read-only proxy to sync-frontend's
/// `/foundation/status` endpoint.
///
/// Two auth paths, picked by request shape:
///
///  - **`x-chroma-token` (CLI / direct API consumers).** The caller
///    holds a Cloud-Authz-issued token already; foundation-api forwards
///    it to sync-frontend unchanged.
///
///  - **`Cookie` (foundation-ui browser sessions).** Foundation-api
///    calls dashboard-api's `/api/v1/auth/session-identity` with the
///    cookie forwarded, gets back the team's internal API key, and
///    uses *that* as `x-chroma-token` to sync-frontend. Browser sees no
///    keys; foundation-api is the translation layer.
///
/// The upstream response body is returned verbatim, preserving its
/// content-type. The status code is propagated so an upstream 401
/// surfaces as 401 here, not as a generic 502.
///
/// Foundation-api intentionally does not run its own authz check —
/// sync-frontend already validates the token via dashboard-api's
/// `check_api_key`, which is the trust boundary for this chain. Adding
/// a second authz layer here would be theatre on the CLI path (and
/// require teaching Cloud-Authz about session cookies for the UI path).
pub async fn foundation_sync_status(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
) -> Result<Response<Body>, ServerError> {
    let sync_frontend_url = server
        .config
        .foundation
        .sync_frontend_url
        .as_deref()
        .ok_or(SyncStatusError::SyncFrontendUrlNotConfigured)?;

    let token = resolve_upstream_token(&headers, &server).await?;

    let _guard = server.scorecard_request(&["op:foundation_sync_status"])?;

    forward_to_sync_frontend(sync_frontend_url, &token)
        .await
        .map_err(ServerError::from)
}

/// Pick which token to send to sync-frontend.
///
/// CLI / API-key callers send `x-chroma-token` and we just forward that.
/// Browser sessions send a `Cookie`; we resolve it via dashboard-api's
/// session-identity endpoint and use the returned `internalApiKey`. If
/// neither is present, this is a 401.
async fn resolve_upstream_token(
    headers: &HeaderMap,
    server: &FoundationApiServer,
) -> Result<HeaderValue, SyncStatusError> {
    if let Some(token) = headers.get(X_CHROMA_TOKEN) {
        return Ok(token.clone());
    }

    if let Some(cookie) = headers.get(COOKIE) {
        let dashboard_api_url = server
            .config
            .foundation
            .dashboard_api_url
            .as_deref()
            .ok_or(SyncStatusError::DashboardApiUrlNotConfigured)?;
        return resolve_session_to_team_key(dashboard_api_url, cookie).await;
    }

    Err(SyncStatusError::MissingAuthCredential)
}

/// Translate a browser session cookie into the team's internal API key
/// by calling dashboard-api's `/api/v1/auth/session-identity` endpoint.
/// The cookie is forwarded verbatim; the endpoint is session-guarded on
/// the dashboard-api side and returns 401/403 if the session is invalid
/// or doesn't have a team context.
async fn resolve_session_to_team_key(
    dashboard_api_url: &str,
    cookie: &HeaderValue,
) -> Result<HeaderValue, SyncStatusError> {
    let url = format!(
        "{}/api/v1/auth/session-identity",
        dashboard_api_url.trim_end_matches('/')
    );

    let response = reqwest::Client::new()
        .get(&url)
        .header(COOKIE, cookie)
        .send()
        .await
        .map_err(SyncStatusError::SessionIdentityRequest)?;

    let upstream_status = response.status();
    if !upstream_status.is_success() {
        return Err(SyncStatusError::SessionIdentityRejected(
            upstream_status.as_u16(),
        ));
    }

    // reqwest in this workspace doesn't have the `json` feature, so
    // parse manually via bytes + serde_json.
    let bytes = response
        .bytes()
        .await
        .map_err(SyncStatusError::SessionIdentityRequest)?;
    let parsed: SessionIdentity =
        serde_json::from_slice(&bytes).map_err(|_| SyncStatusError::SessionIdentityMalformed)?;

    HeaderValue::from_str(&parsed.internal_api_key)
        .map_err(|_| SyncStatusError::SessionIdentityMalformed)
}

#[derive(Deserialize)]
struct SessionIdentity {
    /// Dashboard-api returns the team's decrypted internal key under
    /// the camel-case name `internalApiKey` (TypeBox default
    /// serialization).
    #[serde(rename = "internalApiKey")]
    internal_api_key: String,
}

/// Forward the request to sync-frontend's `/foundation/status` and return
/// the response with its status code + content-type preserved. Extracted
/// from the handler so tests can exercise it without constructing a full
/// `FoundationApiServer` (which needs a real `SysDb`).
async fn forward_to_sync_frontend(
    sync_frontend_url: &str,
    token: &HeaderValue,
) -> Result<Response<Body>, SyncStatusError> {
    let url = format!(
        "{}/foundation/status",
        sync_frontend_url.trim_end_matches('/')
    );

    let upstream = reqwest::Client::new()
        .get(&url)
        .header(X_CHROMA_TOKEN, token)
        .send()
        .await
        .map_err(SyncStatusError::UpstreamRequest)?;

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
    let body = upstream
        .bytes()
        .await
        .map_err(SyncStatusError::UpstreamRequest)?;

    let mut builder = Response::builder().status(status);
    if let Some(ct) = content_type {
        builder = builder.header(CONTENT_TYPE, ct);
    }
    builder
        .body(Body::from(body))
        .map_err(|e| SyncStatusError::ResponseBuild(e.to_string()))
}

#[derive(Debug, thiserror::Error)]
enum SyncStatusError {
    #[error("foundation.sync_frontend_url is not configured")]
    SyncFrontendUrlNotConfigured,
    #[error("foundation.dashboard_api_url is not configured; cookie auth disabled")]
    DashboardApiUrlNotConfigured,
    #[error("request had neither an x-chroma-token nor a session cookie")]
    MissingAuthCredential,
    #[error("dashboard-api session-identity request failed: {0}")]
    SessionIdentityRequest(#[from] reqwest::Error),
    #[error("dashboard-api session-identity returned status {0}")]
    SessionIdentityRejected(u16),
    #[error("dashboard-api session-identity returned a malformed internalApiKey")]
    SessionIdentityMalformed,
    #[error("upstream sync-frontend request failed: {0}")]
    UpstreamRequest(reqwest::Error),
    #[error("could not build response: {0}")]
    ResponseBuild(String),
}

impl ChromaError for SyncStatusError {
    fn code(&self) -> ErrorCodes {
        match self {
            SyncStatusError::SyncFrontendUrlNotConfigured => ErrorCodes::Internal,
            SyncStatusError::DashboardApiUrlNotConfigured => ErrorCodes::Internal,
            SyncStatusError::MissingAuthCredential => ErrorCodes::Unauthenticated,
            // 4xx from dashboard-api means the session was rejected →
            // surface as Unauthenticated. 5xx (or anything weird) is an
            // upstream failure → Unavailable.
            SyncStatusError::SessionIdentityRejected(status) if *status < 500 => {
                ErrorCodes::Unauthenticated
            }
            SyncStatusError::SessionIdentityRejected(_) => ErrorCodes::Unavailable,
            SyncStatusError::SessionIdentityRequest(_) => ErrorCodes::Unavailable,
            SyncStatusError::SessionIdentityMalformed => ErrorCodes::Internal,
            SyncStatusError::UpstreamRequest(_) => ErrorCodes::Unavailable,
            SyncStatusError::ResponseBuild(_) => ErrorCodes::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One-shot tokio HTTP server. Captures the value of one named
    /// request header and replies with a canned body. The returned
    /// JoinHandle yields the captured header value (if any) after the
    /// first request completes.
    async fn spawn_fake_server(
        capture_header: &'static str,
        canned_status: u16,
        canned_body: &'static [u8],
    ) -> (String, tokio::task::JoinHandle<Option<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.ok()?;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.ok()?;
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            let captured = req.lines().find_map(|line| {
                line.split_once(':')
                    .filter(|(k, _)| k.trim().eq_ignore_ascii_case(capture_header))
                    .map(|(_, v)| v.trim().to_string())
            });
            let resp = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                canned_status,
                canned_body.len()
            );
            stream.write_all(resp.as_bytes()).await.ok()?;
            stream.write_all(canned_body).await.ok()?;
            captured
        });
        (format!("http://127.0.0.1:{}", port), handle)
    }

    #[tokio::test]
    async fn forward_passthrough_body() {
        let body = br#"{"jobs":[{"id":"abc","source":"slack","status":"uploaded","completed_at":null,"latest_item":null,"item_count":42}]}"#;
        let (url, handle) = spawn_fake_server(X_CHROMA_TOKEN, 200, body).await;

        let resp = forward_to_sync_frontend(&url, &HeaderValue::from_static("ck-test"))
            .await
            .expect("forward should succeed");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).map(|v| v.as_bytes()),
            Some(&b"application/json"[..])
        );

        let out_body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert_eq!(out_body.as_ref(), body);

        let seen_token = handle.await.unwrap();
        assert_eq!(seen_token.as_deref(), Some("ck-test"));
    }

    #[tokio::test]
    async fn forward_propagates_upstream_non_2xx() {
        let body = br#"{"error":"unauthorized","message":"bad token"}"#;
        let (url, _handle) = spawn_fake_server(X_CHROMA_TOKEN, 401, body).await;

        let resp = forward_to_sync_frontend(&url, &HeaderValue::from_static("ck-test"))
            .await
            .expect("forward should succeed even on upstream 401");

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn forward_dial_failure_returns_unavailable() {
        let err =
            forward_to_sync_frontend("http://127.0.0.1:1", &HeaderValue::from_static("ck-test"))
                .await
                .expect_err("expected dial failure");
        assert_eq!(err.code(), ErrorCodes::Unavailable);
    }

    #[tokio::test]
    async fn cookie_resolves_to_internal_api_key() {
        let body = br#"{"identity":1,"type":"session","team":"t-123","permissions":[],"internalApiKey":"ck-internal-abc"}"#;
        let (url, handle) = spawn_fake_server(COOKIE.as_str(), 200, body).await;

        let cookie = HeaderValue::from_static("sessionId=abc123");
        let token = resolve_session_to_team_key(&url, &cookie)
            .await
            .expect("session resolution should succeed");

        assert_eq!(token.to_str().unwrap(), "ck-internal-abc");

        // Cookie was forwarded verbatim.
        let seen_cookie = handle.await.unwrap();
        assert_eq!(seen_cookie.as_deref(), Some("sessionId=abc123"));
    }

    #[tokio::test]
    async fn cookie_rejection_4xx_maps_to_unauthenticated() {
        let body = br#"{"error":"unauthorized"}"#;
        let (url, _handle) = spawn_fake_server(COOKIE.as_str(), 401, body).await;

        let cookie = HeaderValue::from_static("sessionId=expired");
        let err = resolve_session_to_team_key(&url, &cookie)
            .await
            .expect_err("expected session rejection");
        assert_eq!(err.code(), ErrorCodes::Unauthenticated);
    }

    #[tokio::test]
    async fn cookie_5xx_from_dashboard_api_maps_to_unavailable() {
        let body = b"upstream is sad";
        let (url, _handle) = spawn_fake_server(COOKIE.as_str(), 502, body).await;

        let cookie = HeaderValue::from_static("sessionId=abc");
        let err = resolve_session_to_team_key(&url, &cookie)
            .await
            .expect_err("expected upstream failure");
        assert_eq!(err.code(), ErrorCodes::Unavailable);
    }

    #[test]
    fn error_code_mappings() {
        assert_eq!(
            SyncStatusError::SyncFrontendUrlNotConfigured.code(),
            ErrorCodes::Internal,
        );
        assert_eq!(
            SyncStatusError::DashboardApiUrlNotConfigured.code(),
            ErrorCodes::Internal,
        );
        assert_eq!(
            SyncStatusError::MissingAuthCredential.code(),
            ErrorCodes::Unauthenticated,
        );
        assert_eq!(
            SyncStatusError::SessionIdentityRejected(403).code(),
            ErrorCodes::Unauthenticated,
        );
        assert_eq!(
            SyncStatusError::SessionIdentityRejected(503).code(),
            ErrorCodes::Unavailable,
        );
        assert_eq!(
            SyncStatusError::SessionIdentityMalformed.code(),
            ErrorCodes::Internal,
        );
    }
}

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
};

use crate::{
    auth::AuthzAction, errors::ServerError, routes::whoami::whoami_and_authorize,
    server::FoundationApiServer,
};

use super::error::AskProxyError;
use super::merge::merge_user;

/// HTTP header that the caller's Chroma token is forwarded under, both
/// when the caller sends it to foundation-api and when we forward it on
/// to Modal so Modal can call back into Chroma on the user's behalf.
const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

/// `POST /api/ask` — reverse-proxy to the Modal `/ask` endpoint.
///
/// Authenticates the caller, then forwards the JSON body to Modal with:
/// - `Modal-Key` / `Modal-Secret` for service-to-service auth (from
///   `MODAL_KEY` / `MODAL_SECRET` env vars; same pattern as the chroma
///   worker's `http_generate` attached function).
/// - `x-chroma-token` forwarded unchanged so Modal can call back to
///   Chroma's data plane on the user's behalf.
/// - `user` in the body set to the caller's `user_id`, overriding any
///   caller-supplied `user` so a client can't impersonate.
///
/// Modal's status, `content-type`, and body bytes are relayed verbatim.
/// Upstream/network failure surfaces as 503.
pub(crate) async fn ask(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    body: Bytes,
) -> Result<Response, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;
    let user_id = identity.user_id;

    let _guard = server.scorecard_request(&["op:foundation_ask", &format!("tenant:{}", tenant)])?;

    let endpoint_url = server
        .config
        .foundation
        .ask_endpoint_url
        .as_deref()
        .ok_or(AskProxyError::MissingConfig("foundation.ask_endpoint_url"))?;
    let modal_key = server
        .modal_key
        .as_deref()
        .ok_or(AskProxyError::MissingConfig("MODAL_KEY env var"))?;
    let modal_secret = server
        .modal_secret
        .as_deref()
        .ok_or(AskProxyError::MissingConfig("MODAL_SECRET env var"))?;

    let body_json = merge_user(&body, user_id)?;

    let mut request = server
        .http_client
        .post(endpoint_url)
        .header("Modal-Key", modal_key)
        .header("Modal-Secret", modal_secret)
        .header(header::ACCEPT, "application/json")
        .json(&body_json);

    // Forward the caller's Chroma token verbatim so Modal can call back
    // to Chroma's data plane on the user's behalf. If the caller didn't
    // send one (e.g. dev with the noop auth impl), skip it.
    if let Some(token) = headers.get(CHROMA_TOKEN_HEADER) {
        request = request.header(CHROMA_TOKEN_HEADER, token);
    }

    let upstream = request.send().await.map_err(|e| {
        tracing::warn!(
            upstream = %endpoint_url, tenant = %tenant, error = %e,
            "Modal upstream request failed",
        );
        AskProxyError::Upstream
    })?;

    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let response_bytes = upstream.bytes().await.map_err(|e| {
        tracing::warn!(
            upstream = %endpoint_url, tenant = %tenant, error = %e,
            "failed to read Modal response body",
        );
        AskProxyError::Upstream
    })?;

    tracing::info!(
        upstream = %endpoint_url, tenant = %tenant, op = "foundation_ask",
        status = status.as_u16(), bytes = response_bytes.len(),
        "Modal upstream response relayed",
    );

    let mut response = (status, response_bytes).into_response();
    if let Some(ct) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        ok_auth, post_ask, read_body_json, spawn_modal_stub, spawn_stub, test_server,
        test_server_without_modal_creds, unauthorized_auth,
    };
    use axum::{http::StatusCode, routing::post, Router};

    #[tokio::test]
    async fn forwards_modal_creds_and_user_token_and_injects_user() {
        let (endpoint, captures) = spawn_modal_stub().await;
        let server = test_server(ok_auth(), Some(endpoint));
        let resp = post_ask(
            server,
            r#"{"query":"hi","user":"spoof"}"#,
            Some("caller-tok"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_body_json(resp).await["result"], "ok");

        let captured = captures.lock().unwrap()[0].clone();
        assert_eq!(captured.modal_key.as_deref(), Some("test-key"));
        assert_eq!(captured.modal_secret.as_deref(), Some("test-secret"));
        assert_eq!(captured.chroma_token.as_deref(), Some("caller-tok"));
        assert_eq!(captured.body["user"], "user_42");
        assert_eq!(captured.body["query"], "hi");
    }

    #[tokio::test]
    async fn does_not_forward_chroma_token_when_caller_omits_it() {
        let (endpoint, captures) = spawn_modal_stub().await;
        let server = test_server(ok_auth(), Some(endpoint));
        let resp = post_ask(server, r#"{"query":"hi"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(captures.lock().unwrap()[0].chroma_token, None);
    }

    #[tokio::test]
    async fn returns_401_when_auth_rejects() {
        let server = test_server(unauthorized_auth(), Some("http://127.0.0.1:1/ask".into()));
        let resp = post_ask(server, r#"{"query":"hi"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn returns_503_when_upstream_is_unreachable() {
        let server = test_server(ok_auth(), Some("http://127.0.0.1:1/ask".into()));
        let resp = post_ask(server, r#"{"query":"hi"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn returns_500_when_endpoint_url_is_missing() {
        let server = test_server(ok_auth(), None);
        let resp = post_ask(server, r#"{"query":"hi"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn returns_500_when_modal_creds_are_missing() {
        let (endpoint, _) = spawn_modal_stub().await;
        let server = test_server_without_modal_creds(ok_auth(), Some(endpoint));
        let resp = post_ask(server, r#"{"query":"hi"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn relays_upstream_4xx_verbatim() {
        let stub = Router::new().route(
            "/ask",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({"error": "bad_request"})),
                )
            }),
        );
        let endpoint = spawn_stub(stub).await;
        let server = test_server(ok_auth(), Some(endpoint));
        let resp = post_ask(server, r#"{"bogus":"body"}"#, None).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(read_body_json(resp).await["error"], "bad_request");
    }
}

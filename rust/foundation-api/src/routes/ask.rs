use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
};
use chroma_error::{ChromaError, ErrorCodes};
use serde_json::Value;

use super::whoami::whoami_and_authorize;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};

/// HTTP header that the caller's Chroma token is forwarded under, both
/// when the caller sends it to foundation-api and when we forward it on
/// to Modal so Modal can call back into Chroma on the user's behalf.
const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

/// `POST /api/ask` — reverse-proxy to the Modal `/ask` endpoint.
///
/// Authenticates the caller via the Chroma auth layer, then forwards the
/// JSON body to Modal with:
/// - `Modal-Key` / `Modal-Secret` for service-to-service auth (sourced
///   from `MODAL_KEY` / `MODAL_SECRET` env vars; same pattern as the
///   chroma worker's `http_generate` attached function).
/// - The caller's `x-chroma-token` header forwarded unchanged so Modal
///   can call back to Chroma on the user's behalf.
/// - `user` in the JSON body set to the caller's `user_id` (the team-
///   membership id from `GetUserIdentityResponse`). Overrides any
///   caller-supplied `user` so a client can't impersonate.
///
/// Modal's status, `content-type`, and body bytes are relayed verbatim;
/// Modal's own 4xx/5xx pass through unchanged. An upstream/network
/// failure surfaces as 503.
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
            upstream = %endpoint_url,
            tenant = %tenant,
            error = %e,
            "Modal upstream request failed",
        );
        AskProxyError::Upstream
    })?;

    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let response_bytes = upstream.bytes().await.map_err(|e| {
        tracing::warn!(
            upstream = %endpoint_url,
            tenant = %tenant,
            error = %e,
            "failed to read Modal response body",
        );
        AskProxyError::Upstream
    })?;

    tracing::info!(
        upstream = %endpoint_url,
        tenant = %tenant,
        op = "foundation_ask",
        status = status.as_u16(),
        bytes = response_bytes.len(),
        "Modal upstream response relayed",
    );

    let mut response = (status, response_bytes).into_response();
    if let Some(ct) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    Ok(response)
}

/// Inject the auth-resolved `user` into the caller's JSON body. Overwrites
/// any caller-supplied `user` so a client can't impersonate. An empty
/// body is treated as `{}` so a body-less POST still reaches Modal with
/// just `{"user": "<id>"}` (Modal's schema rejects missing required
/// fields, which the proxy relays verbatim).
fn merge_user(body: &Bytes, user_id: String) -> Result<Value, AskProxyError> {
    let mut value: Value = if body.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(body).map_err(|e| AskProxyError::InvalidBody(e.to_string()))?
    };
    let Value::Object(map) = &mut value else {
        return Err(AskProxyError::InvalidBody(
            "request body must be a JSON object".to_string(),
        ));
    };
    map.insert("user".to_string(), Value::String(user_id));
    Ok(value)
}

#[derive(Debug, thiserror::Error)]
enum AskProxyError {
    #[error("invalid JSON body: {0}")]
    InvalidBody(String),
    #[error("Modal upstream unavailable")]
    Upstream,
    #[error("{0} is not configured")]
    MissingConfig(&'static str),
}

impl ChromaError for AskProxyError {
    fn code(&self) -> ErrorCodes {
        match self {
            AskProxyError::InvalidBody(_) => ErrorCodes::InvalidArgument,
            AskProxyError::Upstream => ErrorCodes::Unavailable,
            AskProxyError::MissingConfig(_) => ErrorCodes::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::{AuthError, AuthenticateAndAuthorize, AuthzResource},
        config::FoundationApiConfig,
    };
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
        routing::post,
        Router,
    };
    use chroma_api_types::GetUserIdentityResponse;
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::System;
    use std::{
        collections::HashSet,
        future::{ready, Future},
        pin::Pin,
        sync::{Arc, Mutex},
    };
    use tower::ServiceExt;

    /// Test auth that returns a fixed identity.
    struct FakeAuth {
        user_id: String,
        tenant: String,
    }

    impl FakeAuth {
        fn new(user_id: &str, tenant: &str) -> Self {
            Self {
                user_id: user_id.to_string(),
                tenant: tenant.to_string(),
            }
        }

        fn identity(&self) -> GetUserIdentityResponse {
            GetUserIdentityResponse {
                user_id: self.user_id.clone(),
                tenant: self.tenant.clone(),
                databases: HashSet::new(),
            }
        }
    }

    impl AuthenticateAndAuthorize for FakeAuth {
        fn authenticate_and_authorize(
            &self,
            _headers: &HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn authenticate_and_authorize_collection(
            &self,
            _headers: &HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
            _collection: chroma_types::Collection,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }

        fn get_user_identity(
            &self,
            _headers: &HeaderMap,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            let identity = self.identity();
            Box::pin(ready(Ok(identity)))
        }
    }

    /// Test auth that always rejects, used to verify 401 propagation.
    struct UnauthorizedAuth;

    impl AuthenticateAndAuthorize for UnauthorizedAuth {
        fn authenticate_and_authorize(
            &self,
            _headers: &HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
        }

        fn authenticate_and_authorize_collection(
            &self,
            _headers: &HeaderMap,
            _action: AuthzAction,
            _resource: AuthzResource,
            _collection: chroma_types::Collection,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
        }

        fn get_user_identity(
            &self,
            _headers: &HeaderMap,
        ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>
        {
            Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
        }
    }

    /// Captured stub-Modal request: the body the proxy forwarded plus the
    /// headers we want to assert on.
    #[derive(Debug, Clone)]
    struct Captured {
        body: Value,
        modal_key: Option<String>,
        modal_secret: Option<String>,
        chroma_token: Option<String>,
    }

    fn build_test_server(
        auth: Arc<dyn AuthenticateAndAuthorize>,
        ask_endpoint_url: Option<String>,
    ) -> FoundationApiServer {
        let mut config = FoundationApiConfig::default();
        config.foundation.ask_endpoint_url = ask_endpoint_url;
        // Short timeout so the upstream-unreachable test fails fast.
        config.foundation.ask_timeout_secs = 5;
        let sysdb = SysDb::Test(TestSysDb::new());
        let system = System::new();
        FoundationApiServer::new(config, auth, sysdb, vec![], system)
            .with_modal_creds("test-key".to_string(), "test-secret".to_string())
    }

    fn build_test_app(server: FoundationApiServer) -> Router {
        Router::new()
            .route("/api/ask", post(ask))
            .with_state(server)
    }

    /// Spawn a tiny axum server on `127.0.0.1:0` that captures the JSON
    /// body and the `Modal-Key` / `Modal-Secret` / `x-chroma-token`
    /// headers from every `/ask` POST, then returns 200 with a canned
    /// success body.
    async fn spawn_modal_stub() -> (String, Arc<Mutex<Vec<Captured>>>) {
        let captures: Arc<Mutex<Vec<Captured>>> = Arc::new(Mutex::new(Vec::new()));
        let captures_in_handler = Arc::clone(&captures);
        let app = Router::new().route(
            "/ask",
            post(
                move |headers: HeaderMap, axum::Json(body): axum::Json<Value>| {
                    let captures = Arc::clone(&captures_in_handler);
                    async move {
                        let modal_key = headers
                            .get("modal-key")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        let modal_secret = headers
                            .get("modal-secret")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        let chroma_token = headers
                            .get(CHROMA_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        captures.lock().unwrap().push(Captured {
                            body,
                            modal_key,
                            modal_secret,
                            chroma_token,
                        });
                        axum::Json(serde_json::json!({"result": "ok", "sources": []}))
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{}/ask", addr), captures)
    }

    async fn read_body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    // --------- merge_user unit tests ---------

    #[test]
    fn merge_user_overrides_caller_supplied_user() {
        let body = Bytes::from(r#"{"query":"hi","user":"attacker"}"#);
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"query": "hi", "user": "42"}));
    }

    #[test]
    fn merge_user_treats_empty_body_as_empty_object() {
        let body = Bytes::new();
        let merged = merge_user(&body, "42".to_string()).unwrap();
        assert_eq!(merged, serde_json::json!({"user": "42"}));
    }

    #[test]
    fn merge_user_rejects_non_object_body() {
        let body = Bytes::from(r#"["not","an","object"]"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, AskProxyError::InvalidBody(_)));
    }

    #[test]
    fn merge_user_rejects_invalid_json() {
        let body = Bytes::from(r#"not json"#);
        let err = merge_user(&body, "42".to_string()).unwrap_err();
        assert!(matches!(err, AskProxyError::InvalidBody(_)));
    }

    #[test]
    fn error_codes_map_to_expected_chroma_errors() {
        assert_eq!(
            AskProxyError::InvalidBody("x".to_string()).code(),
            ErrorCodes::InvalidArgument
        );
        assert_eq!(AskProxyError::Upstream.code(), ErrorCodes::Unavailable);
        assert_eq!(
            AskProxyError::MissingConfig("foo").code(),
            ErrorCodes::Internal
        );
    }

    // --------- handler integration tests ---------

    #[tokio::test]
    async fn ask_forwards_modal_creds_and_user_token_and_injects_user() {
        let (endpoint_url, captures) = spawn_modal_stub().await;
        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let server = build_test_server(auth, Some(endpoint_url));
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .header(CHROMA_TOKEN_HEADER, "caller-token-xyz")
            .body(Body::from(r#"{"query":"hi","user":"spoof"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_body_json(resp).await;
        assert_eq!(body["result"], "ok");

        let received = captures.lock().unwrap().clone();
        assert_eq!(received.len(), 1, "stub should have been called once");
        let captured = &received[0];

        // Modal service-to-service headers from server config.
        assert_eq!(captured.modal_key.as_deref(), Some("test-key"));
        assert_eq!(captured.modal_secret.as_deref(), Some("test-secret"));

        // Caller's Chroma token forwarded verbatim.
        assert_eq!(captured.chroma_token.as_deref(), Some("caller-token-xyz"));

        // Auth's user_id overrides the caller-supplied `user`.
        assert_eq!(captured.body["user"], "user_42");
        assert_eq!(captured.body["query"], "hi");
    }

    #[tokio::test]
    async fn ask_does_not_forward_chroma_token_when_caller_omits_it() {
        let (endpoint_url, captures) = spawn_modal_stub().await;
        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let server = build_test_server(auth, Some(endpoint_url));
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"hi"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let captured = captures.lock().unwrap().clone();
        assert_eq!(captured[0].chroma_token, None);
    }

    #[tokio::test]
    async fn ask_returns_401_when_auth_rejects() {
        let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(UnauthorizedAuth);
        // Endpoint URL doesn't matter — we short-circuit before forwarding.
        let server = build_test_server(auth, Some("http://127.0.0.1:1/ask".to_string()));
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"hi"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ask_returns_503_when_upstream_is_unreachable() {
        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let server = build_test_server(auth, Some("http://127.0.0.1:1/ask".to_string()));
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"hi"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn ask_returns_500_when_endpoint_url_is_missing() {
        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let server = build_test_server(auth, None);
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"hi"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn ask_returns_500_when_modal_creds_are_missing() {
        let (endpoint_url, _) = spawn_modal_stub().await;
        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let mut config = FoundationApiConfig::default();
        config.foundation.ask_endpoint_url = Some(endpoint_url);
        config.foundation.ask_timeout_secs = 5;
        let sysdb = SysDb::Test(TestSysDb::new());
        let system = System::new();
        // Construct without calling `with_modal_creds`, so both creds are
        // None unless MODAL_KEY/MODAL_SECRET happen to be set in the env.
        // To make the test deterministic, force them None.
        let mut server = FoundationApiServer::new(config, auth, sysdb, vec![], system);
        server.modal_key = None;
        server.modal_secret = None;
        let app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"hi"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn ask_relays_upstream_4xx_verbatim() {
        let app = Router::new().route(
            "/ask",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({"error": "bad_request"})),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint_url = format!("http://{}/ask", addr);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let auth: Arc<dyn AuthenticateAndAuthorize> =
            Arc::new(FakeAuth::new("user_42", "team_abc"));
        let server = build_test_server(auth, Some(endpoint_url));
        let proxy_app = build_test_app(server);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/ask")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"bogus":"body"}"#))
            .unwrap();
        let resp = proxy_app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = read_body_json(resp).await;
        assert_eq!(body["error"], "bad_request");
    }
}

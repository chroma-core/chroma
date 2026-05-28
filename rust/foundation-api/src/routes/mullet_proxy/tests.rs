use super::*;
use crate::{
    auth::{AuthError, AuthenticateAndAuthorize},
    config::FoundationApiConfig,
};
use axum::{
    body::Body,
    http::{Method, Request},
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

/// Test auth that returns a fixed identity. Captures the action and
/// resource passed to `authenticate_and_authorize` so tests can assert
/// the two-step auth gotcha (resolved tenant flows into the resource).
struct FakeAuth {
    user_id: String,
    tenant: String,
    captured_action: Mutex<Option<AuthzAction>>,
    captured_resource: Mutex<Option<AuthzResource>>,
}

impl FakeAuth {
    fn new(user_id: &str, tenant: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            tenant: tenant.to_string(),
            captured_action: Mutex::new(None),
            captured_resource: Mutex::new(None),
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
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        *self.captured_action.lock().unwrap() = Some(action);
        *self.captured_resource.lock().unwrap() = Some(resource);
        let identity = self.identity();
        Box::pin(ready(Ok(identity)))
    }

    fn authenticate_and_authorize_collection(
        &self,
        _headers: &HeaderMap,
        _action: AuthzAction,
        _resource: AuthzResource,
        _collection: chroma_types::Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        let identity = self.identity();
        Box::pin(ready(Ok(identity)))
    }

    fn get_user_identity(
        &self,
        _headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
    }

    fn authenticate_and_authorize_collection(
        &self,
        _headers: &HeaderMap,
        _action: AuthzAction,
        _resource: AuthzResource,
        _collection: chroma_types::Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
    }

    fn get_user_identity(
        &self,
        _headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(Err(AuthError(StatusCode::UNAUTHORIZED))))
    }
}

/// Build a `FoundationApiServer` for tests, pointed at the given
/// upstream `mullet_url`. Uses an in-memory `TestSysDb`; the proxy
/// handler doesn't touch sysdb so it's effectively unused, just
/// required by the constructor.
fn build_test_server(
    auth: Arc<dyn AuthenticateAndAuthorize>,
    mullet_url: String,
) -> FoundationApiServer {
    let mut config = FoundationApiConfig::default();
    config.foundation.mullet_url = mullet_url;
    // Short timeout so the upstream-unreachable test fails fast
    // rather than waiting 120s. Connection refused fails immediately
    // anyway, but this guards against environments that hang.
    config.foundation.mullet_timeout_secs = 5;
    let sysdb = SysDb::Test(TestSysDb::new());
    let system = System::new();
    FoundationApiServer::new(config, auth, sysdb, vec![], system)
}

fn build_test_app(server: FoundationApiServer) -> Router {
    Router::new()
        .route("/api/ask", post(ask))
        .with_state(server)
}

/// Spawn a tiny axum server on `127.0.0.1:0` that records every JSON
/// body it receives on `/api/ask` and returns the supplied response.
/// Returns the base URL (`http://127.0.0.1:<port>`) and the shared
/// capture buffer.
async fn spawn_mullet_stub() -> (String, Arc<Mutex<Vec<Value>>>) {
    let captures: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let captures_in_handler = Arc::clone(&captures);
    let app = Router::new().route(
        "/api/ask",
        post(move |axum::Json(body): axum::Json<Value>| {
            let captures = Arc::clone(&captures_in_handler);
            async move {
                captures.lock().unwrap().push(body);
                axum::Json(serde_json::json!({"result": "ok", "sources": []}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{}", addr), captures)
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
fn merge_user_inserts_into_empty_object() {
    let body = Bytes::from(r#"{}"#);
    let merged = merge_user(&body, "42".to_string()).unwrap();
    assert_eq!(merged, serde_json::json!({"user": "42"}));
}

#[test]
fn merge_user_overrides_caller_supplied_user() {
    let body = Bytes::from(r#"{"query":"hi","user":"attacker"}"#);
    let merged = merge_user(&body, "42".to_string()).unwrap();
    assert_eq!(merged, serde_json::json!({"query": "hi", "user": "42"}));
}

#[test]
fn merge_user_preserves_other_keys_without_email_or_tenant() {
    let body = Bytes::from(r#"{"query":"q","session_id":"s","repo":"r","source":"cli"}"#);
    let merged = merge_user(&body, "42".to_string()).unwrap();
    assert_eq!(
        merged,
        serde_json::json!({
            "query": "q",
            "session_id": "s",
            "repo": "r",
            "source": "cli",
            "user": "42",
        })
    );
    let obj = merged.as_object().unwrap();
    assert!(!obj.contains_key("email"));
    assert!(!obj.contains_key("tenant"));
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
    assert!(matches!(err, MulletProxyError::InvalidBody(_)));
}

#[test]
fn merge_user_rejects_invalid_json() {
    let body = Bytes::from(r#"not json"#);
    let err = merge_user(&body, "42".to_string()).unwrap_err();
    assert!(matches!(err, MulletProxyError::InvalidBody(_)));
}

#[test]
fn error_codes_map_to_expected_chroma_errors() {
    assert_eq!(
        MulletProxyError::InvalidBody("x".to_string()).code(),
        ErrorCodes::InvalidArgument
    );
    assert_eq!(MulletProxyError::Upstream.code(), ErrorCodes::Unavailable);
}

// --------- handler integration tests ---------

#[tokio::test]
async fn ask_injects_auth_resolved_user_and_relays_response() {
    let (mullet_url, captures) = spawn_mullet_stub().await;
    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(FakeAuth::new("user_42", "team_abc"));
    let server = build_test_server(Arc::clone(&auth), mullet_url);
    let app = build_test_app(server);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/ask")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"query":"hi","user":"spoof"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = read_body_json(resp).await;
    assert_eq!(body["result"], "ok");

    let received = captures.lock().unwrap().clone();
    assert_eq!(
        received.len(),
        1,
        "stub should have been called exactly once"
    );
    let upstream_body = &received[0];
    // Auth's user_id overrides the caller-supplied `user`.
    assert_eq!(upstream_body["user"], "user_42");
    assert_eq!(upstream_body["query"], "hi");
    // Email/tenant are intentionally not injected — see plan.
    assert!(upstream_body.get("email").is_none());
    assert!(upstream_body.get("tenant").is_none());
}

#[tokio::test]
async fn ask_passes_resolved_tenant_to_authz_check() {
    // Regression: the Cloud authz impl 403s if
    // `resource.tenant != identity.tenant` (including `tenant: None`).
    // The proxy must resolve the identity first and pass that tenant
    // explicitly to `authenticate_and_authorize`.
    let (mullet_url, _) = spawn_mullet_stub().await;
    let fake = Arc::new(FakeAuth::new("user_42", "team_abc"));
    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::clone(&fake) as _;
    let server = build_test_server(auth, mullet_url);
    let app = build_test_app(server);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/ask")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"query":"hi"}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert_eq!(
        *fake.captured_action.lock().unwrap(),
        Some(AuthzAction::ViewFoundation)
    );
    let captured = fake.captured_resource.lock().unwrap().clone().unwrap();
    assert_eq!(captured.tenant, Some("team_abc".to_string()));
    assert_eq!(captured.database, None);
    assert_eq!(captured.collection, None);
}

#[tokio::test]
async fn ask_returns_401_when_auth_rejects() {
    // Mullet URL won't be hit; point at an unbound port to confirm
    // we short-circuit before forwarding.
    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(UnauthorizedAuth);
    let server = build_test_server(auth, "http://127.0.0.1:1".to_string());
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
    // Port 1 is not listening; connection should refuse immediately.
    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(FakeAuth::new("user_42", "team_abc"));
    let server = build_test_server(auth, "http://127.0.0.1:1".to_string());
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
async fn ask_relays_mullet_4xx_verbatim() {
    // Stub that returns 400 with a mullet-style zod-error JSON body.
    let app = Router::new().route(
        "/api/ask",
        post(|| async {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "ZodError", "issues": []})),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mullet_url = format!("http://{}", addr);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(FakeAuth::new("user_42", "team_abc"));
    let server = build_test_server(auth, mullet_url);
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
    assert_eq!(body["error"], "ZodError");
}

#[tokio::test]
async fn ask_rejects_non_object_body_with_400() {
    let (mullet_url, captures) = spawn_mullet_stub().await;
    let auth: Arc<dyn AuthenticateAndAuthorize> = Arc::new(FakeAuth::new("user_42", "team_abc"));
    let server = build_test_server(auth, mullet_url);
    let app = build_test_app(server);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/ask")
        .header("content-type", "application/json")
        .body(Body::from(r#"["not","an","object"]"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(
        captures.lock().unwrap().is_empty(),
        "upstream must not be called when body validation fails",
    );
}

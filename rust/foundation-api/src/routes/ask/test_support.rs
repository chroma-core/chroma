//! Shared integration-test fixtures for the `/api/ask` handler tests.
//! `#[cfg(test)]`-only; not part of any production code path.

use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use chroma_api_types::GetUserIdentityResponse;
use chroma_sysdb::{SysDb, TestSysDb};
use chroma_system::System;
use serde_json::Value;
use std::{
    collections::HashSet,
    future::{ready, Future},
    pin::Pin,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;

use crate::{
    auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource},
    config::FoundationApiConfig,
    server::FoundationApiServer,
};

use super::handler::ask;

/// HTTP header the `/api/ask` handler accepts and forwards to Modal.
pub(super) const CHROMA_TOKEN_HEADER: &str = "x-chroma-token";

/// Test auth that returns a fixed `Ok` identity or a fixed `Err` status,
/// for every trait method. Reconstructs the identity on each call since
/// `GetUserIdentityResponse` isn't `Clone`.
pub(super) enum TestAuth {
    Ok { user_id: String, tenant: String },
    Err(StatusCode),
}

impl TestAuth {
    pub(super) fn ok(user_id: &str, tenant: &str) -> Self {
        Self::Ok {
            user_id: user_id.to_string(),
            tenant: tenant.to_string(),
        }
    }

    pub(super) fn err(code: StatusCode) -> Self {
        Self::Err(code)
    }

    fn answer(&self) -> Result<GetUserIdentityResponse, AuthError> {
        match self {
            TestAuth::Ok { user_id, tenant } => Ok(GetUserIdentityResponse {
                user_id: user_id.clone(),
                tenant: tenant.clone(),
                databases: HashSet::new(),
            }),
            TestAuth::Err(code) => Err(AuthError(*code)),
        }
    }
}

type IdentityFut = Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>>;

impl AuthenticateAndAuthorize for TestAuth {
    fn authenticate_and_authorize(
        &self,
        _h: &HeaderMap,
        _a: AuthzAction,
        _r: AuthzResource,
    ) -> IdentityFut {
        Box::pin(ready(self.answer()))
    }
    fn authenticate_and_authorize_collection(
        &self,
        _h: &HeaderMap,
        _a: AuthzAction,
        _r: AuthzResource,
        _c: chroma_types::Collection,
    ) -> IdentityFut {
        Box::pin(ready(self.answer()))
    }
    fn get_user_identity(&self, _h: &HeaderMap) -> IdentityFut {
        Box::pin(ready(self.answer()))
    }
}

/// One captured stub-Modal request.
#[derive(Debug, Clone)]
pub(super) struct Captured {
    pub(super) body: Value,
    pub(super) modal_key: Option<String>,
    pub(super) modal_secret: Option<String>,
    pub(super) chroma_token: Option<String>,
}

/// Spawn a stub Modal `/ask` server on `127.0.0.1:0`. Captures every
/// request body + header set, returns 200 with a canned success body.
pub(super) async fn spawn_modal_stub() -> (String, Arc<Mutex<Vec<Captured>>>) {
    let captures: Arc<Mutex<Vec<Captured>>> = Arc::new(Mutex::new(Vec::new()));
    let captures_for_handler = Arc::clone(&captures);
    let app = Router::new().route(
        "/ask",
        post(
            move |headers: HeaderMap, axum::Json(body): axum::Json<Value>| {
                let captures = Arc::clone(&captures_for_handler);
                async move {
                    let h = |k: &str| {
                        headers
                            .get(k)
                            .and_then(|v| v.to_str().ok())
                            .map(String::from)
                    };
                    captures.lock().unwrap().push(Captured {
                        body,
                        modal_key: h("modal-key"),
                        modal_secret: h("modal-secret"),
                        chroma_token: h(CHROMA_TOKEN_HEADER),
                    });
                    axum::Json(serde_json::json!({"result": "ok", "sources": []}))
                }
            },
        ),
    );
    (spawn_stub(app).await, captures)
}

/// Bind `app` to `127.0.0.1:0`, spawn it, return the base URL with `/ask`.
pub(super) async fn spawn_stub(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}/ask", addr)
}

/// Build a `FoundationApiServer` with the given auth + endpoint URL,
/// seeded with test Modal credentials so tests don't touch env.
pub(super) fn test_server(
    auth: Arc<dyn AuthenticateAndAuthorize>,
    ask_endpoint_url: Option<String>,
) -> FoundationApiServer {
    let mut config = FoundationApiConfig::default();
    config.foundation.ask_endpoint_url = ask_endpoint_url;
    config.foundation.ask_timeout_secs = 5;
    FoundationApiServer::new(
        config,
        auth,
        SysDb::Test(TestSysDb::new()),
        vec![],
        System::new(),
    )
    .with_modal_creds("test-key".to_string(), "test-secret".to_string())
}

/// Like `test_server` but with Modal credentials explicitly cleared, so
/// the missing-creds test is deterministic regardless of ambient
/// `MODAL_KEY` / `MODAL_SECRET` env vars.
pub(super) fn test_server_without_modal_creds(
    auth: Arc<dyn AuthenticateAndAuthorize>,
    ask_endpoint_url: Option<String>,
) -> FoundationApiServer {
    let mut server = test_server(auth, ask_endpoint_url);
    server.modal_key = None;
    server.modal_secret = None;
    server
}

/// Send `POST /api/ask` through the proxy, optionally attaching the
/// caller's Chroma token.
pub(super) async fn post_ask(
    server: FoundationApiServer,
    body: &str,
    token: Option<&str>,
) -> Response {
    let app = Router::new()
        .route("/api/ask", post(ask))
        .with_state(server);
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/api/ask")
        .header("content-type", "application/json");
    if let Some(t) = token {
        builder = builder.header(CHROMA_TOKEN_HEADER, t);
    }
    let req = builder.body(Body::from(body.to_string())).unwrap();
    app.oneshot(req).await.unwrap()
}

pub(super) async fn read_body_json(resp: Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub(super) fn ok_auth() -> Arc<dyn AuthenticateAndAuthorize> {
    Arc::new(TestAuth::ok("user_42", "team_abc"))
}

pub(super) fn unauthorized_auth() -> Arc<dyn AuthenticateAndAuthorize> {
    Arc::new(TestAuth::err(StatusCode::UNAUTHORIZED))
}

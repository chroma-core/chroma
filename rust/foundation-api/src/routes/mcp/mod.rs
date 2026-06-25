//! Foundation MCP endpoint: route wiring, CORS, and the bearer-token auth gate.
//!
//! The MCP server handler and its tools live in [`server`]; OAuth
//! protected-resource discovery lives in [`oauth`].

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        HeaderMap, HeaderValue, Method, Request, StatusCode,
    },
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rmcp::transport::streamable_http_server::{
    session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};

use crate::{
    auth::AuthzAction,
    routes::{whoami::whoami_and_authorize, CHROMA_TOKEN_HEADER},
    server::FoundationApiServer,
};

use oauth::{mcp_resource_origin, protected_resource_metadata};
use server::FoundationMcpServer;

mod oauth;
mod server;

const MCP_PATH: &str = "/mcp/foundation";
const PROTECTED_RESOURCE_METADATA_PATH: &str =
    "/.well-known/oauth-protected-resource/mcp/foundation";
const FOUNDATION_SCOPE: &str = "foundation";
const MCP_SERVER_NAME: &str = "Foundation MCP";
const MCP_SERVER_VERSION: &str = "0.1.0";

/// Builds the MCP routes. Unlike the JSON routes this needs the server value up
/// front: the rmcp [`StreamableHttpService`] is constructed once here (it is
/// cheap to clone and is mounted directly via `route_service`, the way rmcp
/// expects), and the auth layer needs the server to render the OAuth metadata
/// pointer on a 401.
pub(crate) fn router(server: FoundationApiServer) -> Router<FoundationApiServer> {
    let mcp_service = StreamableHttpService::new(
        {
            let server = server.clone();
            move || Ok(FoundationMcpServer::new(server.clone()))
        },
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig::default()
            .disable_allowed_hosts()
            .with_stateful_mode(false)
            .with_json_response(true),
    );

    // The bearer-token gate only guards the MCP endpoint; the protected-resource
    // metadata document must stay public so unauthenticated clients can discover
    // the authorization server. Keep it out of the layered sub-router.
    let mcp = Router::new()
        .route_service(MCP_PATH, mcp_service)
        .layer(from_fn_with_state(server, mcp_authenticate));

    Router::new()
        .route(
            PROTECTED_RESOURCE_METADATA_PATH,
            get(protected_resource_metadata),
        )
        .merge(mcp)
        // CORS is applied outside the auth layer so browser preflights are
        // answered before the bearer check (a preflight carries no token).
        .layer(mcp_cors())
}

/// Arbitrary-origin CORS for the public MCP machine endpoints. These are reached
/// directly by browser-based MCP clients (ChatGPT, Claude) from origins we do
/// not control, and the bearer token — not the origin — is the security
/// boundary, so any origin is permitted. `WWW-Authenticate` is exposed so the
/// browser can read the 401 challenge that points at the OAuth metadata; cookie
/// credentials are intentionally not enabled (MCP authenticates with a bearer
/// header, which also keeps the `*` origin legal).
fn mcp_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .expose_headers([WWW_AUTHENTICATE])
}

/// Bearer-token gate in front of the MCP service. MCP clients authenticate with
/// `Authorization: Bearer <token>`; downstream foundation code reads the token
/// from [`CHROMA_TOKEN_HEADER`], so translate it here. The rmcp service then
/// carries the rewritten request through to the tool handlers.
///
/// The token is *validated* here — not merely required to be present — so that
/// an expired or revoked token returns a 401 with the OAuth challenge. That 401
/// is the signal MCP clients use to silently refresh their access token; if the
/// failure were instead deferred to the tool handlers it would surface as a 200
/// JSON-RPC tool error, which clients treat as success and never refresh on.
async fn mcp_authenticate(
    State(server): State<FoundationApiServer>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(token) = bearer_token(request.headers()).map(str::to_string) else {
        return mcp_unauthorized(&server);
    };

    let Ok(value) = HeaderValue::from_str(&token) else {
        return mcp_unauthorized(&server);
    };

    request
        .headers_mut()
        .insert(CHROMA_TOKEN_HEADER, value.clone());

    // Reject expired/revoked/invalid tokens with a 401 so clients refresh. The
    // tool handlers re-run this via `authorize_and_meter` to resolve the tenant
    // and meter the call; the auth layer caches results, so the second lookup
    // is cheap.
    let mut auth_headers = HeaderMap::new();
    auth_headers.insert(CHROMA_TOKEN_HEADER, value);
    if whoami_and_authorize(&*server.auth, &auth_headers, AuthzAction::ViewFoundation)
        .await
        .is_err()
    {
        return mcp_unauthorized(&server);
    }

    next.run(request).await
}

fn mcp_unauthorized(server: &FoundationApiServer) -> Response {
    let metadata_url = format!(
        "{}{}",
        mcp_resource_origin(&server.config),
        PROTECTED_RESOURCE_METADATA_PATH
    );
    (
        StatusCode::UNAUTHORIZED,
        [(
            WWW_AUTHENTICATE,
            format!("Bearer resource_metadata=\"{metadata_url}\""),
        )],
        Json(json!({
            "jsonrpc": "2.0",
            "error": { "code": -32000, "message": "Unauthorized" },
            "id": null
        })),
    )
        .into_response()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_token_reads_authorization_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-token"),
        );
        assert_eq!(bearer_token(&headers), Some("secret-token"));
    }

    #[test]
    fn bearer_token_scheme_is_case_insensitive_and_trimmed() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("bearer   spaced  "));
        assert_eq!(bearer_token(&headers), Some("spaced"));
    }

    #[tokio::test]
    async fn mcp_cors_preflight_allows_any_origin() {
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt;

        // The CORS layer answers preflights before any handler runs, so a
        // trivial stateless route is enough to exercise its configuration.
        let app = Router::<()>::new()
            .route("/mcp/foundation", get(|| async { "ok" }))
            .layer(mcp_cors());

        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/mcp/foundation")
                    .header("Origin", "https://chatgpt.com")
                    .header("Access-Control-Request-Method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );
        let allow_methods = res
            .headers()
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(allow_methods.contains("POST"));
    }

    #[tokio::test]
    async fn mcp_cors_exposes_www_authenticate_on_actual_response() {
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt;

        let app = Router::<()>::new()
            .route("/mcp/foundation", get(|| async { "ok" }))
            .layer(mcp_cors());

        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/mcp/foundation")
                    .header("Origin", "https://chatgpt.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            res.headers().get("access-control-allow-origin").unwrap(),
            "*"
        );
        // Browser MCP clients must be able to read the bearer challenge.
        let exposed = res
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(exposed.contains("www-authenticate"));
    }
}

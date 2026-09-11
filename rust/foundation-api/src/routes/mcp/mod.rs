//! Foundation MCP endpoint: route wiring, CORS, and the bearer-token auth gate.
//!
//! The same MCP service answers on two paths. The bare one addresses the key's
//! tenant and the configured default Foundation; the prefixed one names a
//! tenant and a Foundation in the path, which is what lets one key reach any
//! Foundation in its tenant.
//!
//! The two paths take different credentials. The bare path is the OAuth
//! resource: it is the only path named by the protected-resource metadata
//! document, the only one an authorization server issues tokens for, and the
//! only one that ever answers with the challenge a browser client rediscovers
//! itself from — on a 401, which is the one refusal a fresh token lifts. The
//! prefixed path takes a Chroma API key in the bearer header, so a browser
//! client keeps using the bare path.
//!
//! The MCP server handler and its tools live in [`server`]; OAuth
//! protected-resource discovery lives in [`oauth`].

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
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
    auth::{AuthError, AuthzAction},
    routes::{
        whoami::{authorize_scope, ScopeError, ScopePolicy},
        FoundationScope, CHROMA_TOKEN_HEADER,
    },
    server::FoundationApiServer,
};

use oauth::{mcp_resource_origin, protected_resource_metadata};
use server::FoundationMcpServer;

mod oauth;
mod server;

const MCP_PATH: &str = "/mcp/foundation";
/// MCP endpoint that names its tenant and Foundation in the path.
///
/// The parameters are spelled the way the REST prefix spells them, and the
/// path is four segments where the bare one is two, so the two mounts cannot
/// collide.
const MCP_SCOPED_PATH: &str = "/mcp/f/{tenant}/{foundation}";
const PROTECTED_RESOURCE_METADATA_PATH: &str =
    "/.well-known/oauth-protected-resource/mcp/foundation";
const FOUNDATION_SCOPE: &str = "foundation";
const MCP_SERVER_NAME: &str = "Foundation MCP";
const MCP_SERVER_VERSION: &str = "0.1.0";
/// Icon advertised to MCP clients, served as raw bytes from the public repo.
const MCP_SERVER_ICON_URL: &str =
    "https://raw.githubusercontent.com/chroma-core/chroma/main/rust/foundation-api/assets/mcp-logo.png";

/// Which Foundation an MCP request addresses, as the authentication gate read
/// it off the path.
///
/// The gate inserts one of these into the request extensions, and the tools read
/// it back out of the request parts the MCP library hands them. Invariants:
/// 1. Exactly one value is inserted per request, on either mount. A tool that
///    finds none is running without the gate in front of it, and refuses rather
///    than guessing which Foundation the caller meant.
/// 2. [`McpScope::Named`] carries the pair the path spelled, after the name and
///    the tenant cleared validation. The gate authorized the caller against
///    exactly that pair, so a tool uses it verbatim and makes no second
///    authorization call.
/// 3. [`McpScope::Bare`] names no Foundation, so a tool resolves the key's
///    tenant and the configured default Foundation itself.
#[derive(Clone, Debug)]
pub(super) enum McpScope {
    /// The request arrived on the bare mount, which names no Foundation.
    Bare,
    /// The request arrived on the prefixed mount, naming this tenant and this
    /// database.
    Named { tenant: String, database: String },
}

impl McpScope {
    /// The same pair as a [`FoundationScope`], for the helpers that take one.
    pub(super) fn as_foundation_scope(&self) -> FoundationScope {
        match self {
            McpScope::Bare => FoundationScope::default(),
            McpScope::Named { tenant, database } => FoundationScope {
                tenant: Some(tenant.clone()),
                foundation: Some(database.clone()),
            },
        }
    }
}

/// Builds the MCP routes. Unlike the JSON routes this needs the server value up
/// front: the rmcp [`StreamableHttpService`] is constructed once here (it is
/// cheap to clone and is mounted directly via `route_service`, the way rmcp
/// expects), and the auth layer needs the server to render the OAuth metadata
/// pointer on a 401.
///
/// Both paths are mounted with `route_service` rather than a nested router: a
/// nested router rewrites the request URI, and the MCP library refuses a
/// request that carries neither a `Host` header nor an authority in its URI,
/// which a rewritten URI can leave it without.
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

    // The bearer-token gate only guards the MCP endpoints; the
    // protected-resource metadata document must stay public so unauthenticated
    // clients can discover the authorization server. Keep it out of the gated
    // sub-router.
    //
    // The gate is a `route_layer`, so it runs only for a request that matched
    // one of these two routes. A plain `layer` would also wrap the router's
    // fallback, and merging propagates that fallback to the whole service: an
    // unmatched path would then be answered by the bare mount's gate, so a
    // near-miss such as `/mcp/f/{tenant}/{foundation}/extra` would carry the
    // bare mount's OAuth challenge — telling an unauthenticated caller which
    // path shapes exist — and every unmatched path would cost a token round
    // trip before its 404.
    //
    // Cloning the service shares its session manager and its handler factory
    // between the two mounts rather than standing up a second pair.
    let mcp = Router::new()
        .route_service(MCP_PATH, mcp_service.clone())
        .route_service(MCP_SCOPED_PATH, mcp_service)
        .route_layer(from_fn_with_state(server, mcp_authenticate));

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

/// Bearer-token gate in front of both MCP mounts. MCP clients authenticate with
/// `Authorization: Bearer <token>`; downstream foundation code reads the token
/// from [`CHROMA_TOKEN_HEADER`], so translate it here. The rmcp service then
/// carries the rewritten request through to the tool handlers.
///
/// The token is *validated* here — not merely required to be present — so that
/// an expired or revoked token is refused with a status rather than a tool
/// error. If the failure were deferred to the tool handlers it would surface as
/// a 200 JSON-RPC tool error, which clients treat as success.
///
/// The two mounts fail differently, so each has its own gate below. Both answer
/// a refusal with its status alone; the bare one additionally carries the OAuth
/// challenge that MCP clients refresh on, and only on the 401 a refresh lifts.
async fn mcp_authenticate(
    State(server): State<FoundationApiServer>,
    Path(scope): Path<FoundationScope>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match scope.tenant.is_some() || scope.foundation.is_some() {
        true => authenticate_prefixed(server, scope, request, next).await,
        false => authenticate_bare(server, request, next).await,
    }
}

/// Gate for the bare endpoint, which names no Foundation: the empty scope
/// resolves to the key's tenant and the configured default Foundation.
///
/// A refusal carries the OAuth challenge only when a fresh access token would
/// lift it. The challenge is the signal an MCP client silently refreshes on, so
/// it belongs on a 401 and on nothing else: a client that read one on a key
/// whose Foundation permission was revoked would refresh and retry against the
/// same 403 forever instead of reporting it.
async fn authenticate_bare(
    server: FoundationApiServer,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(value) = forward_bearer_token(&mut request) else {
        return mcp_unauthorized(&server);
    };

    // The tool handlers re-run this to resolve the tenant and meter the call;
    // the auth layer caches results, so the second lookup is cheap.
    let mut auth_headers = HeaderMap::new();
    auth_headers.insert(CHROMA_TOKEN_HEADER, value);
    if let Err(err) = authorize_scope(
        &*server.auth,
        &auth_headers,
        AuthzAction::ViewFoundation,
        &FoundationScope::default(),
        &server.config.foundation.database_name,
        ScopePolicy::DefaultToConfig,
    )
    .await
    {
        return match scope_error_status(&err) {
            StatusCode::UNAUTHORIZED => mcp_unauthorized(&server),
            status => mcp_scope_error(status),
        };
    }

    request.extensions_mut().insert(McpScope::Bare);

    next.run(request).await
}

/// Gate for the prefixed endpoint, which names the tenant and Foundation it
/// addresses.
///
/// One authorization call settles three of the four questions the path raises:
/// the token is valid, the key owns the named tenant, and the key may view a
/// Foundation. The tenant check is the cross-tenant guard for this path, and it
/// lives in the authorization implementation rather than here — the Cloud one
/// refuses a resource tenant that is not the key's, while the no-op one the
/// open-source binary runs enforces nothing.
///
/// The fourth question — whether the key may view *this* Foundation rather than
/// some Foundation — is not settled here, because a Foundation permission claim
/// names no database and the authorizer accepts such a claim against any
/// database. The frontend settles it instead, on every proxied call, against the
/// database the collection actually lives in.
///
/// The pair reaches the tools through the request extensions, so they use it
/// verbatim instead of repeating the call this gate already made.
async fn authenticate_prefixed(
    server: FoundationApiServer,
    scope: FoundationScope,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(value) = forward_bearer_token(&mut request) else {
        return mcp_scope_error(StatusCode::UNAUTHORIZED);
    };

    let mut auth_headers = HeaderMap::new();
    auth_headers.insert(CHROMA_TOKEN_HEADER, value);
    let (tenant, database, _identity) = match authorize_scope(
        &*server.auth,
        &auth_headers,
        AuthzAction::ViewFoundation,
        &scope,
        &server.config.foundation.database_name,
        ScopePolicy::Required,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(err) => return mcp_scope_error(scope_error_status(&err)),
    };

    request
        .extensions_mut()
        .insert(McpScope::Named { tenant, database });

    next.run(request).await
}

/// Copies the request's bearer token onto [`CHROMA_TOKEN_HEADER`], which the
/// downstream foundation code reads, and returns it for the gate's own
/// authorization call. `None` when the request carries no usable bearer token.
fn forward_bearer_token(request: &mut Request<Body>) -> Option<HeaderValue> {
    let token = bearer_token(request.headers())?.to_string();
    let value = HeaderValue::from_str(&token).ok()?;
    request
        .headers_mut()
        .insert(CHROMA_TOKEN_HEADER, value.clone());
    Some(value)
}

/// The status a scope failure is answered with.
///
/// A refusal that turns on the request's own shape — a Foundation name or a
/// tenant that cannot be one — is a bad request. A refusal that turns on the
/// caller's credentials keeps the status the authorizer chose, so a bad or
/// expired token stays a 401 and a tenant the key does not own stays a 403.
fn scope_error_status(err: &ScopeError) -> StatusCode {
    match err {
        ScopeError::ScopeRequired
        | ScopeError::InvalidFoundation { .. }
        | ScopeError::InvalidTenant { .. } => StatusCode::BAD_REQUEST,
        ScopeError::Auth(AuthError(status)) => *status,
    }
}

/// JSON-RPC error response carrying `status` and no `WWW-Authenticate` header.
///
/// Both mounts answer through this. The prefixed mount uses it for every
/// refusal: the challenge points at the protected-resource metadata document,
/// which names the bare endpoint as the resource, so sending it from the
/// prefixed endpoint would send a client to a different resource than the one it
/// asked for. The bare mount uses it for every refusal a fresh token would not
/// lift, keeping the challenge for its 401 alone.
fn mcp_scope_error(status: StatusCode) -> Response {
    (
        status,
        Json(json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32000,
                "message": status.canonical_reason().unwrap_or("Error"),
            },
            "id": null
        })),
    )
        .into_response()
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

    use axum::body::Body;
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::System;
    use httpmock::MockServer;
    use tower::ServiceExt;

    use crate::auth::AuthenticateAndAuthorize;
    use crate::config::FoundationApiConfig;
    use crate::routes::test_auth::FakeAuth;

    /// Origin the test server advertises, so the challenge's metadata URL is a
    /// fixed string rather than a bound port.
    const PUBLIC_ORIGIN: &str = "https://foundation.example.com";

    fn test_server(
        auth: Arc<dyn AuthenticateAndAuthorize>,
        frontend_ingress_url: Option<String>,
    ) -> FoundationApiServer {
        let mut config = FoundationApiConfig::default();
        config.foundation.api_public_origin = Some(PUBLIC_ORIGIN.to_string());
        config.foundation.frontend_ingress_url = frontend_ingress_url;

        FoundationApiServer::new(
            config,
            auth,
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        )
    }

    /// The real MCP router, state and auth layer included, ready for `oneshot`.
    fn app(auth: Arc<dyn AuthenticateAndAuthorize>) -> Router {
        app_against(auth, None)
    }

    /// The real MCP router, pointed at a frontend the test controls so a tool
    /// run's data-plane call can be observed.
    fn app_against(
        auth: Arc<dyn AuthenticateAndAuthorize>,
        frontend_ingress_url: Option<String>,
    ) -> Router {
        let server = test_server(auth, frontend_ingress_url);
        router(server.clone()).with_state(server)
    }

    /// The FE path that resolves a collection by name. It carries the tenant
    /// and the database, so hitting it is proof of which pair a tool ran
    /// against.
    fn get_collection_path(tenant: &str, database: &str, collection: &str) -> String {
        format!("/api/v2/tenants/{tenant}/databases/{database}/collections/{collection}")
    }

    /// A JSON-RPC POST carrying `body`, with the headers the MCP transport
    /// demands: `accept`, `content-type`, and a `Host`, without which the
    /// transport refuses a request that also carries no authority in its URI
    /// (a real HTTP/1.1 request always carries one). `token` is the bearer
    /// token, or `None` for a request that carries no credentials at all.
    fn jsonrpc_post(uri: &str, token: Option<&str>, body: serde_json::Value) -> Request<Body> {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("host", "foundation.example.com")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        builder
            .body(Body::from(body.to_string()))
            .expect("request should build")
    }

    /// A `tools/list` call, which exercises the gate without running a tool.
    fn tools_list() -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })
    }

    /// A `read_page` call, which runs a tool all the way to its data-plane
    /// lookup.
    fn read_page_call(slug: &str) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "read_page", "arguments": { "slug": slug } },
        })
    }

    #[tokio::test]
    async fn a_bare_request_without_a_token_is_challenged() {
        // The challenge is what makes an MCP client refresh its access token,
        // so it has to name the protected-resource metadata document.
        let response = app(Arc::new(FakeAuth::new("user_99", "team_abc")))
            .oneshot(jsonrpc_post("/mcp/foundation", None, tools_list()))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response
            .headers()
            .get(WWW_AUTHENTICATE)
            .expect("the bare path should challenge")
            .to_str()
            .expect("challenge should be ascii");
        assert_eq!(
            challenge,
            format!(
                "Bearer resource_metadata=\"{PUBLIC_ORIGIN}\
                 /.well-known/oauth-protected-resource/mcp/foundation\""
            )
        );
    }

    #[tokio::test]
    async fn a_bare_request_the_key_may_not_make_is_forbidden_without_a_challenge() {
        // A refusal a fresh token cannot fix must not carry the challenge: a
        // client that read one here would refresh its access token and retry
        // against the same refusal forever rather than reporting it. The two
        // mounts have to agree on this, and the prefixed one already does.
        let auth = Arc::new(FakeAuth::refusing(StatusCode::FORBIDDEN));

        let response = app(auth)
            .oneshot(jsonrpc_post(
                "/mcp/foundation",
                Some("valid-but-unpermitted"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers().get(WWW_AUTHENTICATE), None);
    }

    #[tokio::test]
    async fn a_bare_request_with_a_rejected_token_is_still_challenged() {
        // A 401 is the one refusal a refresh does fix, so the challenge stays.
        // The stub refuses at the authorization call rather than at the identity
        // lookup the gate makes first, which reaches the same branch: the gate
        // maps whatever status the scope failure carries.
        let auth = Arc::new(FakeAuth::refusing(StatusCode::UNAUTHORIZED));

        let response = app(auth)
            .oneshot(jsonrpc_post(
                "/mcp/foundation",
                Some("rejected"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_some());
    }

    #[tokio::test]
    async fn a_prefixed_request_without_a_token_is_refused_without_a_challenge() {
        // The metadata document names the bare endpoint as the resource, so
        // advertising it here would send the client to a different resource
        // than the one it asked for.
        let response = app(Arc::new(FakeAuth::new("user_99", "team_abc")))
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc/wiki_team",
                None,
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers().get(WWW_AUTHENTICATE), None);
    }

    #[tokio::test]
    async fn a_path_tenant_the_key_does_not_own_is_forbidden() {
        let auth = Arc::new(FakeAuth::enforcing_tenant_match("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team_other/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers().get(WWW_AUTHENTICATE), None);
        assert_eq!(auth.authorize_calls(), 1);
    }

    #[tokio::test]
    async fn a_rejected_token_on_the_prefixed_path_stays_unauthorized() {
        let auth = Arc::new(FakeAuth::refusing(StatusCode::UNAUTHORIZED));

        let response = app(auth)
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc/wiki_team",
                Some("expired"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers().get(WWW_AUTHENTICATE), None);
    }

    #[tokio::test]
    async fn a_prefixed_request_authorizes_the_path_pair_in_one_call() {
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        // The service answers on the prefixed mount, so the gate let the
        // request through rather than stopping at the path.
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(auth.captured_action(), AuthzAction::ViewFoundation);
        let resource = auth.captured_resource();
        assert_eq!(resource.tenant.as_deref(), Some("team_abc"));
        assert_eq!(resource.database.as_deref(), Some("wiki_team"));
        // One authorization call settles both the permission and the tenant,
        // so the tenant is never looked up separately.
        assert_eq!(auth.authorize_calls(), 1);
        assert_eq!(auth.identity_calls(), 0);
    }

    #[tokio::test]
    async fn a_bare_request_authorizes_the_configured_default_foundation() {
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/foundation",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::OK);
        let resource = auth.captured_resource();
        assert_eq!(resource.tenant.as_deref(), Some("team_abc"));
        assert_eq!(resource.database.as_deref(), Some("FOUNDATION"));
        // The tenant is not in the URL, so it costs one identity lookup.
        assert_eq!(auth.identity_calls(), 1);
    }

    #[tokio::test]
    async fn a_tool_run_on_the_prefixed_path_reads_the_named_foundation() {
        // The end of the chain the path opens: the gate resolves the pair, the
        // MCP library carries it into the tool through the request extensions,
        // and the tool's data-plane call names it. The frontend path carries
        // both halves, so the call it receives is proof of which pair ran.
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path("team_abc", "wiki_team", "wiki"));
                then.status(404).json_body(json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;

        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));
        let response = app_against(auth.clone(), Some(mock_server.base_url()))
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc/wiki_team",
                Some("secret"),
                read_page_call("onboarding"),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(resolve.calls(), 1);
        // The gate's call is the only one: the tool reuses the pair the gate
        // resolved rather than authorizing a second time, and the tenant is
        // never looked up separately.
        assert_eq!(auth.authorize_calls(), 1);
        assert_eq!(auth.identity_calls(), 0);
    }

    #[tokio::test]
    async fn a_tool_run_on_the_bare_path_reads_the_configured_default_foundation() {
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path("team_abc", "FOUNDATION", "wiki"));
                then.status(404).json_body(json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;

        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));
        let response = app_against(auth.clone(), Some(mock_server.base_url()))
            .oneshot(jsonrpc_post(
                "/mcp/foundation",
                Some("secret"),
                read_page_call("onboarding"),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(resolve.calls(), 1);
        // The bare path carries no pair for the tool to reuse, so the gate and
        // the tool each resolve and authorize it. The auth layer caches, so the
        // second pair of calls is cheap.
        assert_eq!(auth.authorize_calls(), 2);
        assert_eq!(auth.identity_calls(), 2);
    }

    #[tokio::test]
    async fn a_percent_encoded_separator_in_the_tenant_is_refused() {
        // A client that builds this path from unchecked values can be steered
        // at another Foundation, and percent-encoding is what a check that
        // merely lists forbidden characters misses: a proxy can normalize the
        // escape back into a separator after the check passed. The framework
        // decodes a path parameter before the validator runs, so the validator
        // sees the separator and refuses.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc%2F..%2Fteam_other/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers().get(WWW_AUTHENTICATE), None);
        // Refused on the request's shape alone, before any authorization call.
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_near_miss_path_is_not_answered_by_the_gate() {
        // A path that matches neither mount must fall through to a plain 404.
        // It must not carry the bare mount's challenge, which would tell an
        // unauthenticated caller which path shapes exist, and it must not spend
        // a token round trip on its way to the 404.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        for uri in ["/mcp/f/team_abc/wiki_team/extra", "/mcp/f/team_abc"] {
            let response = app(auth.clone())
                .oneshot(jsonrpc_post(uri, Some("secret"), tools_list()))
                .await
                .expect("router should answer");

            assert_eq!(response.status(), StatusCode::NOT_FOUND, "for {uri}");
            assert_eq!(response.headers().get(WWW_AUTHENTICATE), None, "for {uri}");
        }
        assert_eq!(auth.authorize_calls(), 0);
        assert_eq!(auth.identity_calls(), 0);
    }

    #[tokio::test]
    async fn a_request_target_that_is_not_a_path_costs_no_authorization() {
        // `OPTIONS *` carries no path for the scope extractor to read. CORS
        // answers it before the gate, so the gate never meets a request target
        // its extractor cannot parse.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("*")
                    .header("host", "foundation.example.com")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn an_empty_tenant_segment_is_refused() {
        // The prefixed route matches an empty segment, so the tenant validator
        // is what stops it rather than the router.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f//wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_tenant_that_is_only_a_traversal_is_refused() {
        // `..` carries no separator of its own, so a check listing forbidden
        // characters lets it through; resolving the data-plane path then drops
        // the segment and addresses a shorter path than the URL spells.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/%2e%2e/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_doubly_encoded_separator_in_the_tenant_is_refused() {
        // One round of decoding turns `%252F` into the text `%2F`, which
        // carries no separator yet and so clears a check that only lists
        // forbidden characters. Whatever decodes the data-plane URL next turns
        // it into one, so the tenant must clear a check that no second
        // decoding can change.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc%252F..%252Fteam_other/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn an_escape_that_is_not_utf8_is_refused() {
        // Decoding runs before the gate, so a segment that is not valid UTF-8
        // once decoded never reaches the validators or the authorizer.
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team%FFabc/wiki_team",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[tokio::test]
    async fn a_percent_encoded_traversal_in_the_foundation_is_refused() {
        let auth = Arc::new(FakeAuth::new("user_99", "team_abc"));

        let response = app(auth.clone())
            .oneshot(jsonrpc_post(
                "/mcp/f/team_abc/wiki%2e%2e%2fother",
                Some("secret"),
                tools_list(),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(auth.authorize_calls(), 0);
    }

    #[test]
    fn a_credential_failure_keeps_the_status_the_authorizer_chose() {
        assert_eq!(
            scope_error_status(&ScopeError::Auth(AuthError(StatusCode::FORBIDDEN))),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            scope_error_status(&ScopeError::Auth(AuthError(StatusCode::UNAUTHORIZED))),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn a_malformed_path_is_a_bad_request() {
        assert_eq!(
            scope_error_status(&ScopeError::InvalidFoundation {
                name: "my..db".to_string(),
                message: "invalid".to_string(),
            }),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            scope_error_status(&ScopeError::InvalidTenant {
                name: "a/b".to_string(),
                message: "invalid".to_string(),
            }),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            scope_error_status(&ScopeError::ScopeRequired),
            StatusCode::BAD_REQUEST
        );
    }

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

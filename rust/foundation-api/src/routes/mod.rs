use crate::server::FoundationApiServer;
use axum::response::sse::Event;
use axum::{
    http::HeaderMap,
    routing::{get, post, MethodRouter},
    Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
pub(crate) mod foundations;
pub(crate) mod init;
pub(crate) mod init_schema;
pub(crate) mod links;
pub(crate) mod mcp;
pub(crate) mod read_page;
pub(crate) mod search;
pub(crate) mod subagent_search;
#[cfg(test)]
mod test_auth;
pub(crate) mod trajectories;
pub(crate) mod upsert_page;
pub(super) mod whoami;

/// Path prefix that names a request's tenant and Foundation.
///
/// A request reaches the same handler at its bare `/api/...` path or under this
/// prefix; the prefix is what lets one key address any Foundation in its
/// tenant.
pub(crate) const SCOPE_PREFIX: &str = "/api/f/{tenant}/{foundation}";

/// The tenant and Foundation a request named in its path. Both fields are
/// absent on a bare `/api/...` request, which means "the key's tenant and the
/// configured default Foundation".
///
/// Deserialized with `Path<FoundationScope>`, never `Option<Path<_>>`. axum
/// records an empty parameter set on a route that declares no parameters, and
/// deserializes a struct from that set as a map, so a struct whose every field
/// is optional resolves to its default on a bare path and to the named pair on
/// a prefixed one. The option wrapper buys nothing here: it answers `None` only
/// when deserialization reports zero parameters, which a struct never does
/// because it defaults them instead.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct FoundationScope {
    #[serde(default)]
    pub(crate) tenant: Option<String>,
    #[serde(default)]
    pub(crate) foundation: Option<String>,
}

/// The path parameters of a trajectory route: the trajectory id plus the same
/// optional scope every route carries.
///
/// A handler may declare one path extractor, so the trajectory routes carry
/// their id and their scope in one struct rather than two.
#[derive(Debug, Deserialize)]
pub(crate) struct TrajectoryScope {
    pub(crate) id: Uuid,
    #[serde(default)]
    pub(crate) tenant: Option<String>,
    #[serde(default)]
    pub(crate) foundation: Option<String>,
}

impl TrajectoryScope {
    /// The scope half of the path parameters, for [`whoami::authorize_scope`].
    pub(crate) fn scope(&self) -> FoundationScope {
        FoundationScope {
            tenant: self.tenant.clone(),
            foundation: self.foundation.clone(),
        }
    }
}

/// The web origin that page links are built from, or `None` when no link can be
/// built for this request.
///
/// The page-redirect route resolves a tenant and a slug and has no Foundation
/// parameter, so every link it builds opens the default Foundation's page. A
/// request addressing any other Foundation therefore gets no link, because the
/// link would resolve to the wrong page. Naming the default Foundation in the
/// path is not one of those cases: it addresses the same database the bare path
/// does, so it keeps its links and the two paths answer alike.
pub(crate) fn ui_origin_for<'a>(
    server: &'a FoundationApiServer,
    scope: &FoundationScope,
) -> Option<&'a str> {
    let foundation = &server.config.foundation;
    match scope.foundation.as_deref() {
        Some(named) if named != foundation.database_name => None,
        _ => foundation.foundation_ui_origin.as_deref(),
    }
}

/// The scope policy a write route runs under.
///
/// A write that names no Foundation resolves to the configured default one
/// while `require_scope_for_writes` is unset, and is refused with a 400 once it
/// is set.
pub(in crate::routes) fn write_scope_policy(server: &FoundationApiServer) -> whoami::ScopePolicy {
    if server.config.foundation.require_scope_for_writes {
        whoami::ScopePolicy::Required
    } else {
        whoami::ScopePolicy::DefaultToConfig
    }
}

/// Registers `handler` at both `/api{suffix}` and `{SCOPE_PREFIX}{suffix}`.
///
/// Both registrations share one `MethodRouter`, which is `Clone`, so the two
/// paths cannot drift onto different handlers and the prefixed path spells the
/// scope parameters exactly once. Spelling them differently across routes makes
/// the router panic when it is built.
fn dual(
    router: Router<FoundationApiServer>,
    suffix: &str,
    handler: MethodRouter<FoundationApiServer>,
) -> Router<FoundationApiServer> {
    router
        .route(&format!("/api{suffix}"), handler.clone())
        .route(&format!("{SCOPE_PREFIX}{suffix}"), handler)
}

pub(crate) fn router() -> Router<FoundationApiServer> {
    // Initialize is registered only at its bare path, so the database it
    // provisions is always the configured default. It creates a database, seven
    // collections and two attached functions while checking only the initialize
    // permission, so a prefixed registration would let any key holding that
    // permission create unlimited arbitrarily-named databases in its tenant
    // without holding the create-database permission. Provisioning a
    // caller-named Foundation needs a route that checks for that permission.
    let router = Router::new().route("/api/init", post(init::foundation_init));
    // The Foundation CRUD routes occupy the static segment `foundations`, which
    // is reserved as a Foundation name for exactly that reason: a path segment
    // that matches a static route never falls through to the parameter route
    // beside it, so a Foundation carrying this name could not be addressed.
    let router = router
        .route(
            "/api/f/{tenant}/foundations",
            post(foundations::foundation_create).get(foundations::foundation_list),
        )
        .route(
            "/api/f/{tenant}/foundations/{name}",
            get(foundations::foundation_describe),
        );
    let router = dual(
        router,
        "/upsert-page",
        post(upsert_page::foundation_upsert_page),
    );
    let router = dual(
        router,
        "/apply-patch",
        post(apply_patch::foundation_apply_patch),
    );
    let router = dual(router, "/search", post(search::foundation_search));
    let router = dual(router, "/read-page", post(read_page::foundation_read_page));
    let router = dual(
        router,
        "/trajectories/save",
        post(trajectories::foundation_save_trajectory),
    );
    let router = dual(
        router,
        "/trajectories/open",
        post(trajectories::foundation_open_trajectory),
    );
    let router = dual(
        router,
        "/trajectories/{id}/entries",
        post(trajectories::foundation_append_trajectory_entries),
    );
    let router = dual(
        router,
        "/trajectories/{id}/finalize",
        post(trajectories::foundation_finalize_trajectory),
    );
    let router = dual(
        router,
        "/trajectories/{id}/reasoning",
        get(trajectories::foundation_get_trajectory_reasoning),
    );
    let router = dual(
        router,
        "/trajectories/{id}",
        get(trajectories::foundation_get_trajectory),
    );
    let router = dual(
        router,
        "/subagent_search",
        post(subagent_search::foundation_subagent_search),
    );
    dual(router, "/agent", post(agent::foundation_agent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FoundationApiConfig;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use chroma_sysdb::{SysDb, TestSysDb};
    use chroma_system::System;
    use httpmock::MockServer;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// The tenant the no-op auth impl reports for every caller, which is what a
    /// bare path resolves to.
    const DEFAULT_TENANT: &str = "default_tenant";

    fn test_server(
        frontend_ingress_url: String,
        require_scope_for_writes: bool,
    ) -> FoundationApiServer {
        let mut config = FoundationApiConfig::default();
        config.foundation.frontend_ingress_url = Some(frontend_ingress_url);
        config.foundation.require_scope_for_writes = require_scope_for_writes;

        FoundationApiServer::new(
            config,
            Arc::new(()),
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        )
    }

    /// The FE path that resolves a collection by name. It carries the tenant and
    /// the database, so hitting it is proof of which pair a route resolved to.
    fn get_collection_path(tenant: &str, database: &str, collection: &str) -> String {
        format!("/api/v2/tenants/{tenant}/databases/{database}/collections/{collection}")
    }

    /// A mock matching every request, so a test can assert that a route refused
    /// a request before it reached the frontend.
    async fn any_request_mock(mock_server: &MockServer) -> httpmock::Mock<'_> {
        mock_server
            .mock_async(|when, then| {
                when.any_request();
                then.status(500);
            })
            .await
    }

    fn json_post(uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(CHROMA_TOKEN_HEADER, "user-token")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request should build")
    }

    fn upsert_body() -> serde_json::Value {
        serde_json::json!({
            "slug": "onboarding",
            "content": "# Onboarding",
            "source_ids": [],
            "categories": [],
            "last_written_by": "00000000-0000-0000-0000-000000000001",
            "expected_version": 0,
        })
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(uri)
            .header(CHROMA_TOKEN_HEADER, "user-token")
            .body(Body::empty())
            .expect("request should build")
    }

    #[test]
    fn router_builds_without_conflicting_routes() {
        // Two routes that spell the same path, or a prefixed route whose
        // parameters differ from its siblings', make the router panic as it is
        // built.
        let _ = router();
    }

    #[tokio::test]
    async fn a_prefixed_read_route_reaches_the_handler_with_the_path_pair() {
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path("team-1", "other_foundation", "wiki"));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/other_foundation/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn a_bare_read_route_resolves_to_the_key_tenant_and_configured_database() {
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path(DEFAULT_TENANT, "FOUNDATION", "wiki"));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        app.oneshot(json_post(
            "/api/search",
            serde_json::json!({ "query": "onboarding" }),
        ))
        .await
        .expect("router should answer");

        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn a_bare_write_is_refused_once_the_scope_is_required() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), true));

        let response = app
            .oneshot(json_post("/api/upsert-page", upsert_body()))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // The refusal is decided on the request's shape, so nothing downstream
        // is contacted.
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn a_bare_write_is_accepted_while_the_scope_is_optional() {
        // This is the shipped default: the policy lands disabled because two
        // clients still post to bare paths. A change that made the scope
        // unconditionally required would break both on deploy, and every other
        // write test here runs with the flag on.
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path(DEFAULT_TENANT, "FOUNDATION", "wiki"));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post("/api/upsert-page", upsert_body()))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn a_bare_trajectory_write_is_refused_once_the_scope_is_required() {
        // Every write route reads the same policy, so the refusal must not be
        // specific to upsert-page.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), true));

        let response = app
            .oneshot(json_post(
                "/api/trajectories/open",
                serde_json::json!({
                    "trajectory": {
                        "id": "00000000-0000-0000-0000-000000000001",
                        "entries": [],
                    },
                }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[test]
    fn only_a_foundation_the_redirect_cannot_resolve_loses_its_page_links() {
        // The page-redirect route carries no Foundation, so it always resolves
        // to the default one. Naming that same Foundation in the path must
        // therefore keep its links: it addresses the database the bare path
        // does, and the two must answer alike.
        let mut config = FoundationApiConfig::default();
        config.foundation.foundation_ui_origin = Some("https://wiki.example.com".to_string());
        let server = FoundationApiServer::new(
            config,
            Arc::new(()),
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        );

        let named = |foundation: &str| FoundationScope {
            tenant: Some("team-1".to_string()),
            foundation: Some(foundation.to_string()),
        };

        assert_eq!(
            ui_origin_for(&server, &FoundationScope::default()),
            Some("https://wiki.example.com")
        );
        assert_eq!(
            ui_origin_for(&server, &named("FOUNDATION")),
            Some("https://wiki.example.com")
        );
        assert_eq!(ui_origin_for(&server, &named("other_foundation")), None);
    }

    #[test]
    fn the_write_policy_follows_the_config_flag() {
        let mock_url = "https://foundation-fe.internal".to_string();
        assert_eq!(
            write_scope_policy(&test_server(mock_url.clone(), false)),
            whoami::ScopePolicy::DefaultToConfig
        );
        assert_eq!(
            write_scope_policy(&test_server(mock_url, true)),
            whoami::ScopePolicy::Required
        );
    }

    #[tokio::test]
    async fn a_prefixed_write_is_accepted_once_the_scope_is_required() {
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path("team-1", "other_foundation", "wiki"));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), true));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/other_foundation/upsert-page",
                upsert_body(),
            ))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn a_prefixed_trajectory_route_extracts_both_the_id_and_the_scope() {
        // Regression guard for the path extractor: one handler may declare one
        // path extractor, so the id and the scope arrive in a single struct. A
        // handler that asked for them separately would reject this request
        // before reaching the collection lookup.
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET").path(get_collection_path(
                    "team-1",
                    "other_foundation",
                    "generate_trajectories",
                ));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(get(
                "/api/f/team-1/other_foundation/trajectories/00000000-0000-0000-0000-000000000001",
            ))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn a_bare_trajectory_route_still_extracts_its_id() {
        let mock_server = MockServer::start_async().await;
        let resolve = mock_server
            .mock_async(|when, then| {
                when.method("GET").path(get_collection_path(
                    DEFAULT_TENANT,
                    "FOUNDATION",
                    "generate_trajectories",
                ));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(get(
                "/api/trajectories/00000000-0000-0000-0000-000000000001",
            ))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resolve.calls(), 1);
    }

    #[tokio::test]
    async fn an_invalid_foundation_name_in_the_path_is_a_bad_request() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/my..db/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn a_percent_encoded_topology_prefix_is_still_rejected() {
        // axum percent-decodes a path parameter before the handler sees it, so
        // the ban on `+` has to hold against `%2B` as well or a caller could
        // address a different database than the name spells.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/topo%2Bdatabase/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn the_reserved_foundations_segment_belongs_to_the_crud_routes() {
        // A segment that matches a static route never falls through to the
        // parameter route beside it, so `/api/f/{tenant}/foundations/...` is
        // always the describe route and never a Foundation named `foundations`.
        // Describe serves GET alone, so a page write under that name is refused
        // on its method before any handler runs. This is what the name
        // reservation in the validator protects against.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/foundations/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn a_deeper_path_under_the_reserved_segment_is_refused_by_the_validator() {
        // A path the CRUD routes do not spell falls back to the parameter
        // route, which reads `foundations` as a Foundation name. The name
        // validator is what refuses it there, so the reservation has to hold in
        // the validator and not only in the route table.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(get(
                "/api/f/team-1/foundations/trajectories/00000000-0000-0000-0000-000000000001",
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn the_three_foundation_crud_paths_resolve() {
        // Each assertion below proves the request reached its handler and got an
        // answer that only that handler produces. A path the router does not
        // know answers 404 with an empty body, which none of these is.
        //
        // The tenant in the path is the one the caller's key belongs to,
        // because list and describe reach only the caller's own tenant.
        let mock_server = MockServer::start_async().await;
        let search = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{DEFAULT_TENANT}/collections"))
                    .query_param("name", "wiki");
                then.status(200).json_body(serde_json::json!([]));
            })
            .await;
        let database = mock_server
            .mock_async(|when, then| {
                when.method("GET").path(format!(
                    "/api/v2/tenants/{DEFAULT_TENANT}/databases/wiki_team"
                ));
                then.status(200).json_body(serde_json::json!({
                    "id": "8f1c0a3e-0b6d-4a2f-9a1e-2f0c6d4b8a11",
                    "name": "wiki_team",
                    "tenant": DEFAULT_TENANT,
                }));
            })
            .await;
        let wiki = mock_server
            .mock_async(|when, then| {
                when.method("GET")
                    .path(get_collection_path(DEFAULT_TENANT, "wiki_team", "wiki"));
                then.status(404).json_body(serde_json::json!({
                    "error": "NotFoundError",
                    "message": "collection not found",
                }));
            })
            .await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let listed = app
            .clone()
            .oneshot(get(&format!("/api/f/{DEFAULT_TENANT}/foundations")))
            .await
            .expect("router should answer");
        assert_eq!(listed.status(), StatusCode::OK);
        assert_eq!(search.calls(), 1);

        // No function endpoint is configured, so create reaches its own
        // configuration error rather than a routing miss.
        let created = app
            .clone()
            .oneshot(json_post(
                &format!("/api/f/{DEFAULT_TENANT}/foundations"),
                serde_json::json!({ "name": "wiki_team" }),
            ))
            .await
            .expect("router should answer");
        assert_eq!(created.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // Describe answers for the database the frontend holds, and reports it
        // as no Foundation because the frontend holds no wiki collection in it.
        let described = app
            .oneshot(get(&format!(
                "/api/f/{DEFAULT_TENANT}/foundations/wiki_team"
            )))
            .await
            .expect("router should answer");
        assert_eq!(described.status(), StatusCode::OK);
        let body = axum::body::to_bytes(described.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let described: serde_json::Value =
            serde_json::from_slice(&body).expect("describe should answer with JSON");
        assert_eq!(described["name"], "wiki_team");
        assert_eq!(described["tenant"], DEFAULT_TENANT);
        assert_eq!(described["provisioned"], false);
        assert_eq!(database.calls(), 1);
        assert_eq!(wiki.calls(), 1);
    }

    #[tokio::test]
    async fn the_bare_init_route_still_extracts_an_empty_scope() {
        // `/api/init` declares no path parameters, so its `Path<FoundationScope>`
        // has to resolve to the default rather than reject the request. The
        // handler is reached when the response carries its own configuration
        // error instead of a path-extraction rejection.
        let mock_server = MockServer::start_async().await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post("/api/init", serde_json::json!({})))
            .await
            .expect("router should answer");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("function_endpoint_url"),
            "expected the handler's own error, got: {body}"
        );
    }

    #[tokio::test]
    async fn init_is_reachable_only_at_its_bare_path() {
        // Initialize creates a database while checking only the initialize
        // permission, so it must not be callable with a tenant and Foundation
        // chosen by the caller.
        let mock_server = MockServer::start_async().await;
        let app = router().with_state(test_server(mock_server.base_url(), false));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/other_foundation/init",
                serde_json::json!({}),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

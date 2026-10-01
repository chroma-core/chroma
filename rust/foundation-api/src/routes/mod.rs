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
/// Shared by the SSE routes (`/api/tenants/{tenant}/foundations/{foundation}/agent`, `/api/tenants/{tenant}/foundations/{foundation}/subagent_search`): they each
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
/// This prefix lets one key address any authorized Foundation in its tenant.
pub(crate) const SCOPE_PREFIX: &str = "/api/tenants/{tenant}/foundations/{foundation}";

/// The tenant and Foundation a request names in its path. Path extraction
/// requires both fields, so no request selects a Foundation implicitly.
#[derive(Debug, Deserialize)]
pub(crate) struct FoundationScope {
    pub(crate) tenant: String,
    pub(crate) foundation: String,
}

/// The path parameters of a trajectory route: the trajectory id plus its
/// required tenant and Foundation.
///
/// A handler may declare one path extractor, so the trajectory routes carry
/// their id and their scope in one struct rather than two.
#[derive(Debug, Deserialize)]
pub(crate) struct TrajectoryScope {
    pub(crate) id: Uuid,
    pub(crate) tenant: String,
    pub(crate) foundation: String,
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
/// path is not one of those cases: it addresses the default database.
pub(crate) fn ui_origin_for<'a>(
    server: &'a FoundationApiServer,
    scope: &FoundationScope,
) -> Option<&'a str> {
    let foundation = &server.config.foundation;
    match scope.foundation.as_str() {
        named if named == foundation.database_name => foundation.foundation_ui_origin.as_deref(),
        _ => None,
    }
}

/// Registers `handler` at the route that explicitly names its tenant and
/// Foundation. Every record request must carry that pair in its URL.
fn scoped(
    router: Router<FoundationApiServer>,
    suffix: &str,
    handler: MethodRouter<FoundationApiServer>,
) -> Router<FoundationApiServer> {
    router.route(&format!("{SCOPE_PREFIX}{suffix}"), handler)
}

pub(crate) fn router() -> Router<FoundationApiServer> {
    // Default initialization checks the initialize permission. Creating any
    // other Foundation also requires database-creation permission on the
    // lifecycle route.
    let router = Router::new().route(&format!("{SCOPE_PREFIX}/init"), post(init::foundation_init));
    // Lifecycle routes name the tenant and Foundation resources explicitly.
    let router = router
        .route(
            "/api/tenants/{tenant}/foundations",
            post(foundations::foundation_create).get(foundations::foundation_list),
        )
        .route(
            "/api/tenants/{tenant}/foundations/{foundation}",
            get(foundations::foundation_describe),
        );
    let router = scoped(
        router,
        "/upsert-page",
        post(upsert_page::foundation_upsert_page),
    );
    let router = scoped(
        router,
        "/apply-patch",
        post(apply_patch::foundation_apply_patch),
    );
    let router = scoped(router, "/search", post(search::foundation_search));
    let router = scoped(router, "/read-page", post(read_page::foundation_read_page));
    let router = scoped(
        router,
        "/trajectories/save",
        post(trajectories::foundation_save_trajectory),
    );
    let router = scoped(
        router,
        "/trajectories/open",
        post(trajectories::foundation_open_trajectory),
    );
    let router = scoped(
        router,
        "/trajectories/{id}/entries",
        post(trajectories::foundation_append_trajectory_entries),
    );
    let router = scoped(
        router,
        "/trajectories/{id}/finalize",
        post(trajectories::foundation_finalize_trajectory),
    );
    let router = scoped(
        router,
        "/trajectories/{id}/reasoning",
        get(trajectories::foundation_get_trajectory_reasoning),
    );
    let router = scoped(
        router,
        "/trajectories/{id}",
        get(trajectories::foundation_get_trajectory),
    );
    let router = scoped(
        router,
        "/subagent_search",
        post(subagent_search::foundation_subagent_search),
    );
    scoped(router, "/agent", post(agent::foundation_agent))
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

    /// The tenant the no-op auth impl reports for every caller.
    const DEFAULT_TENANT: &str = "default_tenant";

    fn test_server(frontend_ingress_url: String) -> FoundationApiServer {
        let mut config = FoundationApiConfig::default();
        config.foundation.frontend_ingress_url = Some(frontend_ingress_url);

        FoundationApiServer::new(
            config,
            Arc::new(()),
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        )
    }

    async fn registered_test_server(mock: &MockServer) -> FoundationApiServer {
        use crate::registry::{FoundationRegistry, ReserveFoundation};
        let registry = Arc::new(foundations::tests::MemoryRegistry::default());
        for (tenant, name) in [
            (DEFAULT_TENANT, "FOUNDATION"),
            ("team-1", "other_foundation"),
            (DEFAULT_TENANT, "wiki_team"),
        ] {
            let record = registry
                .reserve(
                    &HeaderMap::new(),
                    ReserveFoundation {
                        tenant: tenant.into(),
                        name: name.into(),
                        database_id: None,
                    },
                )
                .await
                .unwrap();
            registry
                .mark_ready(
                    &HeaderMap::new(),
                    tenant,
                    name,
                    record.id,
                    record.database_id,
                )
                .await
                .unwrap();
            mock.mock_async(|when, then| {
                when.method("GET")
                    .path(format!("/api/v2/tenants/{tenant}/databases/{name}"));
                then.status(200).json_body(
                    serde_json::json!({"id":record.database_id,"name":name,"tenant":tenant}),
                );
            })
            .await;
        }
        test_server(mock.base_url()).with_foundation_registry(registry)
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
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/team-1/foundations/other_foundation/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_ne!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(resolve.calls(), 1);
    }

    #[test]
    fn only_a_foundation_the_redirect_cannot_resolve_loses_its_page_links() {
        // The page-redirect route carries no Foundation, so it always resolves
        // to the default one. Naming that same Foundation in the path must
        // therefore keep its links: it addresses the default database.
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
            tenant: "team-1".to_string(),
            foundation: foundation.to_string(),
        };

        assert_eq!(
            ui_origin_for(&server, &named("FOUNDATION")),
            Some("https://wiki.example.com")
        );
        assert_eq!(ui_origin_for(&server, &named("other_foundation")), None);
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
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/team-1/foundations/other_foundation/upsert-page",
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
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(get(
                "/api/tenants/team-1/foundations/other_foundation/trajectories/00000000-0000-0000-0000-000000000001",
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
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/team-1/foundations/my..db/read-page",
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
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/team-1/foundations/topo%2Bdatabase/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn the_reserved_foundations_segment_is_rejected() {
        // The explicit hierarchy preserves the product-reserved name rule.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/team-1/foundations/foundations/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn a_deeper_path_under_the_reserved_segment_is_refused_by_the_validator() {
        // The same reserved-name rule applies to every memory route.
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(get(
                "/api/tenants/team-1/foundations/foundations/trajectories/00000000-0000-0000-0000-000000000001",
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn the_three_foundation_crud_paths_resolve() {
        let mock_server = MockServer::start_async().await;
        let app = router().with_state(registered_test_server(&mock_server).await);
        let listed = app
            .clone()
            .oneshot(get(&format!("/api/tenants/{DEFAULT_TENANT}/foundations")))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed["foundations"].as_array().unwrap().len(), 2);

        // Creation reaches the configuration check before provisioning.
        let created = app
            .clone()
            .oneshot(json_post(
                &format!("/api/tenants/{DEFAULT_TENANT}/foundations"),
                serde_json::json!({ "name": "wiki_team" }),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let described = app
            .oneshot(get(&format!(
                "/api/tenants/{DEFAULT_TENANT}/foundations/wiki_team"
            )))
            .await
            .unwrap();
        assert_eq!(described.status(), StatusCode::OK);
        let body = axum::body::to_bytes(described.into_body(), usize::MAX)
            .await
            .unwrap();
        let described: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(described["name"], "wiki_team");
        assert_eq!(described["tenant"], DEFAULT_TENANT);
        assert_eq!(described["provisioned"], true);
        assert_eq!(described["storage_available"], true);
    }

    #[tokio::test]
    async fn abbreviated_lifecycle_paths_are_not_registered() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url()));

        for uri in [
            "/api/f/team-1/foundations",
            "/api/f/team-1/foundations/wiki_team",
        ] {
            let response = app
                .clone()
                .oneshot(get(uri))
                .await
                .expect("router should answer");
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        let response = app
            .oneshot(json_post(
                "/api/f/team-1/foundations",
                serde_json::json!({ "name": "wiki_team" }),
            ))
            .await
            .expect("router should answer");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn explicit_default_init_reaches_provisioning() {
        let mock_server = MockServer::start_async().await;
        let app = router().with_state(registered_test_server(&mock_server).await);

        let response = app
            .oneshot(json_post(
                "/api/tenants/default_tenant/foundations/FOUNDATION/init",
                serde_json::json!({}),
            ))
            .await
            .expect("router should answer");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert!(
            String::from_utf8_lossy(&body).contains("function_endpoint_url"),
            "the explicit default route should reach the shared provisioner"
        );
    }

    #[tokio::test]
    async fn explicit_init_cannot_create_a_named_foundation() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url()));

        let response = app
            .oneshot(json_post(
                "/api/tenants/default_tenant/foundations/other_foundation/init",
                serde_json::json!({}),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn legacy_implicit_routes_are_not_registered() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url()));

        for path in [
            "/api/init",
            "/api/search",
            "/api/read-page",
            "/api/upsert-page",
            "/api/apply-patch",
            "/api/subagent_search",
            "/api/agent",
            "/api/trajectories/open",
            "/api/trajectories/save",
            "/api/trajectories/00000000-0000-0000-0000-000000000001",
        ] {
            let response = app
                .clone()
                .oneshot(json_post(path, serde_json::json!({})))
                .await
                .expect("router should answer");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
        assert_eq!(downstream.calls(), 0);
    }

    #[tokio::test]
    async fn abbreviated_scope_paths_are_not_registered() {
        let mock_server = MockServer::start_async().await;
        let downstream = any_request_mock(&mock_server).await;
        let app = router().with_state(test_server(mock_server.base_url()));

        let response = app
            .oneshot(json_post(
                "/api/f/team-1/other_foundation/read-page",
                serde_json::json!({ "slug": "onboarding" }),
            ))
            .await
            .expect("router should answer");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(downstream.calls(), 0);
    }
}

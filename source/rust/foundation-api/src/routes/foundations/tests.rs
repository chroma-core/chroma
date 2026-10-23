mod provisioning;

use super::*;
use crate::{
    config::FoundationApiConfig,
    registry::{FoundationPage, FoundationRegistry},
    routes::test_auth::{expect_ok, server_with_config, FakeAuth},
};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::{SysDb, TestSysDb};
use httpmock::MockServer;
use std::sync::{Arc, Mutex};

const TENANT: &str = "team_1";

/// A deterministic registry for testing provisioning and failure recovery.
/// Production access filtering is exercised by the hosted catalog tests.
#[derive(Default)]
pub(in crate::routes) struct MemoryRegistry {
    records: Mutex<Vec<FoundationRecord>>,
}
#[async_trait]
impl FoundationRegistry for MemoryRegistry {
    async fn reserve(
        &self,
        _: &HeaderMap,
        request: ReserveFoundation,
    ) -> Result<FoundationRecord, RegistryError> {
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records
            .iter()
            .find(|record| record.tenant == request.tenant && record.name == request.name)
        {
            if request
                .database_id
                .is_some_and(|id| id != record.database_id)
            {
                return Err(RegistryError::Conflict);
            }
            return Ok(record.clone());
        }
        let record = FoundationRecord {
            id: Uuid::new_v4(),
            tenant: request.tenant,
            name: request.name,
            database_id: request.database_id.unwrap_or_else(Uuid::new_v4),
            state: FoundationState::Provisioning,
            created_at: "2026-09-21T00:00:00Z".into(),
            updated_at: "2026-09-21T00:00:00Z".into(),
        };
        records.push(record.clone());
        Ok(record)
    }
    async fn get(
        &self,
        _: &HeaderMap,
        tenant: &str,
        name: &str,
    ) -> Result<FoundationRecord, RegistryError> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.tenant == tenant && record.name == name)
            .cloned()
            .ok_or(RegistryError::NotFound)
    }
    async fn list(
        &self,
        _: &HeaderMap,
        tenant: &str,
        limit: u32,
        offset: u32,
    ) -> Result<FoundationPage, RegistryError> {
        let mut records: Vec<_> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.tenant == tenant)
            .cloned()
            .collect();
        records.sort_by(|a, b| a.name.cmp(&b.name));
        let next_offset =
            (records.len() > offset as usize + limit as usize).then_some(offset + limit);
        Ok(FoundationPage {
            foundations: records
                .into_iter()
                .skip(offset as usize)
                .take(limit as usize)
                .collect(),
            next_offset,
        })
    }
    async fn mark_ready(
        &self,
        _: &HeaderMap,
        tenant: &str,
        name: &str,
        id: Uuid,
        database_id: Uuid,
    ) -> Result<FoundationRecord, RegistryError> {
        let mut records = self.records.lock().unwrap();
        let record = records
            .iter_mut()
            .find(|r| {
                r.tenant == tenant && r.name == name && r.id == id && r.database_id == database_id
            })
            .ok_or(RegistryError::Conflict)?;
        record.state = FoundationState::Ready;
        Ok(record.clone())
    }
}

fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-chroma-token", "ck-token".parse().unwrap());
    headers
}
fn setup(mock: &MockServer, registry: Arc<MemoryRegistry>) -> FoundationApiServer {
    let mut config = FoundationApiConfig::default();
    config.foundation.frontend_ingress_url = Some(mock.base_url());
    server_with_config(
        config,
        Arc::new(FakeAuth::new("user_1", TENANT)),
        SysDb::Test(TestSysDb::new()),
    )
    .with_foundation_registry(registry)
}
async fn reserve(registry: &MemoryRegistry, name: &str) -> FoundationRecord {
    registry
        .reserve(
            &headers(),
            ReserveFoundation {
                tenant: TENANT.into(),
                name: name.into(),
                database_id: None,
            },
        )
        .await
        .unwrap()
}
async fn ready(registry: &MemoryRegistry, name: &str) -> FoundationRecord {
    let record = reserve(registry, name).await;
    registry
        .mark_ready(&headers(), TENANT, name, record.id, record.database_id)
        .await
        .unwrap()
}
fn assert_error<T>(result: Result<T, ServerError>, code: ErrorCodes) {
    match result {
        Ok(_) => panic!("expected error"),
        Err(error) => assert_eq!(error.0.code(), code),
    }
}

#[tokio::test]
async fn reservation_retry_keeps_both_identities() {
    let registry = MemoryRegistry::default();
    let first = reserve(&registry, "alice").await;
    let retry = reserve(&registry, "alice").await;
    assert_eq!(first, retry);
    assert!(matches!(
        registry
            .reserve(
                &headers(),
                ReserveFoundation {
                    tenant: TENANT.into(),
                    name: "alice".into(),
                    database_id: Some(Uuid::new_v4())
                }
            )
            .await,
        Err(RegistryError::Conflict)
    ));
}

#[tokio::test]
async fn named_creation_does_not_adopt_an_existing_database() {
    let mut sysdb = SysDb::Test(TestSysDb::new());
    let name = DatabaseName::new("alice").unwrap();
    let original_id = Uuid::new_v4();
    sysdb
        .create_database(original_id, name.clone(), TENANT.into())
        .await
        .unwrap();
    assert_error(
        crate::collections::ensure_reserved_database(
            &mut sysdb,
            name.clone(),
            TENANT.into(),
            Uuid::new_v4(),
        )
        .await,
        ErrorCodes::AlreadyExists,
    );
    assert_eq!(
        sysdb.get_database(name, TENANT.into()).await.unwrap().id,
        original_id
    );
}

#[tokio::test]
async fn partial_provisioning_retry_resumes_the_reserved_database() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let server = setup(&mock, registry.clone());
    let first = expect_ok(
        reserve_for_provisioning(&server, &headers(), TENANT, "alice", false).await,
        "reserve",
    );
    let mut sysdb = server.sysdb.clone();
    let name = DatabaseName::new("alice").unwrap();
    expect_ok(
        crate::collections::ensure_reserved_database(
            &mut sysdb,
            name.clone(),
            TENANT.into(),
            first.database_id,
        )
        .await,
        "create",
    );
    let retry = expect_ok(
        reserve_for_provisioning(&server, &headers(), TENANT, "alice", false).await,
        "retry",
    );
    let id = expect_ok(
        crate::collections::ensure_reserved_database(
            &mut sysdb,
            name,
            TENANT.into(),
            retry.database_id,
        )
        .await,
        "resume",
    );
    assert_eq!(first.id, retry.id);
    assert_eq!(id, first.database_id);
    assert_eq!(
        registry
            .get(&headers(), TENANT, "alice")
            .await
            .unwrap()
            .state,
        FoundationState::Provisioning
    );
}

#[tokio::test]
async fn default_init_adopts_only_the_frontend_verified_database() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let server = setup(&mock, registry.clone());
    let name = &server.config.foundation.database_name;
    let id = Uuid::new_v4();
    let lookup = mock
        .mock_async(|when, then| {
            when.method("GET")
                .path(format!("/api/v2/tenants/{TENANT}/databases/{name}"))
                .header("x-chroma-token", "ck-token");
            then.status(200)
                .json_body(serde_json::json!({"id":id,"name":name,"tenant":TENANT}));
        })
        .await;
    let record = expect_ok(
        reserve_for_provisioning(&server, &headers(), TENANT, name, true).await,
        "adopt",
    );
    assert_eq!(record.database_id, id);
    lookup.assert_calls_async(1).await;
    assert_error(
        reserve_for_provisioning(&server, &headers(), TENANT, "alice", true).await,
        ErrorCodes::AlreadyExists,
    );
}

#[tokio::test]
async fn catalog_identity_survives_a_missing_wiki_collection() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let record = ready(&registry, "alice").await;
    let server = setup(&mock, registry);
    mock.mock_async(|when, then| {
        when.method("GET")
            .path(format!("/api/v2/tenants/{TENANT}/databases/alice"));
        then.status(200)
            .json_body(serde_json::json!({"id":record.database_id,"name":"alice","tenant":TENANT}));
    })
    .await;
    let wiki = mock
        .mock_async(|when, then| {
            when.method("GET").path(format!(
                "/api/v2/tenants/{TENANT}/databases/alice/collections/wiki"
            ));
            then.status(404);
        })
        .await;
    let result = expect_ok(
        foundation_describe(
            headers(),
            State(server),
            Path(FoundationPath {
                tenant: TENANT.into(),
                name: "alice".into(),
            }),
        )
        .await,
        "describe",
    )
    .0;
    assert_eq!(result.foundation.id, record.id);
    assert!(result.provisioned);
    assert!(result.storage_available);
    wiki.assert_calls_async(0).await;
}

#[tokio::test]
async fn reused_name_never_resolves_to_the_replacement_database() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    ready(&registry, "alice").await;
    let server = setup(&mock, registry);
    mock.mock_async(|when, then| {
        when.method("GET")
            .path(format!("/api/v2/tenants/{TENANT}/databases/alice"));
        then.status(200)
            .json_body(serde_json::json!({"id":Uuid::new_v4(),"name":"alice","tenant":TENANT}));
    })
    .await;
    assert_error(
        require_ready_foundation(&server, &headers(), TENANT, "alice").await,
        ErrorCodes::Unavailable,
    );
}

#[tokio::test]
async fn ready_foundation_with_missing_storage_is_not_recreated() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    ready(&registry, "alice").await;
    let server = setup(&mock, registry);
    mock.mock_async(|when, then| {
        when.method("GET");
        then.status(404)
            .json_body(serde_json::json!({"error":"NotFoundError","message":"missing"}));
    })
    .await;
    assert_error(
        reserve_for_provisioning(&server, &headers(), TENANT, "alice", false).await,
        ErrorCodes::Unavailable,
    );
}

#[tokio::test]
async fn catalog_listing_pages_without_consulting_collections() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    reserve(&registry, "alice").await;
    reserve(&registry, "bob").await;
    let server = setup(&mock, registry);
    let first = expect_ok(
        foundation_list(
            headers(),
            State(server.clone()),
            Path(TenantPath {
                tenant: TENANT.into(),
            }),
            Query(ListFoundationParams {
                limit: 1,
                offset: 0,
            }),
        )
        .await,
        "list",
    )
    .0;
    assert_eq!(first.foundations[0].name, "alice");
    assert_eq!(first.next_offset, Some(1));
    let second = expect_ok(
        foundation_list(
            headers(),
            State(server),
            Path(TenantPath {
                tenant: TENANT.into(),
            }),
            Query(ListFoundationParams {
                limit: 1,
                offset: 1,
            }),
        )
        .await,
        "list second",
    )
    .0;
    assert_eq!(second.foundations[0].name, "bob");
    assert_eq!(second.next_offset, None);
}

#[tokio::test]
async fn unavailable_registry_never_falls_back_to_storage() {
    let mock = MockServer::start_async().await;
    let server = setup(&mock, Arc::new(MemoryRegistry::default()))
        .with_foundation_registry(Arc::new(crate::registry::UnconfiguredRegistry));
    assert_error(
        foundation_list(
            headers(),
            State(server.clone()),
            Path(TenantPath {
                tenant: TENANT.into(),
            }),
            Query(ListFoundationParams {
                limit: 100,
                offset: 0,
            }),
        )
        .await,
        ErrorCodes::Unavailable,
    );
    assert_error(
        require_ready_foundation(&server, &headers(), TENANT, "alice").await,
        ErrorCodes::Unavailable,
    );
}

#[tokio::test]
async fn creation_checks_both_permissions_before_reserving() {
    use crate::auth::AuthzAction;
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let auth = Arc::new(FakeAuth::new("user_1", TENANT));
    let mut config = FoundationApiConfig::default();
    config.foundation.frontend_ingress_url = Some(mock.base_url());
    let server = server_with_config(config, auth.clone(), SysDb::Test(TestSysDb::new()))
        .with_foundation_registry(registry.clone());
    // The absent function endpoint fails after permission checks and before
    // reservation; no incomplete Foundation is created for invalid config.
    let result = foundation_create(
        headers(),
        State(server),
        Path(TenantPath {
            tenant: TENANT.into(),
        }),
        Query(FoundationInitParams::default()),
        Json(CreateFoundationRequest {
            name: "alice".into(),
        }),
    )
    .await;
    assert_error(result, ErrorCodes::Internal);
    let actions: Vec<_> = auth
        .authorizations()
        .into_iter()
        .map(|(action, _)| action)
        .collect();
    assert_eq!(
        actions,
        vec![AuthzAction::CreateDatabase, AuthzAction::InitFoundation]
    );
    assert!(registry.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cross_tenant_listing_is_refused_before_catalog_lookup() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    reserve(&registry, "alice").await;
    let server = setup(&mock, registry);
    assert_error(
        foundation_list(
            headers(),
            State(server),
            Path(TenantPath {
                tenant: "another_team".into(),
            }),
            Query(ListFoundationParams {
                limit: 100,
                offset: 0,
            }),
        )
        .await,
        ErrorCodes::PermissionDenied,
    );
}

#[tokio::test]
async fn a_database_and_wiki_without_a_catalog_record_are_not_a_foundation() {
    let mock = MockServer::start_async().await;
    let all_reads = mock
        .mock_async(|when, then| {
            when.any_request();
            then.status(200);
        })
        .await;
    let server = setup(&mock, Arc::new(MemoryRegistry::default()));
    assert_error(
        require_ready_foundation(&server, &headers(), TENANT, "alice").await,
        ErrorCodes::NotFound,
    );
    all_reads.assert_calls_async(0).await;
}

#[tokio::test]
async fn frontend_outage_is_an_error_instead_of_absent_storage() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    ready(&registry, "alice").await;
    let server = setup(&mock, registry);
    mock.mock_async(|when, then| {
        when.method("GET");
        then.status(503).json_body(
            serde_json::json!({"error":"Unavailable","message":"control plane unavailable"}),
        );
    })
    .await;
    assert!(foundation_describe(
        headers(),
        State(server),
        Path(FoundationPath {
            tenant: TENANT.into(),
            name: "alice".into()
        })
    )
    .await
    .is_err());
}

#[tokio::test]
async fn frontend_refusal_cannot_be_reported_as_missing_storage() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    ready(&registry, "alice").await;
    let server = setup(&mock, registry);
    mock.mock_async(|when, then| {
        when.method("GET");
        then.status(403)
            .json_body(serde_json::json!({"error":"AuthError","message":"denied"}));
    })
    .await;
    assert_error(
        foundation_describe(
            headers(),
            State(server),
            Path(FoundationPath {
                tenant: TENANT.into(),
                name: "alice".into(),
            }),
        )
        .await,
        ErrorCodes::PermissionDenied,
    );
}

#[tokio::test]
async fn default_init_cannot_rebind_an_existing_identity() {
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let server = setup(&mock, registry.clone());
    let name = &server.config.foundation.database_name;
    let record = ready(&registry, name).await;
    mock.mock_async(|when, then| {
        when.method("GET")
            .path(format!("/api/v2/tenants/{TENANT}/databases/{name}"));
        then.status(200)
            .json_body(serde_json::json!({"id":Uuid::new_v4(),"name":name,"tenant":TENANT}));
    })
    .await;
    assert_error(
        reserve_for_provisioning(&server, &headers(), TENANT, name, true).await,
        ErrorCodes::AlreadyExists,
    );
    assert_eq!(
        registry.get(&headers(), TENANT, name).await.unwrap(),
        record
    );
}

#[tokio::test]
async fn a_warm_collection_cache_cannot_bypass_default_identity_validation() {
    use crate::auth::AuthzAction;
    use crate::routes::whoami::{authorize_registered_scope, ScopePolicy};
    use chroma_types::{Collection, CollectionUuid};
    let mock = MockServer::start_async().await;
    let registry = Arc::new(MemoryRegistry::default());
    let server = setup(&mock, registry.clone());
    let name = &server.config.foundation.database_name;
    ready(&registry, name).await;
    let collection = Collection {
        collection_id: CollectionUuid::new(),
        name: "wiki".into(),
        tenant: TENANT.into(),
        database: name.clone(),
        ..Default::default()
    };
    let collection_lookup = mock
        .mock_async(|when, then| {
            when.method("GET").path(format!(
                "/api/v2/tenants/{TENANT}/databases/{name}/collections/wiki"
            ));
            then.status(200)
                .json_body(serde_json::to_value(&collection).unwrap());
        })
        .await;
    server
        .foundation_chroma_client
        .as_ref()
        .unwrap()
        .wiki_collection(TENANT, name, "ck-token")
        .await
        .unwrap();
    mock.mock_async(|when, then| {
        when.method("GET")
            .path(format!("/api/v2/tenants/{TENANT}/databases/{name}"));
        then.status(200)
            .json_body(serde_json::json!({"id":Uuid::new_v4(),"name":name}));
    })
    .await;
    // The default API aliases and MCP tools share this exact resolver.
    assert_error(
        authorize_registered_scope(
            &server,
            &headers(),
            AuthzAction::ViewFoundation,
            &FoundationScope::default(),
            name,
            ScopePolicy::DefaultToConfig,
        )
        .await,
        ErrorCodes::Unavailable,
    );
    collection_lookup.assert_calls_async(1).await;
}

#[tokio::test]
async fn provisioning_pause_refuses_both_routes_without_catalog_or_storage_mutation() {
    use crate::routes::init::foundation_init;
    use axum::response::IntoResponse;

    let registry = Arc::new(MemoryRegistry::default());
    let mut config = FoundationApiConfig::default();
    config.foundation.provisioning_paused = true;
    // The pause applies even without a configured frontend or function endpoint.
    let server = server_with_config(
        config,
        Arc::new(FakeAuth::new("user_1", TENANT)),
        SysDb::Test(TestSysDb::new()),
    )
    .with_foundation_registry(registry.clone());
    let results = [
        foundation_init(
            headers(),
            State(server.clone()),
            Path(FoundationScope::default()),
            Query(FoundationInitParams::default()),
        )
        .await,
        foundation_create(
            headers(),
            State(server.clone()),
            Path(TenantPath {
                tenant: TENANT.into(),
            }),
            Query(FoundationInitParams::default()),
            Json(CreateFoundationRequest {
                name: "alice".into(),
            }),
        )
        .await,
    ];
    for result in results {
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("provisioning must be refused while paused"),
        };
        assert_eq!(error.0.code(), ErrorCodes::Unavailable);
        assert_eq!(
            error.into_response().status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert!(registry.records.lock().unwrap().is_empty());
    let mut sysdb = server.sysdb.clone();
    for name in ["FOUNDATION", "alice"] {
        assert!(matches!(
            sysdb
                .get_database(DatabaseName::new(name).unwrap(), TENANT.into())
                .await,
            Err(chroma_types::GetDatabaseError::NotFound(_))
        ));
    }
    // Catalog reads remain available during the pause.
    let page = expect_ok(
        foundation_list(
            headers(),
            State(server),
            Path(TenantPath {
                tenant: TENANT.into(),
            }),
            Query(ListFoundationParams {
                limit: 100,
                offset: 0,
            }),
        )
        .await,
        "listing while paused",
    );
    assert!(page.foundations.is_empty());
}

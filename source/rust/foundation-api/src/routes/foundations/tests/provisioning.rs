use super::*;
use chroma_sysdb::{DatabaseOrTopology, GetCollectionsOptions};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, PartialEq, Eq)]
struct StorageSnapshot {
    database_id: Uuid,
    collections: BTreeMap<String, Uuid>,
    attachments: Vec<(Uuid, Uuid, Uuid)>,
}

/// Records the actual storage graph at the registry-completion boundary.
/// Injecting failure here exercises the public provisioning flow after all
/// storage operations have succeeded, before the catalog becomes ready.
struct CompletionRegistry {
    inner: MemoryRegistry,
    sysdb: SysDb,
    fail_completion: AtomicBool,
    snapshots: Mutex<Vec<StorageSnapshot>>,
}

impl CompletionRegistry {
    async fn snapshot(&self, tenant: &str, name: &str) -> StorageSnapshot {
        let mut sysdb = self.sysdb.clone();
        let database_name = DatabaseName::new(name).unwrap();
        let database = sysdb
            .get_database(database_name.clone(), tenant.into())
            .await
            .unwrap();
        let collections = sysdb
            .get_collections(GetCollectionsOptions {
                tenant: Some(tenant.into()),
                database_or_topology: Some(DatabaseOrTopology::Database(database_name)),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut attachments = Vec::new();
        for collection in &collections {
            assert_eq!(collection.database_id.0, database.id);
            let functions = sysdb
                .get_attached_functions(None, Some(collection.collection_id), vec![], false)
                .await
                .unwrap();
            for function in functions {
                assert_eq!(function.tenant_id, tenant);
                assert_eq!(function.database_id, name);
                let output = function
                    .output_collection_id
                    .expect("every attachment is finished before registry completion");
                assert!(collections
                    .iter()
                    .any(|collection| collection.collection_id == output));
                attachments.push((function.id.0, function.input_collection_id.0, output.0));
            }
        }
        attachments.sort();
        StorageSnapshot {
            database_id: database.id,
            collections: collections
                .into_iter()
                .map(|collection| (collection.name, collection.collection_id.0))
                .collect(),
            attachments,
        }
    }
}

#[async_trait]
impl FoundationRegistry for CompletionRegistry {
    async fn reserve(
        &self,
        headers: &HeaderMap,
        request: ReserveFoundation,
    ) -> Result<FoundationRecord, RegistryError> {
        self.inner.reserve(headers, request).await
    }
    async fn get(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        name: &str,
    ) -> Result<FoundationRecord, RegistryError> {
        self.inner.get(headers, tenant, name).await
    }
    async fn list(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        limit: u32,
        offset: u32,
    ) -> Result<FoundationPage, RegistryError> {
        self.inner.list(headers, tenant, limit, offset).await
    }
    async fn mark_ready(
        &self,
        headers: &HeaderMap,
        tenant: &str,
        name: &str,
        id: Uuid,
        database_id: Uuid,
    ) -> Result<FoundationRecord, RegistryError> {
        let record = self.inner.get(headers, tenant, name).await?;
        assert_eq!(record.state, FoundationState::Provisioning);
        let snapshot = self.snapshot(tenant, name).await;
        assert_eq!(snapshot.database_id, database_id);
        let expected = [
            "wiki",
            "wiki_revisions",
            "generate_trajectories",
            "currents",
            "file_uploads_user_1",
            "agent_sessions_user_1",
            "slack_raw",
            "notion",
            "gdrive",
            "granola",
        ];
        let mut names: Vec<_> = snapshot.collections.keys().map(String::as_str).collect();
        names.sort();
        let mut expected = expected.to_vec();
        expected.sort();
        assert_eq!(names, expected);
        // Revision history and currents consume wiki. Foundation generation
        // consumes slack_raw, each indexed source, and the user's sessions.
        assert_eq!(snapshot.attachments.len(), 7);
        self.snapshots.lock().unwrap().push(snapshot);
        if self.fail_completion.swap(false, Ordering::SeqCst) {
            return Err(RegistryError::Unavailable(
                "injected completion failure".into(),
            ));
        }
        self.inner
            .mark_ready(headers, tenant, name, id, database_id)
            .await
    }
}

fn provisioning_server(fail_completion: bool) -> (FoundationApiServer, Arc<CompletionRegistry>) {
    let sysdb = SysDb::Test(TestSysDb::new());
    let registry = Arc::new(CompletionRegistry {
        inner: MemoryRegistry::default(),
        sysdb: sysdb.clone(),
        fail_completion: AtomicBool::new(fail_completion),
        snapshots: Mutex::new(Vec::new()),
    });
    let mut config = FoundationApiConfig::default();
    // These endpoints are stored as function configuration; provisioning never
    // invokes them. Every storage operation runs against the in-memory sysdb.
    config.foundation.function_endpoint_url = Some("https://wiki.example".into());
    config.foundation.enable_currents_function = true;
    let server = server_with_config(config, Arc::new(FakeAuth::new("user_1", TENANT)), sysdb)
        .with_foundation_registry(registry.clone());
    (server, registry)
}

async fn provision(server: &FoundationApiServer) -> Result<FoundationInitResponse, ServerError> {
    provision_foundation(
        server,
        &headers(),
        TENANT.into(),
        "user_1".into(),
        DatabaseName::new("alice").unwrap(),
        false,
        false,
    )
    .await
}

#[tokio::test]
async fn shared_provisioning_marks_ready_only_after_the_complete_storage_graph_exists() {
    let (server, registry) = provisioning_server(false);
    let response = expect_ok(provision(&server).await, "full provisioning");
    let record = registry.get(&headers(), TENANT, "alice").await.unwrap();
    assert_eq!(record.state, FoundationState::Ready);
    assert_eq!(response.foundation_id, record.id.to_string());
    assert_eq!(response.database_id, record.database_id.to_string());
    assert!(!response.already_initialized);
    let snapshots = registry.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].collections["wiki"].to_string(),
        response.wiki_collection_id
    );
    assert_eq!(
        snapshots[0].collections["slack_raw"].to_string(),
        response.slack_raw_collection_id
    );
}

#[tokio::test]
async fn completion_failure_retries_full_provisioning_without_replacing_any_identity() {
    let (server, registry) = provisioning_server(true);
    assert_error(provision(&server).await, ErrorCodes::Unavailable);
    let reserved = registry.get(&headers(), TENANT, "alice").await.unwrap();
    assert_eq!(reserved.state, FoundationState::Provisioning);
    assert_eq!(registry.snapshots.lock().unwrap().len(), 1);

    let response = expect_ok(provision(&server).await, "retry after completion failure");
    let ready = registry.get(&headers(), TENANT, "alice").await.unwrap();
    assert_eq!(ready.state, FoundationState::Ready);
    assert_eq!(ready.id, reserved.id);
    assert_eq!(ready.database_id, reserved.database_id);
    assert_eq!(response.foundation_id, reserved.id.to_string());
    assert!(response.already_initialized);
    let snapshots = registry.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(
        snapshots[0], snapshots[1],
        "retry preserves database, collection, and attached-function identities"
    );
}

#[tokio::test]
async fn an_interrupted_attachment_finish_resumes_without_restarting_ready_functions() {
    // Exercise every finish boundary: revision history, currents, the base
    // generation attachment, and each additional input collection.
    for interrupted_call in 1..=7 {
        let (server, registry) = provisioning_server(false);
        let SysDb::Test(mut test_sysdb) = server.sysdb.clone() else {
            unreachable!()
        };
        test_sysdb.fail_finish_attached_function_on_call(interrupted_call);
        assert_error(provision(&server).await, ErrorCodes::Internal);
        let reserved = registry.get(&headers(), TENANT, "alice").await.unwrap();
        assert_eq!(reserved.state, FoundationState::Provisioning);
        assert!(
            registry.snapshots.lock().unwrap().is_empty(),
            "incomplete storage cannot reach mark-ready"
        );
        let mut sysdb = server.sysdb.clone();
        let partial = sysdb
            .get_collections(GetCollectionsOptions {
                tenant: Some(TENANT.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut partial_attachments = Vec::new();
        for collection in &partial {
            partial_attachments.extend(
                sysdb
                    .get_attached_functions(None, Some(collection.collection_id), vec![], false)
                    .await
                    .unwrap(),
            );
        }
        assert!(partial_attachments
            .iter()
            .any(|function| function.output_collection_id.is_none()));

        let response = expect_ok(provision(&server).await, "resume interrupted attachment");
        let ready = registry.get(&headers(), TENANT, "alice").await.unwrap();
        assert_eq!(ready.state, FoundationState::Ready);
        assert_eq!(ready.id, reserved.id);
        assert_eq!(ready.database_id, reserved.database_id);
        assert_eq!(response.foundation_id, reserved.id.to_string());
        let snapshots = registry.snapshots.lock().unwrap();
        assert_eq!(snapshots.len(), 1);
        for collection in partial {
            assert_eq!(
                snapshots[0].collections[&collection.name],
                collection.collection_id.0
            );
        }
        for function in partial_attachments {
            assert!(snapshots[0]
                .attachments
                .iter()
                .any(|(id, input, _)| *id == function.id.0
                    && *input == function.input_collection_id.0));
        }
        assert_eq!(test_sysdb.finish_attached_function_calls(),8,"seven completed inputs plus one interrupted call; ready inputs are never finished again");
    }
}

use crate::{
    config::GarbageCollectorConfig, garbage_collector_orchestrator::GarbageCollectorOrchestrator,
};
use crate::{
    garbage_collector_orchestrator::{GarbageCollectorError, GarbageCollectorResponse},
    types::CleanupMode,
};
use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_storage::Storage;
use chroma_sysdb::{CollectionToGcInfo, SysDb, SysDbConfig};
use chroma_system::{
    Component, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
};
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use opentelemetry::metrics::{Counter, Histogram};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    str::FromStr,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tracing::{instrument, span, Instrument, Span};
use uuid::Uuid;

#[allow(dead_code)]
pub(crate) struct GarbageCollector {
    gc_interval_mins: u64,
    relative_cutoff_time: Duration,
    max_collections_to_gc: u32,
    disabled_collections: HashSet<CollectionUuid>,
    sysdb_client: SysDb,
    storage: Storage,
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    system: Option<chroma_system::System>,
    default_cleanup_mode: CleanupMode,
    tenant_mode_overrides: Option<HashMap<String, CleanupMode>>,
    total_jobs_metric: Counter<u64>,
    job_duration_ms_metric: Histogram<u64>,
    total_files_deleted_metric: Counter<u64>,
    total_versions_deleted_metric: Counter<u64>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[derive(Debug, Error)]
enum GarbageCollectCollectionError {
    #[error("Uninitialized: missing dispatcher or system")]
    Uninitialized,
    #[error("Failed to run garbage collection orchestrator: {0}")]
    OrchestratorError(#[from] GarbageCollectorError),
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollector {
    pub fn new(
        gc_interval_mins: u64,
        relative_cutoff_time: Duration,
        max_collections_to_gc: u32,
        disabled_collections: HashSet<CollectionUuid>,
        sysdb_client: SysDb,
        storage: Storage,
        default_cleanup_mode: CleanupMode,
        tenant_mode_overrides: Option<HashMap<String, CleanupMode>>,
    ) -> Self {
        let meter = opentelemetry::global::meter("chroma");

        Self {
            gc_interval_mins,
            relative_cutoff_time,
            max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
            dispatcher: None,
            system: None,
            default_cleanup_mode,
            tenant_mode_overrides,
            total_jobs_metric: meter
                .u64_counter("garbage_collector.total_jobs")
                .with_description("Total number of garbage collection jobs executed")
                .build(),
            job_duration_ms_metric: meter
                .u64_histogram("garbage_collector.job_duration_ms")
                .with_description("Duration of garbage collection jobs in milliseconds")
                .with_unit("ms")
                .build(),
            total_files_deleted_metric: meter
                .u64_counter("garbage_collector.total_files_deleted")
                .with_description("Total number of files deleted during garbage collection")
                .build(),
            total_versions_deleted_metric: meter
                .u64_counter("garbage_collector.total_versions_deleted")
                .with_description("Total number of versions deleted during garbage collection")
                .build(),
        }
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: chroma_system::System) {
        self.system = Some(system);
    }

    #[instrument]
    async fn garbage_collect_collection(
        &self,
        absolute_cutoff_time: DateTime<Utc>,
        collection: CollectionToGcInfo,
        cleanup_mode: CleanupMode,
    ) -> Result<GarbageCollectorResponse, GarbageCollectCollectionError> {
        if let Some(dispatcher) = self.dispatcher.as_ref() {
            let orchestrator = GarbageCollectorOrchestrator::new(
                collection.id,
                collection.version_file_path,
                absolute_cutoff_time,
                self.sysdb_client.clone(),
                dispatcher.clone(),
                self.storage.clone(),
                cleanup_mode,
            );

            if let Some(system) = self.system.as_ref() {
                let started_at = SystemTime::now();
                let result = orchestrator.run(system.clone()).await?;
                let duration_ms = started_at
                    .elapsed()
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                self.job_duration_ms_metric.record(duration_ms, &[]);
                self.total_files_deleted_metric.add(
                    result.deletion_list.len() as u64,
                    &[opentelemetry::KeyValue::new(
                        "cleanup_mode",
                        format!("{:?}", cleanup_mode),
                    )],
                );
                self.total_versions_deleted_metric.add(
                    result.num_versions_deleted as u64,
                    &[opentelemetry::KeyValue::new(
                        "cleanup_mode",
                        format!("{:?}", cleanup_mode),
                    )],
                );
                return Ok(result);
            }
        }

        Err(GarbageCollectCollectionError::Uninitialized)
    }
}

#[async_trait]
impl Component for GarbageCollector {
    fn get_name() -> &'static str {
        "GarbageCollector"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        ctx.scheduler.schedule(
            GarbageCollectMessage {},
            Duration::from_secs(self.gc_interval_mins * 60),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );
    }

    fn on_stop_timeout(&self) -> Duration {
        // NOTE: Increased timeout for remaining jobs to finish
        Duration::from_secs(60)
    }
}

#[derive(Debug)]
struct GarbageCollectMessage {}

#[async_trait]
impl Handler<GarbageCollectMessage> for GarbageCollector {
    type Result = ();

    async fn handle(
        &mut self,
        _message: GarbageCollectMessage,
        ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        let absolute_cutoff_time =
            DateTime::<Utc>::from(SystemTime::now() - self.relative_cutoff_time);
        tracing::info!(
            "Using absolute cutoff time: {} (relative cutoff time: {:?})",
            absolute_cutoff_time,
            self.relative_cutoff_time
        );

        // Get all collections to gc and create gc orchestrator for each.
        tracing::info!("Getting collections to gc");
        let collections_to_gc = self
            .sysdb_client
            .get_collections_to_gc(
                Some(absolute_cutoff_time.into()),
                Some(self.max_collections_to_gc.into()),
            )
            .await
            .expect("Failed to get collections to gc");
        tracing::info!("Got {} collections to gc", collections_to_gc.len());

        let mut jobs = FuturesUnordered::new();
        for collection in collections_to_gc {
            if self.disabled_collections.contains(&collection.id) {
                tracing::warn!(
                    "Skipping garbage collection for disabled collection: {}",
                    collection.id
                );
                continue;
            }

            tracing::info!(
                "Processing collection: {} (tenant: {}, version_file_path: {})",
                collection.id,
                collection.tenant,
                collection.version_file_path
            );

            let cleanup_mode = if let Some(tenant_mode_overrides) = &self.tenant_mode_overrides {
                tenant_mode_overrides
                    .get(&collection.tenant)
                    .cloned()
                    .unwrap_or(self.default_cleanup_mode)
            } else {
                self.default_cleanup_mode
            };

            tracing::info!("Creating gc orchestrator for collection: {}", collection.id);

            let instrumented_span = span!(parent: None, tracing::Level::INFO, "Garbage collection job", collection_id = ?collection.id, tenant_id = %collection.tenant, cleanup_mode = ?cleanup_mode);
            instrumented_span.follows_from(Span::current());

            jobs.push(
                self.garbage_collect_collection(absolute_cutoff_time, collection, cleanup_mode)
                    .instrument(instrumented_span),
            );
        }
        tracing::info!("GC {} jobs", jobs.len());
        let mut num_completed_jobs = 0;
        let mut num_failed_jobs = 0;
        while let Some(job) = jobs.next().await {
            match job {
                Ok(result) => {
                    tracing::info!("Garbage collection completed. Deleted {} files over {} versions for collection {}.", result.deletion_list.len(), result.num_versions_deleted, result.collection_id);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    tracing::info!("Garbage collection failed: {:?}", e);
                    num_failed_jobs += 1;
                }
            }
        }
        tracing::info!(
            "Completed {} jobs, failed {} jobs",
            num_completed_jobs,
            num_failed_jobs
        );

        self.total_jobs_metric.add(
            num_completed_jobs as u64,
            &[opentelemetry::KeyValue::new("status", "success")],
        );
        self.total_jobs_metric.add(
            num_failed_jobs as u64,
            &[opentelemetry::KeyValue::new("status", "failure")],
        );

        // Schedule next run
        ctx.scheduler.schedule(
            GarbageCollectMessage {},
            Duration::from_secs(self.gc_interval_mins * 60),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );
    }
}

#[async_trait]
impl Configurable<GarbageCollectorConfig> for GarbageCollector {
    async fn try_from_config(
        config: &GarbageCollectorConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = SysDbConfig::Grpc(config.sysdb_config.clone());
        let sysdb_client = SysDb::try_from_config(&sysdb_config, registry).await?;
        let storage = Storage::try_from_config(&config.storage_config, registry).await?;

        let mut disabled_collections = HashSet::new();
        for collection_id_str in config.disallow_collections.iter() {
            let collection_uuid = match Uuid::from_str(collection_id_str) {
                Ok(uuid) => uuid,
                Err(e) => {
                    // TODO(Sanket): Return a proper error here.
                    panic!("Invalid collection id: {}", e);
                }
            };
            let collection_id = CollectionUuid(collection_uuid);
            disabled_collections.insert(collection_id);
        }

        Ok(GarbageCollector::new(
            config.gc_interval_mins as u64,
            config.relative_cutoff_time,
            config.max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
            config.default_mode,
            config.tenant_mode_overrides.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helper::ChromaGrpcClients;
    use chroma_storage::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };
    use chroma_sysdb::GrpcSysDbConfig;
    use chroma_system::{DispatcherConfig, System};
    use tracing_test::traced_test;

    async fn wait_for_new_version(
        clients: &mut ChromaGrpcClients,
        collection_id: String,
        tenant_id: String,
        current_version_count: usize,
        max_attempts: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for attempt in 1..=max_attempts {
            tracing::info!(
                attempt,
                max_attempts,
                "Waiting for new version to be created..."
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            let versions = clients
                .list_collection_versions(
                    collection_id.clone(),
                    tenant_id.clone(),
                    Some(100),
                    None,
                    None,
                    None,
                )
                .await?;

            if versions.versions.len() > current_version_count {
                tracing::info!(
                    previous_count = current_version_count,
                    new_count = versions.versions.len(),
                    "New version detected"
                );
                return Ok(());
            }
        }

        Err("Timeout waiting for new version to be created".into())
    }

    const TEST_COLLECTIONS_SIZE: usize = 33;

    async fn create_test_collection(
        tenant_id: String,
        clients: &mut ChromaGrpcClients,
    ) -> (CollectionUuid, String) {
        let test_uuid = uuid::Uuid::new_v4();
        let database_name = format!("test_db_{}", test_uuid);
        let collection_name = format!("test_collection_{}", test_uuid);

        let collection_id = clients
            .create_database_and_collection(&tenant_id, &database_name, &collection_name)
            .await
            .unwrap();

        tracing::info!(collection_id = %collection_id, "Created collection");

        let mut embeddings = vec![];
        let mut ids = vec![];

        for i in 0..TEST_COLLECTIONS_SIZE {
            let mut embedding = vec![0.0; 3];
            embedding[i % 3] = 1.0;
            embeddings.push(embedding);
            ids.push(format!("id{}", i));
        }

        // Get initial version count
        let initial_versions = clients
            .list_collection_versions(
                collection_id.clone(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let initial_version_count = initial_versions.versions.len();

        tracing::info!(
            initial_count = initial_version_count,
            "Initial version count"
        );

        // Add first batch of 11 records
        tracing::info!("Adding first batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[..11].to_vec(),
                ids[..11].to_vec(),
            )
            .await
            .unwrap();

        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count,
            10,
        )
        .await
        .unwrap();

        // Add second batch of 11 records
        tracing::info!("Adding second batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[11..22].to_vec(),
                ids[11..22].to_vec(),
            )
            .await
            .unwrap();
        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 1,
            10,
        )
        .await
        .unwrap();

        // After adding second batch and waiting for version, add a third batch
        tracing::info!("Adding third batch of embeddings (modified records)");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[22..].to_vec(),
                ids[22..].to_vec(),
            )
            .await
            .unwrap();

        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 2,
            10,
        )
        .await
        .unwrap();

        let collection_id = CollectionUuid::from_str(&collection_id).unwrap();

        (collection_id, tenant_id)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_tenant_mode_override() {
        // Setup
        let tenant_id_for_delete_mode = format!("tenant-delete-mode-{}", Uuid::new_v4());
        let tenant_id_for_dry_run_mode = format!("tenant-dry-run-mode-{}", Uuid::new_v4());

        let mut tenant_mode_overrides = HashMap::new();
        tenant_mode_overrides.insert(tenant_id_for_delete_mode.clone(), CleanupMode::Delete);

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            relative_cutoff_time: Duration::from_secs(1),
            max_collections_to_gc: 100,
            gc_interval_mins: 10,
            disallow_collections: vec![],
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: StorageConfig::ObjectStore(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "chroma-storage".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024,   // 1MB
                download_part_size_bytes: 1024 * 1024, // 1MB
                max_concurrent_requests: 10,
            }),
            default_mode: CleanupMode::DryRun,
            tenant_mode_overrides: Some(tenant_mode_overrides),
        };
        let registry = Registry::new();

        // Create collections
        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let collection_in_dry_run_mode_handle = tokio::spawn({
            let mut clients = clients.clone();
            let tenant_id = tenant_id_for_dry_run_mode.clone();
            async move { create_test_collection(tenant_id, &mut clients).await }
        });
        let collection_in_delete_mode_handle = tokio::spawn({
            let mut clients = clients.clone();
            let tenant_id = tenant_id_for_delete_mode.clone();
            async move { create_test_collection(tenant_id, &mut clients).await }
        });
        let (collection_in_dry_run_mode, _) = collection_in_dry_run_mode_handle.await.unwrap();
        let (collection_in_delete_mode, _) = collection_in_delete_mode_handle.await.unwrap();

        // Wait 1 second for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Run garbage collection
        let mut garbage_collector_component = GarbageCollector::try_from_config(&config, &registry)
            .await
            .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
            .await
            .unwrap();

        let system = System::new();
        let dispatcher_handle = system.start_component(dispatcher);

        garbage_collector_component.set_dispatcher(dispatcher_handle);
        garbage_collector_component.set_system(system.clone());
        let garbage_collector_handle = system.start_component(garbage_collector_component);

        garbage_collector_handle
            .request(GarbageCollectMessage {}, Some(Span::current()))
            .await
            .unwrap();

        // Get versions for dry run mode
        let dry_run_mode_versions = clients
            .list_collection_versions(
                collection_in_dry_run_mode.0.to_string(),
                tenant_id_for_dry_run_mode,
                None,
                None,
                None,
                Some(true),
            )
            .await
            .unwrap();

        // Dry run should have 4 versions, one marked for deletion
        assert_eq!(
            dry_run_mode_versions.versions.len(),
            4,
            "Expected 4 versions in dry run mode, found {}",
            dry_run_mode_versions.versions.len()
        );
        assert!(
            dry_run_mode_versions
                .versions
                .iter()
                .any(|v| v.marked_for_deletion),
            "Expected at least one version to be marked for deletion in dry run mode"
        );

        let delete_mode_versions = clients
            .list_collection_versions(
                collection_in_delete_mode.0.to_string(),
                tenant_id_for_delete_mode,
                None,
                None,
                None,
                Some(true),
            )
            .await
            .unwrap();

        // There should be 3 versions left in delete mode, since the version 1 should have been deleted.
        assert_eq!(
            delete_mode_versions.versions.len(),
            3,
            "Expected 3 versions in delete mode, found {}",
            delete_mode_versions.versions.len()
        );
        assert!(
            delete_mode_versions
                .versions
                .iter()
                .all(|v| !v.marked_for_deletion),
            "Expected no versions to be marked for deletion in delete mode"
        );
    }
}

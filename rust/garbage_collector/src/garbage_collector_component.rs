use std::{collections::HashSet, fmt::Debug, fmt::Formatter, str::FromStr, time::Duration};

use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_storage::Storage;
use chroma_sysdb::{SysDb, SysDbConfig};
use chroma_system::{
    Component, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
};
use chroma_types::CollectionUuid;
use futures::{stream::FuturesUnordered, StreamExt};
use tracing::{span, Instrument, Span};
use uuid::Uuid;

use crate::{
    config::GarbageCollectorConfig, garbage_collector_orchestrator::GarbageCollectorOrchestrator,
};

#[allow(dead_code)]
pub(crate) struct GarbageCollector {
    gc_interval_mins: u64,
    cutoff_time_hours: u32,
    max_collections_to_gc: u32,
    disabled_collections: HashSet<CollectionUuid>,
    sysdb_client: Box<SysDb>,
    storage: Storage,
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    system: Option<chroma_system::System>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

impl GarbageCollector {
    pub fn new(
        gc_interval_mins: u64,
        cutoff_time_hours: u32,
        max_collections_to_gc: u32,
        disabled_collections: HashSet<CollectionUuid>,
        sysdb_client: Box<SysDb>,
        storage: Storage,
    ) -> Self {
        Self {
            gc_interval_mins,
            cutoff_time_hours,
            max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
            dispatcher: None,
            system: None,
        }
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: chroma_system::System) {
        self.system = Some(system);
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

    async fn start(&mut self, ctx: &ComponentContext<Self>) {
        ctx.scheduler.schedule(
            GarbageCollectMessage {},
            Duration::from_secs(self.gc_interval_mins * 60),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );
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
        _ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        // Get all collections to gc and create gc orchestrator for each.
        let collections_to_gc = self
            .sysdb_client
            .get_collections_to_gc()
            .await
            .expect("Failed to get collections to gc");
        let mut jobs = FuturesUnordered::new();
        for collection in collections_to_gc {
            if self.disabled_collections.contains(&collection.id) {
                tracing::warn!(
                    "Skipping garbage collection for disabled collection: {}",
                    collection.id
                );
                continue;
            }
            let dispatcher = match self.dispatcher {
                Some(ref dispatcher) => dispatcher.clone(),
                None => {
                    // TODO(Sanket): Error handling.
                    panic!("No dispatcher found");
                }
            };
            let instrumented_span =
                span!(parent: None, tracing::Level::INFO, "GC job", collection_id = ?collection.id);
            instrumented_span.follows_from(Span::current());
            match self.system {
                Some(ref system) => {
                    let orchestrator = GarbageCollectorOrchestrator::new(
                        collection.id,
                        collection.version_file_path,
                        self.cutoff_time_hours,
                        self.sysdb_client.clone(),
                        dispatcher,
                        self.storage.clone(),
                    );

                    jobs.push(
                        orchestrator
                            .run(system.clone())
                            .instrument(instrumented_span),
                    );
                }
                None => {
                    panic!("No system found");
                }
            };
        }
        tracing::info!("GC {} jobs", jobs.len());
        let mut num_completed_jobs = 0;
        let mut num_failed_jobs = 0;
        while let Some(job) = jobs.next().await {
            match job {
                Ok(result) => {
                    tracing::info!("GC completed: {:?}", result);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    tracing::info!("Compaction failed: {:?}", e);
                    num_failed_jobs += 1;
                }
            }
        }
        tracing::info!(
            "Completed {} jobs, failed {} jobs",
            num_completed_jobs,
            num_failed_jobs
        )
    }
}

#[async_trait]
impl Configurable<GarbageCollectorConfig> for GarbageCollector {
    async fn try_from_config(
        config: &GarbageCollectorConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = SysDbConfig::Grpc(config.sysdb_config.clone());
        let sysdb_client = chroma_sysdb::from_config(&sysdb_config).await?;

        let storage = chroma_storage::from_config(&config.storage_config).await?;

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
            config.cutoff_time_hours,
            config.max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
        ))
    }
}

use std::{collections::HashSet, str::FromStr, time::Duration};

use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::{SysDb, SysDbConfig};
use chroma_system::{Component, ComponentContext, ComponentHandle, Dispatcher, Handler};
use chroma_types::CollectionUuid;
use tracing::span;
use uuid::Uuid;

use crate::config::GarbageCollectorConfig;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct GarbageCollector {
    gc_interval_mins: u64,
    cutoff_time_hours: u32,
    max_collections_to_gc: u32,
    disabled_collections: HashSet<CollectionUuid>,
    sysdb_client: Box<SysDb>,
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    system: Option<chroma_system::System>,
}

impl GarbageCollector {
    pub fn new(
        gc_interval_mins: u64,
        cutoff_time_hours: u32,
        max_collections_to_gc: u32,
        disabled_collections: HashSet<CollectionUuid>,
        sysdb_client: Box<SysDb>,
    ) -> Self {
        Self {
            gc_interval_mins,
            cutoff_time_hours,
            max_collections_to_gc,
            disabled_collections,
            sysdb_client,
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
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled compaction")),
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
        // TODO(Sanket): Implement the garbage collection logic.
        todo!()
    }
}

#[async_trait]
impl Configurable<GarbageCollectorConfig> for GarbageCollector {
    async fn try_from_config(
        config: &GarbageCollectorConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = SysDbConfig::Grpc(config.sysdb_config.clone());
        let sysdb_client = chroma_sysdb::from_config(&sysdb_config).await?;

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
        ))
    }
}

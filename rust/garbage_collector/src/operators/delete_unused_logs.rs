use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionUuid, DatabaseName, TopologyName};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::Level;
use wal3::{
    create_repl_factories, create_s3_factories, FragmentSeqNo, FragmentUuid,
    GarbageCollectionOptions, GarbageCollectionState, GarbageCollector, LogPosition,
    LogReaderOptions, LogWriterOptions, ManifestManager, ManifestManagerFactory,
    ReplicatedFragmentManagerFactory, ReplicatedManifestManagerFactory, S3FragmentManagerFactory,
    S3ManifestManagerFactory, SnapshotOptions, StorageWrapper, ThrottleOptions,
};

use crate::mcmr::RegionsAndTopologies;
use crate::types::CleanupMode;

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsOperator {
    pub enabled: bool,
    pub mode: CleanupMode,
    pub storage: Storage,
    pub logs: Log,
    pub regions_and_topologies: Option<Arc<RegionsAndTopologies>>,
    pub enable_dangerous_option_to_ignore_min_versions_for_wal3: bool,
}

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsInput {
    pub collections_to_destroy: HashSet<CollectionUuid>,
    pub collections_to_garbage_collect: HashMap<CollectionUuid, LogPosition>,
    pub database_name: Option<DatabaseName>,
}

pub type DeleteUnusedLogsOutput = ();

#[derive(Debug, Error)]
pub enum DeleteUnusedLogsError {
    #[error("failed to garbage collect in wal3 for {collection_id}: {err}")]
    Wal3 {
        collection_id: CollectionUuid,
        err: wal3::Error,
    },
    #[error("missing MCMR regions_and_topologies config for database {database_name}")]
    MissingRegionsAndTopologies { database_name: String },
    #[error("invalid topology {topology} for database {database_name}")]
    InvalidTopology {
        database_name: String,
        topology: String,
    },
    #[error("topology {topology} not found in regions_and_topologies config")]
    MissingTopology { topology: String },
    #[error("preferred region {preferred_region} is not present in topology {topology}")]
    PreferredRegionNotInTopology {
        preferred_region: String,
        topology: String,
    },
    #[error(transparent)]
    Gc(#[from] chroma_log::GarbageCollectError),
}

impl ChromaError for DeleteUnusedLogsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

struct ReplTopologyContext {
    storage_wrappers: Arc<Vec<StorageWrapper>>,
    region_names: Vec<String>,
    preferred_index: usize,
    spanner: Arc<google_cloud_spanner::client::Client>,
    repl_options: wal3::ReplicatedFragmentOptions,
}

#[async_trait]
trait CollectionGarbageCollector: Send {
    async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<Option<GarbageCollectionState>, wal3::Error>;

    async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
        gc_state: &GarbageCollectionState,
    ) -> Result<(), wal3::Error>;
}

#[async_trait]
impl<P, FP, MP> CollectionGarbageCollector for GarbageCollector<P, FP, MP>
where
    P: wal3::FragmentPointer + Send + Sync + 'static,
    FP: wal3::FragmentManagerFactory<FragmentPointer = P> + Send + Sync + 'static,
    MP: wal3::ManifestManagerFactory<FragmentPointer = P> + Send + Sync + 'static,
{
    async fn garbage_collect_phase1_compute_garbage(
        &self,
        options: &GarbageCollectionOptions,
        keep_at_least: Option<LogPosition>,
    ) -> Result<Option<GarbageCollectionState>, wal3::Error> {
        GarbageCollector::garbage_collect_phase1_compute_garbage(self, options, keep_at_least).await
    }

    async fn garbage_collect_phase3_delete_garbage(
        &self,
        options: &GarbageCollectionOptions,
        gc_state: &GarbageCollectionState,
    ) -> Result<(), wal3::Error> {
        GarbageCollector::garbage_collect_phase3_delete_garbage(self, options, gc_state).await
    }
}

impl DeleteUnusedLogsOperator {
    fn repl_topology_context(
        &self,
        database_name: Option<&DatabaseName>,
        collection_id: CollectionUuid,
    ) -> Result<Option<ReplTopologyContext>, DeleteUnusedLogsError> {
        let Some(database_name) = database_name else {
            return Ok(None);
        };
        let Some(topology) = database_name.topology() else {
            return Ok(None);
        };
        let Some(regions_and_topologies) = &self.regions_and_topologies else {
            return Err(DeleteUnusedLogsError::MissingRegionsAndTopologies {
                database_name: database_name.as_ref().to_string(),
            });
        };

        let topology_name = TopologyName::new(topology.clone()).map_err(|_| {
            DeleteUnusedLogsError::InvalidTopology {
                database_name: database_name.as_ref().to_string(),
                topology: topology.clone(),
            }
        })?;
        let Some((regions, topology_config)) =
            regions_and_topologies.lookup_topology(&topology_name)
        else {
            return Err(DeleteUnusedLogsError::MissingTopology {
                topology: topology_name.to_string(),
            });
        };

        let prefix = collection_id.storage_prefix_for_log();
        let mut storage_wrappers = Vec::with_capacity(regions.len());
        let mut region_names = Vec::with_capacity(regions.len());
        for region in regions {
            region_names.push(region.name().to_string());
            storage_wrappers.push(StorageWrapper::new(
                region.name().to_string(),
                region.config.storage.clone(),
                prefix.clone(),
            ));
        }

        let preferred_index = storage_wrappers
            .iter()
            .position(|region| region.region.as_str() == regions_and_topologies.preferred.as_str())
            .ok_or_else(|| DeleteUnusedLogsError::PreferredRegionNotInTopology {
                preferred_region: regions_and_topologies.preferred.to_string(),
                topology: topology_name.to_string(),
            })?;

        Ok(Some(ReplTopologyContext {
            storage_wrappers: Arc::new(storage_wrappers),
            region_names,
            preferred_index,
            spanner: Arc::new(topology_config.config.spanner.clone()),
            repl_options: topology_config.config.repl.clone(),
        }))
    }

    async fn open_collection_garbage_collector(
        &self,
        database_name: Option<&DatabaseName>,
        collection_id: CollectionUuid,
        storage: Arc<Storage>,
    ) -> Result<Box<dyn CollectionGarbageCollector>, DeleteUnusedLogsError> {
        let prefix = collection_id.storage_prefix_for_log();
        let options = LogWriterOptions::default();

        if let Some(repl) = self.repl_topology_context(database_name, collection_id)? {
            let (fragment_manager_factory, manifest_manager_factory) = create_repl_factories(
                options.clone(),
                repl.repl_options,
                repl.preferred_index,
                repl.storage_wrappers,
                repl.spanner,
                repl.region_names,
                collection_id.0,
            );
            let writer =
                GarbageCollector::<
                    FragmentUuid,
                    ReplicatedFragmentManagerFactory,
                    ReplicatedManifestManagerFactory,
                >::open(options, fragment_manager_factory, manifest_manager_factory)
                .await
                .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })?;
            Ok(Box::new(writer))
        } else {
            let (fragment_manager_factory, manifest_manager_factory) = create_s3_factories(
                options.clone(),
                LogReaderOptions::default(),
                storage,
                prefix,
                "garbage collection service".to_string(),
                Arc::new(()),
                Arc::new(()),
            );
            let writer =
                GarbageCollector::<
                    (FragmentSeqNo, LogPosition),
                    S3FragmentManagerFactory,
                    S3ManifestManagerFactory,
                >::open(options, fragment_manager_factory, manifest_manager_factory)
                .await
                .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })?;
            Ok(Box::new(writer))
        }
    }

    async fn destroy_collection_log(
        &self,
        database_name: Option<&DatabaseName>,
        collection_id: CollectionUuid,
        storage: Arc<Storage>,
    ) -> Result<(), DeleteUnusedLogsError> {
        let prefix = collection_id.storage_prefix_for_log();

        if let Some(repl) = self.repl_topology_context(database_name, collection_id)? {
            let local_region = repl.region_names[repl.preferred_index].clone();
            let preferred_storage =
                Arc::new(repl.storage_wrappers[repl.preferred_index].storage.clone());
            let manifest_manager_factory = ReplicatedManifestManagerFactory::new(
                repl.spanner,
                repl.region_names,
                local_region,
                collection_id.0,
            );
            let manifest_manager = manifest_manager_factory
                .open_publisher()
                .await
                .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })?;
            wal3::destroy(preferred_storage, &prefix, &manifest_manager)
                .await
                .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })
        } else {
            let manifest_manager = ManifestManager::new(
                ThrottleOptions::default(),
                SnapshotOptions::default(),
                storage.clone(),
                prefix.clone(),
                "destroy service".to_string(),
                Arc::new(()),
                Arc::new(()),
            )
            .await
            .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })?;
            wal3::destroy(storage, &prefix, &manifest_manager)
                .await
                .map_err(|err| DeleteUnusedLogsError::Wal3 { collection_id, err })
        }
    }
}

#[async_trait]
impl Operator<DeleteUnusedLogsInput, DeleteUnusedLogsOutput> for DeleteUnusedLogsOperator {
    type Error = DeleteUnusedLogsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &DeleteUnusedLogsInput,
    ) -> Result<DeleteUnusedLogsOutput, DeleteUnusedLogsError> {
        tracing::info!("Garbage collecting logs: {input:?}");
        if !self.enabled {
            tracing::info!("Skipping log GC because it is not enabled for this tenant");
            return Ok(());
        }

        let storage_arc = Arc::new(self.storage.clone());
        if !input.collections_to_garbage_collect.is_empty() {
            let mut log_gc_futures = Vec::with_capacity(input.collections_to_garbage_collect.len());
            for (collection_id, minimum_log_offset_to_keep) in &input.collections_to_garbage_collect
            {
                let collection_id = *collection_id;
                let storage_clone = storage_arc.clone();
                let database_name = input.database_name.clone();
                let mut logs = self.logs.clone();
                log_gc_futures.push(async move {
                    let writer = match self
                        .open_collection_garbage_collector(
                            database_name.as_ref(),
                            collection_id,
                            storage_clone.clone(),
                        )
                        .await
                    {
                        Ok(log_writer) => log_writer,
                        Err(DeleteUnusedLogsError::Wal3 {
                            collection_id: _,
                            err: wal3::Error::UninitializedLog,
                        }) => return Ok(()),
                        Err(err) => {
                            tracing::error!(
                                "Unable to initialize log writer for collection [{collection_id}]: {err}"
                            );
                            return Err(err);
                        }
                    };
                    // NOTE(rescrv):  Once upon a time, we had a bug where we would not pass the
                    // min log offset into the wal3 garbage collect process.  For disaster
                    // recovery, we need to keep N versions per the config, but the collections
                    // (staging only) were collected up to the most recent version.  The fix is
                    // this hack to allow a corrupt garbage error to be masked iff the appropriate
                    // configuration value is set.
                    //
                    // The configuration is
                    // enable_dangerous_option_to_ignore_min_versions_for_wal3.  Its default is
                    // false.  Setting it to true enables this loop to skip the min_log_offset.
                    //
                    // To remove this hack, search for the warning below, and then make every
                    // collection that appears with that warning compact min-versions-to-keep
                    // times.
                    let mut min_log_offset = Some(*minimum_log_offset_to_keep);
                    let mut gc_state = wal3::GarbageCollectionState::empty();
                    for _ in 0..if self.enable_dangerous_option_to_ignore_min_versions_for_wal3 {
                        2
                    } else {
                        1
                    } {
                        // See README.md in wal3 for a description of why this happens in three phases.
                        match writer
                            .garbage_collect_phase1_compute_garbage(
                                &GarbageCollectionOptions::default(),
                                min_log_offset,
                            )
                            .await
                        {
                            Ok(Some(state)) => {
                                gc_state = state;
                            }
                            Ok(None) => return Ok(()),
                            Err(wal3::Error::CorruptGarbage(c))
                                if c.starts_with("First to keep does not overlap manifest") =>
                            {
                                if self.enable_dangerous_option_to_ignore_min_versions_for_wal3 {
                                    tracing::event!(Level::WARN, name = "encountered enable_dangerous_option_to_ignore_min_versions_for_wal3 path", collection_id =? collection_id);
                                    min_log_offset.take();
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    "Unable to garbage collect log for collection [{collection_id}]: {err}"
                                );
                                return Err(DeleteUnusedLogsError::Wal3 { collection_id, err });
                            }
                        };
                    }
                    if let Err(err) = logs
                        .garbage_collect_phase2(database_name.clone(), collection_id)
                        .await
                    {
                        tracing::error!(
                            "Unable to garbage collect log for collection [{collection_id}]: {err}"
                        );
                        return Err(DeleteUnusedLogsError::Gc(err));
                    };
                    match self.mode {
                        CleanupMode::DeleteV2 => {
                            if let Err(err) = writer
                                .garbage_collect_phase3_delete_garbage(
                                    &GarbageCollectionOptions::default(),
                                    &gc_state,
                                )
                                .await
                            {
                                tracing::error!(
                                    "Unable to garbage collect log for collection [{collection_id}]: {err}"
                                );
                                return Err(DeleteUnusedLogsError::Wal3 { collection_id, err });
                            };
                        }
                        mode => {
                            tracing::info!("Skipping delete phase of log GC in {mode:?} mode");
                        }
                    }
                    Ok(())
                });
            }
            try_join_all(log_gc_futures).await?;
            tracing::info!(
                "Wal3 gc complete for collections: {:?}",
                input.collections_to_garbage_collect
            );
        }
        match self.mode {
            CleanupMode::DeleteV2 => {
                if !input.collections_to_destroy.is_empty() {
                    let mut log_destroy_futures =
                        Vec::with_capacity(input.collections_to_destroy.len());
                    for collection_id in &input.collections_to_destroy {
                        let collection_id = *collection_id;
                        let storage_clone = storage_arc.clone();
                        let database_name = input.database_name.clone();
                        log_destroy_futures.push(async move {
                            match self
                                .destroy_collection_log(
                                    database_name.as_ref(),
                                    collection_id,
                                    storage_clone.clone(),
                                )
                                .await
                            {
                                Ok(()) => Ok(()),
                                Err(DeleteUnusedLogsError::Wal3 {
                                    collection_id: _,
                                    err: wal3::Error::UninitializedLog,
                                }) => Ok(()),
                                Err(err) => {
                                    tracing::error!(
                                        "Unable to destroy log for collection [{collection_id}]: {err:?}"
                                    );
                                    Err(err)
                                }
                            }
                        })
                    }
                    try_join_all(log_destroy_futures).await?;
                    tracing::info!(
                        "Wal3 destruction complete for collections: {:?}",
                        input.collections_to_destroy
                    );
                }
            }
            mode => {
                tracing::info!("Keeping logs for soft deleted collections in {mode:?} mode");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
    use chroma_system::Operator;
    use wal3::{Cursor, CursorName, CursorStore, CursorStoreOptions, LogWriter, SnapshotOptions};

    async fn seed_collection_log(storage: Storage, collection_id: CollectionUuid) -> LogPosition {
        let prefix = collection_id.storage_prefix_for_log();
        let options = LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        };
        let storage = Arc::new(storage);
        let (fragment_factory, manifest_factory) = create_s3_factories(
            options.clone(),
            LogReaderOptions::default(),
            storage.clone(),
            prefix.clone(),
            "test-writer".to_string(),
            Arc::new(()),
            Arc::new(()),
        );
        let log = LogWriter::open_or_initialize(
            options,
            "test-writer",
            fragment_factory,
            manifest_factory,
            None,
        )
        .await
        .expect("log should initialize");

        let mut keep_position = LogPosition::default();
        for i in 0..40 {
            let position = log
                .append_many(
                    (0..5)
                        .map(|j| format!("collection:{i}:{j}").into_bytes())
                        .collect(),
                )
                .await
                .expect("append should succeed");
            if i == 20 {
                keep_position = position;
            }
        }

        let cursors = CursorStore::new(
            CursorStoreOptions::default(),
            storage,
            prefix,
            "cursor-writer".to_string(),
        );
        cursors
            .init(
                &CursorName::new("so_you_may_gc").expect("cursor name should be valid"),
                Cursor {
                    position: keep_position,
                    epoch_us: keep_position.offset(),
                    writer: "test-writer".to_string(),
                },
            )
            .await
            .expect("cursor should initialize");

        keep_position
    }

    #[tokio::test]
    async fn test_k8s_integration_delete_unused_logs_delete_mode_removes_garbage() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        let keep_position = seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();
        let before = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DeleteV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            regions_and_topologies: None,
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::new(),
            collections_to_garbage_collect: HashMap::from([(collection_id, keep_position)]),
            database_name: None,
        })
        .await
        .expect("delete-mode GC should succeed");

        let after = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        assert!(
            after.len() < before.len(),
            "expected GC to delete at least one log artifact, before={} after={}",
            before.len(),
            after.len()
        );
    }

    #[tokio::test]
    #[ignore] // TODO(rescrv, gc week):  Unignore this test.
    async fn test_k8s_integration_delete_unused_logs_dry_run_keeps_files() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        let keep_position = seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();
        let before = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed")
            .into_iter()
            .collect::<HashSet<_>>();

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DryRunV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            regions_and_topologies: None,
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::new(),
            collections_to_garbage_collect: HashMap::from([(collection_id, keep_position)]),
            database_name: None,
        })
        .await
        .expect("dry-run GC should succeed");

        let after = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed")
            .into_iter()
            .collect::<HashSet<_>>();

        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn test_k8s_integration_delete_unused_logs_hard_delete_destroys_collection_prefix() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DeleteV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            regions_and_topologies: None,
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::from([collection_id]),
            collections_to_garbage_collect: HashMap::new(),
            database_name: None,
        })
        .await
        .expect("hard-delete should succeed");

        let remaining = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        assert!(remaining.is_empty(), "expected log prefix to be destroyed");
    }
}

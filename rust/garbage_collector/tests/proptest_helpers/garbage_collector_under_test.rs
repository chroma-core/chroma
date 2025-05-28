use super::garbage_collector_reference::ReferenceGarbageCollector;
use crate::define_thread_local_stats;
use crate::proptest_helpers::proptest_types::{SegmentIds, Transition};
use chroma_blockstore::RootManager;
use chroma_cache::nop::NopCache;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_storage::config::{
    ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
};
use chroma_storage::{GetOptions, Storage};
use chroma_sysdb::{GrpcSysDb, GrpcSysDbConfig, SysDb};
use chroma_system::Orchestrator;
use chroma_system::{Dispatcher, DispatcherConfig, System};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::{CollectionUuid, Segment, SegmentScope, SegmentType, SegmentUuid};
use chrono::DateTime;
use futures::StreamExt;
use garbage_collector_library::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator;
use garbage_collector_library::types::CleanupMode;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::OnceCell;
use tracing::{Instrument, Span};
use uuid::Uuid;

define_thread_local_stats!(STATS);

pub struct GarbageCollectorUnderTest {
    runtime: Arc<tokio::runtime::Runtime>,
    tenant: String,
    database: String,
    sysdb: SysDb,
    storage: Storage,
    root_manager: RootManager,
    collection_id_to_segment_ids: HashMap<CollectionUuid, SegmentIds>,
}

impl Drop for GarbageCollectorUnderTest {
    fn drop(&mut self) {
        STATS.with_borrow_mut(|stats| {
            stats.record_test_case_end();
        });

        self.runtime.block_on(async {
            self.sysdb.reset().await.unwrap();

            let files = self.storage.list_prefix("").await.unwrap();
            if files.is_empty() {
                return;
            }
            futures::stream::iter(files.into_iter())
                .map(|file| {
                    let storage = self.storage.clone();
                    async move {
                        storage.delete(&file).await.unwrap();
                    }
                })
                .buffer_unordered(32)
                .collect()
                .await
        })
    }
}

// The S3 client is a bit expensive to construct, so we cache it since the config is identical across all test cases.
static STORAGE_ONCE: OnceCell<Storage> = OnceCell::const_new();

impl StateMachineTest for GarbageCollectorUnderTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceGarbageCollector;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        tracing::debug!("Starting test");

        let tenant_id = Uuid::new_v4();
        let tenant_name = format!("test_tenant_{}", tenant_id);
        let database_id = Uuid::new_v4();
        let database_name = format!("test_database_{}", database_id);

        ref_state.runtime.block_on(async {
            let registry = Registry::new();

            let storage = STORAGE_ONCE
                .get_or_init(|| async {
                    let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
                        bucket: ObjectStoreBucketConfig {
                            name: "chroma-storage".to_string(),
                            r#type: ObjectStoreType::Minio,
                        },
                        upload_part_size_bytes: 1024 * 1024, // 1MB
                        download_part_size_bytes: 1024 * 1024, // 1MB
                        max_concurrent_requests: 10,
                    });
                    Storage::try_from_config(&storage_config, &registry)
                        .await
                        .unwrap()
                })
                .await;

            let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));
            let config = GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                ..Default::default()
            };

            let mut sysdb = SysDb::Grpc(
                GrpcSysDb::try_from_config(&config, &registry)
                    .await
                    .unwrap(),
            );

            sysdb.create_tenant(tenant_name.clone()).await.unwrap();
            sysdb
                .create_database(database_id, database_name.clone(), tenant_name.clone())
                .await
                .unwrap();

            Self {
                runtime: ref_state.runtime.clone(),
                tenant: tenant_name,
                database: database_name,
                sysdb,
                storage: storage.clone(),
                root_manager,
                collection_id_to_segment_ids: HashMap::new(),
            }
        })
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        tracing::debug!("Applying transition: {:#?}", transition);

        STATS.with_borrow_mut(|stats| {
            stats.record_transition(&transition, ref_state);
        });

        match transition {
            Transition::CreateCollection {
                collection_id,
                segments,
            } => {
                ref_state.runtime.block_on(async {
                    segments.write_files(&state.storage).await;

                    let segments = vec![
                        Segment {
                            id: SegmentUuid::new(),
                            r#type: SegmentType::HnswDistributed,
                            scope: SegmentScope::VECTOR,
                            collection: collection_id,
                            metadata: None,
                            file_path: segments.vector.into(),
                        },
                        Segment {
                            id: SegmentUuid::new(),
                            r#type: SegmentType::BlockfileMetadata,
                            scope: SegmentScope::METADATA,
                            collection: collection_id,
                            metadata: None,
                            file_path: segments.metadata.into(),
                        },
                        Segment {
                            id: SegmentUuid::new(),
                            r#type: SegmentType::BlockfileRecord,
                            scope: SegmentScope::RECORD,
                            collection: collection_id,
                            metadata: None,
                            file_path: segments.record.into(),
                        },
                    ];
                    let segment_ids = SegmentIds {
                        vector: segments[0].id,
                        metadata: segments[1].id,
                        record: segments[2].id,
                    };
                    state
                        .collection_id_to_segment_ids
                        .insert(collection_id, segment_ids);

                    state
                        .sysdb
                        .create_collection(
                            state.tenant.clone(),
                            state.database.clone(),
                            collection_id,
                            format!("Collection {}", collection_id),
                            segments,
                            None,
                            None,
                            None,
                            false,
                        )
                        .await
                        .unwrap();
                });
            }

            Transition::DeleteCollection(collection_id) => {
                ref_state
                    .runtime
                    .block_on(state.sysdb.delete_collection(
                        state.tenant.clone(),
                        state.database.clone(),
                        collection_id,
                        vec![],
                    ))
                    .unwrap();
            }

            Transition::IncrementCollectionVersion {
                collection_id,
                next_segments,
            } => {
                let segment_ids = state
                    .collection_id_to_segment_ids
                    .get(&collection_id)
                    .unwrap();

                ref_state.runtime.block_on(async {
                    next_segments.write_files(&state.storage).await;

                    state
                        .sysdb
                        .flush_compaction(
                            state.tenant.clone(),
                            collection_id,
                            0,
                            ref_state.max_version_for_collection(collection_id).unwrap() as i32 - 1,
                            next_segments.into_segment_flushes(segment_ids),
                            0,
                            0,
                        )
                        .await
                        .unwrap();
                });
            }

            Transition::ForkCollection {
                source_collection_id,
                new_collection_id,
            } => {
                let nonce = Uuid::new_v4().to_string();
                let collection_and_segments = ref_state
                    .runtime
                    .block_on(state.sysdb.fork_collection(
                        source_collection_id,
                        0,
                        0,
                        new_collection_id,
                        format!(
                          "Collection {} (forked from {} @v{}, nonce {})",
                          new_collection_id,
                          source_collection_id,
                          ref_state.max_version_for_collection(
                              source_collection_id
                          ).unwrap(),
                          &nonce[..8]
                      ),
                    ))
                    .unwrap();

                state
                    .collection_id_to_segment_ids
                    .insert(new_collection_id, SegmentIds::from(collection_and_segments));
            }

            Transition::GarbageCollect {
                collection_id,
                min_versions_to_keep,
                ..
            } => {
                ref_state
                    .runtime
                    .block_on(
                        async {
                            let system = System::new();
                            let dispatcher = Dispatcher::new(DispatcherConfig::default());
                            let mut dispatcher_handle = system.start_component(dispatcher);

                            let mut collections = state
                                .sysdb
                                .get_collections(
                                    Some(collection_id),
                                    None,
                                    Some(state.tenant.clone()),
                                    None,
                                    None,
                                    0,
                                )
                                .await
                                .unwrap();
                            let collection_to_gc = collections.pop().unwrap();
                            assert_eq!(collection_to_gc.collection_id, collection_id);
                            let version_file_path = collection_to_gc.version_file_path.unwrap();

                            let mut lineage_file_path = None;
                            if let Some(lineage_file) = collection_to_gc.lineage_file_path {
                                lineage_file_path = Some(lineage_file);
                            } else if let Some(root_collection_id) =
                                collection_to_gc.root_collection_id
                            {
                                lineage_file_path = Some(
                                    state
                                        .sysdb
                                        .get_collections(
                                            Some(root_collection_id),
                                            None,
                                            Some(state.tenant.clone()),
                                            None,
                                            None,
                                            0,
                                        )
                                        .await
                                        .unwrap()
                                        .first()
                                        .unwrap()
                                        .lineage_file_path
                                        .clone()
                                        .unwrap(),
                                );
                            }

                            let orchestrator = GarbageCollectorOrchestrator::new(
                                collection_id,
                                version_file_path,
                                lineage_file_path,
                                // This proptest does not test the cutoff time as the timestamps created by the SysDb (e.g. collection.created_at and timestamps in version files) cannot currently be faked/overridden.
                                DateTime::from_timestamp(
                                    SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs() as i64
                                        + 1,
                                    0,
                                )
                                .unwrap(),
                                state.sysdb.clone(),
                                dispatcher_handle.clone(),
                                system.clone(),
                                state.storage.clone(),
                                state.root_manager.clone(),
                                CleanupMode::Delete,
                                min_versions_to_keep as u32,
                            );
                            let result = orchestrator.run(system.clone()).await;

                            system.stop().await;
                            dispatcher_handle.stop();

                            result
                        }
                        .instrument(Span::current()),
                    )
                    .unwrap();
            }
            Transition::NoOp => {}
        }

        tracing::debug!(
            "Graph after transition: \n{}",
            ref_state.get_graphviz_of_graph()
        );

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // Check invariants in the reference state
        ref_state.check_invariants();

        // Check version files
        let expected_versions_by_collection = ref_state.expected_versions_by_collection();

        ref_state.runtime.block_on({
            let sysdb = state.sysdb.clone();
            let storage = state.storage.clone();

            async move {
                futures::stream::iter(expected_versions_by_collection)
                    .map(move |(collection_id, expected_versions)| {
                        let mut sysdb = sysdb.clone();
                        let storage = storage.clone();

                        async move {
                            let collections = sysdb
                                .get_collections(Some(collection_id), None, None, None, None, 0)
                                .await
                                .unwrap();

                            let collection = collections.first().unwrap();
                            let version_file_path = collection.version_file_path.as_ref().unwrap();
                            tracing::trace!("Version file path for collection {}: {}", collection_id, version_file_path);

                            let version_file = storage
                                .get(version_file_path, GetOptions::default())
                                .await
                                .unwrap();
                            let version_file =
                                CollectionVersionFile::decode(version_file.as_slice()).unwrap();

                            let versions = version_file
                                .version_history
                                .as_ref()
                                .unwrap()
                                .versions
                                .iter()
                                .map(|v| v.version as u64)
                                .collect::<Vec<_>>();

                            let versions_marked_for_deletion = version_file
                                .version_history
                                .unwrap()
                                .versions
                                .iter()
                                .filter_map(|v| {
                                    if v.marked_for_deletion {
                                        Some(v.version as u64)
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>();

                            assert_eq!(
                                versions, expected_versions,
                                "Version file for collection {} does not match expected versions. Expected: {:?}, found: {:?}. The version file has versions {:?} marked for deletion.",
                                collection_id, expected_versions, versions,
                                versions_marked_for_deletion
                            );
                        }
                    })
                    .buffer_unordered(32)
                    .collect::<Vec<_>>()
                    .await;
            }
        });

        let file_ref_counts = ref_state.get_file_ref_counts();
        let files_on_disk = ref_state
            .runtime
            .block_on(state.storage.list_prefix(""))
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();

        for (file_path, refs) in file_ref_counts {
            let on_disk = files_on_disk.contains(&file_path);

            if refs.is_empty() && on_disk {
                panic!(
                    "Invariant violation: file {} has zero references but is still on disk.",
                    file_path
                );
            } else if !refs.is_empty() && !on_disk {
                panic!(
                  "Invariant violation: file reference {} has a non-zero count {} but is not on disk. Referenced by: {:#?}",
                  file_path, refs.len(), refs
              );
            }
        }
    }
}

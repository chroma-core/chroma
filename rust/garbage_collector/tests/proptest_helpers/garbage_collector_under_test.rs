use super::garbage_collector_reference::{CollectionStatus, ReferenceGarbageCollector};
use super::proptest_types::SegmentIds;
use crate::define_thread_local_stats;
use crate::proptest_helpers::proptest_types::Transition;
use chroma_blockstore::RootManager;
use chroma_cache::nop::NopCache;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_log::config::{GrpcLogConfig, LogConfig};
use chroma_log::Log;
use chroma_storage::s3::s3_client_for_test_with_bucket_name;
use chroma_storage::{DeleteOptions, GetOptions, Storage};
use chroma_sysdb::{GetCollectionsOptions, GrpcSysDb, GrpcSysDbConfig, SysDb};
use chroma_system::Orchestrator;
use chroma_system::{Dispatcher, DispatcherConfig, System};
use chroma_types::chroma_proto::CollectionVersionFile;
use chroma_types::{CollectionUuid, Segment, SegmentScope, SegmentType};
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
    sysdb: SysDb,
    storage: Storage,
    logs: Log,
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

            let files = self
                .storage
                .list_prefix("", GetOptions::default())
                .await
                .unwrap();
            if files.is_empty() {
                return;
            }
            futures::stream::iter(files.into_iter())
                .map(|file| {
                    let storage = self.storage.clone();
                    async move {
                        storage
                            .delete(&file, DeleteOptions::default())
                            .await
                            .unwrap();
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

        ref_state.runtime.block_on(async {
            let registry = Registry::new();

            let storage = STORAGE_ONCE
                .get_or_init(|| async {
                    s3_client_for_test_with_bucket_name("chroma-storage").await
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

            sysdb.create_tenant(ref_state.tenant.clone()).await.unwrap();
            sysdb
                .create_database(
                    ref_state.db_id.0,
                    ref_state.db_name.clone(),
                    ref_state.tenant.clone(),
                )
                .await
                .unwrap();
            let system = System::new();
            let log_config = LogConfig::Grpc(GrpcLogConfig::default());
            let logs = Log::try_from_config(&(log_config, system), &registry)
                .await
                .unwrap();

            Self {
                runtime: ref_state.runtime.clone(),
                sysdb,
                storage: storage.clone(),
                logs,
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
                            id: segments.vector.root_segment_id,
                            r#type: SegmentType::HnswDistributed,
                            scope: SegmentScope::VECTOR,
                            collection: collection_id,
                            metadata: None,
                            file_path: segments.vector.into(),
                        },
                        Segment {
                            id: segments.metadata.root_segment_id,
                            r#type: SegmentType::BlockfileMetadata,
                            scope: SegmentScope::METADATA,
                            collection: collection_id,
                            metadata: None,
                            file_path: segments.metadata.into(),
                        },
                        Segment {
                            id: segments.record.root_segment_id,
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
                            ref_state.tenant.clone(),
                            ref_state.db_name.clone(),
                            collection_id,
                            format!("Collection {}", collection_id),
                            segments,
                            None,
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
                        ref_state.tenant.clone(),
                        ref_state.db_name.clone(),
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

                    let segment_flush_info = next_segments.into_segment_flushes(segment_ids);

                    for sfi in segment_flush_info.iter() {
                        assert!(!sfi.file_paths.is_empty());
                    }

                    state
                        .sysdb
                        .flush_compaction(
                            ref_state.tenant.clone(),
                            collection_id,
                            0,
                            ref_state.max_version_for_collection(collection_id).unwrap() as i32 - 1,
                            segment_flush_info,
                            0,
                            0,
                            None,
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
                            let collections = state
                                .sysdb
                                .get_collections_to_gc(None, None, Some(ref_state.tenant.clone()), None)
                                .await
                                .unwrap();
                            tracing::debug!(
                                "Collections eligible for garbage collection: {:#?}",
                                collections
                            );
                            let collection_to_gc =
                                collections.iter().find(|c| c.id == collection_id).expect("get_collections_to_gc() did not return a collection eligible for garbage collection");

                            let system = System::new();
                            let dispatcher = Dispatcher::new(DispatcherConfig::default());
                            let mut dispatcher_handle = system.start_component(dispatcher);

                            let one_second_from_now = DateTime::from_timestamp(
                                SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() as i64
                                    + 1,
                                0,
                            )
                            .unwrap();

                            let orchestrator = GarbageCollectorOrchestrator::new(
                                collection_id,
                                collection_to_gc.version_file_path.clone(),
                                collection_to_gc.lineage_file_path.clone(),
                                // This proptest does not test the cutoff time as the timestamps created by the SysDb (e.g. collection.created_at and timestamps in version files) cannot currently be faked/overridden.
                                one_second_from_now,
                                one_second_from_now,
                                state.sysdb.clone(),
                                dispatcher_handle.clone(),
                                system.clone(),
                                state.storage.clone(),
                                state.logs.clone(),
                                state.root_manager.clone(),
                                CleanupMode::DeleteV2,
                                min_versions_to_keep as u32,
                                true,
                                false,
                                10,
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
            let mut sysdb = state.sysdb.clone();
            let storage = state.storage.clone();

            async move {
                let collection_statuses = sysdb.batch_get_collection_soft_delete_status(ref_state.collection_status.keys().cloned().collect()).await.unwrap();
                for (collection_id, status) in ref_state.collection_status.iter() {
                    match status {
                        CollectionStatus::Deleted => {
                            assert!(
                                !collection_statuses.contains_key(collection_id),
                                "Collection {} is supposed to be hard deleted, but still exists in the sysdb. Is soft deleted: {:?}",
                                collection_id,
                                collection_statuses.get(collection_id)
                            );
                        }
                        CollectionStatus::Alive => {
                            match collection_statuses.get(collection_id) {
                                Some(&true) => {
                                    panic!("Collection {} is supposed to be alive, but is marked as soft deleted in the sysdb.", collection_id);
                                }
                                Some(&false) => {
                                    // Expected case
                                }
                                None => {
                                    panic!("Collection {} is supposed to be alive, but does not exist in the sysdb.", collection_id);
                                }
                            }
                        }
                        CollectionStatus::SoftDeleted => {
                            match collection_statuses.get(collection_id) {
                                Some(&true) => {
                                    // Expected case
                                }
                                Some(&false) => {
                                    panic!("Collection {} is supposed to be soft deleted, but is marked as alive in the sysdb.", collection_id);
                                }
                                None => {
                                    panic!("Collection {} is supposed to be soft deleted, but does not exist in the sysdb.", collection_id);
                                }
                            }
                        }
                    }
                }

                futures::stream::iter(expected_versions_by_collection)
                    .map(move |(collection_id, expected_versions)| {
                        let mut sysdb = sysdb.clone();
                        let storage = storage.clone();

                        async move {
                            let collections = sysdb
                                .get_collections(
                                   GetCollectionsOptions {
                                        collection_id: Some(collection_id),
                                        ..Default::default()
                                })
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
            .block_on(state.storage.list_prefix("", GetOptions::default()))
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

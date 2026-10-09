//! Model-based coverage of the complete local frontend, log, compactor and HNSW stack.
//! Small ID spaces deliberately generate duplicate adds and add/delete/add histories.
use std::{collections::BTreeMap, path::Path, time::Duration};

use chroma_cache::{CacheConfig, FoyerCacheConfig};
use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{impls::service_based_frontend::ServiceBasedFrontend, FrontendConfig};
use chroma_log::LocalCompactionManager;
use chroma_segment::{
    local_hnsw::LocalHnswSegmentReader, local_segment_manager::LocalSegmentManager,
};
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::{ComponentHandle, System};
use chroma_types::*;
use proptest::prelude::*;
use tokio::runtime::Runtime;

struct Running {
    frontend: ServiceBasedFrontend,
    registry: Registry,
    system: System,
    runtime: Runtime,
    // A caller may retain a reader while another collection evicts its cache entry.
    pins: Vec<LocalHnswSegmentReader>,
}

impl Running {
    fn open(root: &Path) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let registry = Registry::new();
        let system = System::new();
        let mut config = FrontendConfig::sqlite_in_memory();
        config.sqlitedb.as_mut().unwrap().url =
            Some(root.join("chroma.sqlite3").to_str().unwrap().into());
        let manager = config.segment_manager.as_mut().unwrap();
        manager.persist_path = Some(root.to_str().unwrap().into());
        manager.hnsw_index_pool_cache_config = CacheConfig::Memory(FoyerCacheConfig {
            capacity: 1,
            shards: 1,
            ..Default::default()
        });
        let frontend = runtime
            .block_on(ServiceBasedFrontend::try_from_config(
                &(config, system.clone()),
                &registry,
            ))
            .unwrap();
        Self {
            frontend,
            registry,
            system,
            runtime,
            pins: vec![],
        }
    }

    fn close(self) {
        self.runtime.block_on(async {
            let mut handle = self
                .registry
                .get::<ComponentHandle<LocalCompactionManager>>()
                .unwrap();
            handle.stop();
            handle.join().await.unwrap();
            self.system.stop().await;
            self.system.join().await;
        });
        drop(self.frontend);
        drop(self.pins);
        drop(self.registry);
        self.runtime.shutdown_timeout(Duration::from_secs(2));
    }

    fn create(&mut self, slot: usize, sync_threshold: usize) -> Collection {
        self.runtime
            .block_on(
                self.frontend.create_collection(
                    CreateCollectionRequest::try_new(
                        "default_tenant".into(),
                        DatabaseName::new("default_database").unwrap(),
                        format!("collection-{slot}"),
                        None,
                        Some(InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Hnsw(
                                InternalHnswConfiguration {
                                    space: Space::L2,
                                    sync_threshold,
                                    ..Default::default()
                                },
                            ),
                            embedding_function: None,
                        }),
                        None,
                        false,
                    )
                    .unwrap(),
                ),
            )
            .unwrap()
    }

    fn check(&mut self, collections: &[Collection], models: &[BTreeMap<String, Vec<f32>>]) {
        self.runtime.block_on(async {
            for (collection, model) in collections.iter().zip(models) {
                let result = Box::pin(
                    self.frontend.get(
                        GetRequest::try_new(
                            collection.tenant.clone(),
                            collection.database.clone(),
                            collection.collection_id,
                            None,
                            None,
                            None,
                            0,
                            IncludeList(vec![Include::Embedding]),
                        )
                        .unwrap(),
                    ),
                )
                .await
                .unwrap();
                assert_eq!(result.ids.len(), model.len());
                let actual: BTreeMap<_, _> = result
                    .ids
                    .into_iter()
                    .zip(result.embeddings.unwrap())
                    .collect();
                assert_eq!(&actual, model, "get after lifecycle transition");
                let query = Box::pin(
                    self.frontend.query(
                        QueryRequest::try_new(
                            collection.tenant.clone(),
                            collection.database.clone(),
                            collection.collection_id,
                            None,
                            None,
                            vec![vec![0.0; 3]],
                            32,
                            IncludeList(vec![Include::Embedding, Include::Distance]),
                        )
                        .unwrap(),
                    ),
                )
                .await
                .unwrap();
                assert_eq!(
                    query.ids[0].len(),
                    model.len(),
                    "query must not duplicate records"
                );
                let actual: BTreeMap<_, _> = query.ids[0]
                    .iter()
                    .cloned()
                    .zip(
                        query.embeddings.unwrap()[0]
                            .iter()
                            .cloned()
                            .map(Option::unwrap),
                    )
                    .zip(
                        query.distances.unwrap()[0]
                            .iter()
                            .copied()
                            .map(Option::unwrap),
                    )
                    .map(|((id, embedding), distance)| (id, (embedding, distance)))
                    .collect();
                let expected: BTreeMap<_, _> = model
                    .iter()
                    .map(|(id, embedding)| {
                        (
                            id.clone(),
                            (
                                embedding.clone(),
                                embedding.iter().map(|v| v * v).sum::<f32>(),
                            ),
                        )
                    })
                    .collect();
                assert_eq!(
                    actual, expected,
                    "query must agree with metadata and embeddings"
                );
            }
        });
    }
}

#[derive(Clone, Debug)]
enum Action {
    Write {
        slot: usize,
        kind: u8,
        id: u8,
        value: i16,
    },
    Delete {
        slot: usize,
        id: u8,
    },
    Read,
    Restart,
    Pin {
        slot: usize,
    },
    Recreate {
        slot: usize,
    },
    WrongDimension {
        slot: usize,
    },
}

fn actions() -> impl Strategy<Value = Action> {
    prop_oneof![
        6 => (0..3usize, 0..3u8, 0..8u8, -8..9i16).prop_map(|(slot, kind, id, value)| Action::Write {slot, kind, id, value}),
        3 => (0..3usize, 0..8u8).prop_map(|(slot, id)| Action::Delete {slot, id}),
        2 => Just(Action::Read),
        2 => Just(Action::Restart),
        2 => (0..3usize).prop_map(|slot| Action::Pin {slot}),
        1 => (0..3usize).prop_map(|slot| Action::Recreate {slot}),
        1 => (0..3usize).prop_map(|slot| Action::WrongDimension {slot}),
    ]
}

fn run_history(sync_threshold: usize, history: &[Action]) {
    let root = tempfile::tempdir().unwrap();
    let mut running = Running::open(root.path());
    let mut collections: Vec<_> = (0..3)
        .map(|slot| running.create(slot, sync_threshold))
        .collect();
    let mut models = vec![BTreeMap::new(); 3];
    for action in history {
        match *action {
            Action::Restart => {
                running.close();
                running = Running::open(root.path());
            }
            Action::Recreate { slot } => {
                let collection = &collections[slot];
                running
                    .runtime
                    .block_on(
                        running.frontend.delete_collection(
                            DeleteCollectionRequest::try_new(
                                collection.tenant.clone(),
                                collection.database.clone(),
                                collection.name.clone(),
                            )
                            .unwrap(),
                        ),
                    )
                    .unwrap();
                collections[slot] = running.create(slot, sync_threshold);
                models[slot].clear();
            }
            Action::Pin { slot } => {
                if !models[slot].is_empty() {
                    let reader = running.runtime.block_on(async {
                        let mut sysdb = running.registry.get::<SysDb>().unwrap();
                        let cs = sysdb
                            .get_collection_with_segments(None, collections[slot].collection_id)
                            .await
                            .unwrap();
                        running
                            .registry
                            .get::<LocalSegmentManager>()
                            .unwrap()
                            .get_hnsw_writer(&cs.collection, &cs.vector_segment, 3)
                            .await
                            .map(|writer| LocalHnswSegmentReader::from_index(writer.index))
                            .unwrap()
                    });
                    running.pins.push(reader);
                }
            }
            Action::Write {
                slot,
                kind,
                id,
                value,
            } => {
                let c = &collections[slot];
                let id = id.to_string();
                let embedding = vec![f32::from(value), f32::from(value) + 1.0, 2.0];
                running.runtime.block_on(async {
                    match kind {
                        0 => {
                            running
                                .frontend
                                .add(
                                    AddCollectionRecordsRequest::try_new(
                                        c.tenant.clone(),
                                        c.database.clone(),
                                        c.collection_id,
                                        vec![id.clone()],
                                        vec![embedding.clone()],
                                        None,
                                        None,
                                        None,
                                    )
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                        }
                        1 => {
                            running
                                .frontend
                                .update(
                                    UpdateCollectionRecordsRequest::try_new(
                                        c.tenant.clone(),
                                        c.database.clone(),
                                        c.collection_id,
                                        vec![id.clone()],
                                        Some(vec![Some(embedding.clone())]),
                                        None,
                                        None,
                                        None,
                                    )
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                        }
                        _ => {
                            running
                                .frontend
                                .upsert(
                                    UpsertCollectionRecordsRequest::try_new(
                                        c.tenant.clone(),
                                        c.database.clone(),
                                        c.collection_id,
                                        vec![id.clone()],
                                        vec![embedding.clone()],
                                        None,
                                        None,
                                        None,
                                    )
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                        }
                    }
                });
                match kind {
                    0 => {
                        models[slot].entry(id).or_insert(embedding);
                    }
                    1 => {
                        if let Some(value) = models[slot].get_mut(&id) {
                            *value = embedding;
                        }
                    }
                    _ => {
                        models[slot].insert(id, embedding);
                    }
                }
            }
            Action::Delete { slot, id } => {
                let c = &collections[slot];
                running
                    .runtime
                    .block_on(Box::pin(
                        running.frontend.delete(
                            DeleteCollectionRecordsRequest::try_new(
                                c.tenant.clone(),
                                c.database.clone(),
                                c.collection_id,
                                Some(vec![id.to_string()]),
                                None,
                                None,
                            )
                            .unwrap(),
                            String::new(),
                        ),
                    ))
                    .unwrap();
                models[slot].remove(&id.to_string());
            }
            Action::WrongDimension { slot } => {
                // Only exercise a mismatch once a successful write fixed dimension.
                if !models[slot].is_empty() {
                    let c = &collections[slot];
                    running.runtime.block_on(async {
                        let db = running.registry.get::<SqliteDb>().unwrap();
                        let before: i64 =
                            sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
                                .fetch_one(db.get_conn())
                                .await
                                .unwrap();
                        assert!(running
                            .frontend
                            .add(
                                AddCollectionRecordsRequest::try_new(
                                    c.tenant.clone(),
                                    c.database.clone(),
                                    c.collection_id,
                                    vec!["bad".into()],
                                    vec![vec![1.0; 2]],
                                    None,
                                    None,
                                    None
                                )
                                .unwrap()
                            )
                            .await
                            .is_err());
                        let after: i64 =
                            sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
                                .fetch_one(db.get_conn())
                                .await
                                .unwrap();
                        assert_eq!(before, after, "rejected writes must not append logs");
                    });
                }
            }
            Action::Read => {}
        }
        running.check(&collections, &models);
    }
    running.close();
    // Every generated history also checks reconstruction from the remaining WAL.
    let mut reopened = Running::open(root.path());
    reopened.check(&collections, &models);
    reopened.close();
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, max_shrink_iters: 512, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/local_persistence.proptest-regressions")))), ..ProptestConfig::default() })]
    #[test]
    fn local_persistence_matches_model(sync_threshold in 2..9usize, history in prop::collection::vec(actions(), 1..50)) {
        run_history(sync_threshold, &history);
    }
}

#[test]
fn single_vector_updates_survive_checkpoint_and_restart() {
    let write = |kind, id, value| Action::Write {
        slot: 0,
        kind,
        id,
        value,
    };
    run_history(
        2,
        &[
            write(0, 0, 0),
            // Duplicate add checkpoints the initial vector without changing it.
            write(0, 0, 9),
            write(1, 0, 1),
            // Updating a missing ID checkpoints the preceding replacement.
            write(1, 1, 0),
            Action::Restart,
            write(2, 0, 2),
            write(1, 1, 0),
            Action::Restart,
        ],
    );
}

#[test]
fn held_reader_survives_eviction_and_subsequent_persistence() {
    run_history(
        2,
        &[
            Action::Write {
                slot: 0,
                kind: 0,
                id: 0,
                value: 0,
            },
            Action::Pin { slot: 0 },
            Action::Write {
                slot: 1,
                kind: 0,
                id: 0,
                value: 0,
            },
            Action::Write {
                slot: 0,
                kind: 2,
                id: 1,
                value: 1,
            },
            Action::Restart,
        ],
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/local_persistence.proptest-regressions")))), ..ProptestConfig::default() })]
    #[test]
    fn corrupt_store_rejects_writes_without_growing_logs(file in 0..4usize, missing in any::<bool>()) {
        let root = tempfile::tempdir().unwrap();
        let mut running = Running::open(root.path());
        let collection = running.create(0, 2);
        let segment = running.runtime.block_on(async {
            running.frontend.add(AddCollectionRecordsRequest::try_new(
                collection.tenant.clone(), collection.database.clone(), collection.collection_id,
                vec!["a".into(), "b".into()], vec![vec![1.0; 3], vec![2.0; 3]], None, None, None,
            ).unwrap()).await.unwrap();
            running.registry.get::<SysDb>().unwrap().get_collection_with_segments(None, collection.collection_id).await.unwrap().vector_segment.id
        });
        running.runtime.block_on(async {
            let db = running.registry.get::<SqliteDb>().unwrap();
            let watermark: i64 = sqlx::query_scalar("SELECT seq_id FROM max_seq_id WHERE segment_id = ?")
                .bind(segment.to_string()).fetch_one(db.get_conn()).await.unwrap();
            assert!(watermark > 0, "fault injection requires a durable checkpoint");
        });
        running.close();
        let filenames = ["index_metadata.pickle", "header.bin", "data_level0.bin", "length.bin"];
        let damaged = root.path().join(segment.to_string()).join(filenames[file]);
        if missing { std::fs::remove_file(&damaged).unwrap(); }
        else { std::fs::write(&damaged, []).unwrap(); }
        let mut reopened = Running::open(root.path());
        reopened.runtime.block_on(async {
            let db = reopened.registry.get::<SqliteDb>().unwrap();
            let before: (i64, Option<i64>) = sqlx::query_as("SELECT COUNT(*), MAX(seq_id) FROM embeddings_queue").fetch_one(db.get_conn()).await.unwrap();
            let watermarks: Vec<(String, i64)> = sqlx::query_as("SELECT segment_id, seq_id FROM max_seq_id ORDER BY segment_id").fetch_all(db.get_conn()).await.unwrap();
            for _ in 0..3 {
                assert!(reopened.frontend.add(AddCollectionRecordsRequest::try_new(
                    collection.tenant.clone(), collection.database.clone(), collection.collection_id,
                    vec!["new".into()], vec![vec![3.0; 3]], None, None, None,
                ).unwrap()).await.is_err());
            }
            let after: (i64, Option<i64>) = sqlx::query_as("SELECT COUNT(*), MAX(seq_id) FROM embeddings_queue").fetch_one(db.get_conn()).await.unwrap();
            let after_watermarks: Vec<(String, i64)> = sqlx::query_as("SELECT segment_id, seq_id FROM max_seq_id ORDER BY segment_id").fetch_all(db.get_conn()).await.unwrap();
            assert_eq!((after, after_watermarks), (before, watermarks));
        });
        reopened.close();
    }
}

#[test]
fn missing_hnsw_configuration_rejects_before_append() {
    let root = tempfile::tempdir().unwrap();
    let mut running = Running::open(root.path());
    let collection = running.create(0, 2);
    running.runtime.block_on(async {
        let db = running.registry.get::<SqliteDb>().unwrap();
        sqlx::query("UPDATE collections SET schema_str = ? WHERE id = ?")
            .bind(serde_json::to_string(&Schema::new_default(KnnIndex::Spann)).unwrap())
            .bind(collection.collection_id.to_string())
            .execute(db.get_conn())
            .await
            .unwrap();
    });
    running.close();
    let mut reopened = Running::open(root.path());
    reopened.runtime.block_on(async {
        assert!(reopened
            .frontend
            .add(
                AddCollectionRecordsRequest::try_new(
                    collection.tenant.clone(),
                    collection.database.clone(),
                    collection.collection_id,
                    vec!["bad".into()],
                    vec![vec![1.0; 3]],
                    None,
                    None,
                    None,
                )
                .unwrap()
            )
            .await
            .is_err());
        let db = reopened.registry.get::<SqliteDb>().unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM embeddings_queue")
            .fetch_one(db.get_conn())
            .await
            .unwrap();
        assert_eq!(count, 0);
    });
    reopened.close();
}

#[test]
fn concurrent_first_writes_commit_only_the_winning_dimension() {
    let root = tempfile::tempdir().unwrap();
    let mut running = Running::open(root.path());
    let collection = running.create(0, 2);
    let winning_dimension = running.runtime.block_on(async {
        let mut first = running.frontend.clone();
        let mut second = running.frontend.clone();
        let request = |dimension: usize| {
            AddCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                vec![dimension.to_string()],
                vec![vec![1.0; dimension]],
                None,
                None,
                None,
            )
            .unwrap()
        };
        let (first, second) = tokio::join!(first.add(request(2usize)), second.add(request(3usize)));
        match (first, second) {
            (Ok(_), Err(_)) => 2usize,
            (Err(_), Ok(_)) => 3usize,
            _ => panic!("exactly one dimension must win"),
        }
    });
    running.close();
    let mut reopened = Running::open(root.path());
    reopened.runtime.block_on(async {
        let result = Box::pin(
            reopened.frontend.get(
                GetRequest::try_new(
                    collection.tenant,
                    collection.database,
                    collection.collection_id,
                    None,
                    None,
                    None,
                    0,
                    IncludeList(vec![Include::Embedding]),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            (result.ids, result.embeddings),
            (
                vec![winning_dimension.to_string()],
                Some(vec![vec![1.0; winning_dimension]])
            )
        );
    });
    reopened.close();
}

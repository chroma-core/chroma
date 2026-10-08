use super::*;
use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{impls::service_based_frontend::ServiceBasedFrontend, FrontendConfig};
use chroma_log::LocalCompactionManager;
use chroma_sqlite::db::SqliteDb;
use chroma_system::{ComponentHandle, System};
use chroma_types::*;
use std::collections::BTreeMap;

async fn open(root: &Path) -> (ServiceBasedFrontend, Registry, System) {
    open_with_hash(root, MigrationHash::MD5).await
}

async fn open_with_hash(
    root: &Path,
    hash_type: MigrationHash,
) -> (ServiceBasedFrontend, Registry, System) {
    let registry = Registry::new();
    let system = System::new();
    let mut config = FrontendConfig::sqlite_in_memory();
    config.sqlitedb.as_mut().unwrap().url =
        Some(root.join("chroma.sqlite3").to_str().unwrap().into());
    config.sqlitedb.as_mut().unwrap().hash_type = hash_type;
    config.segment_manager.as_mut().unwrap().persist_path = Some(root.to_str().unwrap().into());
    let frontend = ServiceBasedFrontend::try_from_config(&(config, system.clone()), &registry)
        .await
        .unwrap();
    (frontend, registry, system)
}

async fn stop(frontend: ServiceBasedFrontend, registry: Registry, system: System) {
    let mut handle = registry
        .get::<ComponentHandle<LocalCompactionManager>>()
        .unwrap();
    handle.stop();
    handle.join().await.unwrap();
    system.stop().await;
    system.join().await;
    drop(frontend);
    drop(registry);
}

fn add_request(collection: &Collection, ids: &[&str]) -> AddCollectionRecordsRequest {
    AddCollectionRecordsRequest::try_new(
        collection.tenant.clone(),
        collection.database.clone(),
        collection.collection_id,
        ids.iter().map(|id| (*id).into()).collect(),
        vec![vec![1.0, 2.0, 3.0]; ids.len()],
        None,
        None,
        None,
    )
    .unwrap()
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            for (path, bytes) in snapshot(&entry.path()) {
                files.insert(PathBuf::from(entry.file_name()).join(path), bytes);
            }
        } else {
            files.insert(entry.file_name().into(), fs::read(entry.path()).unwrap());
        }
    }
    files
}

#[tokio::test]
async fn repairs_legacy_construction_settings_and_preserves_records_and_logs() {
    for (legacy_metadata, replacement, old_version, hash_type, persisted_ef) in [
        (false, 100, None, MigrationHash::MD5, 10000usize),
        (false, 100, None, MigrationHash::MD5, 0usize),
        (true, 1, None, MigrationHash::MD5, 4097usize),
        (true, 100, Some(9), MigrationHash::SHA256, 10000usize),
        (true, 1, Some(6), MigrationHash::MD5, 10000usize),
    ] {
        let parent = tempfile::tempdir().unwrap();
        let source = parent.path().join("source");
        fs::create_dir(&source).unwrap();
        let (mut frontend, registry, system) = open_with_hash(&source, hash_type).await;
        let collection = frontend
            .create_collection(
                CreateCollectionRequest::try_new(
                    "default_tenant".into(),
                    DatabaseName::new("default_database").unwrap(),
                    "repair-test".into(),
                    None,
                    Some(InternalCollectionConfiguration {
                        vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                            space: if legacy_metadata {
                                Space::Cosine
                            } else {
                                Space::L2
                            },
                            sync_threshold: 2,
                            ..Default::default()
                        }),
                        embedding_function: None,
                    }),
                    None,
                    false,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        frontend
            .add(add_request(&collection, &["a", "b"]))
            .await
            .unwrap();
        frontend
            .add(add_request(&collection, &["tail"]))
            .await
            .unwrap();
        let db = registry.get::<SqliteDb>().unwrap();
        let segment: String =
            sqlx::query_scalar("SELECT id FROM segments WHERE collection = ? AND scope = 'VECTOR'")
                .bind(collection.collection_id.to_string())
                .fetch_one(db.get_conn())
                .await
                .unwrap();
        let before_logs: Vec<i64> =
            sqlx::query_scalar("SELECT seq_id FROM embeddings_queue ORDER BY seq_id")
                .fetch_all(db.get_conn())
                .await
                .unwrap();
        assert!(!before_logs.is_empty());
        let before_watermarks: Vec<(String, i64)> =
            sqlx::query_as("SELECT segment_id, seq_id FROM max_seq_id ORDER BY segment_id")
                .fetch_all(db.get_conn())
                .await
                .unwrap();
        // Model persisted settings from an older release, bypassing API validation.
        if legacy_metadata {
            sqlx::query(
                "UPDATE collections SET config_json_str = '{}', schema_str = NULL WHERE id = ?",
            )
            .bind(collection.collection_id.to_string())
            .execute(db.get_conn())
            .await
            .unwrap();
            for (key, value) in [("hnsw:construction_ef", 10000), ("hnsw:sync_threshold", 2)] {
                sqlx::query(
                    "INSERT INTO segment_metadata (segment_id, key, int_value) VALUES (?, ?, ?)",
                )
                .bind(&segment)
                .bind(key)
                .bind(value)
                .execute(db.get_conn())
                .await
                .unwrap();
            }
            sqlx::query("INSERT INTO segment_metadata (segment_id, key, str_value) VALUES (?, 'hnsw:space', 'cosine')")
                .bind(&segment)
                .execute(db.get_conn())
                .await
                .unwrap();
        } else {
            let mut config = collection.config.clone();
            if let VectorIndexConfiguration::Hnsw(hnsw) = &mut config.vector_index {
                hnsw.ef_construction = 10000;
            }
            sqlx::query("UPDATE collections SET config_json_str = ?, schema_str = ? WHERE id = ?")
                .bind(serde_json::to_string(&config).unwrap())
                .bind(serde_json::to_string(&Schema::try_from(&config).unwrap()).unwrap())
                .bind(collection.collection_id.to_string())
                .execute(db.get_conn())
                .await
                .unwrap();
        }
        drop(db);
        stop(frontend, registry, system).await;
        let header_path = source.join(&segment).join(HNSW_HEADER_FILE);
        let mut header = fs::read(&header_path).unwrap();
        let offset = 20 + 9 * std::mem::size_of::<usize>();
        header[offset..offset + std::mem::size_of::<usize>()]
            .copy_from_slice(&persisted_ef.to_ne_bytes());
        fs::write(&header_path, header).unwrap();
        assert!(inspect_persisted_hnsw_index(&source.join(&segment)).is_err());
        // Reopening doesn't fix it, and rejected writes must not append records.
        let (mut frontend, registry, system) = open_with_hash(&source, hash_type).await;
        for _ in 0..3 {
            assert!(frontend
                .add(add_request(&collection, &["rejected"]))
                .await
                .is_err());
        }
        stop(frontend, registry, system).await;
        if let Some(version) = old_version {
            // Restore the pre-schema catalog and its migration ledger, including
            // an older variant that also lacks config_json_str.
            let mut db = SqliteConnection::connect_with(
                &SqliteConnectOptions::new().filename(source.join("chroma.sqlite3")),
            )
            .await
            .unwrap();
            sqlx::query("ALTER TABLE collections DROP COLUMN schema_str")
                .execute(&mut db)
                .await
                .unwrap();
            sqlx::query("DROP TABLE index_cleanup")
                .execute(&mut db)
                .await
                .unwrap();
            if version < 7 {
                sqlx::query("ALTER TABLE collections DROP COLUMN config_json_str")
                    .execute(&mut db)
                    .await
                    .unwrap();
                sqlx::query("DROP TABLE maintenance_log")
                    .execute(&mut db)
                    .await
                    .unwrap();
            }
            sqlx::query("DELETE FROM migrations WHERE dir = 'sysdb' AND version > ?")
                .bind(version)
                .execute(&mut db)
                .await
                .unwrap();
            db.close().await.unwrap();
        }
        let original = snapshot(&source);
        let args = HnswConfigRepairArgs {
            path: source.clone(),
            output: parent.path().join("repaired"),
            collection: collection.collection_id,
            ef_construction: replacement,
        };
        // Configuration repair must not publish a structurally corrupt index.
        let saved_header = fs::read(&header_path).unwrap();
        fs::write(&header_path, b"bad").unwrap();
        assert!(repair(&args).await.is_err());
        assert!(!args.output.exists());
        fs::write(&header_path, saved_header).unwrap();
        repair(&args).await.unwrap();
        assert_eq!(
            snapshot(&source),
            original,
            "source must remain byte-for-byte intact"
        );
        let header = fs::read(args.output.join(&segment).join(HNSW_HEADER_FILE)).unwrap();
        assert_eq!(
            &header[offset..offset + std::mem::size_of::<usize>()],
            &(replacement as usize).max(16).to_ne_bytes()
        );
        assert!(inspect_persisted_hnsw_index(&args.output.join(&segment)).is_ok());
        let (mut frontend, registry, system) = open_with_hash(&args.output, hash_type).await;
        let db = registry.get::<SqliteDb>().unwrap();
        let schema: Option<String> =
            sqlx::query_scalar("SELECT schema_str FROM collections WHERE id = ?")
                .bind(collection.collection_id.to_string())
                .fetch_one(db.get_conn())
                .await
                .unwrap();
        // An absent schema must remain absent: serializing machine-dependent
        // defaults can disable legacy cosine fallback on a different CPU count.
        assert_eq!(schema.is_none(), legacy_metadata);
        let logs: Vec<i64> =
            sqlx::query_scalar("SELECT seq_id FROM embeddings_queue ORDER BY seq_id")
                .fetch_all(db.get_conn())
                .await
                .unwrap();
        let watermarks: Vec<(String, i64)> =
            sqlx::query_as("SELECT segment_id, seq_id FROM max_seq_id ORDER BY segment_id")
                .fetch_all(db.get_conn())
                .await
                .unwrap();
        assert_eq!(logs, before_logs);
        assert_eq!(watermarks, before_watermarks);
        drop(db);
        frontend
            .add(add_request(&collection, &["after"]))
            .await
            .unwrap();
        let result = Box::pin(
            frontend.get(
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
        let records: BTreeMap<_, _> = result
            .ids
            .into_iter()
            .zip(result.embeddings.unwrap())
            .collect();
        assert_eq!(
            records,
            ["a", "b", "tail", "after"]
                .into_iter()
                .map(|id| (id.to_string(), vec![1.0, 2.0, 3.0]))
                .collect()
        );
        stop(frontend, registry, system).await;
        // The same store remains usable after another restart.
        let (mut frontend, registry, system) = open_with_hash(&args.output, hash_type).await;
        frontend
            .add(add_request(&collection, &["restart"]))
            .await
            .unwrap();
        stop(frontend, registry, system).await;
        assert!(
            repair(&args).await.is_err(),
            "never overwrite an existing output"
        );
        assert_eq!(snapshot(&source), original);
    }
}

#[tokio::test]
async fn rejects_invalid_arguments_and_failed_repairs_without_publishing_output() {
    let parent = tempfile::tempdir().unwrap();
    let source = parent.path().join("source");
    fs::create_dir(&source).unwrap();
    let (frontend, registry, system) = open(&source).await;
    stop(frontend, registry, system).await;
    let original = snapshot(&source);
    let mut args = HnswConfigRepairArgs {
        path: source.clone(),
        output: parent.path().join("repaired"),
        collection: CollectionUuid::new(),
        ef_construction: 100,
    };
    for value in [0, 4097, u32::MAX, 100] {
        args.ef_construction = value;
        assert!(repair(&args).await.is_err());
        assert!(!args.output.exists());
        assert_eq!(snapshot(&source), original);
    }
    args.output = source.join("nested");
    assert!(repair(&args).await.is_err());
    assert!(!args.output.exists());

    // Migration validation must fail without modifying the original or
    // publishing a destination, even when the hash algorithm is recognized.
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(source.join("chroma.sqlite3")),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE migrations SET hash = '00000000000000000000000000000000'")
        .execute(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();
    let original = snapshot(&source);
    args.output = parent.path().join("repaired");
    let error = repair(&args).await.unwrap_err();
    assert!(error.to_string().contains("Inconsistent hash"), "{error}");
    assert!(!args.output.exists());
    assert_eq!(snapshot(&source), original);
}

#[path = "../benches/sharding/storage_audit.rs"]
mod storage_audit;
use chroma_blockstore::test_utils::sparse_index_test_utils::create_test_sparse_index;
use chroma_storage::{local::LocalStorage, Storage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const FIELDS: [&str; 5] = [
    "posting_list_id",
    "scalar_metadata_id",
    "vector_data_id",
    "leaf_node_id",
    "internal_node_id",
];
struct Fixture {
    _temporary: tempfile::TempDir,
    directory: PathBuf,
    current_unique: Uuid,
    current_roots: Vec<Uuid>,
}
fn sha(path: &Path) -> String {
    format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
}
async fn make_fixture() -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("index");
    fs::create_dir_all(directory.join("data/block")).unwrap();
    let storage = Storage::Local(LocalStorage::new(directory.join("data").to_str().unwrap()));
    let shared = Uuid::new_v4();
    let old = Uuid::new_v4();
    let current_unique = Uuid::new_v4();
    let orphan = Uuid::new_v4();
    // These are intentionally not valid Arrow blocks. The diagnostic must only
    // read root metadata and stat payload files, so decoding one would fail.
    for (id, n) in [(shared, 11), (old, 13), (current_unique, 17), (orphan, 19)] {
        fs::write(directory.join(format!("data/block/{id}")), vec![7; n]).unwrap();
    }
    let mut current_roots = Vec::new();
    for generation in [1, 2] {
        let mut ids = BTreeMap::new();
        for (index, field) in FIELDS.iter().enumerate() {
            let id = Uuid::new_v4();
            let block = if index == 2 {
                if generation == 1 {
                    old
                } else {
                    current_unique
                }
            } else {
                shared
            };
            create_test_sparse_index(&storage, id, vec![block], None, "".into())
                .await
                .unwrap();
            ids.insert(*field, id.to_string());
            if generation == 2 {
                current_roots.push(id);
            }
        }
        let count = generation + 1;
        let map = directory.join(format!("id-map-{generation}.u64le"));
        fs::write(
            &map,
            (0u64..count).flat_map(u64::to_le_bytes).collect::<Vec<_>>(),
        )
        .unwrap();
        let manifest = json!({"generation":generation,"shard_id":"owner","source_id":"source","dimension":768,"metric":"ip","vector_count":count,
            "block_ids":ids,"id_mapping":{"path":map.file_name().unwrap().to_str().unwrap(),"count":count,"bytes":count*8,"sha256":sha(&map)}});
        fs::write(
            directory.join(format!("manifest-{generation}.json")),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
    create_test_sparse_index(&storage, Uuid::new_v4(), vec![orphan], None, "".into())
        .await
        .unwrap();
    fs::hard_link(
        directory.join("manifest-2.json"),
        directory.join("manifest.json"),
    )
    .unwrap();
    fs::write(directory.join("data/unknown.bin"), b"unknown").unwrap();
    fs::write(directory.join("notes.txt"), b"notes").unwrap();
    Fixture {
        _temporary: temporary,
        directory,
        current_unique,
        current_roots,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_audit_binds_all_generations_deduplicates_shared_blocks_and_never_reads_payloads() {
    let fixture = make_fixture().await;
    let options = storage_audit::Options::new(fixture.directory.clone());
    let report = storage_audit::audit(&options).await.unwrap();
    assert_eq!(report["current_live_objects"]["files"], 7);
    assert_eq!(report["retained_union_live_objects"]["files"], 13);
    assert_eq!(
        report["not_referenced_by_enumerated_retained_manifests"]["summary"]["files"],
        2
    );
    assert_eq!(report["generations"].as_array().unwrap().len(), 2);
    assert_eq!(report["root_objects"].as_object().unwrap().len(), 10);
    assert_eq!(report["read_work"]["raw_block_payloads_read"], false);
    assert_eq!(report["read_work"]["map_bytes_hashed"], 40);
    assert_eq!(report["no_cleanup"], true);
    assert!(report["unknown_paths"].get("data/unknown.bin").is_some());
    assert!(report["unknown_paths"].get("notes.txt").is_some());
    let physical = &report["total_owned_checkpoint_regular_files"];
    assert!(
        physical["logical_file_bytes"].as_u64().unwrap()
            > physical["unique_inode_file_bytes"].as_u64().unwrap()
    );
    let again = storage_audit::audit(&options).await.unwrap();
    assert_eq!(report["inventory"], again["inventory"]);
    assert_eq!(
        report["current_manifest_sha256"],
        sha(&fixture.directory.join("manifest.json"))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_audit_rejects_missing_objects_bad_maps_active_writers_and_resource_limits() {
    let fixture = make_fixture().await;
    let mut options = storage_audit::Options::new(fixture.directory.clone());
    options.expected_current_sha256 = Some("wrong".into());
    assert!(storage_audit::audit(&options).await.is_err());
    options.expected_current_sha256 = None;
    fs::write(fixture.directory.join("build.lock"), b"writer").unwrap();
    assert!(storage_audit::audit(&options)
        .await
        .unwrap_err()
        .to_string()
        .contains("build.lock"));
    fs::remove_file(fixture.directory.join("build.lock")).unwrap();
    for kind in ["entries", "manifests", "root_bytes", "references"] {
        let mut limited = options.clone();
        match kind {
            "entries" => limited.max_inventory_entries = 1,
            "manifests" => limited.max_manifests = 1,
            "root_bytes" => limited.max_root_bytes = 1,
            _ => limited.max_root_references = 1,
        };
        assert!(
            storage_audit::audit(&limited).await.is_err(),
            "limit {kind}"
        );
    }
    let map = fixture.directory.join("id-map-2.u64le");
    let original = fs::read(&map).unwrap();
    let mut changed = original.clone();
    changed[0] ^= 1;
    fs::write(&map, changed).unwrap();
    assert!(storage_audit::audit(&options)
        .await
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    fs::write(&map, original).unwrap();
    fs::remove_file(
        fixture
            .directory
            .join(format!("data/block/{}", fixture.current_unique)),
    )
    .unwrap();
    assert!(storage_audit::audit(&options)
        .await
        .unwrap_err()
        .to_string()
        .contains("missing"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_retained_manifest_preserves_old_roots_and_conflicts_fail_closed() {
    let fixture = make_fixture().await;
    let external = fixture._temporary.path().join("retained.json");
    fs::rename(fixture.directory.join("manifest-1.json"), &external).unwrap();
    let options = storage_audit::Options::new(fixture.directory.clone());
    let current_only = storage_audit::audit(&options).await.unwrap();
    assert_eq!(current_only["retained_union_live_objects"]["files"], 7);
    let mut options = options;
    options.retained_manifests.push(external.clone());
    let report = storage_audit::audit(&options).await.unwrap();
    assert_eq!(report["retained_union_live_objects"]["files"], 13);
    let mut conflicting: Value = serde_json::from_slice(&fs::read(&external).unwrap()).unwrap();
    conflicting["vector_count"] = json!(999);
    let conflict = fixture._temporary.path().join("z-conflict.json");
    fs::write(&conflict, serde_json::to_vec(&conflicting).unwrap()).unwrap();
    options.retained_manifests.push(conflict);
    assert!(storage_audit::audit(&options)
        .await
        .unwrap_err()
        .to_string()
        .contains("conflicting"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_root_metadata_and_symlink_payloads_are_rejected_without_following_links() {
    let fixture = make_fixture().await;
    let options = storage_audit::Options::new(fixture.directory.clone());
    let root = fixture
        .directory
        .join(format!("data/sparse_index/{}", fixture.current_roots[0]));
    fs::write(root, b"invalid root").unwrap();
    assert!(storage_audit::audit(&options).await.is_err());
    #[cfg(unix)]
    {
        let fixture = make_fixture().await;
        let options = storage_audit::Options::new(fixture.directory.clone());
        let outside = fixture._temporary.path().join("outside");
        fs::write(&outside, b"outside").unwrap();
        std::os::unix::fs::symlink(&outside, fixture.directory.join("data/unknown-link")).unwrap();
        let report = storage_audit::audit(&options).await.unwrap();
        assert_eq!(
            report["unknown_paths"]["data/unknown-link"]["kind"],
            "symlink_not_followed"
        );
        let block = fixture
            .directory
            .join(format!("data/block/{}", fixture.current_unique));
        fs::remove_file(&block).unwrap();
        std::os::unix::fs::symlink(outside, block).unwrap();
        assert!(storage_audit::audit(&options)
            .await
            .unwrap_err()
            .to_string()
            .contains("regular"));
    }
}

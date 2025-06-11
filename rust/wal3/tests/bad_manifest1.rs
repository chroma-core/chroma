use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chroma_storage::s3_client_for_test_with_new_bucket;
use wal3::{Garbage, LogPosition, Manifest, Snapshot, SnapshotPointer, ThrottleOptions};

use wal3::SnapshotCache;

struct MockSnapshotCache {
    snapshots: Mutex<HashMap<String, Snapshot>>,
}

impl MockSnapshotCache {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    fn load_from_json(&self, snapshots_json: &str) {
        let snapshots: Vec<Snapshot> =
            serde_json::from_str(snapshots_json).expect("Failed to parse snapshots JSON");

        let mut cache = self.snapshots.lock().unwrap();
        for snapshot in snapshots {
            let key = format!("{}:{}", snapshot.path, snapshot.setsum.hexdigest());
            cache.insert(key, snapshot);
        }
    }
}

#[async_trait::async_trait]
impl SnapshotCache for MockSnapshotCache {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, wal3::Error> {
        let cache = self.snapshots.lock().unwrap();
        let key = format!("{}:{}", ptr.path_to_snapshot, ptr.setsum.hexdigest());
        Ok(cache.get(&key).cloned())
    }

    async fn put(&self, _: &SnapshotPointer, snap: &Snapshot) -> Result<(), wal3::Error> {
        let mut cache = self.snapshots.lock().unwrap();
        let key = format!("{}:{}", snap.path, snap.setsum.hexdigest());
        cache.insert(key, snap.clone());
        Ok(())
    }
}

#[tokio::test]
async fn test_garbage_new_with_bad_manifest1_offset_9340() {
    // Load the bad manifest from the JSON file
    let manifest_json = include_str!("bad_manifest1.json");
    let manifest: Manifest = serde_json::from_str(manifest_json).expect("Failed to parse manifest");

    // Load the snapshots into the mock cache
    let snapshots_json = include_str!("bad_snapshots1.json");
    let snapshot_cache = MockSnapshotCache::new();
    snapshot_cache.load_from_json(snapshots_json);

    let storage = Arc::new(s3_client_for_test_with_new_bucket().await);
    let throttle = ThrottleOptions::default();

    // The bug should occur when calling Garbage::new with offset 9340
    let first_to_keep = LogPosition::from_offset(9340);

    println!("Manifest snapshots:");
    for snap in &manifest.snapshots {
        println!(
            "  {} -> {} (depth: {}, start: {}, limit: {})",
            snap.setsum.hexdigest(),
            snap.path_to_snapshot,
            snap.depth,
            snap.start.offset(),
            snap.limit.offset()
        );
    }

    println!("Manifest fragments:");
    for frag in &manifest.fragments {
        println!(
            "  {} (start: {}, limit: {})",
            frag.path,
            frag.start.offset(),
            frag.limit.offset()
        );
    }

    println!(
        "\nTrying to create Garbage with first_to_keep = {}",
        first_to_keep.offset()
    );

    // This should fail and demonstrate the bug
    let result = Garbage::new(
        &storage,
        "test-prefix",
        &manifest,
        &throttle,
        &snapshot_cache,
        first_to_keep,
    )
    .await;

    match result {
        Ok(garbage) => {
            println!(
                "Garbage creation succeeded with {} actions",
                garbage.actions.len()
            );
            println!("This means the bug may have been fixed already, or conditions aren't right");
            // Let's examine what was created
            for (i, action) in garbage.actions.iter().enumerate() {
                println!("Action {}: {:?}", i, action);
            }
            panic!("Expected the test to fail due to a bug, but it succeeded");
        }
        Err(e) => {
            println!("Bug reproduced! Error: {:?}", e);
            // This is what we expect - the bug should cause an error
            panic!("BUG REPRODUCED");
        }
    }
}

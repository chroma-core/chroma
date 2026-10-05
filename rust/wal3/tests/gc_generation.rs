//! Deterministic interleavings of GC runs. Local storage supplies durable files;
//! mutations are sequenced explicitly, so these tests do not rely on its CAS support.

use std::sync::Arc;

use chroma_storage::{test_storage, GetOptions, PutOptions, Storage};
use setsum::Setsum;
use wal3::{
    create_s3_factories, unprefixed_fragment_path, Cursor, Error, Fragment, FragmentSeqNo, Garbage,
    GarbageCollectionOptions, GarbageCollectionState, GarbageCollector, LogPosition,
    LogReaderOptions, LogWriterOptions, Manifest, S3FragmentManagerFactory,
    S3ManifestManagerFactory,
};

const PREFIX: &str = "gc-generation";
type Collector = GarbageCollector<
    (FragmentSeqNo, LogPosition),
    S3FragmentManagerFactory,
    S3ManifestManagerFactory,
>;

async fn put_json(storage: &Storage, path: &str, value: &impl serde::Serialize) {
    storage
        .put_bytes(
            &format!("{PREFIX}/{path}"),
            serde_json::to_vec(value).unwrap(),
            PutOptions::default(),
        )
        .await
        .unwrap();
}

async fn seed(storage: &Storage) -> Manifest {
    let mut manifest = Manifest::new_empty("gc-test");
    for n in 1..=4 {
        let seq_no = FragmentSeqNo::from_u64(n).into();
        let path = unprefixed_fragment_path(seq_no);
        let mut setsum = Setsum::default();
        setsum.insert(&n.to_le_bytes());
        storage
            .put_bytes(
                &format!("{PREFIX}/{path}"),
                vec![n as u8],
                PutOptions::default(),
            )
            .await
            .unwrap();
        manifest.apply_fragment(Fragment {
            seq_no,
            path,
            start: LogPosition::from_offset(n),
            limit: LogPosition::from_offset(n + 1),
            num_bytes: 1,
            setsum,
        });
    }
    manifest.scrub().unwrap();
    put_json(storage, "manifest/MANIFEST", &manifest).await;
    put_json(
        storage,
        "cursor/compaction.json",
        &Cursor {
            position: LogPosition::from_offset(4),
            epoch_us: 0,
            writer: "gc-test".to_string(),
        },
    )
    .await;
    manifest
}

fn garbage(manifest: &Manifest, cutoff: u64) -> Garbage {
    let mut garbage = Garbage::empty();
    garbage.fragments_to_drop_start = manifest.fragments[0].seq_no.as_seq_no().unwrap();
    garbage.fragments_to_drop_limit = FragmentSeqNo::from_u64(cutoff);
    garbage.first_to_keep = LogPosition::from_offset(cutoff);
    for fragment in &manifest.fragments {
        if fragment.limit <= garbage.first_to_keep {
            garbage.setsum_to_discard += fragment.setsum;
        }
    }
    garbage
}

async fn open(storage: &Storage) -> Collector {
    let options = LogWriterOptions::default();
    let (fragments, manifests) = create_s3_factories(
        options.clone(),
        LogReaderOptions::default(),
        Arc::new(storage.clone()),
        PREFIX.to_string(),
        "gc-test".to_string(),
        Arc::new(()),
        Arc::new(()),
    );
    Collector::open(options, fragments, manifests)
        .await
        .unwrap()
}

async fn phase1(collector: &Collector) -> GarbageCollectionState {
    collector
        .garbage_collect_phase1_compute_garbage(&GarbageCollectionOptions::default(), None)
        .await
        .unwrap()
        .unwrap()
}

async fn files(storage: &Storage) -> Vec<String> {
    let mut files = Vec::new();
    for n in 1..=4 {
        let path = format!(
            "{PREFIX}/{}",
            unprefixed_fragment_path(FragmentSeqNo::from_u64(n).into())
        );
        match storage.get(&path, GetOptions::default()).await {
            Ok(_) => files.push(path),
            Err(chroma_storage::StorageError::NotFound { .. }) => {}
            Err(err) => panic!("could not read fragment: {err}"),
        }
    }
    files
}

async fn read_garbage(storage: &Storage) -> Garbage {
    let bytes = storage
        .get(&Garbage::path(PREFIX), GetOptions::default())
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn overlapping_gc_does_not_delete_a_newer_generation() {
    let (_dir, storage) = test_storage();
    let manifest = seed(&storage).await;
    let first = garbage(&manifest, 2);
    put_json(&storage, "gc/GARBAGE", &first).await;
    let slow = open(&storage).await;
    let slow_state = phase1(&slow).await;
    let fast = open(&storage).await;
    let fast_state = phase1(&fast).await;

    // Both runs have G1; its phase 2 completes, then the faster run deletes it.
    let manifest = manifest.apply_garbage(first).unwrap().unwrap();
    put_json(&storage, "manifest/MANIFEST", &manifest).await;
    fast.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &fast_state)
        .await
        .unwrap();
    assert_eq!(files(&storage).await.len(), 3);

    // A new run publishes G2 but has not yet performed phase 2.
    let second = garbage(&manifest, 4);
    put_json(&storage, "gc/GARBAGE", &second).await;
    let next = open(&storage).await;
    let next_state = phase1(&next).await;
    let before = files(&storage).await;
    let result = slow
        .garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &slow_state)
        .await;
    assert_eq!(files(&storage).await, before, "G2 is still live");
    assert_eq!(read_garbage(&storage).await, second, "preserve G2's plan");
    assert!(matches!(result, Err(Error::GarbageCollection(_))));

    // The new run remains usable after the old run is rejected.
    let manifest = manifest.apply_garbage(second).unwrap().unwrap();
    put_json(&storage, "manifest/MANIFEST", &manifest).await;
    next.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &next_state)
        .await
        .unwrap();
    assert_eq!(files(&storage).await.len(), 1);
    assert!(read_garbage(&storage).await.is_empty());
}

#[tokio::test]
async fn gc_requires_durable_phase2_before_deleting() {
    let (_dir, storage) = test_storage();
    let manifest = seed(&storage).await;
    let plan = garbage(&manifest, 3);
    put_json(&storage, "gc/GARBAGE", &plan).await;
    let collector = open(&storage).await;
    let state = phase1(&collector).await;
    let before = files(&storage).await;
    let result = collector
        .garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &state)
        .await;
    assert_eq!(files(&storage).await, before);
    assert_eq!(read_garbage(&storage).await, plan);
    assert!(matches!(result, Err(Error::GarbageCollection(_))));

    // The collector's cached manifest predates phase 2. Verification must read
    // the newly persisted manifest, so the same token now succeeds.
    let manifest = manifest.apply_garbage(plan).unwrap().unwrap();
    put_json(&storage, "manifest/MANIFEST", &manifest).await;
    collector
        .garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &state)
        .await
        .unwrap();
    assert_eq!(files(&storage).await.len(), 2);
}

#[tokio::test]
async fn gc_without_phase1_state_preserves_files_and_plan() {
    let (_dir, storage) = test_storage();
    let manifest = seed(&storage).await;
    let plan = garbage(&manifest, 3);
    put_json(&storage, "gc/GARBAGE", &plan).await;
    let collector = open(&storage).await;
    let before = files(&storage).await;
    let result = collector
        .garbage_collect_phase3_delete_garbage(
            &GarbageCollectionOptions::default(),
            &GarbageCollectionState::empty(),
        )
        .await;
    assert_eq!(files(&storage).await, before);
    assert_eq!(read_garbage(&storage).await, plan);
    assert!(matches!(result, Err(Error::GarbageCollection(_))));
}

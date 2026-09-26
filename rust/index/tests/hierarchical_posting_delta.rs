#![recursion_limit = "256"]

#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::persistance::PREFIX_VERSION;
use hierarchical_index::writer::{HierarchicalSpannIds, HierarchicalSpannWriter};

fn embedding(id: u32) -> Vec<f32> {
    (0..32).map(|dim| id as f32 + dim as f32 * 0.01).collect()
}

fn config() -> HierarchicalSpannConfig {
    HierarchicalSpannConfig {
        split_threshold: 512,
        merge_threshold: 0,
        ..Default::default()
    }
}

fn provider(dir: &tempfile::TempDir) -> BlockfileProvider {
    BlockfileProvider::new_arrow(
        Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),
        1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        4,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_combines_persisted_postings_and_delta_with_versions() {
    let dir = tempfile::tempdir().unwrap();
    let blockfiles = provider(&dir);
    let writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    writer.add(10, &embedding(10));
    writer.add(20, &embedding(20));
    let first: HierarchicalSpannIds = writer
        .commit(&blockfiles, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    let reopened = HierarchicalSpannWriter::open(
        &blockfiles,
        first.clone(),
        DistanceFunction::Euclidean,
        config(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.leaf_sizes(), vec![2]);
    assert_eq!(reopened.memory_usage().posting_entries, 0);

    // An existing id needs its persisted version, while its posting stays on disk.
    reopened.add(10, &embedding(110));
    reopened.delete(20);
    reopened.add(30, &embedding(30));
    assert_eq!(reopened.stats.posting_loads.load(Ordering::Relaxed), 0);
    assert_eq!(reopened.memory_usage().posting_entries, 2);
    assert_eq!(reopened.total_leaf_entries(), 4);

    let second = reopened
        .commit(&blockfiles, Some(&first))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let postings = blockfiles
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            second.posting_list_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    let posting = postings.get("", 0).await.unwrap().unwrap();
    assert_eq!(posting.ids.to_vec(), vec![10, 20, 10, 30]);
    assert_eq!(posting.versions.to_vec(), vec![1, 1, 2, 1]);
    assert_eq!(posting.codes.len(), 4 * Code::<1>::size(32));

    let versions = blockfiles
        .read::<u32, u32>(BlockfileReaderOptions::new(
            second.scalar_metadata_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(versions.get(PREFIX_VERSION, 10).await.unwrap(), Some(2));
    assert_eq!(versions.get(PREFIX_VERSION, 20).await.unwrap(), Some(0x81));

    let reopened_again =
        HierarchicalSpannWriter::open(&blockfiles, second, DistanceFunction::Euclidean, config())
            .await
            .unwrap();
    reopened_again.add(20, &embedding(220));
    assert_eq!(reopened_again.total_leaf_entries(), 4);
    reopened_again.load_all_postings().await.unwrap();
    assert_eq!(reopened_again.leaf_sizes(), vec![4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_load_and_additions_keep_every_entry() {
    let dir = tempfile::tempdir().unwrap();
    let blockfiles = provider(&dir);
    let writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    writer.add(1, &embedding(1));
    writer.add(2, &embedding(2));
    let first = writer
        .commit(&blockfiles, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    let reopened = Arc::new(
        HierarchicalSpannWriter::open(
            &blockfiles,
            first.clone(),
            DistanceFunction::Euclidean,
            config(),
        )
        .await
        .unwrap(),
    );
    std::thread::scope(|scope| {
        let additions = Arc::clone(&reopened);
        scope.spawn(move || {
            for id in 3..103 {
                additions.add(id, &embedding(id));
            }
        });
        let loading = Arc::clone(&reopened);
        scope.spawn(move || loading.load_posting_sync(0));
    });
    reopened.load_all_postings().await.unwrap();
    assert_eq!(reopened.leaf_sizes(), vec![102]);
    assert_eq!(reopened.total_leaf_entries(), 102);

    let second = reopened
        .commit(&blockfiles, Some(&first))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let postings = blockfiles
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            second.posting_list_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    let posting = postings.get("", 0).await.unwrap().unwrap();
    assert_eq!(posting.ids.len(), 102);
    let mut ids = posting.ids.to_vec();
    ids.sort_unstable();
    assert_eq!(ids, (1..103).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn split_after_reopen_keeps_all_valid_postings() {
    use std::collections::HashSet;

    let dir = tempfile::tempdir().unwrap();
    let blockfiles = provider(&dir);
    let split_config = HierarchicalSpannConfig {
        split_threshold: 4,
        merge_threshold: 0,
        reassign_neighbor_count: 0,
        ..config()
    };
    let writer =
        HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, split_config.clone());
    for id in 1..5 {
        writer.add(id, &embedding(id));
    }
    let first = writer
        .commit(&blockfiles, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();

    let reopened = HierarchicalSpannWriter::open(
        &blockfiles,
        first.clone(),
        DistanceFunction::Euclidean,
        split_config,
    )
    .await
    .unwrap();
    reopened.add(5, &embedding(5));
    assert_eq!(reopened.stats.posting_loads.load(Ordering::Relaxed), 0);
    reopened.balance_index();
    assert!(reopened.stats.posting_loads.load(Ordering::Relaxed) > 0);
    assert_eq!(
        reopened.root_reachable_valid_ids().unwrap(),
        (1..6).collect()
    );

    let second = reopened
        .commit(&blockfiles, Some(&first))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let postings = blockfiles
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            second.posting_list_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    let versions = blockfiles
        .read::<u32, u32>(BlockfileReaderOptions::new(
            second.scalar_metadata_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    let mut valid = HashSet::new();
    for (_, _, posting) in postings.get_range(""..="", ..).await.unwrap() {
        for (&id, &version) in posting.ids.iter().zip(posting.versions.iter()) {
            if versions.get(PREFIX_VERSION, id).await.unwrap() == Some(version as u32) {
                valid.insert(id);
            }
        }
    }
    assert_eq!(valid, (1..6).collect());
}

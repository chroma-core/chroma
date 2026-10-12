#![recursion_limit = "256"]
#[allow(dead_code)]
#[path = "../benches/hierarchical_index/mod.rs"]
mod hierarchical_index;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use hierarchical_index::common::ReadBeamPolicy;
use hierarchical_index::config::HierarchicalSpannConfig;
use hierarchical_index::reader::{HierarchicalSpannReader, NavigationPrecision};
use hierarchical_index::writer::{HierarchicalSpannIds, HierarchicalSpannWriter};

#[tokio::test(flavor = "multi_thread")]
async fn navigation_modes_reuse_an_unchanged_checkpoint_and_match_search_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let provider = BlockfileProvider::new_arrow(
        Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),
        1024 * 1024,
        new_cache_for_test(),
        new_cache_for_test(),
        4,
        0,
    );
    let config = HierarchicalSpannConfig {
        branching_factor: 4,
        split_threshold: 32,
        merge_threshold: 0,
        max_replicas: 1,
        write_rng_factor: 0.0,
        ..Default::default()
    };
    let writer = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config.clone());
    for id in 0..128u32 {
        let embedding: Vec<_> = (0..32)
            .map(|d| ((id * 17 + d * 7) as f32 * 0.3).sin() + (id / 32) as f32)
            .collect();
        writer.add(id, &embedding);
    }
    writer.balance_index_parallel(1);
    let ids: HierarchicalSpannIds = Box::pin(
        Box::pin(writer.commit(&provider, None))
            .await
            .unwrap()
            .flush(),
    )
    .await
    .unwrap();
    let mut reader = HierarchicalSpannReader::open(
        &provider,
        ids.clone(),
        DistanceFunction::Euclidean,
        config.clone(),
    )
    .await
    .unwrap();
    reader.load_all_postings().await.unwrap();
    let query: Vec<_> = (0..32).map(|d| (d as f32 * 0.5).cos()).collect();
    let policy = ReadBeamPolicy::fixed_widths(vec![2; reader.depth()]);
    let original_payload = reader.navigation_code_bytes();
    let original = reader.search_with_policy_sync(&query, 10, 1, &policy).0;
    reader
        .set_navigation_precision(NavigationPrecision::FourBitFourBit)
        .await
        .unwrap();
    let quantized_trace = reader.navigation_trace(&query, &policy).levels;
    let quantized_results = reader.search_with_policy_sync(&query, 10, 2, &policy).0;
    for mode in [
        NavigationPrecision::OneBitFourBit,
        NavigationPrecision::FourBitFourBit,
        NavigationPrecision::FourBitFp,
        NavigationPrecision::FourBitPlanes,
        NavigationPrecision::FourBitPacked,
        NavigationPrecision::FourBitPackedFp,
    ] {
        reader.set_navigation_precision(mode).await.unwrap();
        let trace = reader.navigation_trace(&query, &policy);
        if matches!(
            mode,
            NavigationPrecision::FourBitPlanes | NavigationPrecision::FourBitPacked
        ) {
            assert_eq!(trace.levels, quantized_trace);
            assert_eq!(
                reader.search_with_policy_sync(&query, 10, 2, &policy).0,
                quantized_results
            );
        }
        assert!(trace.levels.iter().all(|level| !level.is_empty()));
        let (_, scanned, leaves, _) = reader.search_with_policy_sync(&query, 10, 1, &policy);
        assert_eq!(leaves, trace.levels.last().unwrap().len());
        assert!(scanned > 0);
        let neighbors = vec![vec![0, 31, 64, 127]];
        let reachable = reader.ground_truth_reachability(&neighbors);
        assert!(neighbors[0]
            .iter()
            .all(|id| reachable[0].values().any(|ids| ids.contains(id))));
        if mode != NavigationPrecision::OneBitFourBit {
            assert!(reader.navigation_code_bytes() > original_payload);
        }
    }
    reader
        .set_navigation_precision(NavigationPrecision::OneBitFourBit)
        .await
        .unwrap();
    assert_eq!(
        reader.search_with_policy_sync(&query, 10, 1, &policy).0,
        original
    );
    reader.clear_loaded_postings();
    let mut reopened =
        HierarchicalSpannReader::open(&provider, ids, DistanceFunction::Euclidean, config)
            .await
            .unwrap();
    assert_eq!(reopened.navigation_code_bytes(), original_payload);
    reopened
        .set_navigation_precision(NavigationPrecision::FourBitFourBit)
        .await
        .unwrap();
    let (_, _, leaves, _) = reopened
        .search_with_policy_lazy(&query, 10, 2, &policy)
        .await
        .unwrap();
    assert_eq!(
        leaves,
        reopened
            .navigation_trace(&query, &policy)
            .levels
            .last()
            .unwrap()
            .len()
    );
}

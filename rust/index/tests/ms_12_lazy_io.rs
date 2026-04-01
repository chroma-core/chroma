mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

fn recall(results: &[chroma_index::sparse::maxscore::Score], expected: &[(u32, f32)]) -> f32 {
    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|s| s.offset).collect();
    let expected_offsets: std::collections::HashSet<u32> =
        expected.iter().map(|(o, _)| *o).collect();
    let overlap = result_offsets.intersection(&expected_offsets).count();
    if expected.is_empty() {
        return 1.0;
    }
    overlap as f32 / expected.len() as f32
}

/// Build an index large enough that some dimensions span 3+ Arrow blocks
/// (at 1 MB block size), then query and verify brute-force recall.
/// With 1 MB blocks and ~256 entries per posting block, a single dimension
/// must have many docs to span multiple Arrow blocks. We use a small
/// vocabulary (few dimensions) and many docs so each dimension is dense.
#[tokio::test]
async fn test_ms_12_lazy_vs_brute_force_recall() {
    let mut rng = StdRng::seed_from_u64(1200);
    let num_docs = 2000;
    let all_dims: Vec<u32> = (0..20).collect();

    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..num_docs)
        .map(|i| {
            let ndims = rng.gen_range(8..20);
            let mut chosen: Vec<u32> =
                all_dims.choose_multiple(&mut rng, ndims).copied().collect();
            chosen.sort();
            let dims: Vec<(u32, f32)> = chosen
                .into_iter()
                .map(|d| (d, rng.gen_range(0.01..1.0)))
                .collect();
            (i as u32, dims)
        })
        .collect();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let mut total_recall = 0.0;
    let num_queries = 100;
    for _ in 0..num_queries {
        let ndims = rng.gen_range(3..10);
        let mut chosen: Vec<u32> = all_dims.choose_multiple(&mut rng, ndims).copied().collect();
        chosen.sort();
        let query: Vec<(u32, f32)> = chosen
            .into_iter()
            .map(|d| (d, rng.gen_range(0.1..1.0)))
            .collect();

        let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
        let expected = brute_force_topk(&vectors, &query, 10, &mask);
        total_recall += recall(&results, &expected);
    }

    let avg_recall = total_recall / num_queries as f32;
    assert!(
        avg_recall >= 0.95,
        "lazy I/O: average recall {avg_recall:.3} < 0.95 threshold"
    );
}

/// Verify that the root stores and reads back dim_max_weights correctly.
#[tokio::test]
async fn test_ms_12_root_dim_max_roundtrip() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = vec![
        (0, vec![(10, 0.5), (20, 0.8)]),
        (1, vec![(10, 0.3), (30, 1.0)]),
        (2, vec![(20, 0.9), (30, 0.6)]),
    ];

    let (_dir, _p, reader) = build_index(vectors).await;

    let dim_max = reader.dim_max_weights();
    assert!(dim_max.is_some(), "dim_max_weights should be present");
    let map = dim_max.unwrap();

    let dim10 = map.get(&chroma_index::sparse::types::encode_u32(10));
    let dim20 = map.get(&chroma_index::sparse::types::encode_u32(20));
    let dim30 = map.get(&chroma_index::sparse::types::encode_u32(30));

    assert!(dim10.is_some(), "dim 10 should have dim_max");
    assert!(dim20.is_some(), "dim 20 should have dim_max");
    assert!(dim30.is_some(), "dim 30 should have dim_max");

    // dim 10 has values 0.5, 0.3; block max_weight is the u8-quantized
    // max of each block. Since all fit in one block, dim_max = block max_weight.
    // With u8 quantization the stored max_weight equals max(values).
    assert!(*dim10.unwrap() >= 0.49, "dim 10 max should be >= 0.49");
    assert!(*dim20.unwrap() >= 0.79, "dim 20 max should be >= 0.79");
    assert!(*dim30.unwrap() >= 0.99, "dim 30 max should be >= 0.99");
}

/// Query with a dimension that doesn't exist; dim_max root pruning should skip it cleanly.
#[tokio::test]
async fn test_ms_12_missing_dim_pruned() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = vec![
        (0, vec![(1, 0.5)]),
        (1, vec![(1, 0.3)]),
    ];

    let (_dir, _p, reader) = build_index(vectors).await;
    let mask = SignedRoaringBitmap::full();

    // Query with dim 1 (exists) and dim 999 (doesn't exist)
    let results = reader
        .query(vec![(1, 1.0), (999, 1.0)], 10, mask)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
}

/// Confirm queries with masks still work through the lazy pipeline.
#[tokio::test]
async fn test_ms_12_lazy_with_mask() {
    let mut rng = StdRng::seed_from_u64(1201);
    let all_dims: Vec<u32> = (0..15).collect();

    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..500)
        .map(|i| {
            let ndims = rng.gen_range(5..15);
            let mut chosen: Vec<u32> =
                all_dims.choose_multiple(&mut rng, ndims).copied().collect();
            chosen.sort();
            let dims: Vec<(u32, f32)> = chosen
                .into_iter()
                .map(|d| (d, rng.gen_range(0.01..1.0)))
                .collect();
            (i as u32, dims)
        })
        .collect();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;

    // Exclude first 250 docs
    let mut rbm = roaring::RoaringBitmap::new();
    for i in 0..250u32 {
        rbm.insert(i);
    }
    let mask = SignedRoaringBitmap::Exclude(rbm);

    let query: Vec<(u32, f32)> = vec![(0, 0.5), (5, 0.8), (10, 0.3)];
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 10, &mask);

    let r = recall(&results, &expected);
    assert!(
        r >= 0.90,
        "lazy + mask: recall {r:.3} < 0.90 threshold"
    );
}

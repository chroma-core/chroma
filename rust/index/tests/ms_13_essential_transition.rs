mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Regression test: when a term transitions from essential → non-essential
/// between windows, drain_essential may have left offset_buf populated
/// but value_buf empty.  current() / score_candidates() must handle this
/// by re-decompressing values before reading them.
///
/// To trigger the bug we need:
///   - >4096 docs so we span multiple windows
///   - terms with widely different max_score so partitioning can shift
///   - enough top-k results that the threshold rises mid-query
#[tokio::test]
async fn test_ms_13_essential_to_nonessential_transition() {
    let mut rng = StdRng::seed_from_u64(1337);
    let num_docs = 8192;

    let dim_configs: [(u32, f32, f32, f64); 4] = [
        (0, 0.5, 1.0, 0.80),
        (1, 0.3, 0.7, 0.60),
        (2, 0.1, 0.4, 0.40),
        (3, 0.05, 0.2, 0.30),
    ];

    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..num_docs)
        .map(|i| {
            let mut dims = Vec::new();
            for &(d, lo, hi, prob) in &dim_configs {
                if rng.gen_bool(prob) {
                    dims.push((d, rng.gen_range(lo..hi)));
                }
            }
            (i as u32, dims)
        })
        .collect();

    let (_dir, _provider, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let query = vec![(0, 1.0f32), (1, 0.6), (2, 0.3), (3, 0.1)];
    let k = 10u32;

    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, k as usize, &mask);

    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|s| s.offset).collect();
    let expected_offsets: std::collections::HashSet<u32> =
        expected.iter().map(|(o, _)| *o).collect();
    let overlap = result_offsets.intersection(&expected_offsets).count();
    let recall = overlap as f32 / expected.len().max(1) as f32;

    assert!(
        recall >= 0.90,
        "recall {recall:.3} < 0.90 — essential→non-essential transition may be broken"
    );
}

/// Same idea but with even more docs (16K) and more query terms to
/// increase the chance of partition shifts across many windows.
#[tokio::test]
async fn test_ms_13_multiwindow_partition_shift() {
    let mut rng = StdRng::seed_from_u64(42);
    let num_docs = 16384;
    let num_dims = 8u32;

    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..num_docs)
        .map(|i| {
            let ndims = rng.gen_range(2..=num_dims as usize);
            let prob = ndims as f64 / num_dims as f64;
            let mut dims = Vec::new();
            for d in 0..num_dims {
                if rng.gen_bool(prob) {
                    dims.push((d, rng.gen_range(0.01f32..1.0)));
                }
            }
            (i as u32, dims)
        })
        .collect();

    let (_dir, _provider, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let mut total_recall = 0.0f32;
    let num_queries = 50;
    for _ in 0..num_queries {
        let query_ndims = rng.gen_range(3..=num_dims as usize);
        let prob = query_ndims as f64 / num_dims as f64;
        let mut query = Vec::new();
        for d in 0..num_dims {
            if rng.gen_bool(prob) {
                query.push((d, rng.gen_range(0.1f32..1.0)));
            }
        }

        if query.is_empty() {
            total_recall += 1.0;
            continue;
        }

        let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
        let expected = brute_force_topk(&vectors, &query, 10, &mask);

        let result_offsets: std::collections::HashSet<u32> =
            results.iter().map(|s| s.offset).collect();
        let expected_offsets: std::collections::HashSet<u32> =
            expected.iter().map(|(o, _)| *o).collect();
        let overlap = result_offsets.intersection(&expected_offsets).count();
        total_recall += overlap as f32 / expected.len().max(1) as f32;
    }

    let avg_recall = total_recall / num_queries as f32;
    assert!(
        avg_recall >= 0.90,
        "average recall {avg_recall:.3} < 0.90 across {num_queries} queries over {num_docs} docs"
    );
}

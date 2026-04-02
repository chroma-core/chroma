mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};

/// Regression test for the window budget bug: `block_upper_bound(window_start)`
/// only returned the weight of the FIRST posting block overlapping the window,
/// but a 4096-doc window can span ~4 blocks (1024 entries each).  When a later
/// block within the same window had higher weights, the budget was
/// underestimated, the SIMD pruning cutoff was too high, and good candidates
/// were incorrectly discarded.
///
/// Setup: 16K docs, 2 dimensions, WINDOW_WIDTH=4096, BLOCK_SIZE=1024.
///
///   Dim 0: all docs, weight 0.9 → always essential.
///   Dim 1: all docs.  Window 0 blocks get weight 0.9 (fills heap, raises
///          threshold to ~1.62).  Window 1 blocks get weight 0.05 EXCEPT
///          block 6 (docs 6144..7167) which gets 0.95.
///
/// After window 0, threshold ≈ 1.62 and dim 1 becomes non-essential
/// (max_score = 0.8 × 0.95 = 0.76, upper_bounds[0] = 0.76 < 1.62).
///
/// In window 1 (docs 4096..8191):
///   Old code: budget = 0.8 × block_upper_bound(4096) = 0.8 × 0.05 = 0.04
///             cutoff = 1.62 − 0.04 = 1.58
///             Essential score = 0.9 < 1.58 → ALL candidates pruned!
///             Docs 6144..7167 (true score 1.66) are lost.
///
///   Fixed:    budget = 0.8 × window_upper_bound(4096,8191) = 0.8 × 0.95 = 0.76
///             cutoff = 1.62 − 0.76 = 0.86
///             Essential score = 0.9 > 0.86 → candidates survive,
///             score_candidates adds 0.76, final score 1.66 enters heap.
#[tokio::test]
async fn test_ms_14_window_budget_cross_block() {
    let num_docs = 16384u32;

    let mut vectors: Vec<(u32, Vec<(u32, f32)>)> = Vec::with_capacity(num_docs as usize);
    for doc_id in 0..num_docs {
        let mut dims: Vec<(u32, f32)> = Vec::new();

        // Dim 0: all docs, uniform weight → essential anchor
        dims.push((0, 0.9));

        // Dim 1: weight depends on which block the doc falls in
        let block = doc_id / 1024;
        let dim1_weight = if block < 4 {
            // Window 0 (blocks 0-3): high weight so top-k fills and threshold rises
            0.9
        } else if block == 6 {
            // Window 1, block 6 (docs 6144..7167): HIGH weight buried in middle
            0.95
        } else {
            // Everything else: low weight
            0.05
        };
        dims.push((1, dim1_weight));

        vectors.push((doc_id, dims));
    }

    let (_dir, _provider, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let query = vec![(0, 1.0f32), (1, 0.8)];
    let k = 10u32;

    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, k as usize, &mask);

    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|s| s.offset).collect();
    let expected_offsets: std::collections::HashSet<u32> =
        expected.iter().map(|(o, _)| *o).collect();
    let overlap = result_offsets.intersection(&expected_offsets).count();
    let recall = overlap as f32 / expected.len().max(1) as f32;

    // The top-k MUST include docs from block 6 (true score 0.9+0.76=1.66)
    // which beat window 0 docs (0.9+0.72=1.62).  With the old bug, all of
    // window 1's candidates were pruned and recall would drop to 0%.
    assert!(
        recall >= 1.0,
        "recall {recall:.3} — window budget underestimation is over-pruning candidates \
         from high-weight blocks in later windows"
    );

    // Verify the top-10 results contain docs from the high-weight block
    let from_block_6 = results
        .iter()
        .filter(|s| (6144..7168).contains(&s.offset))
        .count();
    assert!(
        from_block_6 > 0,
        "no results from block 6 (docs 6144..7167) — they were incorrectly pruned"
    );
}

/// Multi-term variant: multiple non-essential terms each with heterogeneous
/// blocks across the window.  Verifies the budget accumulation across terms.
#[tokio::test]
async fn test_ms_14_multiterm_window_budget() {
    let num_docs = 16384u32;

    // Dim 0: anchor (essential), uniform high weight
    // Dims 1-3: become non-essential; each has a high-weight block at a
    // different offset within window 1.
    let high_block: [u32; 4] = [0 /*unused*/, 4, 5, 7];

    let mut vectors: Vec<(u32, Vec<(u32, f32)>)> = Vec::with_capacity(num_docs as usize);
    for doc_id in 0..num_docs {
        let block = doc_id / 1024;
        let mut dims: Vec<(u32, f32)> = Vec::new();

        dims.push((0, 0.85));

        for d in 1u32..=3 {
            let w = if block < 4 {
                0.85
            } else if block == high_block[d as usize] {
                0.90
            } else {
                0.04
            };
            dims.push((d, w));
        }

        vectors.push((doc_id, dims));
    }

    let (_dir, _provider, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let query = vec![(0, 1.0f32), (1, 0.6), (2, 0.5), (3, 0.4)];
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
        recall >= 0.95,
        "recall {recall:.3} < 0.95 — multi-term window budget is broken"
    );
}

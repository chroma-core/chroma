use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

/// Documents spanning the 4096-entry window boundary.
/// Verifies that the window loop, accumulator reset, and bitmap
/// zeroing work correctly when documents cross window boundaries.
#[tokio::test]
async fn multi_window_boundary_docs() {
    // Place documents at window edges: end of window 0, start of window 1,
    // and further into window 1.
    let vectors = vec![
        (0u32, vec![(1u32, 0.9f32), (2, 0.1)]),
        (4094, vec![(1, 0.8), (2, 0.2)]),
        (4095, vec![(1, 0.7), (2, 0.3)]), // last slot of window 0
        (4096, vec![(1, 0.6), (2, 0.4)]), // first slot of window 1
        (4097, vec![(1, 0.5), (2, 0.5)]),
        (8191, vec![(1, 0.4), (2, 0.6)]), // last slot of window 1
        (8192, vec![(1, 0.3), (2, 0.7)]), // first slot of window 2
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32), (2, 1.0)];
    let k = 5;

    let results = reader.query(query.clone(), k, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, k as usize, &all_mask());

    assert_eq!(results.len(), brute.len());
    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    let recall = common::tie_aware_recall(&offsets, &scores, &brute, 5e-3);
    assert!(
        recall >= 1.0,
        "multi-window recall {recall} < 1.0, maxscore={offsets:?}, brute={brute:?}"
    );
}

/// Spread many documents across several windows and verify correctness
/// against brute force with multiple query dimensions.
#[tokio::test]
async fn multi_window_many_docs_across_windows() {
    // 30 documents spread across 3 windows (offsets 0..12288 with stride 410).
    // Two query dimensions with different weights to exercise the
    // essential/non-essential partition across window boundaries.
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..30)
        .map(|i| {
            let offset = i * 410; // spreads across windows 0, 1, 2
            let w1 = 0.01 * ((i * 7 + 3) % 100) as f32;
            let w2 = 0.01 * ((i * 13 + 11) % 100) as f32;
            (offset, vec![(1u32, w1), (2, w2)])
        })
        .collect();

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 2.0f32), (2, 0.5)];
    let k = 10;

    let results = reader.query(query.clone(), k, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, k as usize, &all_mask());

    assert_eq!(results.len(), brute.len());
    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    let recall = common::tie_aware_recall(&offsets, &scores, &brute, 5e-3);
    assert!(
        recall >= 1.0,
        "multi-window many-docs recall {recall} < 1.0, maxscore={offsets:?}, brute={brute:?}"
    );
}

/// Single document in window 0, single document in window 1. Verifies
/// that the accumulator is properly reset between windows.
#[tokio::test]
async fn multi_window_accumulator_reset() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.5f32)]),
        (5000, vec![(1, 0.9)]), // window 1 (offset >= 4096)
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32)];

    let results = reader.query(query.clone(), 2, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, 2, &all_mask());

    assert_eq!(results.len(), 2);
    // doc 5000 should rank first (score 0.9 > 0.5)
    assert_eq!(results[0].offset, brute[0].0);
    assert_eq!(results[1].offset, brute[1].0);
    common::assert_approx(results[0].score, brute[0].1, 5e-3);
    common::assert_approx(results[1].score, brute[1].1, 5e-3);
}

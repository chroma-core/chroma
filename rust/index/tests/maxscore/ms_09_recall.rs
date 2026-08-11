use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

/// f16 quantization tolerance: scores within this range of the boundary
/// are considered ties that quantization could have swapped.
const F16_TOLERANCE: f32 = 5e-3;

#[tokio::test]
async fn recall_500_docs_10_dims() {
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..500)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..10)
                .filter(|d| (i + d) % 3 != 0)
                .map(|d| (d, 0.01 * ((i * 13 + d * 7) % 100) as f32))
                .collect();
            (i, dims)
        })
        .collect();

    let query: Vec<(u32, f32)> = (0..10).map(|d| (d, 1.0)).collect();
    let mask = all_mask();
    let k = 10;

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let brute = common::brute_force_topk(&docs, &query, k as usize, &mask);

    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    let recall = common::tie_aware_recall(&offsets, &scores, &brute, F16_TOLERANCE);

    assert!(
        recall >= 1.0,
        "tie-aware recall {recall} < 1.0, maxscore={offsets:?}, brute={brute:?}"
    );
}

#[tokio::test]
async fn recall_varied_query_weights() {
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..300)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..5)
                .map(|d| (d, 0.01 * ((i * 11 + d * 3) % 100) as f32))
                .collect();
            (i, dims)
        })
        .collect();

    let query = vec![(0u32, 2.0), (1, 0.5), (2, 1.5), (3, 0.1), (4, 3.0)];
    let mask = all_mask();
    let k = 5;

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let brute = common::brute_force_topk(&docs, &query, k as usize, &mask);

    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    let recall = common::tie_aware_recall(&offsets, &scores, &brute, F16_TOLERANCE);

    assert!(
        recall >= 1.0,
        "tie-aware recall {recall} < 1.0 with varied weights"
    );
}

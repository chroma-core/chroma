use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn maxscore_matches_brute_force_simple() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5), (2, 0.3)]),
        (1, vec![(1, 0.8), (2, 0.1)]),
        (2, vec![(1, 0.2), (2, 0.9)]),
    ];

    let query = vec![(1u32, 1.0f32), (2, 1.0)];
    let mask = all_mask();

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), 2, mask.clone()).await.unwrap();

    let brute = common::brute_force_topk(&docs, &query, 2, &mask);

    assert_eq!(results.len(), 2);
    for (r, b) in results.iter().zip(brute.iter()) {
        assert_eq!(r.offset, b.0);
        common::assert_approx(r.score, b.1, 2e-3);
    }
}

#[tokio::test]
async fn maxscore_k_larger_than_docs() {
    let docs = vec![(0u32, vec![(1u32, 0.5)]), (1, vec![(1, 0.3)])];

    let (_dir, _provider, reader) = common::build_index(docs).await;
    let results = reader
        .query(vec![(1u32, 1.0)], 100, all_mask())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn maxscore_k_zero() {
    let docs = vec![(0u32, vec![(1u32, 0.5)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;
    let results = reader
        .query(vec![(1u32, 1.0)], 0, all_mask())
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn maxscore_missing_query_dim() {
    let docs = vec![(0u32, vec![(1u32, 0.5)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;
    let results = reader
        .query(vec![(999u32, 1.0)], 10, all_mask())
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn maxscore_multi_dim_many_docs() {
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..200)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..5)
                .map(|d| (d, 0.01 * ((i * 7 + d) % 100) as f32))
                .collect();
            (i, dims)
        })
        .collect();

    let query: Vec<(u32, f32)> = (0..5).map(|d| (d, 1.0)).collect();
    let mask = all_mask();

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();

    let brute = common::brute_force_topk(&docs, &query, 10, &mask);

    assert_eq!(results.len(), 10);
    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    let recall = common::tie_aware_recall(&offsets, &scores, &brute, 5e-3);
    assert!(
        recall >= 1.0,
        "tie-aware recall {recall} < 1.0, maxscore={offsets:?}, brute={brute:?}"
    );
}

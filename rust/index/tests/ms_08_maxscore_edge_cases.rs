mod common;

use chroma_types::SignedRoaringBitmap;
use common::build_index;

#[tokio::test]
async fn test_ms_08_k_exceeds_docs() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..10)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 11.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 100, mask).await.unwrap();
    assert_eq!(results.len(), 10);

    // Should be sorted by score descending
    for w in results.windows(2) {
        assert!(w[0].score >= w[1].score);
    }
}

#[tokio::test]
async fn test_ms_08_k_zero() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..10)
        .map(|i| (i, vec![(0u32, 0.5)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 0, mask).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_ms_08_single_doc() {
    let vectors = vec![(42u32, vec![(0u32, 0.8)])];
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 10, mask).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].offset, 42);
}

#[tokio::test]
async fn test_ms_08_duplicate_scores() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..20)
        .map(|i| (i, vec![(0u32, 0.5)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 10, mask).await.unwrap();
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn test_ms_08_large_k() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(88);
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..5000)
        .map(|i| (i, vec![(0u32, rng.gen_range(0.01..1.0))]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 1000, mask).await.unwrap();
    assert_eq!(results.len(), 1000);

    // Should be sorted descending
    for w in results.windows(2) {
        assert!(w[0].score >= w[1].score);
    }
}

#[tokio::test]
async fn test_ms_08_empty_index() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = vec![];
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 10, mask).await.unwrap();
    assert!(results.is_empty());
}

mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};
use roaring::RoaringBitmap;

#[tokio::test]
async fn test_ms_07_include_even() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 1001.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];

    let mut rbm = RoaringBitmap::new();
    for i in (0..1000u32).filter(|i| i % 2 == 0) {
        rbm.insert(i);
    }
    let mask = SignedRoaringBitmap::Include(rbm);

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 10, &mask);

    // All results should be even offsets
    assert!(results.iter().all(|s| s.offset % 2 == 0));
    assert_eq!(results.len(), 10);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();
    assert_eq!(r_offs, e_offs);
}

#[tokio::test]
async fn test_ms_07_exclude_first_100() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 1001.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];

    let mut rbm = RoaringBitmap::new();
    for i in 0..100u32 {
        rbm.insert(i);
    }
    let mask = SignedRoaringBitmap::Exclude(rbm);

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();

    // No results should have offset < 100
    assert!(results.iter().all(|s| s.offset >= 100));
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn test_ms_07_full_mask() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..100)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 101.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 5, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 5, &mask);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();
    assert_eq!(r_offs, e_offs);
}

#[tokio::test]
async fn test_ms_07_empty_include() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..100).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::Include(RoaringBitmap::new());

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 10, mask).await.unwrap();
    assert!(results.is_empty());
}

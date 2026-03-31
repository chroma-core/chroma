mod common;

use chroma_types::SignedRoaringBitmap;
use common::{build_index, commit_writer, fork_writer};

#[tokio::test]
async fn test_ms_10_query_after_insert() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..500)
        .map(|i| (i, vec![(0u32, 0.3)]))
        .collect();
    let (_dir, provider, reader) = build_index(vectors).await;
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let results1 = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    assert_eq!(results1.len(), 10);

    // Add 500 more docs with higher weights
    let writer = fork_writer(&provider, &reader).await;
    for i in 500..1000u32 {
        writer.set(i, vec![(0u32, 0.9)]).await;
    }
    let reader2 = commit_writer(&provider, writer).await;
    let results2 = reader2.query(query, 10, mask).await.unwrap();

    // New docs should appear in top-10 (they have higher weight)
    assert!(results2.iter().all(|s| s.offset >= 500));
}

#[tokio::test]
async fn test_ms_10_query_after_delete() {
    let mut vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| (i, vec![(0u32, 0.3)]))
        .collect();
    // Make doc 42 have a very high weight so it shows up in top-k
    vectors[42] = (42, vec![(0u32, 0.99)]);
    let (_dir, provider, reader) = build_index(vectors).await;
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let results1 = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    assert!(results1.iter().any(|s| s.offset == 42));

    // Delete doc 42
    let writer = fork_writer(&provider, &reader).await;
    writer.delete(42, vec![0u32]).await;
    let reader2 = commit_writer(&provider, writer).await;
    let results2 = reader2.query(query, 10, mask).await.unwrap();

    assert!(!results2.iter().any(|s| s.offset == 42));
}

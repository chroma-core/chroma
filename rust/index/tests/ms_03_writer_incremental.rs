mod common;

use common::{build_index, commit_writer, count_blocks, fork_writer, get_all_entries};

#[tokio::test]
async fn test_ms_03_upsert() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..100).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    writer.set(50, vec![(0u32, 0.9)]).await;
    let reader2 = commit_writer(&provider, writer).await;

    let entries = get_all_entries(&reader2, 0).await;
    assert_eq!(entries.len(), 100);

    let doc50 = entries.iter().find(|(off, _)| *off == 50).unwrap();
    let tol = reader2
        .get_posting_blocks(&chroma_index::sparse::types::encode_u32(0))
        .await
        .unwrap()[0]
        .max_weight
        / 255.0
        + 1e-6;
    assert!((doc50.1 - 0.9).abs() <= tol);
}

#[tokio::test]
async fn test_ms_03_insert_new_doc() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..100).map(|i| (i, vec![(0u32, 0.5), (1, 0.3)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    writer.set(100, vec![(0u32, 0.7), (1, 0.4)]).await;
    let reader2 = commit_writer(&provider, writer).await;

    assert_eq!(get_all_entries(&reader2, 0).await.len(), 101);
    assert_eq!(get_all_entries(&reader2, 1).await.len(), 101);
}

#[tokio::test]
async fn test_ms_03_delete_single() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..100).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    writer.delete(50, vec![0u32]).await;
    let reader2 = commit_writer(&provider, writer).await;

    let entries = get_all_entries(&reader2, 0).await;
    assert_eq!(entries.len(), 99);
    assert!(entries.iter().all(|(off, _)| *off != 50));
}

#[tokio::test]
async fn test_ms_03_delete_removes_dimension() {
    let vectors = vec![(0u32, vec![(42u32, 0.5)])];
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    writer.delete(0, vec![42u32]).await;
    let reader2 = commit_writer(&provider, writer).await;

    assert_eq!(count_blocks(&reader2, 42).await, 0);
}

#[tokio::test]
async fn test_ms_03_batch_delete_500() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..1000).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    for i in 0..500u32 {
        writer.delete(i, vec![0u32]).await;
    }
    let reader2 = commit_writer(&provider, writer).await;

    let entries = get_all_entries(&reader2, 0).await;
    assert_eq!(entries.len(), 500);
    assert!(entries.iter().all(|(off, _)| *off >= 500));
}

#[tokio::test]
async fn test_ms_03_stale_block_cleanup() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..1000).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;
    assert_eq!(count_blocks(&reader, 0).await, 4); // ceil(1000/256)

    let writer = fork_writer(&provider, &reader).await;
    for i in 0..800u32 {
        writer.delete(i, vec![0u32]).await;
    }
    let reader2 = commit_writer(&provider, writer).await;

    let entries = get_all_entries(&reader2, 0).await;
    assert_eq!(entries.len(), 200);
    assert_eq!(count_blocks(&reader2, 0).await, 1);
}

#[tokio::test]
async fn test_ms_03_dimension_grows() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..100).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, provider, reader) = build_index(vectors).await;
    assert_eq!(count_blocks(&reader, 0).await, 1);

    let writer = fork_writer(&provider, &reader).await;
    for i in 100..300u32 {
        writer.set(i, vec![(0u32, 0.5)]).await;
    }
    let reader2 = commit_writer(&provider, writer).await;

    assert_eq!(get_all_entries(&reader2, 0).await.len(), 300);
    assert_eq!(count_blocks(&reader2, 0).await, 2); // 256 + 44
}

#[tokio::test]
async fn test_ms_03_unchanged_dimensions_preserved() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..100)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..10).map(|d| (d, 0.5)).collect();
            (i, dims)
        })
        .collect();
    let (_dir, provider, reader) = build_index(vectors).await;

    // Only update dim 0
    let writer = fork_writer(&provider, &reader).await;
    writer.set(0, vec![(0u32, 0.9)]).await;
    let reader2 = commit_writer(&provider, writer).await;

    // Dims 1-9 should be unchanged
    for dim in 1..10u32 {
        let entries = get_all_entries(&reader2, dim).await;
        assert_eq!(entries.len(), 100, "dim {dim} should have 100 entries");
    }
}

mod common;

use chroma_blockstore::{test_arrow_blockfile_provider, BlockfileWriterOptions};
use chroma_index::sparse::maxscore::{
    BlockSparseWriter, SparsePostingBlock, SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use common::{build_index, commit_writer, fork_writer, get_all_entries};

#[tokio::test]
async fn test_ms_04_empty_commit() {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);

    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let writer = BlockSparseWriter::new(posting_writer, None);
    let flusher = writer.commit().await.unwrap();
    flusher.flush().await.unwrap();

    drop(temp_dir);
}

#[tokio::test]
async fn test_ms_04_set_then_delete() {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);

    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let writer = BlockSparseWriter::new(posting_writer, None);
    writer.set(5, vec![(0u32, 1.0)]).await;
    writer.delete(5, vec![0u32]).await;
    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                posting_id,
                "".to_string(),
            ),
        )
        .await
        .unwrap();
    let reader = chroma_index::sparse::maxscore::BlockSparseReader::new(posting_reader);
    let entries = get_all_entries(&reader, 0).await;
    assert!(entries.is_empty());

    drop(temp_dir);
}

#[tokio::test]
async fn test_ms_04_delete_nonexistent() {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);

    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let writer = BlockSparseWriter::new(posting_writer, None);
    writer.delete(999, vec![0u32]).await;
    let flusher = writer.commit().await.unwrap();
    flusher.flush().await.unwrap();

    drop(temp_dir);
}

#[tokio::test]
async fn test_ms_04_overwrite_same_commit() {
    let vectors = vec![(5u32, vec![(0u32, 1.0)])];
    let (_dir, provider, reader) = build_index(vectors).await;

    let writer = fork_writer(&provider, &reader).await;
    writer.set(5, vec![(0u32, 2.0)]).await;
    let reader2 = commit_writer(&provider, writer).await;

    let entries = get_all_entries(&reader2, 0).await;
    assert_eq!(entries.len(), 1);
    let tol = reader2
        .get_posting_blocks(&chroma_index::sparse::types::encode_u32(0))
        .await
        .unwrap()[0]
        .max_weight
        / 255.0
        + 1e-6;
    assert!((entries[0].1 - 2.0).abs() <= tol);
}

#[tokio::test]
async fn test_ms_04_parallel_writes() {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);

    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let writer = BlockSparseWriter::new(posting_writer, None);

    let mut handles = vec![];
    for task_id in 0..10u32 {
        let w = writer.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100u32 {
                let offset = task_id * 100 + i;
                w.set(offset, vec![(0u32, 0.5)]).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                posting_id,
                "".to_string(),
            ),
        )
        .await
        .unwrap();
    let reader = chroma_index::sparse::maxscore::BlockSparseReader::new(posting_reader);
    let entries = get_all_entries(&reader, 0).await;
    assert_eq!(entries.len(), 1000);

    drop(temp_dir);
}

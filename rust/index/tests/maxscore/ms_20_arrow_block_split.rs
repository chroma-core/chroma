//! Regression test: incremental commits must not lose postings when a
//! dimension's posting blocks straddle an Arrow blockfile block boundary.
//!
//! With small Arrow blocks, the Arrow block containing a dimension's
//! first posting block can start at a sparse-index delimiter carrying an
//! EARLIER dimension's prefix (the boundary split mid-prefix). The
//! blockfile's range-read fast path used to return no blocks for that
//! dimension, so `MaxScoreWriter::commit`'s suffix rewrite loaded an
//! empty suffix and deleted the dimension's existing posting blocks —
//! silent data loss.

use crate::common;
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::{test_arrow_blockfile_provider, BlockfileWriterOptions};
use chroma_index::sparse::maxscore::{MaxScoreReader, MaxScoreWriter};
use chroma_types::SparsePostingBlock;

/// Small Arrow blocks so posting data spans several of them.
const ARROW_BLOCK_SIZE_BYTES: usize = 4096;
/// Small posting blocks so each dimension has many blockfile records.
const POSTING_BLOCK_SIZE: u32 = 4;
const DIM_A: u32 = 1;
const DIM_B: u32 = 2;
const DOC_COUNT: u32 = 360;

/// Every doc has both dimensions: DIM_A's posting blocks fill the first
/// Arrow block(s) and DIM_B's posting blocks begin mid-Arrow-block, so
/// the Arrow block owning (DIM_B, seq 0) starts at a DIM_A delimiter.
fn doc_vector(i: u32) -> Vec<(u32, f32)> {
    let weight = 0.1 + (i as f32) * 0.001;
    vec![(DIM_A, weight), (DIM_B, weight)]
}

#[tokio::test]
async fn incremental_commit_preserves_postings_across_arrow_split() {
    let (_dir, provider) = test_arrow_blockfile_provider(ARROW_BLOCK_SIZE_BYTES);

    // Initial build: DOC_COUNT postings on each dimension.
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(ARROW_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();
    let writer = MaxScoreWriter::new(posting_writer, None).with_block_size(POSTING_BLOCK_SIZE);
    for i in 0..DOC_COUNT {
        writer.set(i, doc_vector(i)).await;
    }
    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader = MaxScoreReader::new(posting_reader);

    assert_eq!(
        common::get_all_entries(&reader, DIM_A).await.len(),
        DOC_COUNT as usize
    );
    assert_eq!(
        common::get_all_entries(&reader, DIM_B).await.len(),
        DOC_COUNT as usize
    );

    // Incremental commit: a single low-offset update on DIM_B only. The
    // suffix rewrite must load all of DIM_B's existing posting blocks.
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(ARROW_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();
    let writer = MaxScoreWriter::new(posting_writer, Some(reader.clone()))
        .with_block_size(POSTING_BLOCK_SIZE);
    writer.set(0, vec![(DIM_B, 0.9)]).await;

    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader2 = MaxScoreReader::new(posting_reader);

    // The untouched dimension is carried over by the fork.
    assert_eq!(
        common::get_all_entries(&reader2, DIM_A).await.len(),
        DOC_COUNT as usize
    );

    // The regression: pre-fix, DIM_B collapsed to just the delta entry.
    let entries = common::get_all_entries(&reader2, DIM_B).await;
    assert_eq!(
        entries.len(),
        DOC_COUNT as usize,
        "incremental commit lost postings on DIM_B"
    );
    common::assert_approx(entries[0].1, 0.9, 1e-3);
}

use std::collections::BTreeSet;

use crate::common;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider, BlockfileWriterOptions,
};
use chroma_index::sparse::maxscore::{MaxScoreReader, SPARSE_POSTING_BLOCK_SIZE_BYTES};
use chroma_index::sparse::types::encode_u32;
use chroma_types::{Directory, DirectoryBlock, SparsePostingBlock, DIRECTORY_PREFIX};

async fn count(reader: &MaxScoreReader<'_>, dim: u32) -> usize {
    reader.count_postings(&encode_u32(dim)).await.unwrap()
}

async fn stored_count(reader: &MaxScoreReader<'_>, dim: u32) -> Option<u32> {
    let (dir, _) = reader
        .get_directory(&encode_u32(dim))
        .await
        .unwrap()
        .expect("directory should exist");
    dir.posting_count().ok()
}

/// Brute-force recount: total entries actually stored for a dimension.
async fn recount(reader: &MaxScoreReader<'_>, dim: u32) -> usize {
    common::get_all_entries(reader, dim).await.len()
}

/// Per-dimension posting blocks, each block a list of (offset, weight).
type DimBlocks = Vec<Vec<(u32, f32)>>;

/// Write a legacy (version-0) index directly through the blockfile:
/// posting blocks with the given per-block entries, plus a directory
/// carrying no posting count — the format written before counts existed.
async fn build_legacy_index(
    dims: Vec<(u32, DimBlocks)>,
) -> (
    tempfile::TempDir,
    BlockfileProvider,
    MaxScoreReader<'static>,
) {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let mut encoded_dims: Vec<(String, &DimBlocks)> = dims
        .iter()
        .map(|(dim, blocks)| (encode_u32(*dim), blocks))
        .collect();
    encoded_dims.sort_by(|a, b| a.0.cmp(&b.0));

    // Posting blocks first, then directory parts — same ordered-mutations
    // discipline as MaxScoreWriter::commit.
    let mut directories: Vec<(String, Vec<u32>, Vec<f32>)> = Vec::new();
    for (encoded_dim, blocks) in &encoded_dims {
        let mut max_offsets = Vec::new();
        let mut max_weights = Vec::new();
        for (seq, entries) in blocks.iter().enumerate() {
            let block = SparsePostingBlock::from_sorted_entries(entries).unwrap();
            max_offsets.push(block.header.max_offset);
            max_weights.push(block.header.max_weight);
            posting_writer
                .set(encoded_dim.as_str(), seq as u32, block)
                .await
                .unwrap();
        }
        let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded_dim);
        directories.push((dir_prefix, max_offsets, max_weights));
    }
    for (dir_prefix, max_offsets, max_weights) in directories {
        let part = DirectoryBlock::new(&max_offsets, &max_weights).unwrap();
        posting_writer
            .set(dir_prefix.as_str(), 0u32, part.into_block())
            .await
            .unwrap();
    }

    let flusher = posting_writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let posting_id = flusher.id();
    flusher.flush::<u32, SparsePostingBlock>().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader = MaxScoreReader::new(posting_reader);
    // SAFETY: same pattern as common::build_index — the provider and
    // TempDir outlive the reader in every caller.
    let reader: MaxScoreReader<'static> =
        unsafe { std::mem::transmute::<MaxScoreReader<'_>, MaxScoreReader<'static>>(reader) };

    (temp_dir, provider, reader)
}

#[tokio::test]
async fn fresh_index_count_is_exact() {
    const DIM: u32 = 7;
    // 25 entries at block size 10 → blocks of 10/10/5. The legacy
    // estimate (num_blocks * first_block_len) would report 30.
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..25).map(|i| (i, vec![(DIM, 0.5)])).collect();
    let (_dir, _provider, reader) = common::build_index_with_block_size(docs, Some(10)).await;

    assert_eq!(count(&reader, DIM).await, 25);
    assert_eq!(stored_count(&reader, DIM).await, Some(25));
}

#[tokio::test]
async fn legacy_index_estimates_until_touched() {
    const DIM: u32 = 3;
    // Stranded partial interior block: block 0 holds 3 entries, block 1
    // holds 10 — the shape append-only compactions leave behind. The
    // legacy estimate is 2 * 3 = 6 for 13 real postings.
    let block0: Vec<(u32, f32)> = (0..3).map(|i| (i, 0.5)).collect();
    let block1: Vec<(u32, f32)> = (10..20).map(|i| (i, 0.5)).collect();
    let (_dir, provider, reader) = build_legacy_index(vec![(DIM, vec![block0, block1])]).await;

    assert_eq!(stored_count(&reader, DIM).await, None);
    assert_eq!(count(&reader, DIM).await, 6, "legacy estimate undercounts");
    assert_eq!(recount(&reader, DIM).await, 13);

    // Touch the dimension: append one posting past all existing blocks,
    // so the exact count must come from prefix headers, not the suffix.
    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(100, vec![(DIM, 0.9)]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    assert_eq!(stored_count(&reader2, DIM).await, Some(14));
    assert_eq!(count(&reader2, DIM).await, 14);
    assert_eq!(recount(&reader2, DIM).await, 14);
}

#[tokio::test]
async fn legacy_upgrade_with_prefix_intact_and_suffix_rewritten() {
    const DIM: u32 = 5;
    let block0: Vec<(u32, f32)> = (0..3).map(|i| (i, 0.5)).collect();
    let block1: Vec<(u32, f32)> = (10..20).map(|i| (i, 0.5)).collect();
    let (_dir, provider, reader) = build_legacy_index(vec![(DIM, vec![block0, block1])]).await;

    // Overwrite an offset inside block 1: block 0 stays an untouched
    // prefix (counted via its header) while block 1 is rewritten.
    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(15, vec![(DIM, 0.9)]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    assert_eq!(stored_count(&reader2, DIM).await, Some(13));
    assert_eq!(count(&reader2, DIM).await, 13);
    assert_eq!(recount(&reader2, DIM).await, 13);
}

#[tokio::test]
async fn untouched_legacy_dimension_keeps_estimate() {
    const TOUCHED: u32 = 1;
    const UNTOUCHED: u32 = 2;
    let partial_blocks = vec![
        (0..3).map(|i| (i, 0.5)).collect::<Vec<_>>(),
        (10..20).map(|i| (i, 0.5)).collect::<Vec<_>>(),
    ];
    let (_dir, provider, reader) = build_legacy_index(vec![
        (TOUCHED, partial_blocks.clone()),
        (UNTOUCHED, partial_blocks),
    ])
    .await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(100, vec![(TOUCHED, 0.9)]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    assert_eq!(stored_count(&reader2, TOUCHED).await, Some(14));
    assert_eq!(count(&reader2, TOUCHED).await, 14);

    // The upgrade is lazy: dimensions the commit never touched keep
    // their legacy directory and estimated count.
    assert_eq!(stored_count(&reader2, UNTOUCHED).await, None);
    assert_eq!(count(&reader2, UNTOUCHED).await, 6);
}

#[tokio::test]
async fn suffix_fully_deleted_keeps_prefix_count() {
    const DIM: u32 = 9;
    // Two full blocks at block size 4: offsets 0..8.
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..8).map(|i| (i, vec![(DIM, 0.5)])).collect();
    let (_dir, provider, reader) = common::build_index_with_block_size(docs, Some(4)).await;
    assert_eq!(stored_count(&reader, DIM).await, Some(8));

    // Delete every offset in the tail block only: the rewritten suffix
    // is empty, but the prefix block survives, so the directory (and
    // its exact count) must be carried forward from the prefix alone.
    let writer = common::fork_writer_with_block_size(&provider, &reader, Some(4)).await;
    for off in 4..8 {
        writer.delete(off, vec![DIM]).await;
    }
    let reader2 = common::commit_writer(&provider, writer).await;

    assert_eq!(common::count_blocks(&reader2, DIM).await, 1);
    assert_eq!(stored_count(&reader2, DIM).await, Some(4));
    assert_eq!(count(&reader2, DIM).await, 4);
    assert_eq!(recount(&reader2, DIM).await, 4);
}

#[tokio::test]
async fn rolling_downgrade_estimates_then_converges() {
    const DIM: u32 = 11;
    // 10 entries at block size 4 → blocks of 4/4/2, so the legacy
    // estimate (num_blocks * first_block_len) would report 12.
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..10).map(|i| (i, vec![(DIM, 0.5)])).collect();
    let (_dir, provider, reader) = common::build_index_with_block_size(docs, Some(4)).await;
    assert_eq!(stored_count(&reader, DIM).await, Some(10));

    // Simulate a rolling downgrade: an old writer forks the index and
    // rewrites the directory part without a count stamp (version 0),
    // exactly as any pre-count writer would serialize it.
    let encoded_dim = encode_u32(DIM);
    let (dir, _) = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let legacy_part = DirectoryBlock::new(dir.max_offsets(), dir.max_weights()).unwrap();
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();
    let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded_dim);
    posting_writer
        .set(dir_prefix.as_str(), 0u32, legacy_part.into_block())
        .await
        .unwrap();
    let flusher = posting_writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let posting_id = flusher.id();
    flusher.flush::<u32, SparsePostingBlock>().await.unwrap();
    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader2 = MaxScoreReader::new(posting_reader);

    // The count stamp is gone; the reader degrades to the estimate.
    assert_eq!(stored_count(&reader2, DIM).await, None);
    assert_eq!(count(&reader2, DIM).await, 12, "estimate after downgrade");
    assert_eq!(recount(&reader2, DIM).await, 10);

    // Convergence: the next new-writer commit that touches the dimension
    // re-backfills the exact count from the prefix block headers.
    let writer = common::fork_writer_with_block_size(&provider, &reader2, Some(4)).await;
    writer.set(100, vec![(DIM, 0.9)]).await;
    let reader3 = common::commit_writer(&provider, writer).await;

    assert_eq!(stored_count(&reader3, DIM).await, Some(11));
    assert_eq!(count(&reader3, DIM).await, 11);
    assert_eq!(recount(&reader3, DIM).await, 11);
}

#[tokio::test]
async fn corrupt_stored_count_self_heals_on_touch() {
    const DIM: u32 = 13;
    // 10 entries at block size 4 → blocks of 4/4/2.
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..10).map(|i| (i, vec![(DIM, 0.5)])).collect();
    let (_dir, provider, reader) = common::build_index_with_block_size(docs, Some(4)).await;
    assert_eq!(stored_count(&reader, DIM).await, Some(10));

    // Corrupt the stored count: rewrite the directory part with a
    // deliberately-low count of 1, as a buggy past writer might have
    // persisted. Same raw-write pattern as the rolling-downgrade test.
    let encoded_dim = encode_u32(DIM);
    let (dir, _) = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let corrupt_part = Directory::new(dir.max_offsets().to_vec(), dir.max_weights().to_vec())
        .unwrap()
        .with_posting_count(1)
        .into_parts(100)
        .remove(0);
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();
    let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded_dim);
    posting_writer
        .set(dir_prefix.as_str(), 0u32, corrupt_part.into_block())
        .await
        .unwrap();
    let flusher = posting_writer
        .commit::<u32, SparsePostingBlock>()
        .await
        .unwrap();
    let posting_id = flusher.id();
    flusher.flush::<u32, SparsePostingBlock>().await.unwrap();
    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();
    let reader2 = MaxScoreReader::new(posting_reader);

    assert_eq!(stored_count(&reader2, DIM).await, Some(1));
    assert_eq!(recount(&reader2, DIM).await, 10);

    // Suffix rewrite that underflows the corrupt count: overwriting
    // offset 5 (block 1) makes blocks 1..3 the rewritten suffix — 6 old
    // entries against a stored count of 1. The writer must detect the
    // underflow and recount the prefix from block headers instead of
    // laundering a saturated 0 into the new count.
    let writer = common::fork_writer_with_block_size(&provider, &reader2, Some(4)).await;
    writer.set(5, vec![(DIM, 0.9)]).await;
    let reader3 = common::commit_writer(&provider, writer).await;

    assert_eq!(
        stored_count(&reader3, DIM).await,
        Some(10),
        "count self-heals to the true value"
    );
    assert_eq!(count(&reader3, DIM).await, 10);
    assert_eq!(recount(&reader3, DIM).await, 10);
}

#[tokio::test]
async fn count_stays_exact_across_mixed_commits() {
    const DIM: u32 = 1;
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..20)
        .map(|i| (i, vec![(DIM, 0.1 + i as f32 * 0.01)]))
        .collect();
    let (_dir, provider, reader) = common::build_index_with_block_size(docs, Some(4)).await;
    let mut expected: BTreeSet<u32> = (0..20).collect();
    assert_eq!(count(&reader, DIM).await, expected.len());

    // Commit 2: adds, an overwrite of an existing offset, deletes, and
    // a delete of an absent offset (only real membership changes count).
    let writer = common::fork_writer_with_block_size(&provider, &reader, Some(4)).await;
    for off in 20..25 {
        writer.set(off, vec![(DIM, 0.5)]).await;
        expected.insert(off);
    }
    writer.set(7, vec![(DIM, 0.9)]).await;
    writer.delete(3, vec![DIM]).await;
    expected.remove(&3);
    writer.delete(11, vec![DIM]).await;
    expected.remove(&11);
    writer.delete(999, vec![DIM]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    assert_eq!(count(&reader2, DIM).await, expected.len());
    assert_eq!(
        stored_count(&reader2, DIM).await,
        Some(expected.len() as u32)
    );
    assert_eq!(recount(&reader2, DIM).await, expected.len());

    // Commit 3: tail-only touch, so untouched prefix blocks are carried
    // over and the count is maintained incrementally from the suffix.
    let writer = common::fork_writer_with_block_size(&provider, &reader2, Some(4)).await;
    writer.set(23, vec![(DIM, 0.7)]).await;
    writer.set(30, vec![(DIM, 0.2)]).await;
    expected.insert(30);
    let reader3 = common::commit_writer(&provider, writer).await;

    assert_eq!(count(&reader3, DIM).await, expected.len());
    assert_eq!(
        stored_count(&reader3, DIM).await,
        Some(expected.len() as u32)
    );
    assert_eq!(recount(&reader3, DIM).await, expected.len());

    // Commit 4: delete everything — the directory (and its count) goes.
    let writer = common::fork_writer_with_block_size(&provider, &reader3, Some(4)).await;
    for off in &expected {
        writer.delete(*off, vec![DIM]).await;
    }
    let reader4 = common::commit_writer(&provider, writer).await;

    assert_eq!(count(&reader4, DIM).await, 0);
    assert!(reader4
        .get_directory(&encode_u32(DIM))
        .await
        .unwrap()
        .is_none());
}

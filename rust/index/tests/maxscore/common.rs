#![allow(dead_code)]

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider, BlockfileWriterOptions,
};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_index::sparse::maxscore::{
    BlockSparseReader, BlockSparseWriter, SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use chroma_index::sparse::types::encode_u32;
use chroma_types::SparsePostingBlock;

pub fn make_block(entries: &[(u32, f32)]) -> SparsePostingBlock {
    SparsePostingBlock::from_sorted_entries(entries).expect("make_block: invalid entries")
}

pub fn assert_approx(actual: f32, expected: f32, tolerance: f32) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected} ± {tolerance}, got {actual}"
    );
}

pub fn sequential_entries(start: u32, step: u32, count: usize, weight: f32) -> Vec<(u32, f32)> {
    (0..count)
        .map(|i| (start + step * i as u32, weight))
        .collect()
}

/// Build a fresh index from sparse vectors, returning a 'static reader.
pub async fn build_index(
    vectors: Vec<(u32, Vec<(u32, f32)>)>,
) -> (tempfile::TempDir, BlockfileProvider, BlockSparseReader<'static>) {
    build_index_with_block_size(vectors, None).await
}

pub async fn build_index_with_block_size(
    vectors: Vec<(u32, Vec<(u32, f32)>)>,
    block_size: Option<u32>,
) -> (tempfile::TempDir, BlockfileProvider, BlockSparseReader<'static>) {
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);

    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES),
        )
        .await
        .unwrap();

    let mut writer = BlockSparseWriter::new(posting_writer, None);
    if let Some(bs) = block_size {
        writer = writer.with_block_size(bs);
    }

    for (offset, vector) in vectors {
        writer.set(offset, vector).await;
    }

    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();

    let reader = BlockSparseReader::new(posting_reader);
    let reader: BlockSparseReader<'static> = unsafe { std::mem::transmute(reader) };

    (temp_dir, provider, reader)
}

/// Fork the index to create a new writer for incremental updates.
pub async fn fork_writer<'a>(
    provider: &BlockfileProvider,
    reader: &'a BlockSparseReader<'a>,
) -> BlockSparseWriter<'a> {
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();

    BlockSparseWriter::new(posting_writer, Some(reader.clone()))
}

/// Commit a writer and return a new 'static reader.
pub async fn commit_writer(
    provider: &BlockfileProvider,
    writer: BlockSparseWriter<'_>,
) -> BlockSparseReader<'static> {
    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();

    let reader = BlockSparseReader::new(posting_reader);
    unsafe { std::mem::transmute(reader) }
}

/// Brute-force top-k scoring for reference comparisons.
pub fn brute_force_topk(
    doc_vectors: &[(u32, Vec<(u32, f32)>)],
    query: &[(u32, f32)],
    k: usize,
    mask: &chroma_types::SignedRoaringBitmap,
) -> Vec<(u32, f32)> {
    let mut scores: Vec<(u32, f32)> = doc_vectors
        .iter()
        .filter(|(off, _)| match mask {
            chroma_types::SignedRoaringBitmap::Include(rbm) => rbm.contains(*off),
            chroma_types::SignedRoaringBitmap::Exclude(rbm) => !rbm.contains(*off),
        })
        .map(|(off, dims)| {
            let score: f32 = query
                .iter()
                .map(|(qd, qw)| {
                    dims.iter()
                        .find(|(dd, _)| dd == qd)
                        .map(|(_, dv)| qw * dv)
                        .unwrap_or(0.0)
                })
                .sum();
            (*off, score)
        })
        .collect();

    scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    scores.truncate(k);
    scores
}

/// Count total blocks for a dimension.
pub async fn count_blocks(reader: &BlockSparseReader<'_>, dim: u32) -> usize {
    let blocks = reader
        .get_posting_blocks(&encode_u32(dim))
        .await
        .unwrap();
    blocks.len()
}

/// Get all entries for a dimension from a reader.
pub async fn get_all_entries(reader: &BlockSparseReader<'_>, dim: u32) -> Vec<(u32, f32)> {
    let blocks = reader
        .get_posting_blocks(&encode_u32(dim))
        .await
        .unwrap();
    blocks
        .into_iter()
        .flat_map(|b| b.offsets().to_vec().into_iter().zip(b.values().to_vec()))
        .collect()
}

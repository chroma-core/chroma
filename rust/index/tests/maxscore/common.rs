#![allow(dead_code)]

use chroma_blockstore::provider::BlockfileProvider;
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, test_arrow_blockfile_provider, BlockfileWriterOptions,
};
use chroma_index::sparse::maxscore::{
    MaxScoreReader, MaxScoreWriter, SPARSE_POSTING_BLOCK_SIZE_BYTES,
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
) -> (
    tempfile::TempDir,
    BlockfileProvider,
    MaxScoreReader<'static>,
) {
    build_index_with_block_size(vectors, None).await
}

pub async fn build_index_with_block_size(
    vectors: Vec<(u32, Vec<(u32, f32)>)>,
    block_size: Option<u32>,
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

    let mut writer = MaxScoreWriter::new(posting_writer, None);
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

    let reader = MaxScoreReader::new(posting_reader);
    // SAFETY: The reader borrows from the BlockfileProvider's block cache.
    // Both the provider and TempDir (which owns the backing storage) are
    // returned alongside the reader and must outlive it in the caller.
    let reader: MaxScoreReader<'static> =
        unsafe { std::mem::transmute::<MaxScoreReader<'_>, MaxScoreReader<'static>>(reader) };

    (temp_dir, provider, reader)
}

/// Fork the index to create a new writer for incremental updates.
pub async fn fork_writer<'a>(
    provider: &BlockfileProvider,
    reader: &'a MaxScoreReader<'a>,
) -> MaxScoreWriter<'a> {
    fork_writer_with_block_size(provider, reader, None).await
}

/// Fork the index with an optional custom block size.
pub async fn fork_writer_with_block_size<'a>(
    provider: &BlockfileProvider,
    reader: &'a MaxScoreReader<'a>,
    block_size: Option<u32>,
) -> MaxScoreWriter<'a> {
    let posting_writer = provider
        .write::<u32, SparsePostingBlock>(
            BlockfileWriterOptions::new("".to_string())
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES)
                .fork(reader.posting_id()),
        )
        .await
        .unwrap();

    let mut writer = MaxScoreWriter::new(posting_writer, Some(reader.clone()));
    if let Some(bs) = block_size {
        writer = writer.with_block_size(bs);
    }
    writer
}

/// Commit a writer and return a new 'static reader.
pub async fn commit_writer(
    provider: &BlockfileProvider,
    writer: MaxScoreWriter<'_>,
) -> MaxScoreReader<'static> {
    let flusher = writer.commit().await.unwrap();
    let posting_id = flusher.id();
    flusher.flush().await.unwrap();

    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(posting_id, "".to_string()))
        .await
        .unwrap();

    let reader = MaxScoreReader::new(posting_reader);
    // SAFETY: The reader borrows from the BlockfileProvider's block cache.
    // The caller must ensure the provider outlives the returned reader.
    unsafe { std::mem::transmute::<MaxScoreReader<'_>, MaxScoreReader<'static>>(reader) }
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
pub async fn count_blocks(reader: &MaxScoreReader<'_>, dim: u32) -> usize {
    let blocks = reader.get_posting_blocks(&encode_u32(dim)).await.unwrap();
    blocks.len()
}

/// Get all entries for a dimension from a reader.
pub async fn get_all_entries(reader: &MaxScoreReader<'_>, dim: u32) -> Vec<(u32, f32)> {
    let blocks = reader.get_posting_blocks(&encode_u32(dim)).await.unwrap();
    blocks
        .into_iter()
        .flat_map(|mut b| {
            let (offsets, values) = b.decode();
            offsets
                .iter()
                .copied()
                .zip(values.iter().copied())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Tie-aware recall: a result is a hit if it appears in the brute-force
/// top-k, OR if its score is within `tolerance` of the k-th brute-force
/// score (i.e. it's tied at the boundary and f16 quantization swapped
/// the ranking).
pub fn tie_aware_recall(
    result_offsets: &[u32],
    result_scores: &[f32],
    brute: &[(u32, f32)],
    tolerance: f32,
) -> f64 {
    if brute.is_empty() {
        return 1.0;
    }
    let k = brute.len();
    let boundary_score = brute[k - 1].1;
    let brute_offsets: std::collections::HashSet<u32> = brute.iter().map(|(o, _)| *o).collect();

    let mut hits = 0;
    for (i, &off) in result_offsets.iter().enumerate() {
        if brute_offsets.contains(&off) {
            hits += 1;
        } else if (result_scores[i] - boundary_score).abs() <= tolerance {
            // Score is within f16 tolerance of the boundary — a tie that
            // f16 quantization could have swapped.
            hits += 1;
        }
    }
    hits as f64 / k as f64
}

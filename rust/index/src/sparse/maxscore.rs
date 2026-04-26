use std::sync::Arc;

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{
    Directory, DirectoryBlock, SignedRoaringBitmap, SparsePostingBlock, SparsePostingBlockError,
    DIRECTORY_PREFIX, MAX_BLOCK_ENTRIES,
};
use dashmap::DashMap;

use std::iter;
use thiserror::Error;
use uuid::Uuid;

use crate::sparse::types::{decode_u32, encode_u32, Score, TopKHeap};

const DEFAULT_BLOCK_SIZE: u32 = 1024;

pub const SPARSE_POSTING_BLOCK_SIZE_BYTES: usize = 1024 * 1024;

/// Dimensions with at most this many Arrow blocks use a View cursor
/// (loaded eagerly into cache). Larger dimensions use Lazy cursors
/// whose blocks are loaded on demand in Batch 2/3.
const MAX_VIEW_BLOCKS: usize = 2;

#[derive(Debug, Error)]
pub enum MaxScoreError {
    #[error(transparent)]
    Blockfile(#[from] Box<dyn ChromaError>),
    #[error("posting block error: {0}")]
    PostingBlock(#[from] SparsePostingBlockError),
}

impl ChromaError for MaxScoreError {
    fn code(&self) -> ErrorCodes {
        match self {
            MaxScoreError::Blockfile(err) => err.code(),
            MaxScoreError::PostingBlock(_) => ErrorCodes::Internal,
        }
    }
}

// ── MaxScoreFlusher ──────────────────────────────────────────────

pub struct MaxScoreFlusher {
    posting_flusher: BlockfileFlusher,
}

impl MaxScoreFlusher {
    pub async fn flush(self) -> Result<(), MaxScoreError> {
        self.posting_flusher
            .flush::<u32, SparsePostingBlock>()
            .await?;
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.posting_flusher.id()
    }
}

// ── MaxScoreWriter ───────────────────────────────────────────────

#[derive(Clone)]
pub struct MaxScoreWriter<'me> {
    block_size: u32,
    delta: Arc<DashMap<u32, DashMap<u32, Option<f32>>>>,
    posting_writer: BlockfileWriter,
    old_reader: Option<MaxScoreReader<'me>>,
}

impl<'me> MaxScoreWriter<'me> {
    pub fn new(posting_writer: BlockfileWriter, old_reader: Option<MaxScoreReader<'me>>) -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            delta: Default::default(),
            posting_writer,
            old_reader,
        }
    }

    pub fn with_block_size(mut self, block_size: u32) -> Self {
        if block_size > MAX_BLOCK_ENTRIES as u32 {
            tracing::warn!(
                requested = block_size,
                max = MAX_BLOCK_ENTRIES,
                "block_size exceeds MAX_BLOCK_ENTRIES, clamping"
            );
        }
        self.block_size = block_size.min(MAX_BLOCK_ENTRIES as u32);
        self
    }

    pub async fn set(&self, offset: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {
        for (dimension_id, value) in sparse_vector {
            self.delta
                .entry(dimension_id)
                .or_default()
                .insert(offset, Some(value));
        }
    }

    pub async fn delete(&self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        for dimension_id in sparse_indices {
            self.delta
                .entry(dimension_id)
                .or_default()
                .insert(offset, None);
        }
    }

    pub async fn commit(self) -> Result<MaxScoreFlusher, MaxScoreError> {
        let mut all_dim_ids: Vec<u32> = self.delta.iter().map(|e| *e.key()).collect();

        if let Some(ref reader) = self.old_reader {
            let old_dims = reader.get_all_dimension_ids().await?;
            all_dim_ids.extend(old_dims);
        }

        all_dim_ids.sort_unstable();
        all_dim_ids.dedup();

        let mut encoded_dims: Vec<(String, u32)> = all_dim_ids
            .into_iter()
            .map(|id| (encode_u32(id), id))
            .collect();
        encoded_dims.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Two-pass commit: posting blocks first (sorted by encoded_dim),
        // then directory parts (sorted by dir_prefix). This satisfies the
        // blockfile's ordered_mutations requirement since all "d"-prefixed
        // directory keys sort after the plain base64 posting keys for
        // realistic dimension IDs.
        debug_assert!(
            encoded_dims
                .iter()
                .all(|(enc, _)| enc.as_str() < DIRECTORY_PREFIX),
            "encoded dimension prefix >= DIRECTORY_PREFIX; ordered_mutations invariant broken"
        );
        struct DirWork {
            prefix: String,
            directory: Option<Directory>,
            old_part_count: u32,
        }
        let mut dir_work: Vec<DirWork> = Vec::with_capacity(encoded_dims.len());

        // ── Pass 1: posting blocks ─────────────────────────────────────
        for (encoded_dim, dimension_id) in &encoded_dims {
            let Some((_, updates)) = self.delta.remove(dimension_id) else {
                continue;
            };

            let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded_dim);

            // ── Suffix-rewrite optimization ────────────────────────────
            //
            // When an old reader exists AND a directory is available, we
            // only need to load and rewrite posting blocks from the first
            // affected block onward. Blocks before the smallest affected
            // offset are guaranteed unchanged and are carried over by the
            // forked blockfile.
            //
            // Fallback: if there is no old reader or no directory, we do
            // a full write (same as a fresh dimension).
            let old_directory = if let Some(ref reader) = self.old_reader {
                reader.get_directory(encoded_dim).await?
            } else {
                None
            };

            if let Some((ref directory, old_dir_part_count)) = old_directory {
                let old_block_count = directory.num_blocks() as u32;

                // Find the smallest offset touched by any delta.
                let Some(min_affected_offset) = updates.iter().map(|e| *e.key()).min() else {
                    continue;
                };

                // Find the first block whose max_offset >= min_affected_offset.
                // All blocks before this index are untouched.
                let first_affected = directory
                    .max_offsets()
                    .partition_point(|&max_off| max_off < min_affected_offset)
                    as u32;

                // Load only the suffix of posting blocks.
                let suffix_blocks = if let Some(ref reader) = self.old_reader {
                    reader
                        .get_posting_blocks_range(encoded_dim, first_affected)
                        .await?
                } else {
                    vec![]
                };

                // Decompress suffix blocks into entries.
                let mut entries = std::collections::HashMap::new();
                for mut block in suffix_blocks {
                    let (offsets, values) = block.decode();
                    for (off, val) in offsets.iter().zip(values.iter()) {
                        entries.insert(*off, *val);
                    }
                }

                // Apply deltas.
                for entry in updates.into_iter() {
                    let (off, update) = entry;
                    if let Some(val) = update {
                        entries.insert(off, val);
                    } else {
                        entries.remove(&off);
                    }
                }

                // Carry forward directory entries for untouched prefix blocks.
                let prefix_max_offsets = &directory.max_offsets()[..first_affected as usize];
                let prefix_max_weights = &directory.max_weights()[..first_affected as usize];

                if entries.is_empty() && first_affected == 0 {
                    // All entries deleted — remove all posting blocks.
                    for seq in 0..old_block_count {
                        self.posting_writer
                            .delete::<_, SparsePostingBlock>(encoded_dim, seq)
                            .await?;
                    }
                    dir_work.push(DirWork {
                        prefix: dir_prefix,
                        directory: None,
                        old_part_count: old_dir_part_count as u32,
                    });
                    continue;
                }

                // Sort suffix entries and re-chunk.
                let mut sorted_suffix: Vec<(u32, f32)> = entries.into_iter().collect();
                sorted_suffix.sort_unstable_by_key(|(off, _)| *off);

                let mut dir_max_offsets: Vec<u32> = prefix_max_offsets.to_vec();
                let mut dir_max_weights: Vec<f32> = prefix_max_weights.to_vec();

                if sorted_suffix.is_empty() {
                    // Suffix is now empty (all suffix entries deleted), but
                    // prefix blocks remain. Delete old suffix blocks.
                    for seq in first_affected..old_block_count {
                        self.posting_writer
                            .delete::<_, SparsePostingBlock>(encoded_dim, seq)
                            .await?;
                    }
                } else {
                    let new_suffix_block_count =
                        sorted_suffix.chunks(self.block_size as usize).len() as u32;
                    for (i, chunk) in sorted_suffix.chunks(self.block_size as usize).enumerate() {
                        let block = SparsePostingBlock::from_sorted_entries(chunk)?;
                        dir_max_offsets.push(block.header.max_offset);
                        dir_max_weights.push(block.header.max_weight);
                        let seq = first_affected + i as u32;
                        self.posting_writer.set(encoded_dim, seq, block).await?;
                    }

                    // Delete trailing old blocks beyond the new suffix.
                    let new_total = first_affected + new_suffix_block_count;
                    for seq in new_total..old_block_count {
                        self.posting_writer
                            .delete::<_, SparsePostingBlock>(encoded_dim, seq)
                            .await?;
                    }
                }

                let directory = Directory::new(dir_max_offsets, dir_max_weights)?;
                dir_work.push(DirWork {
                    prefix: dir_prefix,
                    directory: Some(directory),
                    old_part_count: old_dir_part_count as u32,
                });
            } else {
                // ── Fresh dimension (no old reader or no directory) ─────
                // Full write path: all entries come from deltas only.
                let mut entries: Vec<(u32, f32)> = Vec::new();
                for entry in updates.into_iter() {
                    let (off, update) = entry;
                    if let Some(val) = update {
                        entries.push((off, val));
                    }
                }

                if entries.is_empty() {
                    continue;
                }

                entries.sort_unstable_by_key(|(off, _)| *off);

                let mut dir_max_offsets = Vec::new();
                let mut dir_max_weights = Vec::new();

                for (seq, chunk) in entries.chunks(self.block_size as usize).enumerate() {
                    let block = SparsePostingBlock::from_sorted_entries(chunk)?;
                    dir_max_offsets.push(block.header.max_offset);
                    dir_max_weights.push(block.header.max_weight);
                    self.posting_writer
                        .set(encoded_dim, seq as u32, block)
                        .await?;
                }

                let directory = Directory::new(dir_max_offsets, dir_max_weights)?;
                dir_work.push(DirWork {
                    prefix: dir_prefix,
                    directory: Some(directory),
                    old_part_count: 0,
                });
            }
        }

        // ── Pass 2: directory parts (all dir prefixes sort after posting
        //    prefixes because DIRECTORY_PREFIX = "d" > base64 uppercase) ─
        dir_work.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        let max_entries = Directory::max_entries_for_block_size(SPARSE_POSTING_BLOCK_SIZE_BYTES);
        for dw in dir_work {
            if let Some(directory) = dw.directory {
                let parts = directory.into_parts(max_entries);
                let new_count = parts.len() as u32;
                for (seq, part) in parts.into_iter().enumerate() {
                    self.posting_writer
                        .set(&dw.prefix, seq as u32, part.into_block())
                        .await?;
                }
                for seq in new_count..dw.old_part_count {
                    self.posting_writer
                        .delete::<_, SparsePostingBlock>(&dw.prefix, seq)
                        .await?;
                }
            } else {
                for seq in 0..dw.old_part_count {
                    self.posting_writer
                        .delete::<_, SparsePostingBlock>(&dw.prefix, seq)
                        .await?;
                }
            }
        }

        let flusher = self
            .posting_writer
            .commit::<u32, SparsePostingBlock>()
            .await?;

        Ok(MaxScoreFlusher {
            posting_flusher: flusher,
        })
    }
}

pub use super::cursor::PostingCursor;

// ── MaxScoreReader ───────────────────────────────────────────────

#[derive(Clone)]
pub struct MaxScoreReader<'me> {
    posting_reader: BlockfileReader<'me, u32, SparsePostingBlock>,
}

impl<'me> MaxScoreReader<'me> {
    pub fn new(posting_reader: BlockfileReader<'me, u32, SparsePostingBlock>) -> Self {
        Self { posting_reader }
    }

    pub fn posting_id(&self) -> Uuid {
        self.posting_reader.id()
    }

    pub fn posting_reader(&self) -> &BlockfileReader<'me, u32, SparsePostingBlock> {
        &self.posting_reader
    }

    pub async fn get_posting_blocks(
        &self,
        encoded_dim: &str,
    ) -> Result<Vec<SparsePostingBlock>, MaxScoreError> {
        let blocks: Vec<(u32, SparsePostingBlock)> =
            self.posting_reader.get_prefix(encoded_dim).await?.collect();
        Ok(blocks.into_iter().map(|(_, b)| b).collect())
    }

    /// Load posting blocks for a dimension from `start_seq` onward.
    pub async fn get_posting_blocks_range(
        &self,
        encoded_dim: &str,
        start_seq: u32,
    ) -> Result<Vec<SparsePostingBlock>, MaxScoreError> {
        let blocks: Vec<(&str, u32, SparsePostingBlock)> = self
            .posting_reader
            .get_range(encoded_dim..=encoded_dim, start_seq..)
            .await?
            .collect();
        Ok(blocks.into_iter().map(|(_, _, b)| b).collect())
    }

    /// Load the directory for a dimension, returning the reconstructed
    /// `Directory` and the number of on-disk directory parts.
    pub async fn get_directory(
        &self,
        encoded_dim: &str,
    ) -> Result<Option<(Directory, usize)>, MaxScoreError> {
        let dir_prefix = format!("{}{}", DIRECTORY_PREFIX, encoded_dim);
        let parts: Vec<(u32, SparsePostingBlock)> =
            self.posting_reader.get_prefix(&dir_prefix).await?.collect();
        if parts.is_empty() {
            return Ok(None);
        }
        let part_count = parts.len();
        let dir_blocks: Vec<DirectoryBlock> = parts
            .into_iter()
            .filter_map(|(_, b)| DirectoryBlock::from_block(b).ok())
            .collect();
        Ok(Directory::from_parts(dir_blocks)
            .ok()
            .map(|d| (d, part_count)))
    }

    /// Estimate total posting entries for a dimension.
    ///
    /// Used by the IDF operator to compute document frequency. Loads only
    /// the directory (usually cached) and the first posting block to learn
    /// the block size, then returns `num_blocks * block_size`. This
    /// overestimates by at most `block_size - 1` on the last block, which
    /// is negligible for the IDF formula.
    pub async fn count_postings(&self, encoded_dim: &str) -> Result<usize, MaxScoreError> {
        let Some((dir, _)) = self.get_directory(encoded_dim).await? else {
            return Ok(0);
        };
        let num_blocks = dir.num_blocks();
        if num_blocks == 0 {
            return Ok(0);
        }
        let first_block = self.posting_reader.get(encoded_dim, 0u32).await?;
        let block_size = first_block.map(|b| b.len()).unwrap_or(0);
        if block_size == 0 {
            return Ok(0);
        }
        Ok(num_blocks * block_size)
    }

    /// Return all dimension IDs stored in the blockfile.
    ///
    /// Scans only directory entries (prefix "d"...) which are much fewer
    /// than posting blocks. A key-only scan API on BlockfileReader would
    /// avoid deserializing even the directory values.
    pub async fn get_all_dimension_ids(&self) -> Result<Vec<u32>, MaxScoreError> {
        let dir_entries: Vec<(&str, u32, SparsePostingBlock)> = self
            .posting_reader
            .get_range(DIRECTORY_PREFIX.., ..)
            .await?
            .collect();

        let mut dims: Vec<u32> = dir_entries
            .iter()
            .filter_map(|(prefix, _, _)| {
                prefix
                    .strip_prefix(DIRECTORY_PREFIX)
                    .and_then(|rest| decode_u32(rest).ok())
            })
            .collect();
        dims.sort_unstable();
        dims.dedup();
        Ok(dims)
    }

    /// Open a cursor for a dimension by loading all its posting blocks
    /// eagerly. Returns `None` if the dimension has no data.
    pub async fn open_cursor(
        &'me self,
        encoded_dim: &str,
    ) -> Result<Option<PostingCursor<'me>>, MaxScoreError> {
        let blocks = self.get_posting_blocks(encoded_dim).await?;
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(PostingCursor::from_blocks(blocks)))
    }

    /// BlockMaxMaxScore query using the 3-batch I/O pipeline.
    ///
    /// 1. **Batch 1 — directories**: load directory blocks for every
    ///    query dimension in parallel, parse metadata.
    /// 2. **Batch 2 — essential data**: small dims (≤2 Arrow blocks)
    ///    get View cursors immediately; large dims get Lazy cursors
    ///    whose blocks are loaded and populated in bulk.
    /// 3. **Batch 3 — non-essential data**: after the threshold
    ///    stabilizes, load remaining blocks for non-essential terms,
    ///    pruning blocks that can't beat the threshold.
    pub async fn query(
        &'me self,
        query_vector: impl IntoIterator<Item = (u32, f32)>,
        k: u32,
        mask: SignedRoaringBitmap,
    ) -> Result<Vec<Score>, MaxScoreError> {
        if k == 0 {
            return Ok(vec![]);
        }

        let collected: Vec<(u32, f32)> = query_vector.into_iter().collect();
        let encoded_dims: Vec<String> = collected.iter().map(|(d, _)| encode_u32(*d)).collect();

        // ── Batch 1: load directory parts for all query dims ────────
        let dir_prefixes: Vec<String> = encoded_dims
            .iter()
            .map(|d| format!("{}{}", DIRECTORY_PREFIX, d))
            .collect();
        self.posting_reader
            .load_blocks_for_prefixes(dir_prefixes.iter().map(|s| s.as_str()))
            .await;

        struct TermMeta {
            encoded_dim: String,
            dir_max_offsets: Vec<u32>,
            dir_max_weights: Vec<f32>,
            query_weight: f32,
            max_score: f32,
        }

        let mut metas: Vec<TermMeta> = Vec::new();
        for (idx, &(_, query_weight)) in collected.iter().enumerate() {
            let encoded_dim = encoded_dims[idx].clone();
            let Some((dir, _part_count)) = self.get_directory(&encoded_dim).await? else {
                continue;
            };
            if dir.num_blocks() == 0 {
                continue;
            }
            let max_score = query_weight * dir.dim_max_weight();
            metas.push(TermMeta {
                encoded_dim,
                dir_max_offsets: dir.max_offsets().to_vec(),
                dir_max_weights: dir.max_weights().to_vec(),
                query_weight,
                max_score,
            });
        }

        if metas.is_empty() {
            return Ok(vec![]);
        }

        // ── Build cursors ──────────────────────────────────────────
        // Small dimensions (≤2 Arrow blocks) use the eager View path;
        // large dimensions use Lazy cursors populated in Batch 2.
        let mut terms: Vec<TermState<'me>> = Vec::new();
        for meta in metas {
            let block_count = self
                .posting_reader
                .count_blocks_for_prefix(&meta.encoded_dim);

            if block_count <= MAX_VIEW_BLOCKS {
                self.posting_reader
                    .load_blocks_for_prefixes(iter::once(meta.encoded_dim.as_str()))
                    .await;
                let n = meta.dir_max_offsets.len();
                let raw_blocks: Vec<&[u8]> = (0..n)
                    .filter_map(|seq| {
                        self.posting_reader
                            .get_raw_from_cache(&meta.encoded_dim, seq as u32)
                    })
                    .collect();

                let mut cursor = if raw_blocks.len() == n {
                    PostingCursor::open(raw_blocks, meta.dir_max_offsets, meta.dir_max_weights)
                } else {
                    let blocks = self.get_posting_blocks(&meta.encoded_dim).await?;
                    if blocks.is_empty() {
                        continue;
                    }
                    PostingCursor::from_blocks(blocks)
                };
                cursor.advance(0, &mask);
                terms.push(TermState {
                    cursor,
                    encoded_dim: meta.encoded_dim,
                    query_weight: meta.query_weight,
                    max_score: meta.max_score,
                    window_score: meta.max_score,
                });
            } else {
                let cursor = PostingCursor::open_lazy(meta.dir_max_offsets, meta.dir_max_weights);
                terms.push(TermState {
                    cursor,
                    encoded_dim: meta.encoded_dim,
                    query_weight: meta.query_weight,
                    max_score: meta.max_score,
                    window_score: meta.max_score,
                });
            }
        }

        if terms.is_empty() {
            return Ok(vec![]);
        }

        terms.sort_by(|a, b| a.max_score.total_cmp(&b.max_score));

        // ── Window loop ────────────────────────────────────────────
        let k_usize = k as usize;
        let mut heap = TopKHeap::new(k_usize);
        let mut threshold = heap.threshold();

        const WINDOW_WIDTH: u32 = 4096;
        const BITMAP_WORDS: usize = (WINDOW_WIDTH as usize).div_ceil(64);
        const LOAD_WIDTH: u32 = WINDOW_WIDTH * 256;
        let mut accum = vec![0.0f32; WINDOW_WIDTH as usize];
        let mut bitmap = [0u64; BITMAP_WORDS];
        let mut cand_docs: Vec<u32> = Vec::with_capacity(WINDOW_WIDTH as usize);
        let mut cand_scores: Vec<f32> = Vec::with_capacity(WINDOW_WIDTH as usize);

        let max_doc_id = terms
            .iter()
            .filter_map(|t| t.cursor.dir_max_offsets.last().copied())
            .max()
            .unwrap_or(0);

        let mut window_start = 0u32;
        let mut load_end = 0u32;
        let mut keys_to_load: Vec<(String, u32)> = Vec::new();
        let mut keys_per_term: Vec<Vec<usize>> = (0..terms.len()).map(|_| Vec::new()).collect();
        let mut overlapping: Vec<usize> = Vec::new();

        while window_start <= max_doc_id {
            let window_end = (window_start + WINDOW_WIDTH - 1).min(max_doc_id);

            for t in terms.iter_mut() {
                t.window_score =
                    t.query_weight * t.cursor.window_upper_bound(window_start, window_end);
            }
            terms.sort_unstable_by(|a, b| a.window_score.total_cmp(&b.window_score));

            let mut essential_idx = terms.len();
            {
                let mut prefix = 0.0f32;
                for (i, t) in terms.iter().enumerate() {
                    prefix += t.window_score;
                    if prefix >= threshold {
                        essential_idx = i;
                        break;
                    }
                }
            }

            // ── Chunked prefetch: load all blocks for the next
            // LOAD_WIDTH-offset chunk. Fires roughly once per ~256
            // windows; most iterations skip this entirely. ────────
            if load_end < window_end {
                let load_start = load_end + 1;
                load_end += LOAD_WIDTH;

                keys_to_load.clear();
                for v in keys_per_term.iter_mut() {
                    v.clear();
                }

                for (ti, t) in terms.iter().enumerate() {
                    overlapping.clear();
                    t.cursor
                        .collect_overlapping_blocks(load_start, load_end, &mut overlapping);
                    for &bi in &overlapping {
                        keys_to_load.push((t.encoded_dim.clone(), bi as u32));
                        keys_per_term[ti].push(bi);
                    }
                }

                if !keys_to_load.is_empty() {
                    self.posting_reader
                        .load_data_for_keys(keys_to_load.drain(..))
                        .await;
                    for (ti, indices) in keys_per_term.iter().enumerate() {
                        if indices.is_empty() {
                            continue;
                        }
                        let dim = terms[ti].encoded_dim.clone();
                        terms[ti]
                            .cursor
                            .populate_from_cache(&self.posting_reader, &dim, indices);
                    }
                }
            }

            for term in terms[essential_idx..].iter_mut() {
                term.cursor.drain_essential(
                    window_start,
                    window_end,
                    term.query_weight,
                    &mut accum,
                    &mut bitmap,
                    &mask,
                );
            }

            cand_docs.clear();
            cand_scores.clear();
            for (word_idx, &word) in bitmap.iter().enumerate().take(BITMAP_WORDS) {
                let mut bits = word;
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    let idx = word_idx * 64 + bit;
                    cand_docs.push(window_start + idx as u32);
                    cand_scores.push(accum[idx]);
                    bits &= bits.wrapping_sub(1);
                }
            }

            // No essential term touched any doc in this window. Skip
            // Phase 2, heap update, and accumulator reset.
            if cand_docs.is_empty() {
                window_start = window_end.wrapping_add(1);
                if window_start == 0 {
                    break;
                }
                continue;
            }

            if essential_idx > 0 {
                let mut remaining_budget: f32 =
                    terms[..essential_idx].iter().map(|t| t.window_score).sum();

                for i in (0..essential_idx).rev() {
                    if heap.len() >= k_usize && remaining_budget > 0.0 {
                        let cutoff = threshold - remaining_budget;
                        filter_competitive(&mut cand_docs, &mut cand_scores, cutoff);
                    }
                    // All candidates pruned by budget filter; no point
                    // scoring further non-essential terms.
                    if cand_docs.is_empty() {
                        break;
                    }

                    if terms[i].window_score == 0.0 {
                        continue;
                    }

                    let qw = terms[i].query_weight;
                    terms[i].cursor.score_candidates(
                        window_start,
                        window_end,
                        qw,
                        &cand_docs,
                        &mut cand_scores,
                    );

                    remaining_budget -= terms[i].window_score;
                }
            }

            for (ci, &doc) in cand_docs.iter().enumerate() {
                threshold = heap.push(cand_scores[ci], doc);
            }

            for (word_idx, word) in bitmap.iter_mut().enumerate().take(BITMAP_WORDS) {
                let mut bits = *word;
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    accum[word_idx * 64 + bit] = 0.0;
                    bits &= bits.wrapping_sub(1);
                }
                *word = 0;
            }

            window_start = window_end.wrapping_add(1);
            if window_start == 0 {
                break;
            }
        }

        Ok(heap.into_sorted_vec())
    }
}

struct TermState<'a> {
    cursor: PostingCursor<'a>,
    encoded_dim: String,
    query_weight: f32,
    max_score: f32,
    window_score: f32,
}

// ── Budget pruning ──────────────────────────────────────────────────

/// Remove candidates whose score <= cutoff. Both parallel arrays are
/// compacted in-place. Dispatches to SIMD on supported architectures.
fn filter_competitive(cand_docs: &mut Vec<u32>, cand_scores: &mut Vec<f32>, cutoff: f32) {
    debug_assert_eq!(cand_docs.len(), cand_scores.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            // SAFETY: avx512f detected at runtime; both slices have
            // equal length (debug_assert above), and write <= read index
            // guarantees no out-of-bounds writes.
            unsafe { filter_competitive_avx512(cand_docs, cand_scores, cutoff) };
            return;
        }
        if is_x86_feature_detected!("sse2") {
            // SAFETY: SSE2 detected at runtime; same index invariants.
            unsafe { filter_competitive_sse2(cand_docs, cand_scores, cutoff) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is always available on aarch64. Same index invariants.
        unsafe { filter_competitive_neon(cand_docs, cand_scores, cutoff) };
        return;
    }

    #[allow(unreachable_code)]
    filter_competitive_scalar(cand_docs, cand_scores, cutoff);
}

fn filter_competitive_scalar(cand_docs: &mut Vec<u32>, cand_scores: &mut Vec<f32>, cutoff: f32) {
    let n = cand_docs.len();
    let mut write = 0;
    for i in 0..n {
        if cand_scores[i] > cutoff {
            cand_docs[write] = cand_docs[i];
            cand_scores[write] = cand_scores[i];
            write += 1;
        }
    }
    cand_docs.truncate(write);
    cand_scores.truncate(write);
}

/// AVX-512: 16-wide `_mm512_cmp_ps_mask` returns a `__mmask16` directly,
/// then scatter surviving elements.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn filter_competitive_avx512(
    cand_docs: &mut Vec<u32>,
    cand_scores: &mut Vec<f32>,
    cutoff: f32,
) {
    use std::arch::x86_64::*;

    let n = cand_docs.len();
    let chunks = n / 16;
    let mut write = 0;

    let vcutoff = _mm512_set1_ps(cutoff);

    for c in 0..chunks {
        let base = c * 16;
        let vs = _mm512_loadu_ps(cand_scores.as_ptr().add(base));
        let mask = _mm512_cmp_ps_mask::<_CMP_GT_OQ>(vs, vcutoff);

        for bit in 0..16u32 {
            if mask & (1 << bit) != 0 {
                *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + bit as usize);
                *cand_scores.get_unchecked_mut(write) =
                    *cand_scores.get_unchecked(base + bit as usize);
                write += 1;
            }
        }
    }

    for i in (chunks * 16)..n {
        if *cand_scores.get_unchecked(i) > cutoff {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(i);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(i);
            write += 1;
        }
    }

    cand_docs.truncate(write);
    cand_scores.truncate(write);
}

/// SSE2: 4-wide `_mm_cmpgt_ps` + `_mm_movemask_ps` for branchless comparison,
/// then scatter surviving elements.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn filter_competitive_sse2(
    cand_docs: &mut Vec<u32>,
    cand_scores: &mut Vec<f32>,
    cutoff: f32,
) {
    use std::arch::x86_64::*;

    let n = cand_docs.len();
    let chunks = n / 4;
    let mut write = 0;

    let vcutoff = _mm_set1_ps(cutoff);

    // SAFETY: `base + bit` is in 0..n for all iterations. `write <= base + bit`
    // because we only advance write when an element passes, so writes never
    // overtake reads.
    for c in 0..chunks {
        let base = c * 4;
        let vs = _mm_loadu_ps(cand_scores.as_ptr().add(base));
        let cmp = _mm_cmpgt_ps(vs, vcutoff);
        let mask = _mm_movemask_ps(cmp) as u32;

        for bit in 0..4u32 {
            if mask & (1 << bit) != 0 {
                *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + bit as usize);
                *cand_scores.get_unchecked_mut(write) =
                    *cand_scores.get_unchecked(base + bit as usize);
                write += 1;
            }
        }
    }

    // Scalar remainder
    for i in (chunks * 4)..n {
        if *cand_scores.get_unchecked(i) > cutoff {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(i);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(i);
            write += 1;
        }
    }

    cand_docs.truncate(write);
    cand_scores.truncate(write);
}

/// NEON: 4-wide `vcgtq_f32` comparison, extract per-lane masks, scatter survivors.
#[cfg(target_arch = "aarch64")]
unsafe fn filter_competitive_neon(
    cand_docs: &mut Vec<u32>,
    cand_scores: &mut Vec<f32>,
    cutoff: f32,
) {
    use std::arch::aarch64::*;

    let n = cand_docs.len();
    let chunks = n / 4;
    let mut write = 0;

    let vcutoff = vdupq_n_f32(cutoff);

    // SAFETY: same index invariants as SSE2 — write <= read, all indices in 0..n.
    for c in 0..chunks {
        let base = c * 4;
        let vs = vld1q_f32(cand_scores.as_ptr().add(base));
        let cmp = vcgtq_f32(vs, vcutoff);

        let m0 = vgetq_lane_u32(cmp, 0);
        let m1 = vgetq_lane_u32(cmp, 1);
        let m2 = vgetq_lane_u32(cmp, 2);
        let m3 = vgetq_lane_u32(cmp, 3);

        if m0 != 0 {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(base);
            write += 1;
        }
        if m1 != 0 {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + 1);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(base + 1);
            write += 1;
        }
        if m2 != 0 {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + 2);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(base + 2);
            write += 1;
        }
        if m3 != 0 {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + 3);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(base + 3);
            write += 1;
        }
    }

    // Scalar remainder
    for i in (chunks * 4)..n {
        if *cand_scores.get_unchecked(i) > cutoff {
            *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(i);
            *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(i);
            write += 1;
        }
    }

    cand_docs.truncate(write);
    cand_scores.truncate(write);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_competitive_removes_below_cutoff() {
        let mut docs = vec![1, 2, 3, 4, 5];
        let mut scores = vec![0.1, 0.5, 0.2, 0.8, 0.3];
        filter_competitive(&mut docs, &mut scores, 0.25);
        assert_eq!(docs, vec![2, 4, 5]);
        assert_eq!(scores, vec![0.5, 0.8, 0.3]);
    }

    #[test]
    fn filter_competitive_empty() {
        let mut docs: Vec<u32> = vec![];
        let mut scores: Vec<f32> = vec![];
        filter_competitive(&mut docs, &mut scores, 0.0);
        assert!(docs.is_empty());
    }

    #[test]
    fn filter_competitive_simd_matches_scalar() {
        // Test at various sizes including remainder paths (not multiple of 4).
        for n in [1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 100] {
            let docs: Vec<u32> = (0..n as u32).collect();
            let scores: Vec<f32> = (0..n).map(|i| 0.1 * (i as f32 + 1.0)).collect();
            let cutoff = 0.5;

            let mut scalar_docs = docs.clone();
            let mut scalar_scores = scores.clone();
            filter_competitive_scalar(&mut scalar_docs, &mut scalar_scores, cutoff);

            let mut simd_docs = docs.clone();
            let mut simd_scores = scores.clone();
            filter_competitive(&mut simd_docs, &mut simd_scores, cutoff);

            assert_eq!(scalar_docs, simd_docs, "docs mismatch at n={n}");
            assert_eq!(scalar_scores, simd_scores, "scores mismatch at n={n}");
        }
    }

    #[test]
    fn filter_competitive_all_pass() {
        let mut docs = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut scores = vec![1.0; 8];
        filter_competitive(&mut docs, &mut scores, 0.0);
        assert_eq!(docs.len(), 8);
    }

    #[test]
    fn filter_competitive_none_pass() {
        let mut docs = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut scores = vec![0.1; 8];
        filter_competitive(&mut docs, &mut scores, 1.0);
        assert!(docs.is_empty());
    }

    #[test]
    fn cursor_from_blocks_single() {
        let block = SparsePostingBlock::from_sorted_entries(&[(0, 0.5), (10, 0.9)]).unwrap();
        let cursor = PostingCursor::from_blocks(vec![block]);
        assert_eq!(cursor.block_count(), 1);
        assert_eq!(cursor.dimension_max(), 0.9);
    }

    #[test]
    fn cursor_advance_basic() {
        let block =
            SparsePostingBlock::from_sorted_entries(&[(5, 0.1), (10, 0.2), (15, 0.3), (20, 0.4)])
                .unwrap();
        let all = SignedRoaringBitmap::Exclude(Default::default());
        let mut cursor = PostingCursor::from_blocks(vec![block]);

        let r = cursor.advance(10, &all);
        assert_eq!(r, Some((10, 0.2)));

        let r = cursor.advance(16, &all);
        assert_eq!(r, Some((20, 0.4)));

        let r = cursor.advance(21, &all);
        assert_eq!(r, None);
    }

    #[test]
    fn cursor_get_value() {
        let block =
            SparsePostingBlock::from_sorted_entries(&[(5, 0.1), (10, 0.2), (15, 0.3)]).unwrap();
        let mut cursor = PostingCursor::from_blocks(vec![block]);

        assert_eq!(cursor.get_value(10), Some(0.2));
        assert_eq!(cursor.get_value(7), None);
        assert_eq!(cursor.get_value(99), None);
    }
}

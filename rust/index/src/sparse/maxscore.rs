use std::{collections::BinaryHeap, sync::Arc};

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
pub use chroma_types::SparsePostingBlock;
use chroma_types::SignedRoaringBitmap;
use dashmap::DashMap;
use thiserror::Error;
use uuid::Uuid;

use crate::sparse::types::encode_u32;

const DEFAULT_BLOCK_SIZE: u32 = 1024;
const DIRECTORY_KEY: u32 = u32::MAX;

/// Recommended Arrow block size for the sparse posting blockfile.
/// 1 MB gives a good trade-off between S3 GET count and bandwidth waste.
pub const SPARSE_POSTING_BLOCK_SIZE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum BlockSparseError {
    #[error(transparent)]
    Blockfile(#[from] Box<dyn ChromaError>),
}

impl ChromaError for BlockSparseError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockSparseError::Blockfile(err) => err.code(),
        }
    }
}

// ── Score type ──────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
pub struct Score {
    pub score: f32,
    pub offset: u32,
}

impl Eq for Score {}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then(self.offset.cmp(&other.offset))
            .reverse()
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ── BlockSparseFlusher ──────────────────────────────────────────────

pub struct BlockSparseFlusher {
    posting_flusher: BlockfileFlusher,
}

impl BlockSparseFlusher {
    pub async fn flush(self) -> Result<(), BlockSparseError> {
        self.posting_flusher
            .flush::<u32, SparsePostingBlock>()
            .await?;
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.posting_flusher.id()
    }
}

// ── BlockSparseWriter ───────────────────────────────────────────────

#[derive(Clone)]
pub struct BlockSparseWriter<'me> {
    block_size: u32,
    delta: Arc<DashMap<u32, DashMap<u32, Option<f32>>>>,
    posting_writer: BlockfileWriter,
    old_reader: Option<BlockSparseReader<'me>>,
}

impl<'me> BlockSparseWriter<'me> {
    pub fn new(
        posting_writer: BlockfileWriter,
        old_reader: Option<BlockSparseReader<'me>>,
    ) -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            delta: Default::default(),
            posting_writer,
            old_reader,
        }
    }

    pub fn with_block_size(mut self, block_size: u32) -> Self {
        self.block_size = block_size;
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

    pub async fn commit(mut self) -> Result<BlockSparseFlusher, BlockSparseError> {
        let mut all_dim_ids: Vec<u32> = self.delta.iter().map(|e| *e.key()).collect();

        if let Some(ref reader) = self.old_reader {
            let old_dims = reader.get_all_dimension_ids().await?;
            all_dim_ids.extend(old_dims);
        }

        all_dim_ids.sort_unstable();
        all_dim_ids.dedup();

        // Sort by encoded string (base64) order for the ordered blockfile writer.
        let mut encoded_dims: Vec<(String, u32)> = all_dim_ids
            .into_iter()
            .map(|id| (encode_u32(id), id))
            .collect();
        encoded_dims.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut dim_max_entries: Vec<(String, f32)> = Vec::with_capacity(encoded_dims.len());

        for (encoded_dim, dimension_id) in &encoded_dims {
            let delta_updates = self.delta.remove(dimension_id);

            if delta_updates.is_none() {
                continue;
            }

            let (_, updates) = delta_updates.unwrap();

            // Load old entries
            let mut entries = std::collections::HashMap::new();
            let mut old_block_count = 0u32;
            if let Some(ref reader) = self.old_reader {
                let blocks = reader.get_posting_blocks(encoded_dim).await?;
                old_block_count = blocks.len() as u32;
                for block in blocks {
                    for (off, val) in block.offsets().iter().zip(block.values().iter()) {
                        entries.insert(*off, *val);
                    }
                }
            }

            // Apply deltas
            for entry in updates.into_iter() {
                let (off, update) = entry;
                match update {
                    Some(val) => {
                        entries.insert(off, val);
                    }
                    None => {
                        entries.remove(&off);
                    }
                }
            }

            if entries.is_empty() {
                for seq in 0..old_block_count {
                    self.posting_writer
                        .delete::<_, SparsePostingBlock>(encoded_dim, seq)
                        .await?;
                }
                self.posting_writer
                    .delete::<_, SparsePostingBlock>(encoded_dim, DIRECTORY_KEY)
                    .await?;
                continue;
            }

            // Sort and chunk into blocks
            let mut sorted: Vec<(u32, f32)> = entries.into_iter().collect();
            sorted.sort_unstable_by_key(|(off, _)| *off);

            let mut dir_max_offsets = Vec::new();
            let mut dir_max_weights = Vec::new();

            let new_block_count = sorted.chunks(self.block_size as usize).len() as u32;
            for (seq, chunk) in sorted.chunks(self.block_size as usize).enumerate() {
                let block = SparsePostingBlock::from_sorted_entries(chunk);
                dir_max_offsets.push(block.max_offset);
                dir_max_weights.push(block.max_weight);
                self.posting_writer
                    .set(encoded_dim, seq as u32, block)
                    .await?;
            }

            // Delete stale trailing blocks (must come before directory for key ordering)
            for seq in new_block_count..old_block_count {
                self.posting_writer
                    .delete::<_, SparsePostingBlock>(encoded_dim, seq)
                    .await?;
            }

            // Write block directory at DIRECTORY_KEY — always last per prefix
            let dim_max = dir_max_weights.iter().copied().fold(0.0f32, f32::max);
            dim_max_entries.push((encoded_dim.clone(), dim_max));

            let directory = SparsePostingBlock::from_directory(&dir_max_offsets, &dir_max_weights);
            self.posting_writer
                .set(encoded_dim, DIRECTORY_KEY, directory)
                .await?;
        }

        self.posting_writer.set_dim_max_weights(dim_max_entries);

        let flusher = self
            .posting_writer
            .commit::<u32, SparsePostingBlock>()
            .await?;

        Ok(BlockSparseFlusher {
            posting_flusher: flusher,
        })
    }
}

// ── PostingCursor ───────────────────────────────────────────────────

enum CursorSource<'view> {
    View {
        raw_blocks: Vec<&'view [u8]>,
    },
    Lazy {
        raw_blocks: Vec<Option<&'view [u8]>>,
    },
    Eager {
        blocks: Vec<SparsePostingBlock>,
    },
}

pub struct PostingCursor<'view> {
    source: CursorSource<'view>,
    dir_max_offsets: Vec<u32>,
    pub(crate) dir_max_weights: Vec<f32>,
    dim_max: f32,
    block_count: usize,
    // Forward scan state
    block_idx: usize,
    pos: usize,
    // Reusable decompression buffers (View mode)
    offset_buf: Vec<u32>,
    value_buf: Vec<f32>,
    buf_block_idx: usize,
    // Separate tracking for drain_essential's offset-only decompress.
    // drain_essential reads raw u8 weights directly (fused path) and
    // only decompresses offsets — buf_block_idx must NOT be set in
    // that case or ensure_forward_block will skip value decompression.
    drain_buf_block_idx: usize,
    // Point lookup buffer -- offsets only (View mode)
    lookup_offset_buf: Vec<u32>,
    lookup_buf_block_idx: usize,
}

impl<'view> PostingCursor<'view> {
    /// Create a cursor backed by pre-extracted raw block slices.
    /// Loads all blocks for this dimension once (async), then extracts
    /// raw byte slices for O(1) block access in the hot path.
    pub async fn open(
        reader: &'view BlockfileReader<'view, u32, SparsePostingBlock>,
        encoded_dim: String,
    ) -> Result<Option<Self>, BlockSparseError> {
        let view = reader.open_prefix_view(&encoded_dim).await?;

        let dir_block: Option<SparsePostingBlock> =
            view.get::<u32, SparsePostingBlock>(DIRECTORY_KEY);

        let Some(dir_block) = dir_block.filter(|b| b.is_directory()) else {
            let blocks: Vec<SparsePostingBlock> = view
                .iter::<u32, SparsePostingBlock>()
                .filter(|(k, _)| *k != DIRECTORY_KEY)
                .map(|(_, b)| b)
                .collect();
            if blocks.is_empty() {
                return Ok(None);
            }
            return Ok(Some(Self::from_blocks(blocks)));
        };

        let (dir_max_offsets, dir_max_weights) = dir_block.directory_entries();
        let block_count = dir_max_offsets.len();
        if block_count == 0 {
            return Ok(None);
        }
        let dim_max = dir_block.max_weight;

        // Pre-extract raw byte slices for all posting blocks in O(N) linear scan.
        // Blocks are in key order (0, 1, ..., N-1, DIRECTORY_KEY); take first N.
        let all_raw = view.collect_raw_binary_in_order();
        let raw_blocks: Vec<&'view [u8]> = all_raw.into_iter().take(block_count).collect();

        Ok(Some(PostingCursor {
            source: CursorSource::View { raw_blocks },
            dir_max_offsets,
            dir_max_weights,
            dim_max,
            block_count,
            block_idx: 0,
            pos: 0,
            offset_buf: Vec::new(),
            value_buf: Vec::new(),
            buf_block_idx: usize::MAX,
            drain_buf_block_idx: usize::MAX,
            lookup_offset_buf: Vec::new(),
            lookup_buf_block_idx: usize::MAX,
        }))
    }

    /// Create an eager cursor from pre-loaded blocks (used by tests).
    pub fn from_blocks(blocks: Vec<SparsePostingBlock>) -> Self {
        let dir_max_offsets: Vec<u32> = blocks.iter().map(|b| b.max_offset).collect();
        let dir_max_weights: Vec<f32> = blocks.iter().map(|b| b.max_weight).collect();
        let dim_max = dir_max_weights.iter().copied().fold(0.0f32, f32::max);
        let block_count = blocks.len();

        PostingCursor {
            source: CursorSource::Eager { blocks },
            dir_max_offsets,
            dir_max_weights,
            dim_max,
            block_count,
            block_idx: 0,
            pos: 0,
            offset_buf: Vec::new(),
            value_buf: Vec::new(),
            buf_block_idx: usize::MAX,
            drain_buf_block_idx: usize::MAX,
            lookup_offset_buf: Vec::new(),
            lookup_buf_block_idx: usize::MAX,
        }
    }

    /// Create a lazy cursor that only loads the directory block up-front.
    /// Posting data blocks start as `None` and are populated later via
    /// `populate_from_cache`.
    pub async fn open_lazy(
        reader: &'view BlockfileReader<'view, u32, SparsePostingBlock>,
        encoded_dim: String,
    ) -> Result<Option<Self>, BlockSparseError> {
        let dir_raw: Option<SparsePostingBlock> = reader
            .get(&encoded_dim, DIRECTORY_KEY)
            .await
            .map_err(BlockSparseError::Blockfile)?;

        let Some(dir_block) = dir_raw.filter(|b| b.is_directory()) else {
            return Ok(None);
        };

        let (dir_max_offsets, dir_max_weights) = dir_block.directory_entries();
        let block_count = dir_max_offsets.len();
        if block_count == 0 {
            return Ok(None);
        }
        let dim_max = dir_block.max_weight;

        let raw_blocks = vec![None; block_count];

        Ok(Some(PostingCursor {
            source: CursorSource::Lazy { raw_blocks },
            dir_max_offsets,
            dir_max_weights,
            dim_max,
            block_count,
            block_idx: 0,
            pos: 0,
            offset_buf: Vec::new(),
            value_buf: Vec::new(),
            buf_block_idx: usize::MAX,
            drain_buf_block_idx: usize::MAX,
            lookup_offset_buf: Vec::new(),
            lookup_buf_block_idx: usize::MAX,
        }))
    }

    /// Whether this is a lazy cursor with potentially unloaded blocks.
    pub fn is_lazy(&self) -> bool {
        matches!(self.source, CursorSource::Lazy { .. })
    }

    /// Check whether a specific posting block has been loaded.
    pub fn is_block_loaded(&self, idx: usize) -> bool {
        match &self.source {
            CursorSource::Lazy { raw_blocks } => {
                idx < raw_blocks.len() && raw_blocks[idx].is_some()
            }
            _ => true,
        }
    }

    /// Return all block keys (0..block_count) for this dimension's posting
    /// list. Used by the I/O pipeline to schedule fetches.
    pub fn all_block_keys(&self) -> Vec<u32> {
        (0..self.block_count as u32).collect()
    }

    /// Populate lazy blocks from the reader's cache. For each block index
    /// in `block_indices`, attempts a synchronous cache lookup and fills
    /// the `Option` slot. Returns the number of blocks successfully loaded.
    pub fn populate_from_cache(
        &mut self,
        reader: &'view BlockfileReader<'view, u32, SparsePostingBlock>,
        encoded_dim: &str,
        block_indices: &[usize],
    ) -> usize {
        let CursorSource::Lazy { raw_blocks } = &mut self.source else {
            return 0;
        };
        let mut loaded = 0;
        for &idx in block_indices {
            if idx >= raw_blocks.len() || raw_blocks[idx].is_some() {
                continue;
            }
            if let Some(bytes) =
                reader.get_raw_binary_from_cache(encoded_dim, idx as u32)
            {
                raw_blocks[idx] = Some(bytes);
                loaded += 1;
            }
        }
        loaded
    }

    /// Populate all unloaded blocks from the reader's cache. Returns the
    /// number of blocks successfully loaded.
    pub fn populate_all_from_cache(
        &mut self,
        reader: &'view BlockfileReader<'view, u32, SparsePostingBlock>,
        encoded_dim: &str,
    ) -> usize {
        let indices: Vec<usize> = (0..self.block_count).collect();
        self.populate_from_cache(reader, encoded_dim, &indices)
    }

    /// Number of posting data blocks in this dimension.
    pub fn block_count(&self) -> usize {
        self.block_count
    }

    /// Ensure `offset_buf` / `value_buf` contain the decompressed data for
    /// block `idx`.  Returns `false` if the block could not be loaded.
    fn ensure_forward_block(&mut self, idx: usize) -> bool {
        if self.buf_block_idx == idx {
            return true;
        }
        match &self.source {
            CursorSource::View { raw_blocks } => {
                let raw = raw_blocks[idx];
                let hdr = SparsePostingBlock::peek_header(raw);
                if self.drain_buf_block_idx != idx {
                    SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut self.offset_buf);
                }
                SparsePostingBlock::decompress_values_into(raw, &hdr, &mut self.value_buf);
                self.buf_block_idx = idx;
                self.drain_buf_block_idx = idx;
                true
            }
            CursorSource::Lazy { raw_blocks } => {
                let Some(raw) = raw_blocks[idx] else {
                    return false;
                };
                let hdr = SparsePostingBlock::peek_header(raw);
                if self.drain_buf_block_idx != idx {
                    SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut self.offset_buf);
                }
                SparsePostingBlock::decompress_values_into(raw, &hdr, &mut self.value_buf);
                self.buf_block_idx = idx;
                self.drain_buf_block_idx = idx;
                true
            }
            CursorSource::Eager { .. } => true,
        }
    }

    /// Ensure `lookup_offset_buf` contains offsets for block `idx`.
    /// Only decompresses offsets -- values are read on demand via
    /// `read_value_at`.
    fn ensure_lookup_offsets(&mut self, idx: usize) -> bool {
        if self.lookup_buf_block_idx == idx {
            return true;
        }
        match &self.source {
            CursorSource::View { raw_blocks } => {
                let raw = raw_blocks[idx];
                let hdr = SparsePostingBlock::peek_header(raw);
                SparsePostingBlock::decompress_offsets_into(
                    raw,
                    &hdr,
                    &mut self.lookup_offset_buf,
                );
                self.lookup_buf_block_idx = idx;
                true
            }
            CursorSource::Lazy { raw_blocks } => {
                let Some(raw) = raw_blocks[idx] else {
                    return false;
                };
                let hdr = SparsePostingBlock::peek_header(raw);
                SparsePostingBlock::decompress_offsets_into(
                    raw,
                    &hdr,
                    &mut self.lookup_offset_buf,
                );
                self.lookup_buf_block_idx = idx;
                true
            }
            CursorSource::Eager { .. } => true,
        }
    }

    fn forward_offsets(&self) -> &[u32] {
        match &self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].offsets(),
            CursorSource::View { .. } | CursorSource::Lazy { .. } => &self.offset_buf,
        }
    }

    fn forward_values(&self) -> &[f32] {
        match &self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].values(),
            CursorSource::View { .. } | CursorSource::Lazy { .. } => &self.value_buf,
        }
    }

    pub fn current(&self) -> Option<(u32, f32)> {
        if self.block_idx >= self.block_count {
            return None;
        }
        let offsets = self.forward_offsets();
        let values = self.forward_values();
        if self.pos < offsets.len() {
            Some((offsets[self.pos], values[self.pos]))
        } else {
            None
        }
    }

    pub fn advance(
        &mut self,
        target: u32,
        mask: &SignedRoaringBitmap,
    ) -> Option<(u32, f32)> {
        while self.block_idx < self.block_count {
            if self.dir_max_offsets[self.block_idx] < target {
                self.block_idx += 1;
                self.pos = 0;
                continue;
            }

            if !self.ensure_forward_block(self.block_idx) {
                self.block_idx += 1;
                self.pos = 0;
                continue;
            }

            let (offsets, values) = match &self.source {
                CursorSource::Eager { blocks } => {
                    (blocks[self.block_idx].offsets(), blocks[self.block_idx].values())
                }
                CursorSource::View { .. } | CursorSource::Lazy { .. } => {
                    (&self.offset_buf[..], &self.value_buf[..])
                }
            };

            if self.pos == 0 || offsets.get(self.pos).is_none_or(|&o| o < target) {
                let start = self.pos;
                self.pos = start + offsets[start..].partition_point(|&o| o < target);
            }

            while self.pos < offsets.len() {
                let off = offsets[self.pos];
                if passes_mask(off, mask) {
                    return Some((off, values[self.pos]));
                }
                self.pos += 1;
            }

            self.block_idx += 1;
            self.pos = 0;
        }
        None
    }

    /// Point lookup for a single doc_id.
    ///
    /// Fast path: reuses the forward buffer if the target block is already
    /// loaded there (no decompression needed).
    ///
    /// Slow path (View): decompresses only offsets, then reads a single
    /// quantized byte on hit via `read_value_at`.
    pub fn get_value(&mut self, doc_id: u32) -> Option<f32> {
        let bi = self
            .dir_max_offsets
            .partition_point(|&max_off| max_off < doc_id);
        if bi >= self.block_count {
            return None;
        }

        // Fast path: forward buffer already has this block fully decompressed
        if self.buf_block_idx == bi {
            return match self.offset_buf.binary_search(&doc_id) {
                Ok(idx) => Some(self.value_buf[idx]),
                Err(_) => None,
            };
        }

        // Eager path: direct access to pre-loaded blocks
        if let CursorSource::Eager { blocks } = &self.source {
            let offsets = blocks[bi].offsets();
            let values = blocks[bi].values();
            if offsets.is_empty() || doc_id < offsets[0] {
                return None;
            }
            return match offsets.binary_search(&doc_id) {
                Ok(idx) => Some(values[idx]),
                Err(_) => None,
            };
        }

        // View/Lazy path: decompress offsets only, read single value on hit
        if !self.ensure_lookup_offsets(bi) {
            return None;
        }
        let offsets = &self.lookup_offset_buf;
        if offsets.is_empty() || doc_id < offsets[0] {
            return None;
        }
        match offsets.binary_search(&doc_id) {
            Ok(idx) => {
                let raw = match &self.source {
                    CursorSource::View { raw_blocks } => raw_blocks[bi],
                    CursorSource::Lazy { raw_blocks } => raw_blocks[bi]?,
                    CursorSource::Eager { .. } => unreachable!(),
                };
                let hdr = SparsePostingBlock::peek_header(raw);
                Some(SparsePostingBlock::read_value_at(raw, &hdr, idx))
            }
            Err(_) => None,
        }
    }

    pub fn current_block_max(&self) -> f32 {
        self.dir_max_weights
            .get(self.block_idx)
            .copied()
            .unwrap_or(0.0)
    }

    pub fn dimension_max(&self) -> f32 {
        self.dim_max
    }

    /// Return the block-level upper bound weight for a given doc_id.
    /// Finds the first block whose max_offset >= doc_id and returns
    /// that block's max_weight. O(log B) where B is number of blocks.
    pub fn block_upper_bound(&self, doc_id: u32) -> f32 {
        let bi = self.dir_max_offsets.partition_point(|&max| max < doc_id);
        if bi >= self.block_count {
            0.0
        } else {
            self.dir_max_weights[bi]
        }
    }

    /// O(1) sequential advance to the next entry. Handles block transitions.
    /// Used by the batched window loop to drain one iterator many times in a
    /// row, giving perfect cache prefetching and branch prediction.
    pub fn next(&mut self) {
        if self.block_idx >= self.block_count {
            return;
        }
        self.pos += 1;
        let len = match &self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].len(),
            CursorSource::View { .. } | CursorSource::Lazy { .. } => self.offset_buf.len(),
        };
        if self.pos >= len {
            self.block_idx += 1;
            self.pos = 0;
            if self.block_idx < self.block_count {
                self.ensure_forward_block(self.block_idx);
            }
        }
    }

    /// Max doc_id in the cursor's current block (from the directory).
    pub fn current_block_end(&self) -> Option<u32> {
        self.dir_max_offsets.get(self.block_idx).copied()
    }

    /// Batch-drain all entries in [window_start, window_end] into a flat
    /// accumulator array.  Each doc's score is accumulated as
    /// `accum[(doc - window_start)] += query_weight * value`.
    ///
    /// `doc_set` collects the first occurrence of each doc_id (used to
    /// enumerate non-zero slots afterwards without scanning the whole array).
    ///
    /// The cursor is left positioned at the first entry > window_end (or
    /// exhausted).  This is the hot path — no per-entry function calls,
    /// no enum dispatch inside the inner loop.
    pub fn drain_essential(
        &mut self,
        window_start: u32,
        window_end: u32,
        query_weight: f32,
        accum: &mut [f32],
        bitmap: &mut [u64],
        mask: &SignedRoaringBitmap,
    ) {
        while self.block_idx < self.block_count {
            if self.dir_max_offsets[self.block_idx] < window_start {
                self.block_idx += 1;
                self.pos = 0;
                continue;
            }

            match &self.source {
                // Fused path for View/Lazy: decompress offsets only, read
                // u8 weights directly from raw bytes.  Saves writing+reading
                // value_buf and one f32 multiply per entry (the dequant
                // scale and query_weight are fused into a single `factor`).
                CursorSource::View { raw_blocks } => {
                    let raw = raw_blocks[self.block_idx];
                    let hdr = SparsePostingBlock::peek_header(raw);
                    if self.drain_buf_block_idx != self.block_idx {
                        SparsePostingBlock::decompress_offsets_into(
                            raw, &hdr, &mut self.offset_buf,
                        );
                        self.drain_buf_block_idx = self.block_idx;
                        self.buf_block_idx = usize::MAX;
                    }
                    let weights = SparsePostingBlock::raw_weights(raw, &hdr);
                    let factor = query_weight * hdr.max_weight / 255.0;

                    if self.pos == 0 && self.offset_buf[0] < window_start {
                        self.pos = self.offset_buf.partition_point(|&o| o < window_start);
                    }
                    while self.pos < self.offset_buf.len() {
                        let doc = self.offset_buf[self.pos];
                        if doc > window_end {
                            return;
                        }
                        if passes_mask(doc, mask) {
                            let idx = (doc - window_start) as usize;
                            bitmap[idx >> 6] |= 1u64 << (idx & 63);
                            accum[idx] += weights[self.pos] as f32 * factor;
                        }
                        self.pos += 1;
                    }
                }
                CursorSource::Lazy { raw_blocks } => {
                    let Some(raw) = raw_blocks[self.block_idx] else {
                        self.block_idx += 1;
                        self.pos = 0;
                        continue;
                    };
                    let hdr = SparsePostingBlock::peek_header(raw);
                    if self.drain_buf_block_idx != self.block_idx {
                        SparsePostingBlock::decompress_offsets_into(
                            raw, &hdr, &mut self.offset_buf,
                        );
                        self.drain_buf_block_idx = self.block_idx;
                        self.buf_block_idx = usize::MAX;
                    }
                    let weights = SparsePostingBlock::raw_weights(raw, &hdr);
                    let factor = query_weight * hdr.max_weight / 255.0;

                    if self.pos == 0 && self.offset_buf[0] < window_start {
                        self.pos = self.offset_buf.partition_point(|&o| o < window_start);
                    }
                    while self.pos < self.offset_buf.len() {
                        let doc = self.offset_buf[self.pos];
                        if doc > window_end {
                            return;
                        }
                        if passes_mask(doc, mask) {
                            let idx = (doc - window_start) as usize;
                            bitmap[idx >> 6] |= 1u64 << (idx & 63);
                            accum[idx] += weights[self.pos] as f32 * factor;
                        }
                        self.pos += 1;
                    }
                }
                CursorSource::Eager { blocks } => {
                    let offsets = blocks[self.block_idx].offsets();
                    let qw = blocks[self.block_idx].quantized_weights();
                    let factor = query_weight * blocks[self.block_idx].max_weight / 255.0;

                    if self.pos == 0 && offsets[0] < window_start {
                        self.pos = offsets.partition_point(|&o| o < window_start);
                    }
                    while self.pos < offsets.len() {
                        let doc = offsets[self.pos];
                        if doc > window_end {
                            return;
                        }
                        if passes_mask(doc, mask) {
                            let idx = (doc - window_start) as usize;
                            bitmap[idx >> 6] |= 1u64 << (idx & 63);
                            accum[idx] += qw[self.pos] as f32 * factor;
                        }
                        self.pos += 1;
                    }
                }
            }

            self.block_idx += 1;
            self.pos = 0;
        }
    }

    /// Merge-join this (non-essential) cursor against sorted candidates,
    /// accumulating matched scores directly into contiguous `cand_scores`.
    pub fn score_candidates(
        &mut self,
        window_start: u32,
        window_end: u32,
        query_weight: f32,
        cand_docs: &[u32],
        cand_scores: &mut [f32],
    ) {
        if cand_docs.is_empty() {
            return;
        }

        let mut ci = 0;

        while self.block_idx < self.block_count && ci < cand_docs.len() {
            if self.dir_max_offsets[self.block_idx] < window_start
                || self.dir_max_offsets[self.block_idx] < cand_docs[ci]
            {
                self.block_idx += 1;
                self.pos = 0;
                continue;
            }

            if !self.ensure_forward_block(self.block_idx) {
                self.block_idx += 1;
                self.pos = 0;
                continue;
            }

            let (offsets, values) = match &self.source {
                CursorSource::Eager { blocks } => {
                    (blocks[self.block_idx].offsets(), blocks[self.block_idx].values())
                }
                CursorSource::View { .. } | CursorSource::Lazy { .. } => {
                    (&self.offset_buf[..], &self.value_buf[..])
                }
            };

            if self.pos == 0 && offsets[0] < window_start {
                self.pos = offsets.partition_point(|&o| o < window_start);
            }

            while self.pos < offsets.len() && ci < cand_docs.len() {
                let doc = offsets[self.pos];
                if doc > window_end {
                    return;
                }
                let cand = cand_docs[ci];
                if doc < cand {
                    self.pos += 1;
                } else if doc > cand {
                    ci += 1;
                } else {
                    cand_scores[ci] += query_weight * values[self.pos];
                    self.pos += 1;
                    ci += 1;
                }
            }

            if self.pos >= offsets.len() {
                self.block_idx += 1;
                self.pos = 0;
            }
        }
    }
}

fn passes_mask(offset: u32, mask: &SignedRoaringBitmap) -> bool {
    match mask {
        SignedRoaringBitmap::Include(rbm) => rbm.contains(offset),
        SignedRoaringBitmap::Exclude(rbm) => !rbm.contains(offset),
    }
}

// ── BlockSparseReader ───────────────────────────────────────────────

#[derive(Clone)]
pub struct BlockSparseReader<'me> {
    posting_reader: BlockfileReader<'me, u32, SparsePostingBlock>,
}

impl<'me> BlockSparseReader<'me> {
    pub fn new(posting_reader: BlockfileReader<'me, u32, SparsePostingBlock>) -> Self {
        Self { posting_reader }
    }

    pub fn posting_id(&self) -> Uuid {
        self.posting_reader.id()
    }

    /// Return the per-dimension max_weight map from the root, if present.
    pub fn dim_max_weights(&self) -> Option<&std::collections::HashMap<String, f32>> {
        self.posting_reader.dim_max_weights()
    }

    /// Load all posting blocks for a dimension (excluding directory).
    /// Used by the writer's commit path to read old entries.
    pub async fn get_posting_blocks(
        &self,
        encoded_dim: &str,
    ) -> Result<Vec<SparsePostingBlock>, BlockSparseError> {
        let blocks: Vec<(u32, SparsePostingBlock)> = self
            .posting_reader
            .get_prefix(encoded_dim)
            .await?
            .collect();
        Ok(blocks
            .into_iter()
            .filter(|(key, _)| *key != DIRECTORY_KEY)
            .map(|(_, b)| b)
            .collect())
    }

    pub async fn get_all_dimension_ids(&self) -> Result<Vec<u32>, BlockSparseError> {
        use crate::sparse::types::decode_u32;

        let all: Vec<(&str, u32, SparsePostingBlock)> = self
            .posting_reader
            .get_range(.., ..)
            .await?
            .collect();

        let mut dims: Vec<u32> = all
            .iter()
            .filter_map(|(prefix, _, _)| decode_u32(prefix).ok())
            .collect();
        dims.sort_unstable();
        dims.dedup();
        Ok(dims)
    }

    /// BlockMaxMaxScore query algorithm with 3-batch I/O pipeline.
    ///
    /// 1. **Batch 1**: Fetch directory blocks for all query dimensions
    ///    (builds lazy cursors cheaply). Small dimensions (<= 2 Arrow
    ///    blocks) use the eager path instead.
    /// 2. Sort terms, partition essential / non-essential. Load ALL
    ///    blocks for essential terms. Run initial windows to stabilize
    ///    the threshold.
    /// 3. **Batch 2**: With the real threshold, prune non-essential
    ///    blocks and load only those that survive. Resume the sync loop.
    ///
    /// The sync inner loop is completely unchanged — all MaxScore
    /// hot-loop optimizations (flat buffers, budget pruning, adaptive
    /// windows, zero-copy decompress) operate on cached data.
    pub async fn query(
        &'me self,
        query_vector: impl IntoIterator<Item = (u32, f32)>,
        k: u32,
        mask: SignedRoaringBitmap,
    ) -> Result<Vec<Score>, BlockSparseError> {
        if k == 0 {
            return Ok(vec![]);
        }

        let collected: Vec<(u32, f32)> = query_vector.into_iter().collect();
        let encoded_dims: Vec<String> = collected.iter().map(|(d, _)| encode_u32(*d)).collect();

        // ── Phase 0: dim_max root pruning (zero I/O) ─────────────
        // If the root contains per-dimension max_weight, skip
        // dimensions that can't contribute to any result.
        let root_dim_max = self.posting_reader.dim_max_weights();
        let surviving: Vec<(usize, f32)> = collected
            .iter()
            .enumerate()
            .filter(|(idx, &(_, _qw))| {
                root_dim_max
                    .and_then(|m| m.get(&encoded_dims[*idx]))
                    .map_or(true, |&dmax| dmax > 0.0)
            })
            .map(|(idx, &(_, qw))| (idx, qw))
            .collect();

        // ── Batch 1: directory blocks for surviving dimensions ────
        let dir_keys: Vec<(String, u32)> = surviving
            .iter()
            .map(|(idx, _)| (encoded_dims[*idx].clone(), DIRECTORY_KEY))
            .collect();
        self.posting_reader.load_data_for_keys(dir_keys).await;

        // Build cursors. Small dimensions (<= 2 Arrow blocks) use the
        // eager PrefixView path; large dimensions use lazy cursors.
        let mut terms: Vec<TermState<'me>> = Vec::new();
        for &(idx, query_weight) in &surviving {
            let encoded = &encoded_dims[idx];
            let block_count = self.posting_reader.count_blocks_for_prefix(encoded);

            if block_count <= 2 {
                // Eagerly load small dimensions -- no benefit from lazy.
                self.posting_reader
                    .load_blocks_for_prefixes(std::iter::once(encoded.as_str()))
                    .await;
                let Some(mut cursor) =
                    PostingCursor::open(&self.posting_reader, encoded.clone()).await?
                else {
                    continue;
                };
                cursor.advance(0, &mask);
                let max_score = query_weight * cursor.dimension_max();
                terms.push(TermState {
                    cursor,
                    query_weight,
                    max_score,
                    encoded_dim: encoded.clone(),
                });
            } else {
                let Some(cursor) =
                    PostingCursor::open_lazy(&self.posting_reader, encoded.clone()).await?
                else {
                    continue;
                };
                let max_score = query_weight * cursor.dimension_max();
                terms.push(TermState {
                    cursor,
                    query_weight,
                    max_score,
                    encoded_dim: encoded.clone(),
                });
            }
        }

        if terms.is_empty() {
            return Ok(vec![]);
        }

        // Sort terms by max_score ascending (lowest first → non-essential)
        terms.sort_by(|a, b| a.max_score.total_cmp(&b.max_score));
        let upper_bounds = prefix_sum(&terms);

        let k_usize = k as usize;
        let mut threshold = f32::MIN;
        let mut heap: BinaryHeap<Score> = BinaryHeap::with_capacity(k_usize);
        let mut essential_idx = 0usize;
        let full_mask = SignedRoaringBitmap::full();

        // ── Batch 2: all blocks for essential terms ──────────────
        // At threshold=MIN all terms are essential. Load their posting
        // blocks so the first windows can run without blocking.
        {
            let mut keys_to_load: Vec<(String, u32)> = Vec::new();
            for t in terms[essential_idx..].iter() {
                if t.cursor.is_lazy() {
                    for bk in t.cursor.all_block_keys() {
                        keys_to_load.push((t.encoded_dim.clone(), bk));
                    }
                }
            }
            if !keys_to_load.is_empty() {
                self.posting_reader.load_data_for_keys(keys_to_load).await;
                for t in terms[essential_idx..].iter_mut() {
                    let dim = t.encoded_dim.clone();
                    t.cursor
                        .populate_all_from_cache(&self.posting_reader, &dim);
                }
            }
        }

        // Track whether we've done the non-essential prefetch (Batch 3).
        let mut non_essential_loaded = false;

        // ── L1-resident window accumulator ──────────────────────────
        // 4K window → 16KB accum fits entirely in L1 cache.
        // Essential terms scatter into accum; a u64 bitmap tracks
        // touched slots (branchless, 512 bytes).  After drain we scan
        // the bitmap to produce sorted cand_docs + contiguous
        // cand_scores, enabling SIMD budget pruning between
        // non-essential terms.
        const WINDOW_WIDTH: u32 = 4096;
        const BITMAP_WORDS: usize = (WINDOW_WIDTH as usize + 63) / 64;
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

        while window_start <= max_doc_id {
            let window_end = (window_start + WINDOW_WIDTH - 1).min(max_doc_id);

            // ── Phase 1: batch-drain essential terms into accumulator ──
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

            // Scan bitmap → sorted cand_docs + contiguous cand_scores.
            // The bitmap scan produces sorted order naturally (no sort needed).
            cand_docs.clear();
            cand_scores.clear();
            for word_idx in 0..BITMAP_WORDS {
                let mut bits = bitmap[word_idx];
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    let idx = word_idx * 64 + bit;
                    cand_docs.push(window_start + idx as u32);
                    cand_scores.push(accum[idx]);
                    bits &= bits.wrapping_sub(1);
                }
            }

            if cand_docs.is_empty() {
                window_start = window_end.wrapping_add(1);
                if window_start == 0 {
                    break;
                }
                continue;
            }

            // ── Batch 3: lazy-load non-essential blocks (once) ────
            if essential_idx > 0 && !non_essential_loaded {
                non_essential_loaded = true;
                let mut ne_keys: Vec<(String, u32)> = Vec::new();
                for i in 0..essential_idx {
                    if !terms[i].cursor.is_lazy() {
                        continue;
                    }
                    let w = terms[i].query_weight;
                    for bi in 0..terms[i].cursor.block_count() {
                        if terms[i].cursor.is_block_loaded(bi) {
                            continue;
                        }
                        let block_max = terms[i].cursor.dir_max_weights[bi];
                        if w * block_max > threshold {
                            ne_keys.push((terms[i].encoded_dim.clone(), bi as u32));
                        }
                    }
                }
                if !ne_keys.is_empty() {
                    self.posting_reader.load_data_for_keys(ne_keys).await;
                    for i in 0..essential_idx {
                        if terms[i].cursor.is_lazy() {
                            let dim = terms[i].encoded_dim.clone();
                            terms[i]
                                .cursor
                                .populate_all_from_cache(&self.posting_reader, &dim);
                        }
                    }
                }
            }

            // ── Phase 2: non-essential merge-join with SIMD pruning ──
            if essential_idx > 0 {
                let mut remaining_budget: f32 = (0..essential_idx)
                    .map(|j| {
                        terms[j].query_weight
                            * terms[j].cursor.block_upper_bound(window_start)
                    })
                    .sum();

                for i in (0..essential_idx).rev() {
                    if heap.len() >= k_usize && remaining_budget > 0.0 {
                        let cutoff = threshold - remaining_budget;
                        filter_competitive(&mut cand_docs, &mut cand_scores, cutoff);
                    }
                    if cand_docs.is_empty() {
                        break;
                    }

                    let qw = terms[i].query_weight;
                    let bub = terms[i].cursor.block_upper_bound(window_start);
                    if bub == 0.0 {
                        remaining_budget -= qw * bub;
                        continue;
                    }

                    if terms[i]
                        .cursor
                        .current()
                        .is_some_and(|(d, _)| d < window_start)
                    {
                        terms[i].cursor.advance(window_start, &full_mask);
                    }

                    terms[i].cursor.score_candidates(
                        window_start,
                        window_end,
                        qw,
                        &cand_docs,
                        &mut cand_scores,
                    );

                    remaining_budget -= qw * bub;
                }
            }

            // ── Phase 3: extract to heap and reset accumulator ─────
            for (ci, &doc) in cand_docs.iter().enumerate() {
                let score = cand_scores[ci];
                if score > threshold || heap.len() < k_usize {
                    heap.push(Score {
                        score,
                        offset: doc,
                    });
                    if heap.len() > k_usize {
                        heap.pop();
                    }
                    if heap.len() == k_usize {
                        threshold = heap.peek().map(|s| s.score).unwrap_or(f32::MIN);
                    }
                }
            }

            // Zero accum slots + clear bitmap using the bitmap itself.
            for word_idx in 0..BITMAP_WORDS {
                let mut bits = bitmap[word_idx];
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    accum[word_idx * 64 + bit] = 0.0;
                    bits &= bits.wrapping_sub(1);
                }
                bitmap[word_idx] = 0;
            }

            // Re-partition essential vs non-essential
            while essential_idx < terms.len() {
                if upper_bounds[essential_idx] >= threshold {
                    break;
                }
                essential_idx += 1;
            }

            window_start = window_end.wrapping_add(1);
            if window_start == 0 {
                break;
            }
        }

        let mut results: Vec<Score> = heap.into_vec();
        results.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then(a.offset.cmp(&b.offset))
        });
        Ok(results)
    }
}

struct TermState<'me> {
    cursor: PostingCursor<'me>,
    query_weight: f32,
    max_score: f32,
    encoded_dim: String,
}

fn prefix_sum(terms: &[TermState<'_>]) -> Vec<f32> {
    let mut sums = Vec::with_capacity(terms.len());
    let mut acc = 0.0f32;
    for t in terms {
        acc += t.max_score;
        sums.push(acc);
    }
    sums
}

// ── SIMD-accelerated budget pruning (SereneDB FilterCompetitiveHits) ──

/// Remove candidates whose score <= cutoff.  Both parallel arrays are
/// compacted in-place.  Uses SIMD comparison on contiguous `cand_scores`
/// for branch-free 4-/8-wide filtering.
fn filter_competitive(
    cand_docs: &mut Vec<u32>,
    cand_scores: &mut Vec<f32>,
    cutoff: f32,
) {
    debug_assert_eq!(cand_docs.len(), cand_scores.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse2") {
            unsafe { filter_competitive_sse2(cand_docs, cand_scores, cutoff) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe { filter_competitive_neon(cand_docs, cand_scores, cutoff) };
        return;
    }

    #[allow(unreachable_code)]
    filter_competitive_scalar(cand_docs, cand_scores, cutoff);
}

fn filter_competitive_scalar(
    cand_docs: &mut Vec<u32>,
    cand_scores: &mut Vec<f32>,
    cutoff: f32,
) {
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

    for c in 0..chunks {
        let base = c * 4;
        let vs = _mm_loadu_ps(cand_scores.as_ptr().add(base));
        let cmp = _mm_cmpgt_ps(vs, vcutoff);
        let mask = _mm_movemask_ps(cmp) as u32;

        for bit in 0..4u32 {
            if mask & (1 << bit) != 0 {
                *cand_docs.get_unchecked_mut(write) = *cand_docs.get_unchecked(base + bit as usize);
                *cand_scores.get_unchecked_mut(write) = *cand_scores.get_unchecked(base + bit as usize);
                write += 1;
            }
        }
    }

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

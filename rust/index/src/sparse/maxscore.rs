use std::{collections::{BinaryHeap, HashMap}, sync::Arc};

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
pub use chroma_types::SparsePostingBlock;
use chroma_types::SignedRoaringBitmap;
use dashmap::DashMap;
use thiserror::Error;
use uuid::Uuid;

use crate::sparse::types::encode_u32;

const DEFAULT_BLOCK_SIZE: u32 = 256;
const DIRECTORY_KEY: u32 = u32::MAX;

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

    pub async fn commit(self) -> Result<BlockSparseFlusher, BlockSparseError> {
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
            let directory = SparsePostingBlock::from_directory(&dir_max_offsets, &dir_max_weights);
            self.posting_writer
                .set(encoded_dim, DIRECTORY_KEY, directory)
                .await?;
        }

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
    Eager {
        blocks: Vec<SparsePostingBlock>,
    },
}

pub struct PostingCursor<'view> {
    source: CursorSource<'view>,
    dir_max_offsets: Vec<u32>,
    dir_max_weights: Vec<f32>,
    dim_max: f32,
    block_count: usize,
    // Forward scan state
    block_idx: usize,
    pos: usize,
    // Reusable decompression buffers (View mode)
    offset_buf: Vec<u32>,
    value_buf: Vec<f32>,
    buf_block_idx: usize,
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
            lookup_offset_buf: Vec::new(),
            lookup_buf_block_idx: usize::MAX,
        }
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
                SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut self.offset_buf);
                SparsePostingBlock::decompress_values_into(raw, &hdr, &mut self.value_buf);
                self.buf_block_idx = idx;
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
            CursorSource::Eager { .. } => true,
        }
    }

    fn forward_offsets(&self) -> &[u32] {
        match &self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].offsets(),
            CursorSource::View { .. } => &self.offset_buf,
        }
    }

    fn forward_values(&self) -> &[f32] {
        match &self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].values(),
            CursorSource::View { .. } => &self.value_buf,
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
                CursorSource::View { .. } => (&self.offset_buf[..], &self.value_buf[..]),
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

        // View path: decompress offsets only, read single value on hit
        self.ensure_lookup_offsets(bi);
        let offsets = &self.lookup_offset_buf;
        if offsets.is_empty() || doc_id < offsets[0] {
            return None;
        }
        match offsets.binary_search(&doc_id) {
            Ok(idx) => {
                let CursorSource::View { raw_blocks } = &self.source else {
                    unreachable!()
                };
                let raw = raw_blocks[bi];
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
            CursorSource::View { .. } => self.offset_buf.len(),
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

    /// BlockMaxMaxScore query algorithm.
    ///
    /// `PostingCursor::open()` is async (builds the PrefixView), but
    /// `advance()` and `get_value()` are fully synchronous — the inner
    /// loop does zero async I/O.
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

        // Pre-warm the blockfile cache for all prefix blocks.
        let prefixes: Vec<String> = collected
            .iter()
            .map(|(dim, _)| encode_u32(*dim))
            .collect();
        self.posting_reader
            .load_blocks_for_prefixes(prefixes.iter().map(|s| s.as_str()))
            .await;

        // Build cursors — open() is async (builds PrefixView), the rest is sync.
        let mut terms: Vec<TermState<'me>> = Vec::new();
        for (dim_id, query_weight) in collected {
            let encoded = encode_u32(dim_id);
            let Some(mut cursor) =
                PostingCursor::open(&self.posting_reader, encoded).await?
            else {
                continue;
            };
            cursor.advance(0, &mask);
            let max_score = query_weight * cursor.dimension_max();
            terms.push(TermState {
                cursor,
                query_weight,
                max_score,
            });
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

        // Per-window accumulator: doc_id → partial score
        let mut accum: HashMap<u32, f32> = HashMap::with_capacity(1024);
        let mut window_start = 0u32;

        loop {
            // Window end = min of essential terms' current block max_offsets
            // (only for terms that still have entries)
            let window_end = terms[essential_idx..]
                .iter()
                .filter(|t| t.cursor.current().is_some())
                .filter_map(|t| t.cursor.current_block_end())
                .min();

            let Some(window_end) = window_end else {
                break;
            };

            accum.clear();

            // ── Phase 1: essential terms populate accumulator ──
            for term in terms[essential_idx..].iter_mut() {
                while let Some((doc, val)) = term.cursor.current() {
                    if doc > window_end {
                        break;
                    }
                    if passes_mask(doc, &mask) {
                        *accum.entry(doc).or_insert(0.0) += term.query_weight * val;
                    }
                    term.cursor.next();
                }
            }

            // ── Phase 2: non-essential terms merge-join with accumulator ──
            if !accum.is_empty() {
                for i in 0..essential_idx {
                    if terms[i].cursor.block_upper_bound(window_start) == 0.0 {
                        continue;
                    }

                    // Position cursor at window_start (block-level skip)
                    if terms[i]
                        .cursor
                        .current()
                        .is_some_and(|(d, _)| d < window_start)
                    {
                        terms[i].cursor.advance(window_start, &full_mask);
                    }

                    while let Some((doc, val)) = terms[i].cursor.current() {
                        if doc > window_end {
                            break;
                        }
                        if let Some(score) = accum.get_mut(&doc) {
                            *score += terms[i].query_weight * val;
                        }
                        terms[i].cursor.next();
                    }
                }
            }

            // ── Phase 3: extract candidates from accumulator ──
            for (&doc, &score) in &accum {
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

            // Re-partition essential vs non-essential based on new threshold
            while essential_idx < terms.len() {
                if upper_bounds[essential_idx] >= threshold {
                    break;
                }
                essential_idx += 1;
            }

            window_start = window_end.saturating_add(1);
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

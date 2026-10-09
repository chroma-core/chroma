use chroma_blockstore::BlockfileReader;
use chroma_types::{SignedRoaringBitmap, SparsePostingBlock};
use half::f16;

enum CursorSource<'view> {
    /// All blocks are fully deserialized in memory.
    Eager { blocks: Vec<SparsePostingBlock> },
    /// Raw serialized byte slices borrowed from the Arrow block cache.
    /// Decompressed on-the-fly into reusable buffers.
    View { raw_blocks: Vec<&'view [u8]> },
    /// Similar to View but blocks start as `None` and are populated via
    /// `populate_from_cache`.
    Lazy {
        raw_blocks: Vec<Option<&'view [u8]>>,
    },
}

/// A cursor over a single dimension's posting list. Three backing modes
/// (`Eager`, `View`, `Lazy`) share the same traversal/accumulation logic
/// but differ in how they obtain raw block data.
///
/// View and Lazy cursors use fused f16 weight reads in hot paths
/// (`drain_essential`, `score_candidates`) — only offsets are
/// decompressed, and individual f16 weights are converted to f32 on
/// demand, avoiding wasteful bulk conversion for entries outside the
/// window or excluded by the mask.
pub struct PostingCursor<'view> {
    source: CursorSource<'view>,
    pub(crate) dir_max_offsets: Vec<u32>,
    pub(crate) dir_max_weights: Vec<f32>,
    dim_max: f32,
    block_count: usize,
    block_idx: usize,
    pos: usize,
    // Forward scan buffers (offsets + values, fully decompressed)
    offset_buf: Vec<u32>,
    value_buf: Vec<f32>,
    buf_block_idx: usize,
    // Drain/score offset-only buffer tracking. drain_essential and
    // score_candidates decompress offsets into offset_buf but read raw
    // f16 weights directly from the block bytes. When this index
    // matches the current block, offset_buf already contains offsets
    // but value_buf is stale — buf_block_idx is set to usize::MAX.
    drain_buf_block_idx: usize,
    // Point-lookup buffer (offsets only, separate from forward scan)
    lookup_offset_buf: Vec<u32>,
    lookup_buf_block_idx: usize,
}

impl<'view> PostingCursor<'view> {
    // ── Constructors ────────────────────────────────────────────────

    fn new_with_source(
        source: CursorSource<'view>,
        dir_max_offsets: Vec<u32>,
        dir_max_weights: Vec<f32>,
    ) -> Self {
        let dim_max = dir_max_weights.iter().copied().fold(0.0f32, f32::max);
        let block_count = dir_max_offsets.len();
        PostingCursor {
            source,
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

    /// Create an eager cursor from fully deserialized posting blocks.
    pub fn from_blocks(blocks: Vec<SparsePostingBlock>) -> Self {
        let dir_max_offsets: Vec<u32> = blocks.iter().map(|b| b.header.max_offset).collect();
        let dir_max_weights: Vec<f32> = blocks.iter().map(|b| b.header.max_weight).collect();
        Self::new_with_source(
            CursorSource::Eager { blocks },
            dir_max_offsets,
            dir_max_weights,
        )
    }

    /// Create a view cursor from raw serialized byte slices already in
    /// the Arrow block cache, plus the pre-loaded directory metadata.
    pub fn open(
        raw_blocks: Vec<&'view [u8]>,
        dir_max_offsets: Vec<u32>,
        dir_max_weights: Vec<f32>,
    ) -> Self {
        debug_assert_eq!(raw_blocks.len(), dir_max_offsets.len());
        debug_assert_eq!(raw_blocks.len(), dir_max_weights.len());
        Self::new_with_source(
            CursorSource::View { raw_blocks },
            dir_max_offsets,
            dir_max_weights,
        )
    }

    /// Create a lazy cursor with unloaded data blocks. The directory
    /// metadata must have been pre-loaded. Blocks start as `None` and
    /// are populated via [`populate_from_cache`].
    pub fn open_lazy(dir_max_offsets: Vec<u32>, dir_max_weights: Vec<f32>) -> Self {
        debug_assert_eq!(dir_max_offsets.len(), dir_max_weights.len());
        let block_count = dir_max_offsets.len();
        Self::new_with_source(
            CursorSource::Lazy {
                raw_blocks: vec![None; block_count],
            },
            dir_max_offsets,
            dir_max_weights,
        )
    }

    // ── Lazy population ──────────────────────────────────────────────

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

    /// Populate specific lazy blocks from the reader's cache. Returns
    /// the number of blocks successfully loaded.
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
            if let Some(bytes) = reader.get_raw_from_cache(encoded_dim, idx as u32) {
                raw_blocks[idx] = Some(bytes);
                loaded += 1;
            }
        }
        loaded
    }

    /// Append indices of unloaded blocks overlapping [window_start, window_end]
    /// to `out`. Walks forward from the current scan position; blocks before
    /// the cursor's current block are not revisited (they were either already
    /// processed or intentionally skipped).
    pub fn collect_overlapping_blocks(
        &self,
        window_start: u32,
        window_end: u32,
        out: &mut Vec<usize>,
    ) {
        if !self.is_lazy() {
            return;
        }
        for bi in self.block_idx..self.block_count {
            if self.is_block_loaded(bi) {
                continue;
            }
            let block_start = if bi == 0 {
                0
            } else {
                self.dir_max_offsets[bi - 1] + 1
            };
            if block_start > window_end {
                break;
            }
            if self.dir_max_offsets[bi] < window_start {
                continue;
            }
            out.push(bi);
        }
    }

    // ── Internal: buffer management ──────────────────────────────────

    /// Get the raw serialized bytes for a block by index. Returns `None`
    /// for Eager sources (caller handles those separately) or for Lazy
    /// blocks that haven't been populated yet.
    fn get_raw_block(&self, idx: usize) -> Option<&'view [u8]> {
        match &self.source {
            CursorSource::Eager { .. } => None,
            CursorSource::View { raw_blocks } => Some(raw_blocks[idx]),
            CursorSource::Lazy { raw_blocks } => raw_blocks[idx],
        }
    }

    /// Ensure `offset_buf` and `value_buf` contain the fully decompressed
    /// data for block `idx`. If drain already decompressed offsets for
    /// this block, only values are decompressed. Returns `false` if the
    /// block is unavailable (lazy not populated).
    fn ensure_forward_block(&mut self, idx: usize) -> bool {
        if self.buf_block_idx == idx {
            return true;
        }
        if matches!(self.source, CursorSource::Eager { .. }) {
            return true;
        }
        let Some(raw) = self.get_raw_block(idx) else {
            return false;
        };
        let hdr = SparsePostingBlock::peek_header(raw).expect("valid block header");
        if self.drain_buf_block_idx != idx {
            SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut self.offset_buf);
        }
        SparsePostingBlock::decompress_values_into(raw, &hdr, &mut self.value_buf);
        self.buf_block_idx = idx;
        self.drain_buf_block_idx = idx;
        true
    }

    /// Ensure `lookup_offset_buf` contains offsets for block `idx`.
    /// Only decompresses offsets — values are read on demand via
    /// `SparsePostingBlock::read_value_at`.
    fn ensure_lookup_offsets(&mut self, idx: usize) -> bool {
        if self.lookup_buf_block_idx == idx {
            return true;
        }
        if matches!(self.source, CursorSource::Eager { .. }) {
            return true;
        }
        let Some(raw) = self.get_raw_block(idx) else {
            return false;
        };
        let hdr = SparsePostingBlock::peek_header(raw).expect("valid block header");
        SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut self.lookup_offset_buf);
        self.lookup_buf_block_idx = idx;
        true
    }

    // ── Public API ──────────────────────────────────────────────────

    pub fn block_count(&self) -> usize {
        self.block_count
    }

    /// Returns `None` when the cursor is exhausted or positioned at an
    /// unloaded lazy block (intentional — lazy cursors skip such blocks).
    pub fn current(&mut self) -> Option<(u32, f32)> {
        if self.block_idx >= self.block_count {
            return None;
        }
        if !self.ensure_forward_block(self.block_idx) {
            return None;
        }
        let (offsets, values) = match &mut self.source {
            CursorSource::Eager { blocks } => blocks[self.block_idx].decode(),
            CursorSource::View { .. } | CursorSource::Lazy { .. } => {
                (&self.offset_buf[..], &self.value_buf[..])
            }
        };
        if self.pos < offsets.len() {
            Some((offsets[self.pos], values[self.pos]))
        } else {
            None
        }
    }

    pub fn advance(&mut self, target: u32, mask: &SignedRoaringBitmap) -> Option<(u32, f32)> {
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

            let (offsets, values) = match &mut self.source {
                CursorSource::Eager { blocks } => blocks[self.block_idx].decode(),
                CursorSource::View { .. } | CursorSource::Lazy { .. } => {
                    (&self.offset_buf[..], &self.value_buf[..])
                }
            };

            if self.pos == 0 || offsets.get(self.pos).is_some_and(|&o| o < target) {
                let start = self.pos;
                self.pos = start + offsets[start..].partition_point(|&o| o < target);
            }

            while self.pos < offsets.len() {
                let off = offsets[self.pos];
                if mask.contains(off) {
                    return Some((off, values[self.pos]));
                }
                self.pos += 1;
            }

            self.block_idx += 1;
            self.pos = 0;
        }
        None
    }

    /// Point lookup for a single doc_id. Currently used only in tests.
    ///
    /// Fast path: reuses the forward buffer if the target block is already
    /// loaded there. View/Lazy slow path: decompresses only offsets, then
    /// reads a single f16 weight on hit via `read_value_at`.
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

        if let CursorSource::Eager { blocks } = &mut self.source {
            let (offsets, values) = blocks[bi].decode();
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
                let hdr = SparsePostingBlock::peek_header(raw).expect("valid block header");
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

    /// Return the MAX block-level weight across all blocks overlapping
    /// [window_start, window_end].
    pub fn window_upper_bound(&self, window_start: u32, window_end: u32) -> f32 {
        let bi_start = self
            .dir_max_offsets
            .partition_point(|&max| max < window_start);
        let mut max_w = 0.0f32;
        for bi in bi_start..self.block_count {
            max_w = max_w.max(self.dir_max_weights[bi]);
            if self.dir_max_offsets[bi] >= window_end {
                break;
            }
        }
        max_w
    }

    /// O(1) sequential advance to the next entry. Handles block
    /// transitions and eagerly decompresses the next block for pipeline
    /// locality.
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

    pub fn current_block_end(&self) -> Option<u32> {
        self.dir_max_offsets.get(self.block_idx).copied()
    }

    /// Batch-drain every posting entry in `[window_start, window_end]`
    /// from this *essential* term into a shared window accumulator.
    ///
    /// # How it fits into the query pipeline
    ///
    /// The BlockMaxMaxScore algorithm partitions query terms into
    /// *essential* (high-contribution) and *non-essential* (low) for each
    /// doc-id window. Essential terms are drained first so that every
    /// candidate document gets at least a partial score. Non-essential
    /// terms are then scored only against those candidates via
    /// [`score_candidates`].
    ///
    /// # Fused f16 reads (View/Lazy)
    ///
    /// For View and Lazy sources, only offsets are decompressed into
    /// `offset_buf`. Weights are read as raw f16 bytes per-entry and
    /// converted to f32 on the fly, avoiding bulk f16→f32 conversion
    /// for entries outside the window or excluded by the mask.
    ///
    /// # Arguments
    ///
    /// * `accum` — flat `f32` array of length ≥ `WINDOW_WIDTH`. Slot
    ///   `accum[(doc - window_start)]` accumulates the running dot-product
    ///   contribution for that document across all essential terms.
    /// * `bitmap` — parallel `u64` bit-array. Bit `(doc - window_start)`
    ///   is set for every document touched, enabling the caller to
    ///   enumerate candidates without scanning the full window.
    /// * `mask` — include/exclude filter applied per document.
    ///
    /// # Cursor state
    ///
    /// On return the cursor is positioned just past `window_end` (or
    /// exhausted). The caller advances `window_start` and calls this
    /// method again for the next window — the cursor resumes where it
    /// left off.
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

            match &mut self.source {
                CursorSource::Eager { blocks } => {
                    let (offsets, vals) = blocks[self.block_idx].decode();

                    if offsets.get(self.pos).is_some_and(|&o| o < window_start) {
                        self.pos = offsets.partition_point(|&o| o < window_start);
                    }
                    while self.pos < offsets.len() {
                        let doc = offsets[self.pos];
                        if doc > window_end {
                            return;
                        }
                        if mask.contains(doc) {
                            let idx = (doc - window_start) as usize;
                            bitmap[idx >> 6] |= 1u64 << (idx & 63);
                            accum[idx] += vals[self.pos] * query_weight;
                        }
                        self.pos += 1;
                    }
                }
                _ => {
                    let Some(raw) = self.get_raw_block(self.block_idx) else {
                        self.block_idx += 1;
                        self.pos = 0;
                        continue;
                    };
                    let hdr = SparsePostingBlock::peek_header(raw).expect("valid block header");
                    if self.drain_buf_block_idx != self.block_idx {
                        SparsePostingBlock::decompress_offsets_into(
                            raw,
                            &hdr,
                            &mut self.offset_buf,
                        );
                        self.drain_buf_block_idx = self.block_idx;
                        self.buf_block_idx = usize::MAX;
                    }
                    let wb = SparsePostingBlock::raw_weight_bytes(raw, &hdr);

                    if self
                        .offset_buf
                        .get(self.pos)
                        .is_some_and(|&o| o < window_start)
                    {
                        self.pos = self.offset_buf.partition_point(|&o| o < window_start);
                    }
                    while self.pos < self.offset_buf.len() {
                        let doc = self.offset_buf[self.pos];
                        if doc > window_end {
                            return;
                        }
                        if mask.contains(doc) {
                            let idx = (doc - window_start) as usize;
                            bitmap[idx >> 6] |= 1u64 << (idx & 63);
                            let bp = self.pos * 2;
                            let w = f16::from_le_bytes([wb[bp], wb[bp + 1]]).to_f32();
                            accum[idx] += w * query_weight;
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
    /// accumulating matched scores into `cand_scores`.
    ///
    /// For View/Lazy sources, this is fused: only offsets are decompressed
    /// and f16 weights are read from raw bytes at matched positions only.
    pub fn score_candidates(
        &mut self,
        window_start: u32,
        window_end: u32,
        query_weight: f32,
        cand_docs: &[u32],
        cand_scores: &mut [f32],
    ) {
        // No candidates survived the essential-term drain for this window,
        // so there is nothing to merge-join against.
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

            match &mut self.source {
                CursorSource::Eager { blocks } => {
                    let (offsets, values) = blocks[self.block_idx].decode();

                    if offsets.get(self.pos).is_some_and(|&o| o < window_start) {
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
                _ => {
                    let Some(raw) = self.get_raw_block(self.block_idx) else {
                        self.block_idx += 1;
                        self.pos = 0;
                        continue;
                    };
                    let hdr = SparsePostingBlock::peek_header(raw).expect("valid block header");
                    if self.drain_buf_block_idx != self.block_idx {
                        SparsePostingBlock::decompress_offsets_into(
                            raw,
                            &hdr,
                            &mut self.offset_buf,
                        );
                        self.drain_buf_block_idx = self.block_idx;
                        self.buf_block_idx = usize::MAX;
                    }
                    let wb = SparsePostingBlock::raw_weight_bytes(raw, &hdr);

                    if self
                        .offset_buf
                        .get(self.pos)
                        .is_some_and(|&o| o < window_start)
                    {
                        self.pos = self.offset_buf.partition_point(|&o| o < window_start);
                    }

                    while self.pos < self.offset_buf.len() && ci < cand_docs.len() {
                        let doc = self.offset_buf[self.pos];
                        if doc > window_end {
                            return;
                        }
                        let cand = cand_docs[ci];
                        if doc < cand {
                            self.pos += 1;
                        } else if doc > cand {
                            ci += 1;
                        } else {
                            let bp = self.pos * 2;
                            let w = f16::from_le_bytes([wb[bp], wb[bp + 1]]).to_f32();
                            cand_scores[ci] += query_weight * w;
                            self.pos += 1;
                            ci += 1;
                        }
                    }
                    if self.pos >= self.offset_buf.len() {
                        self.block_idx += 1;
                        self.pos = 0;
                    }
                }
            }
        }
    }
}

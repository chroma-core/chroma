use bitpacking::{BitPacker, BitPacker4x};
use half::f16;
use std::fmt;
use std::sync::OnceLock;

const BITPACK_GROUP_SIZE: usize = BitPacker4x::BLOCK_LEN; // 128

/// Sentinel value in `bits_per_delta` that marks a directory block (see
/// [`DirectoryBlock`] below).
const DIRECTORY_SENTINEL: u8 = 0xFF;

/// Maximum number of posting entries per block. Chosen so that the
/// decompressed block fits comfortably in L1 cache:
///   4096 * (4 bytes offset + 4 bytes f32 value) = 32 KB.
pub const MAX_BLOCK_ENTRIES: usize = 4096;

const HEADER_SIZE: usize = 16;

// ── Error type ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum SparsePostingBlockError {
    EmptyEntries,
    TooManyEntries { count: usize },
    MismatchedLengths { offsets: usize, weights: usize },
    TruncatedHeader { len: usize },
    TruncatedBody { expected: usize, actual: usize },
}

impl fmt::Display for SparsePostingBlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyEntries => write!(f, "block must have at least one entry"),
            Self::TooManyEntries { count } => {
                write!(f, "block has {count} entries, max is {MAX_BLOCK_ENTRIES}")
            }
            Self::MismatchedLengths { offsets, weights } => {
                write!(
                    f,
                    "directory: max_offsets len ({offsets}) != max_weights len ({weights})"
                )
            }
            Self::TruncatedHeader { len } => {
                write!(f, "expected at least {HEADER_SIZE} header bytes, got {len}")
            }
            Self::TruncatedBody { expected, actual } => {
                write!(f, "expected {expected} body bytes, got {actual}")
            }
        }
    }
}

impl std::error::Error for SparsePostingBlockError {}

/// Header read from the first 16 bytes of a serialized block.
///
/// # Terminology: "offset"
///
/// Throughout this module, "offset" means a document's segment offset ID —
/// its u32 position within a compacted segment. Posting lists are sorted
/// by offset so that block-max pruning can iterate documents in order and
/// merge across dimensions.
///
/// # Layout (little-endian, 16 bytes total)
///
/// ```text
/// [0..2]   num_entries   : u16  — number of (offset, weight) pairs
/// [2]      bits_per_delta: u8   — bits per bitpacked delta (0xFF = directory)
/// [3]      _reserved     : u8   — reserved for future format versioning
/// [4..8]   min_offset    : u32  — smallest doc offset in this block
/// [8..12]  max_offset    : u32  — largest doc offset in this block
/// [12..16] max_weight    : f32  — largest weight in this block (for pruning)
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PostingBlockHeader {
    /// Number of (offset, weight) pairs in the block.
    pub num_entries: u16,
    /// Bits per delta for bitpacked offset decompression.
    /// Set to `0xFF` for directory blocks (different body layout).
    pub bits_per_delta: u8,
    /// Smallest document offset id in this block.
    pub min_offset: u32,
    /// Largest document offset id in this block.
    pub max_offset: u32,
    /// Largest weight in this block. Used by block-max pruning to skip
    /// entire blocks whose max contribution cannot beat the threshold.
    pub max_weight: f32,
}

impl PostingBlockHeader {
    pub fn is_directory(&self) -> bool {
        self.bits_per_delta == DIRECTORY_SENTINEL
    }
}

#[derive(Debug, Clone)]
struct Decompressed {
    offsets: Vec<u32>,
    values: Vec<f32>,
}

/// A compressed block of posting list entries for sparse vector search.
///
/// # On-disk format
///
/// ```text
/// ┌────────────────────────────── 16-byte header ─────────────────────────────┐
/// │ num_entries(u16) │ bits_per_delta(u8) │ reserved(u8) │ min_offset(u32) │ │
/// │ max_offset(u32)  │ max_weight(f32)                                      │
/// └──────────────────────────────────────────────────────────────────────────┘
/// ┌──── body ────────────────────────────────────────────────────────────────┐
/// │ bitpacked delta-encoded doc offsets (BitPacker4x, groups of 128)        │
/// │ remainder offsets as raw little-endian u32 (if num_entries % 128 != 0)  │
/// │ f16 little-endian weights (2 bytes per entry)                           │
/// └──────────────────────────────────────────────────────────────────────────┘
/// ```
///
/// # Dual access modes
///
/// This type supports two access patterns used by different cursor modes in
/// the query pipeline:
///
/// - **Materialized** (`offsets()`, `values()`): Deserializes the full block
///   into owned `Vec`s on first access via `OnceLock`. Used by *eager cursors*
///   for small dimensions where block reuse amortizes the cost.
///
/// - **Zero-copy** (`peek_header`, `decompress_offsets_into`, `read_value_at`,
///   `raw_weight_bytes`): Static methods that operate directly on a `&[u8]`
///   slice (e.g. from an Arrow block cache) without constructing a
///   `SparsePostingBlock`. Used by *lazy/view cursors* for large dimensions
///   where we only touch a fraction of each block's entries.
///
/// # Relationship between struct fields and `raw_body`
///
/// The struct fields (`min_offset`, `max_offset`, etc.) mirror the 16-byte
/// header. `raw_body` stores the *body* bytes that follow the header (byte 16
/// onward). There is no duplication: `serialize()` reconstructs the full blob
/// by writing the header from struct fields then appending `raw_body`.
#[derive(Debug, Clone)]
pub struct SparsePostingBlock {
    pub min_offset: u32,
    pub max_offset: u32,
    pub max_weight: f32,
    num_entries: u16,
    bits_per_delta: u8,
    /// Compressed body bytes (everything after the 16-byte header).
    /// Empty for blocks created via `from_sorted_entries` (data lives in
    /// `decompressed` instead, serialized on demand).
    raw_body: Vec<u8>,
    decompressed: OnceLock<Decompressed>,
}

impl SparsePostingBlock {
    /// Build a block from pre-sorted `(offset, value)` pairs.
    pub fn from_sorted_entries(entries: &[(u32, f32)]) -> Result<Self, SparsePostingBlockError> {
        if entries.is_empty() {
            return Err(SparsePostingBlockError::EmptyEntries);
        }
        if entries.len() > MAX_BLOCK_ENTRIES {
            return Err(SparsePostingBlockError::TooManyEntries {
                count: entries.len(),
            });
        }

        let n = entries.len();
        debug_assert!(
            entries.windows(2).all(|w| w[0].0 <= w[1].0),
            "from_sorted_entries: offsets must be monotonically non-decreasing"
        );
        let min_offset = entries[0].0;
        let max_offset = entries[n - 1].0;
        let max_weight = entries
            .iter()
            .map(|(_, v)| *v)
            .fold(0.0f32, f32::max)
            .max(f32::MIN_POSITIVE);

        let offsets: Vec<u32> = entries.iter().map(|(o, _)| *o).collect();
        let values: Vec<f32> = entries.iter().map(|(_, v)| *v).collect();

        let packer = BitPacker4x::new();
        let relative: Vec<u32> = offsets.iter().map(|&o| o - min_offset).collect();
        let full_groups = n / BITPACK_GROUP_SIZE;
        let mut max_bits = 0u8;
        for g in 0..full_groups {
            let start = g * BITPACK_GROUP_SIZE;
            let initial = if g == 0 { 0 } else { relative[start - 1] };
            max_bits = max_bits
                .max(packer.num_bits_sorted(initial, &relative[start..start + BITPACK_GROUP_SIZE]));
        }

        Ok(SparsePostingBlock {
            min_offset,
            max_offset,
            max_weight,
            num_entries: n as u16,
            bits_per_delta: max_bits,
            raw_body: Vec::new(),
            decompressed: OnceLock::from(Decompressed { offsets, values }),
        })
    }

    pub fn len(&self) -> usize {
        self.num_entries as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Decompressed doc offsets (materializes on first call).
    /// Returns `&[]` for directory blocks.
    pub fn offsets(&self) -> &[u32] {
        if self.is_directory() {
            return &[];
        }
        &self.ensure_decompressed().offsets
    }

    /// Decompressed f32 weights (materializes on first call).
    /// Returns `&[]` for directory blocks.
    pub fn values(&self) -> &[f32] {
        if self.is_directory() {
            return &[];
        }
        &self.ensure_decompressed().values
    }

    fn ensure_decompressed(&self) -> &Decompressed {
        self.decompressed.get_or_init(|| {
            Self::decompress_raw(
                &self.raw_body,
                self.num_entries as usize,
                self.bits_per_delta,
                self.min_offset,
            )
        })
    }

    fn decompress_raw(
        raw_body: &[u8],
        num_entries: usize,
        bits_per_delta: u8,
        min_offset: u32,
    ) -> Decompressed {
        let packer = BitPacker4x::new();
        let full_groups = num_entries / BITPACK_GROUP_SIZE;
        let remainder = num_entries % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (bits_per_delta as usize) / 8;

        let mut offsets = Vec::with_capacity(num_entries);
        let mut pos = 0;
        let mut initial = 0u32;

        for _ in 0..full_groups {
            let end = pos + packed_group_bytes;
            let mut group = vec![0u32; BITPACK_GROUP_SIZE];
            packer.decompress_sorted(initial, &raw_body[pos..end], &mut group, bits_per_delta);
            initial = group[BITPACK_GROUP_SIZE - 1];
            for v in &mut group {
                *v += min_offset;
            }
            offsets.extend_from_slice(&group);
            pos = end;
        }

        for _ in 0..remainder {
            let d = u32::from_le_bytes([
                raw_body[pos],
                raw_body[pos + 1],
                raw_body[pos + 2],
                raw_body[pos + 3],
            ]);
            offsets.push(d + min_offset);
            pos += 4;
        }

        let f16_bytes = &raw_body[pos..pos + num_entries * 2];
        let values: Vec<f32> = f16_bytes
            .chunks_exact(2)
            .map(|b| f16::from_le_bytes([b[0], b[1]]).to_f32())
            .collect();

        Decompressed { offsets, values }
    }

    // ── Serialization ───────────────────────────────────────────────

    /// Serialize to bytes: 16-byte header + bitpacked deltas + f16 weights.
    pub fn serialize(&self) -> Vec<u8> {
        if !self.raw_body.is_empty() {
            let mut buf = Vec::with_capacity(HEADER_SIZE + self.raw_body.len());
            self.write_header(&mut buf);
            buf.extend_from_slice(&self.raw_body);
            return buf;
        }

        let data = self.ensure_decompressed();
        let n = data.offsets.len();
        let packer = BitPacker4x::new();
        let relative: Vec<u32> = data.offsets.iter().map(|&o| o - self.min_offset).collect();

        let full_groups = n / BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (self.bits_per_delta as usize) / 8;

        let mut buf = Vec::with_capacity(self.serialized_size());
        self.write_header(&mut buf);

        for g in 0..full_groups {
            let start = g * BITPACK_GROUP_SIZE;
            let initial = if g == 0 { 0 } else { relative[start - 1] };
            let mut packed = vec![0u8; packed_group_bytes];
            packer.compress_sorted(
                initial,
                &relative[start..start + BITPACK_GROUP_SIZE],
                &mut packed,
                self.bits_per_delta,
            );
            buf.extend_from_slice(&packed);
        }

        let rem_start = full_groups * BITPACK_GROUP_SIZE;
        for &d in &relative[rem_start..] {
            buf.extend_from_slice(&d.to_le_bytes());
        }

        for &v in &data.values {
            buf.extend_from_slice(&f16::from_f32(v).to_le_bytes());
        }

        buf
    }

    /// Byte length of the serialized representation (computable without
    /// decompression).
    pub fn serialized_size(&self) -> usize {
        let n = self.num_entries as usize;
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (self.bits_per_delta as usize) / 8;
        HEADER_SIZE + full_groups * packed_group_bytes + remainder * 4 + n * 2
    }

    /// Deserialize from bytes. Only reads the 16-byte header; body
    /// decompression is lazy.
    ///
    /// Returns an error if the buffer is too small for the header or the
    /// body is shorter than the header implies.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, SparsePostingBlockError> {
        if bytes.len() < HEADER_SIZE {
            return Err(SparsePostingBlockError::TruncatedHeader { len: bytes.len() });
        }

        let num_entries = u16::from_le_bytes([bytes[0], bytes[1]]);
        let bits_per_delta = bytes[2];
        // bytes[3] is reserved
        let min_offset = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let max_offset = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let max_weight = f32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);

        let expected_body = Self::expected_body_size(num_entries as usize, bits_per_delta);
        let actual_body = bytes.len() - HEADER_SIZE;
        if actual_body < expected_body {
            return Err(SparsePostingBlockError::TruncatedBody {
                expected: expected_body,
                actual: actual_body,
            });
        }

        Ok(SparsePostingBlock {
            min_offset,
            max_offset,
            max_weight,
            num_entries,
            bits_per_delta,
            raw_body: bytes[HEADER_SIZE..HEADER_SIZE + expected_body].to_vec(),
            decompressed: OnceLock::new(),
        })
    }

    /// Compute the expected body size (bytes after the header) from header fields.
    fn expected_body_size(num_entries: usize, bits_per_delta: u8) -> usize {
        if bits_per_delta == DIRECTORY_SENTINEL {
            num_entries * 8
        } else {
            let full_groups = num_entries / BITPACK_GROUP_SIZE;
            let remainder = num_entries % BITPACK_GROUP_SIZE;
            let packed_group_bytes = BITPACK_GROUP_SIZE * (bits_per_delta as usize) / 8;
            full_groups * packed_group_bytes + remainder * 4 + num_entries * 2
        }
    }

    fn write_header(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.num_entries.to_le_bytes());
        buf.push(self.bits_per_delta);
        buf.push(0); // reserved — available for format versioning if needed
        buf.extend_from_slice(&self.min_offset.to_le_bytes());
        buf.extend_from_slice(&self.max_offset.to_le_bytes());
        buf.extend_from_slice(&self.max_weight.to_le_bytes());
    }

    // ── Zero-copy access from raw serialized bytes ──────────────────
    //
    // These static methods operate on a raw `&[u8]` (e.g. a pointer into
    // an Arrow block cache) without constructing a `SparsePostingBlock`.
    // They are the hot path for lazy/view cursors in the query pipeline.

    /// Read the 16-byte header without heap allocation.
    pub fn peek_header(bytes: &[u8]) -> Result<PostingBlockHeader, SparsePostingBlockError> {
        if bytes.len() < HEADER_SIZE {
            return Err(SparsePostingBlockError::TruncatedHeader { len: bytes.len() });
        }
        Ok(PostingBlockHeader {
            num_entries: u16::from_le_bytes([bytes[0], bytes[1]]),
            bits_per_delta: bytes[2],
            min_offset: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            max_offset: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            max_weight: f32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        })
    }

    /// Decompress offsets from raw serialized bytes into a reusable buffer.
    /// Must not be called on directory blocks.
    pub fn decompress_offsets_into(bytes: &[u8], hdr: &PostingBlockHeader, buf: &mut Vec<u32>) {
        debug_assert!(
            !hdr.is_directory(),
            "decompress_offsets_into called on directory block"
        );
        let n = hdr.num_entries as usize;
        buf.clear();
        buf.resize(n, 0);

        let raw_body = &bytes[HEADER_SIZE..];
        let packer = BitPacker4x::new();
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (hdr.bits_per_delta as usize) / 8;

        let mut pos = 0;
        let mut write = 0;
        let mut initial = 0u32;

        for _ in 0..full_groups {
            let end = pos + packed_group_bytes;
            let group = &mut buf[write..write + BITPACK_GROUP_SIZE];
            packer.decompress_sorted(initial, &raw_body[pos..end], group, hdr.bits_per_delta);
            initial = group[BITPACK_GROUP_SIZE - 1];
            for v in group.iter_mut() {
                *v += hdr.min_offset;
            }
            write += BITPACK_GROUP_SIZE;
            pos = end;
        }

        for _ in 0..remainder {
            let d = u32::from_le_bytes([
                raw_body[pos],
                raw_body[pos + 1],
                raw_body[pos + 2],
                raw_body[pos + 3],
            ]);
            buf[write] = d + hdr.min_offset;
            write += 1;
            pos += 4;
        }
    }

    /// Zero-copy slice of the raw f16 weight bytes from serialized data.
    /// Each weight is 2 bytes (f16 little-endian). Must not be called on directory blocks.
    pub fn raw_weight_bytes<'a>(bytes: &'a [u8], hdr: &PostingBlockHeader) -> &'a [u8] {
        debug_assert!(
            !hdr.is_directory(),
            "raw_weight_bytes called on directory block"
        );
        let n = hdr.num_entries as usize;
        let w_start = Self::weight_byte_offset(hdr);
        &bytes[w_start..w_start + n * 2]
    }

    /// Read a single f16 weight at `index` and convert to f32. O(1).
    /// Must not be called on directory blocks.
    pub fn read_value_at(bytes: &[u8], hdr: &PostingBlockHeader, index: usize) -> f32 {
        debug_assert!(
            !hdr.is_directory(),
            "read_value_at called on directory block"
        );
        debug_assert!(index < hdr.num_entries as usize);
        let byte_pos = Self::weight_byte_offset(hdr) + index * 2;
        f16::from_le_bytes([bytes[byte_pos], bytes[byte_pos + 1]]).to_f32()
    }

    /// Decompress f16 weights from raw serialized bytes into a reusable
    /// f32 buffer. Must not be called on directory blocks.
    pub fn decompress_values_into(bytes: &[u8], hdr: &PostingBlockHeader, buf: &mut Vec<f32>) {
        debug_assert!(
            !hdr.is_directory(),
            "decompress_values_into called on directory block"
        );
        let n = hdr.num_entries as usize;
        buf.clear();
        buf.resize(n, 0.0);

        let w_start = Self::weight_byte_offset(hdr);
        let f16_bytes = &bytes[w_start..w_start + n * 2];
        convert_f16_to_f32(f16_bytes, buf);
    }

    /// Byte offset of the weight section from the start of the serialized
    /// block (including the 16-byte header).
    fn weight_byte_offset(hdr: &PostingBlockHeader) -> usize {
        let n = hdr.num_entries as usize;
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (hdr.bits_per_delta as usize) / 8;
        HEADER_SIZE + full_groups * packed_group_bytes + remainder * 4
    }

    pub fn is_directory(&self) -> bool {
        self.bits_per_delta == DIRECTORY_SENTINEL
    }

    /// Accumulate `query_weight * weight` for all entries into `scores`.
    pub fn score_block_into(&self, query_weight: f32, scores: &mut [f32]) {
        let vals = self.values();
        debug_assert!(scores.len() >= vals.len());
        for (s, &v) in scores[..vals.len()].iter_mut().zip(vals.iter()) {
            *s += v * query_weight;
        }
    }
}

// ── Directory block ─────────────────────────────────────────────────

/// A metadata block summarizing the posting blocks for a single dimension.
///
/// Stores one `(max_offset, max_weight)` pair per posting block, enabling
/// block-max pruning: the query engine skips entire posting blocks whose
/// `max_weight * query_weight` cannot beat the current top-k threshold.
///
/// Serialized as a [`SparsePostingBlock`] with `bits_per_delta == 0xFF`
/// (the directory sentinel). The body layout is:
///
/// ```text
/// body = [ max_offset: u32 LE, max_weight: f32 LE ] × num_entries
/// ```
///
/// The header's `max_weight` stores the dimension-level maximum weight
/// (max of all per-block max_weights), used for early term pruning.
#[derive(Debug, Clone)]
pub struct DirectoryBlock(SparsePostingBlock);

impl DirectoryBlock {
    /// Create a directory block from per-posting-block metadata.
    ///
    /// - `max_offsets[i]`: largest doc offset in posting block `i`
    /// - `max_weights[i]`: largest weight in posting block `i`
    pub fn new(max_offsets: &[u32], max_weights: &[f32]) -> Result<Self, SparsePostingBlockError> {
        if max_offsets.len() != max_weights.len() {
            return Err(SparsePostingBlockError::MismatchedLengths {
                offsets: max_offsets.len(),
                weights: max_weights.len(),
            });
        }
        let n = max_offsets.len();
        let dim_max = max_weights.iter().copied().fold(0.0f32, f32::max);

        let mut raw_body = Vec::with_capacity(n * 8);
        for i in 0..n {
            raw_body.extend_from_slice(&max_offsets[i].to_le_bytes());
            raw_body.extend_from_slice(&max_weights[i].to_le_bytes());
        }

        Ok(DirectoryBlock(SparsePostingBlock {
            min_offset: max_offsets.first().copied().unwrap_or(0),
            max_offset: max_offsets.last().copied().unwrap_or(0),
            max_weight: dim_max,
            num_entries: n as u16,
            bits_per_delta: DIRECTORY_SENTINEL,
            raw_body,
            decompressed: OnceLock::new(),
        }))
    }

    /// Interpret a `SparsePostingBlock` as a directory block.
    /// Returns `Err` with the original block if it is not a directory.
    pub fn from_block(block: SparsePostingBlock) -> Result<Self, SparsePostingBlock> {
        if block.is_directory() {
            Ok(DirectoryBlock(block))
        } else {
            Err(block)
        }
    }

    /// Dimension-level maximum weight (max of all per-block max_weights).
    pub fn dim_max_weight(&self) -> f32 {
        self.0.max_weight
    }

    /// Number of posting blocks summarized by this directory.
    pub fn num_blocks(&self) -> usize {
        self.0.num_entries as usize
    }

    /// Extract `(max_offsets, max_weights)` — one pair per posting block.
    pub fn entries(&self) -> (Vec<u32>, Vec<f32>) {
        let n = self.0.num_entries as usize;
        let mut max_offsets = Vec::with_capacity(n);
        let mut max_weights = Vec::with_capacity(n);
        for i in 0..n {
            let pos = i * 8;
            max_offsets.push(u32::from_le_bytes([
                self.0.raw_body[pos],
                self.0.raw_body[pos + 1],
                self.0.raw_body[pos + 2],
                self.0.raw_body[pos + 3],
            ]));
            max_weights.push(f32::from_le_bytes([
                self.0.raw_body[pos + 4],
                self.0.raw_body[pos + 5],
                self.0.raw_body[pos + 6],
                self.0.raw_body[pos + 7],
            ]));
        }
        (max_offsets, max_weights)
    }

    /// Consume this directory block into its underlying `SparsePostingBlock`
    /// for storage in the blockstore.
    pub fn into_block(self) -> SparsePostingBlock {
        self.0
    }
}

// ── f16 → f32 bulk conversion (scalar; SIMD added in PR 4) ─────────

pub fn convert_f16_to_f32(f16_bytes: &[u8], out: &mut [f32]) {
    for (o, chunk) in out.iter_mut().zip(f16_bytes.chunks_exact(2)) {
        *o = f16::from_le_bytes([chunk[0], chunk[1]]).to_f32();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F16_TOL: f32 = 1e-3;

    fn make_block(entries: &[(u32, f32)]) -> SparsePostingBlock {
        SparsePostingBlock::from_sorted_entries(entries).expect("make_block: invalid entries")
    }

    fn sequential_entries(start: u32, step: u32, count: usize, weight: f32) -> Vec<(u32, f32)> {
        (0..count)
            .map(|i| (start + step * i as u32, weight))
            .collect()
    }

    fn assert_approx(actual: f32, expected: f32, tol: f32) {
        assert!(
            (actual - expected).abs() <= tol,
            "expected {expected} +/- {tol}, got {actual}"
        );
    }

    fn assert_roundtrip_offsets(entries: &[(u32, f32)]) {
        let block = make_block(entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        assert_eq!(restored.offsets(), block.offsets());
    }

    fn assert_roundtrip_values(entries: &[(u32, f32)]) {
        let block = make_block(entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        for (i, (&orig, &rest)) in block
            .values()
            .iter()
            .zip(restored.values().iter())
            .enumerate()
        {
            assert!(
                (rest - orig).abs() <= F16_TOL,
                "entry {i}: expected {orig} +/- {F16_TOL}, got {rest}"
            );
        }
    }

    #[test]
    fn roundtrip_small() {
        let entries = vec![(0, 1.0), (5, 0.5), (100, 0.8)];
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_256() {
        let entries = sequential_entries(0, 1, 256, 0.7);
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_1_entry() {
        let entries = vec![(42, 0.9)];
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_128_exact_group() {
        let entries = sequential_entries(10, 3, 128, 0.5);
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_129_group_plus_one() {
        let entries = sequential_entries(10, 3, 129, 0.5);
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_255() {
        let entries = sequential_entries(0, 2, 255, 0.6);
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_512() {
        let entries = sequential_entries(0, 1, 512, 0.4);
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn large_deltas() {
        let entries = vec![(0, 0.5), (1_000_000, 0.8), (2_000_000, 0.3)];
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn consecutive_offsets() {
        let entries: Vec<(u32, f32)> = (0..256).map(|i| (i as u32, 0.5)).collect();
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn uniform_weights() {
        let entries: Vec<(u32, f32)> = (0..256).map(|i| (i as u32 * 10, 0.5)).collect();
        let block = make_block(&entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        for &v in restored.values() {
            assert_approx(v, 0.5, F16_TOL);
        }
    }

    #[test]
    fn tiny_weights() {
        let entries = vec![(0, 0.001), (1, 1.0)];
        let block = make_block(&entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        assert_eq!(restored.offsets(), block.offsets());
        assert_approx(restored.values()[1], 1.0, F16_TOL);
        assert!(restored.values()[0] < 0.01);
    }

    #[test]
    fn header_fields() {
        let entries = vec![(10, 0.5), (20, 0.9), (30, 0.2)];
        let block = make_block(&entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        assert_eq!(restored.min_offset, 10);
        assert_eq!(restored.max_offset, 30);
        assert_eq!(restored.max_weight, 0.9);
        assert_eq!(restored.offsets().len(), 3);
    }

    #[test]
    fn peek_header_matches() {
        let entries = sequential_entries(100, 5, 200, 0.42);
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
        assert_eq!(hdr.num_entries, 200);
        assert_eq!(hdr.min_offset, 100);
        assert_eq!(hdr.max_offset, 100 + 5 * 199);
    }

    #[test]
    fn decompress_offsets_into_matches() {
        let entries = sequential_entries(50, 3, 300, 0.5);
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

        let mut buf = Vec::new();
        SparsePostingBlock::decompress_offsets_into(&bytes, &hdr, &mut buf);
        assert_eq!(buf.as_slice(), block.offsets());
    }

    #[test]
    fn decompress_values_into_matches() {
        let entries = sequential_entries(0, 1, 256, 0.7);
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

        let mut buf = Vec::new();
        SparsePostingBlock::decompress_values_into(&bytes, &hdr, &mut buf);
        for (&a, &b) in buf.iter().zip(block.values().iter()) {
            assert_approx(a, b, F16_TOL);
        }
    }

    #[test]
    fn read_value_at_matches() {
        let entries: Vec<(u32, f32)> = (0..64).map(|i| (i * 10, 0.1 + 0.01 * i as f32)).collect();
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

        for i in 0..entries.len() {
            let v = SparsePostingBlock::read_value_at(&bytes, &hdr, i);
            assert_approx(v, block.values()[i], F16_TOL);
        }
    }

    #[test]
    fn raw_weight_bytes_length() {
        let entries = sequential_entries(0, 1, 200, 0.5);
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
        let wb = SparsePostingBlock::raw_weight_bytes(&bytes, &hdr);
        assert_eq!(wb.len(), 200 * 2);
    }

    #[test]
    fn serialized_size_matches_actual() {
        for count in [1, 3, 127, 128, 129, 255, 256, 257, 512, 1024] {
            let entries = sequential_entries(0, 1, count, 0.5);
            let block = make_block(&entries);
            let bytes = block.serialize();
            assert_eq!(
                block.serialized_size(),
                bytes.len(),
                "serialized_size mismatch for count={count}"
            );
        }
    }

    #[test]
    fn directory_block_roundtrip() {
        let max_offsets = vec![100, 500, 1000];
        let max_weights = vec![0.9, 0.7, 0.5];
        let dir = DirectoryBlock::new(&max_offsets, &max_weights).unwrap();
        assert_eq!(dir.dim_max_weight(), 0.9);
        assert_eq!(dir.num_blocks(), 3);

        let block = dir.into_block();
        assert!(block.is_directory());
        let bytes = block.serialize();

        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        assert!(restored.is_directory());
        let dir2 = DirectoryBlock::from_block(restored).unwrap();
        let (offsets, weights) = dir2.entries();
        assert_eq!(offsets, max_offsets);
        assert_eq!(weights, max_weights);
    }

    #[test]
    fn directory_from_block_rejects_posting_block() {
        let entries = vec![(0, 1.0), (5, 0.5)];
        let block = make_block(&entries);
        assert!(!block.is_directory());
        let err = DirectoryBlock::from_block(block).unwrap_err();
        assert!(!err.is_directory());
    }

    #[test]
    fn score_block_into_accumulates() {
        let entries = vec![(0, 0.5), (1, 0.25)];
        let block = make_block(&entries);
        let mut scores = vec![1.0, 2.0];
        block.score_block_into(2.0, &mut scores);
        assert_approx(scores[0], 1.0 + 0.5 * 2.0, F16_TOL);
        assert_approx(scores[1], 2.0 + 0.25 * 2.0, F16_TOL);
    }

    #[test]
    fn deserialize_too_short_returns_err() {
        assert!(SparsePostingBlock::deserialize(&[0u8; 15]).is_err());
        assert!(SparsePostingBlock::deserialize(&[]).is_err());
    }

    #[test]
    fn deserialize_truncated_body_returns_err() {
        let entries = sequential_entries(0, 1, 200, 0.5);
        let block = make_block(&entries);
        let bytes = block.serialize();

        let truncated = &bytes[..bytes.len() - 1];
        let err = SparsePostingBlock::deserialize(truncated).unwrap_err();
        assert!(
            matches!(err, SparsePostingBlockError::TruncatedBody { .. }),
            "expected TruncatedBody, got {err:?}"
        );
    }

    #[test]
    fn deserialize_truncated_directory_body_returns_err() {
        let dir = DirectoryBlock::new(&[10, 20, 30], &[0.5, 0.9, 0.2]).unwrap();
        let bytes = dir.into_block().serialize();

        let truncated = &bytes[..HEADER_SIZE + 3 * 8 - 1];
        let err = SparsePostingBlock::deserialize(truncated).unwrap_err();
        assert!(
            matches!(err, SparsePostingBlockError::TruncatedBody { .. }),
            "expected TruncatedBody, got {err:?}"
        );
    }

    #[test]
    fn deserialize_header_only_data_block_returns_err() {
        let entries = sequential_entries(0, 1, 200, 0.5);
        let block = make_block(&entries);
        let bytes = block.serialize();

        let err = SparsePostingBlock::deserialize(&bytes[..HEADER_SIZE]).unwrap_err();
        assert!(matches!(err, SparsePostingBlockError::TruncatedBody { .. }));
    }

    #[test]
    fn expected_body_size_data_block() {
        for count in [1, 2, 127, 128, 129, 256, 512, 1024, MAX_BLOCK_ENTRIES] {
            let entries = sequential_entries(0, 1, count, 0.5);
            let block = make_block(&entries);
            let bytes = block.serialize();
            assert_eq!(
                SparsePostingBlock::expected_body_size(count, block.bits_per_delta),
                bytes.len() - HEADER_SIZE,
                "body size mismatch for count={count}"
            );
        }
    }

    #[test]
    fn expected_body_size_directory_block() {
        for count in [1, 3, 10, 100] {
            assert_eq!(
                SparsePostingBlock::expected_body_size(count, DIRECTORY_SENTINEL),
                count * 8,
                "directory body size mismatch for count={count}"
            );
        }
    }

    #[test]
    fn deserialize_extra_trailing_bytes_ignored() {
        let entries = sequential_entries(0, 1, 50, 0.5);
        let block = make_block(&entries);
        let mut bytes = block.serialize();
        bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
        assert_eq!(restored.offsets(), block.offsets());
    }

    #[test]
    fn quantization_precision_random() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn cheap_rng(seed: u64, i: usize) -> f32 {
            let mut h = DefaultHasher::new();
            seed.hash(&mut h);
            i.hash(&mut h);
            let bits = h.finish();
            (bits % 1000) as f32 / 1000.0 * 0.99 + 0.01
        }

        let entries: Vec<(u32, f32)> = (0..256)
            .map(|i| (i as u32 * 7, cheap_rng(12345, i)))
            .collect();

        let block = make_block(&entries);
        let bytes = block.serialize();
        let restored = SparsePostingBlock::deserialize(&bytes).unwrap();

        for (i, (&orig, &rest)) in block
            .values()
            .iter()
            .zip(restored.values().iter())
            .enumerate()
        {
            assert!(
                (rest - orig).abs() <= F16_TOL,
                "entry {i}: expected {orig} +/- {F16_TOL}, got {rest}"
            );
        }
    }

    // ── Error path tests ────────────────────────────────────────────

    #[test]
    fn from_sorted_entries_empty_returns_error() {
        let err = SparsePostingBlock::from_sorted_entries(&[]).unwrap_err();
        assert!(matches!(err, SparsePostingBlockError::EmptyEntries));
    }

    #[test]
    fn from_sorted_entries_too_many_returns_error() {
        let entries: Vec<(u32, f32)> = (0..MAX_BLOCK_ENTRIES + 1)
            .map(|i| (i as u32, 0.5))
            .collect();
        let err = SparsePostingBlock::from_sorted_entries(&entries).unwrap_err();
        assert!(
            matches!(err, SparsePostingBlockError::TooManyEntries { count } if count == MAX_BLOCK_ENTRIES + 1)
        );
    }

    #[test]
    fn from_sorted_entries_at_max_succeeds() {
        let entries: Vec<(u32, f32)> = (0..MAX_BLOCK_ENTRIES).map(|i| (i as u32, 0.5)).collect();
        let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
        assert_eq!(block.len(), MAX_BLOCK_ENTRIES);
    }

    #[test]
    fn directory_new_mismatched_lengths_returns_error() {
        let err = DirectoryBlock::new(&[1, 2, 3], &[0.5, 0.5]).unwrap_err();
        assert!(matches!(
            err,
            SparsePostingBlockError::MismatchedLengths {
                offsets: 3,
                weights: 2,
            }
        ));
    }

    // ── Directory block on offsets/values returns empty ──────────────

    #[test]
    fn directory_block_offsets_values_return_empty() {
        let dir = DirectoryBlock::new(&[100], &[0.5]).unwrap();
        let block = dir.into_block();
        assert!(block.is_directory());
        assert_eq!(block.offsets(), &[] as &[u32]);
        assert_eq!(block.values(), &[] as &[f32]);
    }

    // ── len/is_empty coverage ───────────────────────────────────────

    #[test]
    fn len_and_is_empty() {
        let block1 = make_block(&[(0, 1.0)]);
        assert_eq!(block1.len(), 1);
        assert!(!block1.is_empty());

        let block200 = make_block(&sequential_entries(0, 1, 200, 0.5));
        assert_eq!(block200.len(), 200);
        assert!(!block200.is_empty());
    }

    // ── High offset values ──────────────────────────────────────────

    #[test]
    fn roundtrip_high_offsets() {
        let base = u32::MAX - 1000;
        let entries: Vec<(u32, f32)> = (0..10).map(|i| (base + i * 100, 0.5)).collect();
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    #[test]
    fn roundtrip_u32_max_single() {
        let entries = vec![(u32::MAX, 0.42)];
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    // ── Non-uniform deltas ──────────────────────────────────────────

    #[test]
    fn roundtrip_varied_deltas() {
        let entries = vec![
            (0, 0.1),
            (1, 0.2),
            (100, 0.3),
            (101, 0.4),
            (10_000, 0.5),
            (10_001, 0.6),
            (1_000_000, 0.7),
        ];
        assert_roundtrip_offsets(&entries);
        assert_roundtrip_values(&entries);
    }

    // ── Double-serialize stability ──────────────────────────────────

    #[test]
    fn serialize_deserialize_serialize_is_stable() {
        for count in [1, 3, 127, 128, 129, 255, 256, 512] {
            let entries = sequential_entries(0, 7, count, 0.5);
            let block = make_block(&entries);
            let bytes1 = block.serialize();
            let restored = SparsePostingBlock::deserialize(&bytes1).unwrap();
            let bytes2 = restored.serialize();
            assert_eq!(
                bytes1, bytes2,
                "double-serialize mismatch for count={count}"
            );
        }
    }

    // ── raw_weight_bytes content verification ───────────────────────

    #[test]
    fn raw_weight_bytes_content_correct() {
        let entries: Vec<(u32, f32)> = (0..5).map(|i| (i * 10, 0.1 * (i as f32 + 1.0))).collect();
        let block = make_block(&entries);
        let bytes = block.serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
        let wb = SparsePostingBlock::raw_weight_bytes(&bytes, &hdr);
        assert_eq!(wb.len(), 5 * 2);

        for i in 0..5 {
            let f = f16::from_le_bytes([wb[i * 2], wb[i * 2 + 1]]).to_f32();
            assert_approx(f, entries[i].1, F16_TOL);
        }
    }

    // ── peek_header on directory blocks ─────────────────────────────

    #[test]
    fn peek_header_directory_is_directory() {
        let dir = DirectoryBlock::new(&[10, 20, 30], &[0.5, 0.9, 0.2]).unwrap();
        let bytes = dir.into_block().serialize();
        let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
        assert_eq!(hdr.bits_per_delta, DIRECTORY_SENTINEL);
    }

    // ── Directory single entry ──────────────────────────────────────

    #[test]
    fn directory_single_entry() {
        let dir = DirectoryBlock::new(&[42], &[0.99]).unwrap();
        assert_eq!(dir.num_blocks(), 1);
        assert_approx(dir.dim_max_weight(), 0.99, 1e-6);
        let (offsets, weights) = dir.entries();
        assert_eq!(offsets, vec![42]);
        assert_eq!(weights, vec![0.99]);
    }

    // ── score_block_into edge cases ─────────────────────────────────

    #[test]
    fn score_block_into_zero_query_weight() {
        let block = make_block(&[(0, 0.5), (1, 0.25)]);
        let mut scores = vec![1.0, 2.0];
        block.score_block_into(0.0, &mut scores);
        assert_eq!(scores, vec![1.0, 2.0]);
    }

    #[test]
    fn score_block_into_larger_scores_buffer() {
        let block = make_block(&[(0, 0.5), (1, 0.25)]);
        let mut scores = vec![0.0; 10];
        block.score_block_into(1.0, &mut scores);
        assert_approx(scores[0], 0.5, F16_TOL);
        assert_approx(scores[1], 0.25, F16_TOL);
        for &s in &scores[2..] {
            assert_eq!(s, 0.0);
        }
    }

    // ── convert_f16_to_f32 edge cases ───────────────────────────────

    #[test]
    fn convert_f16_to_f32_empty() {
        let mut out = vec![];
        convert_f16_to_f32(&[], &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn convert_f16_to_f32_odd_trailing_byte_ignored() {
        let val = f16::from_f32(0.5);
        let mut input = val.to_le_bytes().to_vec();
        input.push(0xAB); // trailing odd byte
        let mut out = vec![0.0; 2];
        convert_f16_to_f32(&input, &mut out);
        assert_approx(out[0], 0.5, F16_TOL);
        assert_eq!(out[1], 0.0); // not overwritten: chunks_exact skips trailing
    }

    // ── Remainder-path tests (non-multiple-of-128) ──────────────────

    #[test]
    fn remainder_offsets_decompress_into() {
        for count in [1, 2, 63, 127, 129, 130, 255, 257] {
            let entries = sequential_entries(10, 3, count, 0.5);
            let block = make_block(&entries);
            let bytes = block.serialize();
            let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

            let mut buf = Vec::new();
            SparsePostingBlock::decompress_offsets_into(&bytes, &hdr, &mut buf);
            assert_eq!(
                buf.as_slice(),
                block.offsets(),
                "offset mismatch at count={count}"
            );
        }
    }

    #[test]
    fn remainder_values_decompress_into() {
        for count in [1, 2, 63, 127, 129, 130, 255, 257] {
            let entries = sequential_entries(0, 1, count, 0.7);
            let block = make_block(&entries);
            let bytes = block.serialize();
            let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

            let mut buf = Vec::new();
            SparsePostingBlock::decompress_values_into(&bytes, &hdr, &mut buf);
            for (i, (&a, &b)) in buf.iter().zip(block.values().iter()).enumerate() {
                assert!(
                    (a - b).abs() <= F16_TOL,
                    "value mismatch at count={count}, i={i}: {a} vs {b}"
                );
            }
        }
    }

    #[test]
    fn remainder_read_value_at() {
        for count in [1, 2, 63, 127, 129, 130, 255, 257] {
            let entries: Vec<(u32, f32)> = (0..count)
                .map(|i| (i as u32 * 5, 0.1 + 0.001 * i as f32))
                .collect();
            let block = make_block(&entries);
            let bytes = block.serialize();
            let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();

            for i in 0..count {
                let v = SparsePostingBlock::read_value_at(&bytes, &hdr, i);
                assert_approx(v, block.values()[i], F16_TOL);
            }
        }
    }

    #[test]
    fn remainder_roundtrip_coverage() {
        for count in [1, 2, 63, 64, 127, 129, 191, 255, 257, 383, 385] {
            let entries = sequential_entries(0, 1, count, 0.42);
            assert_roundtrip_offsets(&entries);
            assert_roundtrip_values(&entries);
        }
    }

    // ── SparsePostingBlockError Display ─────────────────────────────

    #[test]
    fn error_display_messages() {
        let e1 = SparsePostingBlockError::EmptyEntries;
        assert!(e1.to_string().contains("at least one"));

        let e2 = SparsePostingBlockError::TooManyEntries { count: 5000 };
        assert!(e2.to_string().contains("5000"));

        let e3 = SparsePostingBlockError::MismatchedLengths {
            offsets: 3,
            weights: 2,
        };
        assert!(e3.to_string().contains("3") && e3.to_string().contains("2"));
    }
}

#[cfg(all(test, feature = "testing"))]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_entries(max_count: usize) -> impl Strategy<Value = Vec<(u32, f32)>> {
        (1..=max_count)
            .prop_flat_map(|n| {
                (
                    proptest::collection::vec(0u32..u32::MAX / 2, n),
                    proptest::collection::vec(0.01f32..1.0f32, n),
                )
            })
            .prop_map(|(mut offsets, weights)| {
                offsets.sort();
                offsets.dedup();
                let n = offsets.len().min(weights.len());
                offsets.into_iter().zip(weights).take(n).collect::<Vec<_>>()
            })
            .prop_filter("need at least one entry", |v| !v.is_empty())
    }

    proptest! {
        #[test]
        fn serialize_deserialize_serialize_byte_identical(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let bytes1 = block.serialize();
            let restored = SparsePostingBlock::deserialize(&bytes1).unwrap();
            let bytes2 = restored.serialize();
            prop_assert_eq!(&bytes1, &bytes2);
        }

        #[test]
        fn roundtrip_offsets_always_match(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let bytes = block.serialize();
            let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
            prop_assert_eq!(restored.offsets(), block.offsets());
        }

        #[test]
        fn roundtrip_values_within_f16_tolerance(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let bytes = block.serialize();
            let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
            for (i, (&orig, &rest)) in block
                .values()
                .iter()
                .zip(restored.values().iter())
                .enumerate()
            {
                let diff = (orig - rest).abs();
                prop_assert!(
                    diff <= 1e-3,
                    "entry {}: expected {} ± 1e-3, got {} (diff={})",
                    i, orig, rest, diff
                );
            }
        }

        #[test]
        fn zero_copy_matches_lazy_offsets(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let bytes = block.serialize();
            let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
            let mut buf = Vec::new();
            SparsePostingBlock::decompress_offsets_into(&bytes, &hdr, &mut buf);
            prop_assert_eq!(buf.as_slice(), block.offsets());
        }

        #[test]
        fn zero_copy_matches_lazy_values(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let bytes = block.serialize();
            let hdr = SparsePostingBlock::peek_header(&bytes).unwrap();
            let mut buf = Vec::new();
            SparsePostingBlock::decompress_values_into(&bytes, &hdr, &mut buf);
            for (i, (&a, &b)) in buf.iter().zip(block.values().iter()).enumerate() {
                let diff = (a - b).abs();
                prop_assert!(
                    diff <= 1e-3,
                    "entry {}: zero-copy {} vs lazy {} (diff={})",
                    i, a, b, diff
                );
            }
        }

        #[test]
        fn serialized_size_always_matches(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let actual = block.serialize().len();
            prop_assert_eq!(block.serialized_size(), actual);
        }

        #[test]
        fn serialized_size_survives_roundtrip(entries in arb_entries(512)) {
            let block = SparsePostingBlock::from_sorted_entries(&entries).unwrap();
            let size_before = block.serialized_size();
            let bytes = block.serialize();
            let restored = SparsePostingBlock::deserialize(&bytes).unwrap();
            let size_after = restored.serialized_size();
            prop_assert_eq!(size_before, bytes.len());
            prop_assert_eq!(size_after, bytes.len());
            prop_assert_eq!(size_before, size_after);
        }
    }
}

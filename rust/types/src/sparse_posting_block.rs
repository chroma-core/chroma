use std::sync::OnceLock;

use bitpacking::{BitPacker, BitPacker4x};
use half::f16;

const BITPACK_GROUP_SIZE: usize = BitPacker4x::BLOCK_LEN; // 128

/// Sentinel value in `bits_per_delta` that marks a directory block.
const DIRECTORY_SENTINEL: u8 = 0xFF;

/// Lightweight header read from the first 16 bytes of a serialized
/// `SparsePostingBlock`.  No heap allocation, no decompression.
#[derive(Debug, Clone, Copy)]
pub struct PostingBlockHeader {
    pub num_entries: u16,
    pub bits_per_delta: u8,
    pub min_offset: u32,
    pub max_offset: u32,
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

/// A fixed-size block of compressed posting list entries.
///
/// Supports **lazy deserialization**: `deserialize()` only reads the 16-byte
/// header (min/max offset, max_weight, entry count, bits-per-delta). Full
/// decompression (bitpacking + dequantization) is deferred to the first call
/// to `offsets()` or `values()`. This lets the
/// MaxScore algorithm skip entire blocks without ever paying decompression cost.
#[derive(Debug, Clone)]
pub struct SparsePostingBlock {
    pub min_offset: u32,
    pub max_offset: u32,
    pub max_weight: f32,
    num_entries: u16,
    bits_per_delta: u8,
    /// Raw bytes after the 16-byte header. Empty for eagerly-constructed blocks.
    raw_body: Vec<u8>,
    decompressed: OnceLock<Decompressed>,
}

impl SparsePostingBlock {
    /// Build a block from sorted (offset, value) pairs.
    pub fn from_sorted_entries(entries: &[(u32, f32)]) -> Self {
        assert!(!entries.is_empty(), "block must have at least one entry");
        assert!(entries.len() <= 4096, "block must have at most 4096 entries");

        let n = entries.len();
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
            let group = &relative[start..start + BITPACK_GROUP_SIZE];
            max_bits = max_bits.max(packer.num_bits_sorted(initial, group));
        }
        let bits_per_delta = max_bits;

        SparsePostingBlock {
            min_offset,
            max_offset,
            max_weight,
            num_entries: n as u16,
            bits_per_delta,
            raw_body: Vec::new(),
            decompressed: OnceLock::from(Decompressed { offsets, values }),
        }
    }

    pub fn len(&self) -> usize {
        self.num_entries as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    pub fn offsets(&self) -> &[u32] {
        &self.decompressed().offsets
    }

    pub fn values(&self) -> &[f32] {
        &self.decompressed().values
    }

    fn decompressed(&self) -> &Decompressed {
        self.decompressed.get_or_init(|| {
            assert!(
                self.bits_per_delta != DIRECTORY_SENTINEL,
                "cannot decompress a directory block — use directory_entries() instead"
            );
            Self::decompress_raw(
                &self.raw_body,
                self.num_entries as usize,
                self.bits_per_delta,
                self.min_offset,
                self.max_weight,
            )
        })
    }

    fn decompress_raw(
        raw_body: &[u8],
        num_entries: usize,
        bits_per_delta: u8,
        min_offset: u32,
        _max_weight: f32,
    ) -> Decompressed {
        let packer = BitPacker4x::new();
        let full_groups = num_entries / BITPACK_GROUP_SIZE;
        let remainder = num_entries % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (bits_per_delta as usize) / 8;

        let mut pos = 0;
        let mut relative = Vec::with_capacity(num_entries);
        let mut initial = 0u32;

        for _ in 0..full_groups {
            let end = pos + packed_group_bytes;
            let mut group = vec![0u32; BITPACK_GROUP_SIZE];
            packer.decompress_sorted(initial, &raw_body[pos..end], &mut group, bits_per_delta);
            initial = group[BITPACK_GROUP_SIZE - 1];
            relative.extend_from_slice(&group);
            pos = end;
        }

        for _ in 0..remainder {
            let d = u32::from_le_bytes([
                raw_body[pos],
                raw_body[pos + 1],
                raw_body[pos + 2],
                raw_body[pos + 3],
            ]);
            relative.push(d);
            pos += 4;
        }

        let offsets: Vec<u32> = relative.iter().map(|&d| d + min_offset).collect();
        let f16_bytes = &raw_body[pos..pos + num_entries * 2];
        let values: Vec<f32> = f16_bytes
            .chunks_exact(2)
            .map(|b| f16::from_le_bytes([b[0], b[1]]).to_f32())
            .collect();

        Decompressed { offsets, values }
    }

    /// Serialize to compressed bytes: header + bitpacked deltas + f16 weights.
    pub fn serialize(&self) -> Vec<u8> {
        // Fast path: reconstruct from header + raw body bytes.
        if !self.raw_body.is_empty() {
            let mut buf = Vec::with_capacity(16 + self.raw_body.len());
            buf.extend_from_slice(&self.num_entries.to_le_bytes());
            buf.push(self.bits_per_delta);
            buf.push(0);
            buf.extend_from_slice(&self.min_offset.to_le_bytes());
            buf.extend_from_slice(&self.max_offset.to_le_bytes());
            buf.extend_from_slice(&self.max_weight.to_le_bytes());
            buf.extend_from_slice(&self.raw_body);
            return buf;
        }

        // Slow path: compress from decompressed data (writer-created blocks).
        let data = self.decompressed();
        let n = data.offsets.len();
        let packer = BitPacker4x::new();

        let relative: Vec<u32> = data.offsets.iter().map(|&o| o - self.min_offset).collect();

        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (self.bits_per_delta as usize) / 8;
        let total_size = 16 + full_groups * packed_group_bytes + remainder * 4 + n * 2;

        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&(n as u16).to_le_bytes());
        buf.push(self.bits_per_delta);
        buf.push(0);
        buf.extend_from_slice(&self.min_offset.to_le_bytes());
        buf.extend_from_slice(&self.max_offset.to_le_bytes());
        buf.extend_from_slice(&self.max_weight.to_le_bytes());

        for g in 0..full_groups {
            let start = g * BITPACK_GROUP_SIZE;
            let initial = if g == 0 { 0 } else { relative[start - 1] };
            let group = &relative[start..start + BITPACK_GROUP_SIZE];
            let mut packed = vec![0u8; packed_group_bytes];
            packer.compress_sorted(initial, group, &mut packed, self.bits_per_delta);
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

    /// Serialized byte length (computable from header fields, no decompression).
    pub fn serialized_size(&self) -> usize {
        let n = self.num_entries as usize;
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (self.bits_per_delta as usize) / 8;
        16 + full_groups * packed_group_bytes + remainder * 4 + n * 2
    }

    /// Deserialize from compressed bytes (lazy: only reads 16-byte header).
    pub fn deserialize(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 16, "buffer too small for header");

        let num_entries = u16::from_le_bytes([bytes[0], bytes[1]]);
        let bits_per_delta = bytes[2];
        let min_offset = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let max_offset = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let max_weight = f32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);

        SparsePostingBlock {
            min_offset,
            max_offset,
            max_weight,
            num_entries,
            bits_per_delta,
            raw_body: bytes[16..].to_vec(),
            decompressed: OnceLock::new(),
        }
    }

    // ── Zero-copy header / decompression ────────────────────────────

    /// Read the 16-byte header from raw serialized bytes without any
    /// heap allocation.
    pub fn peek_header(bytes: &[u8]) -> PostingBlockHeader {
        debug_assert!(bytes.len() >= 16);
        PostingBlockHeader {
            num_entries: u16::from_le_bytes([bytes[0], bytes[1]]),
            bits_per_delta: bytes[2],
            min_offset: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            max_offset: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            max_weight: f32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        }
    }

    /// Decompress offsets from raw bytes into a caller-provided buffer.
    /// Reuses `buf` across calls to avoid per-block allocation.
    pub fn decompress_offsets_into(bytes: &[u8], hdr: &PostingBlockHeader, buf: &mut Vec<u32>) {
        let n = hdr.num_entries as usize;
        buf.clear();
        buf.reserve(n);
        unsafe { buf.set_len(n) };

        let raw_body = &bytes[16..];
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
            let min = hdr.min_offset;
            for v in group.iter_mut() {
                *v += min;
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

    /// Return a slice of the raw f16 weight bytes from serialized bytes.
    /// Zero-copy — just a pointer into `bytes`. Each weight is 2 bytes (f16 LE).
    pub fn raw_weight_bytes<'a>(bytes: &'a [u8], hdr: &PostingBlockHeader) -> &'a [u8] {
        let n = hdr.num_entries as usize;
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (hdr.bits_per_delta as usize) / 8;
        let w_start = 16 + full_groups * packed_group_bytes + remainder * 4;
        &bytes[w_start..w_start + n * 2]
    }

    /// Read a single f16 weight at `index` from raw bytes and convert to f32.
    /// O(1) -- reads two bytes. No heap allocation.
    pub fn read_value_at(bytes: &[u8], hdr: &PostingBlockHeader, index: usize) -> f32 {
        let n = hdr.num_entries as usize;
        debug_assert!(index < n);
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (hdr.bits_per_delta as usize) / 8;
        let w_start = 16 + full_groups * packed_group_bytes + remainder * 4;
        let byte_pos = w_start + index * 2;
        f16::from_le_bytes([bytes[byte_pos], bytes[byte_pos + 1]]).to_f32()
    }

    /// Decompress f16 weights from raw bytes into a caller-provided f32 buffer.
    /// Reuses `buf` across calls to avoid per-block allocation.
    pub fn decompress_values_into(bytes: &[u8], hdr: &PostingBlockHeader, buf: &mut Vec<f32>) {
        let n = hdr.num_entries as usize;
        buf.clear();
        buf.reserve(n);

        let raw_body = &bytes[16..];
        let full_groups = n / BITPACK_GROUP_SIZE;
        let remainder = n % BITPACK_GROUP_SIZE;
        let packed_group_bytes = BITPACK_GROUP_SIZE * (hdr.bits_per_delta as usize) / 8;

        let w_start = full_groups * packed_group_bytes + remainder * 4;
        let f16_bytes = &raw_body[w_start..w_start + n * 2];

        unsafe {
            buf.set_len(n);
        }
        convert_f16_to_f32(f16_bytes, buf);
    }

    // ── Block directory support ──────────────────────────────────────

    /// Create a directory block that stores per-block metadata with exact f32.
    /// `max_offsets[i]` is block i's max doc-ID; `max_weights[i]` is block i's
    /// max weight.  The header's `max_weight` stores the dimension-level max
    /// (dim_max) exactly.
    pub fn from_directory(max_offsets: &[u32], max_weights: &[f32]) -> Self {
        assert_eq!(max_offsets.len(), max_weights.len());
        let n = max_offsets.len();
        let dim_max = max_weights.iter().copied().fold(0.0f32, f32::max);

        let mut raw_body = Vec::with_capacity(n * 8);
        for i in 0..n {
            raw_body.extend_from_slice(&max_offsets[i].to_le_bytes());
            raw_body.extend_from_slice(&max_weights[i].to_le_bytes());
        }

        SparsePostingBlock {
            min_offset: max_offsets.first().copied().unwrap_or(0),
            max_offset: max_offsets.last().copied().unwrap_or(0),
            max_weight: dim_max,
            num_entries: n as u16,
            bits_per_delta: DIRECTORY_SENTINEL,
            raw_body,
            decompressed: OnceLock::new(),
        }
    }

    pub fn is_directory(&self) -> bool {
        self.bits_per_delta == DIRECTORY_SENTINEL
    }

    /// Extract `(max_offsets, max_weights)` from a directory block.
    pub fn directory_entries(&self) -> (Vec<u32>, Vec<f32>) {
        assert!(self.is_directory(), "not a directory block");
        let n = self.num_entries as usize;
        let mut max_offsets = Vec::with_capacity(n);
        let mut max_weights = Vec::with_capacity(n);
        for i in 0..n {
            let pos = i * 8;
            let offset = u32::from_le_bytes([
                self.raw_body[pos],
                self.raw_body[pos + 1],
                self.raw_body[pos + 2],
                self.raw_body[pos + 3],
            ]);
            let weight = f32::from_le_bytes([
                self.raw_body[pos + 4],
                self.raw_body[pos + 5],
                self.raw_body[pos + 6],
                self.raw_body[pos + 7],
            ]);
            max_offsets.push(offset);
            max_weights.push(weight);
        }
        (max_offsets, max_weights)
    }

    /// Vectorized block scoring: accumulate `query_weight * weight` for all
    /// entries into the `scores` slice. Weights are stored as f16, converted
    /// to f32 and multiplied by `query_weight`.
    pub fn score_block_into(&self, query_weight: f32, scores: &mut [f32]) {
        let vals = self.values();
        let n = vals.len();
        debug_assert!(scores.len() >= n);

        for (s, &v) in scores[..n].iter_mut().zip(vals.iter()) {
            *s += v * query_weight;
        }
    }
}

// ── Convert f16 weight bytes → f32 values (used by decompress_values_into) ──

fn convert_f16_to_f32(f16_bytes: &[u8], out: &mut [f32]) {
    for (o, chunk) in out.iter_mut().zip(f16_bytes.chunks_exact(2)) {
        *o = f16::from_le_bytes([chunk[0], chunk[1]]).to_f32();
    }
}

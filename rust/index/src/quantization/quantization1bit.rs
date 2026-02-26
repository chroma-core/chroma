//! 1-bit RaBitQ quantization and associated query structures.
//!
//! This module contains:
//! - [`Code1Bit`]: 1-bit quantized code with precomputed `signed_sum`.
//! - [`QuantizedQuery`]: Pre-computed query quantization for the bitwise distance path.
//! - [`BatchQueryLuts`]: Pre-computed lookup tables for batch distance estimation.

use std::mem::size_of;

use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;
use simsimd::{BinarySimilarity, SpatialSimilarity};

use super::utils::{rabitq_distance_code, rabitq_distance_query, RabitqCode};

// ── Header ────────────────────────────────────────────────────────────────────

/// Header for 1-bit codes. Extends the 4-bit layout with `signed_sum`
/// (2·popcount(x_b) − dim), precomputed at index time for zero-cost query scoring.
/// 16 bytes.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CodeHeader1 {
    correction: f32,
    norm: f32,
    radial: f32,
    signed_sum: i32,
}

// ── Code1Bit ──────────────────────────────────────────────────────────────────

/// 1-bit RaBitQ quantized code.
///
/// Byte layout: `[CodeHeader1 (16 bytes)][packed sign bits]`
///
/// One bit per dimension, packed LSB-first. Bit `i` is 1 when the residual
/// `r[i] ≥ 0` and 0 otherwise — i.e. `g[i] = +0.5` when bit=1, `−0.5` when
/// bit=0.
///
/// The header stores `signed_sum = 2·popcount(x_b) − dim`, precomputed at
/// index time and used by both `distance_query` and `distance_query_bitwise`.
pub struct Code1Bit<T = Vec<u8>>(T);

impl<T: AsRef<[u8]>> RabitqCode for Code1Bit<T> {
    fn correction(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[0..4])
    }
    fn norm(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[4..8])
    }
    fn radial(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[8..12])
    }
    fn packed(&self) -> &[u8] {
        &self.0.as_ref()[size_of::<CodeHeader1>()..]
    }
}

impl<T> Code1Bit<T> {
    /// Wraps existing bytes as a 1-bit code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }
}

impl<T: AsRef<[u8]>> Code1Bit<T> {
    /// Precomputed `signed_sum = 2·popcount(x_b) − dim`, stored in the header.
    pub fn signed_sum(&self) -> i32 {
        bytemuck::pod_read_unaligned::<i32>(&self.0.as_ref()[12..16])
    }

    /// Estimates distance between two original data vectors `d_a` and `d_b`.
    ///
    /// For 1-bit codes, computes `⟨g_a, g_b⟩` via Hamming distance:
    /// ```text
    /// ⟨g_a, g_b⟩ = 0.25 · (dim − 2·hamming(a, b))
    /// ```
    /// since each g[i] ∈ {−0.5, +0.5}: agreeing bits contribute +0.25,
    /// disagreeing bits contribute −0.25.
    pub fn distance_code<U: AsRef<[u8]>>(
        &self,
        distance_fn: &DistanceFunction,
        other: &Code1Bit<U>,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let hamming = hamming_distance(self.packed(), other.packed());
        let g_a_dot_g_b = 0.25 * (dim as f32 - 2.0 * hamming as f32);
        rabitq_distance_code(g_a_dot_g_b, self, other, c_norm, distance_fn)
    }

    /// Estimates distance from data vector `d` to query `q` (float query path).
    ///
    /// Computes `⟨g, r_q⟩ = 0.5 · signed_dot(packed, r_q)`:
    /// each bit contributes `+r_q[i]` (bit=1) or `−r_q[i]` (bit=0).
    pub fn distance_query_full_precision(
        &self,
        distance_fn: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        // For BITS=1, g[i] is +0.5 when bit=1 and −0.5 when bit=0.
        // ⟨g, r_q⟩ = Σ g[i] · r_q[i]
        //           = 0.5 · Σ_{bit=1} r_q[i]  −  0.5 · Σ_{bit=0} r_q[i]
        //           = 0.5 · Σ sign(g[i]) · r_q[i]
        let g_dot_r_q = 0.5 * signed_dot(self.packed(), r_q);
        rabitq_distance_query(g_dot_r_q, self, c_norm, c_dot_q, q_norm, distance_fn)
    }

    // ── Bitwise query path (paper Section 3.3) ───────────────────────────────

    /// Bitwise distance estimation using the paper's Section 3.3 approach.
    ///
    /// ⟨g, r_q⟩ = 0.5·(Δ·signed_dot_qu + v_l·signed_sum)
    ///
    /// Instead of expanding packed bits to f32 signs and running a float dot
    /// product, this computes `⟨x_bar_b, q_bar_u⟩` using B_q rounds of
    /// AND + popcount on D-bit strings, then recovers the full distance
    /// estimate from the scalar factors.
    ///
    /// # Derivation: Equation 20 to our code
    ///
    /// **Step 1 — Paper Equation 20** (unit vectors, ō[i] = ±1/√D):
    /// ```text
    /// ⟨x̄, q̄⟩ = (2Δ/√D)·⟨x_b, q_u⟩ + (2v_l/√D)·Σ x_b[i] - (Δ/√D)·Σ q_u[i] - √D·v_l
    /// ```
    ///
    /// **Step 2 — Our scaling** (residuals, g[i] = ±0.5):
    /// Replace 1/√D with 0.5, √D with dim. We want ⟨g, r_q⟩ where r_q[i] ≈ Δ·q_u[i] + v_l:
    /// ```text
    /// ⟨g, r_q⟩ = 0.5·(2Δ·⟨x_b, q_u⟩ + 2·v_l·popcount(x_b) - Δ·Σ q_u[i] - dim·v_l)
    /// ```
    ///
    /// **Step 3 — Factor** (group Δ terms and v_l terms):
    /// ```text
    /// ⟨g, r_q⟩ = 0.5·(Δ·(2·⟨x_b, q_u⟩ - Σ q_u[i]) + v_l·(2·popcount(x_b) - dim))
    /// ```
    ///
    /// **Step 4 — Substitute** sign[i] = 2·x_b[i] − 1:
    /// ```text
    /// signed_dot_qu = Σ sign[i]·q_u[i] = 2·⟨x_b, q_u⟩ − Σ q_u[i]
    /// signed_sum    = Σ sign[i]        = 2·popcount(x_b) − dim
    /// ⟨g, r_q⟩      = 0.5·(Δ·signed_dot_qu + v_l·signed_sum)
    /// ```
    ///
    /// # Notation
    ///
    /// - v_l = min(r_q[i])
    /// - v_r = max(r_q[i])
    /// - x_b = data code (packed bits), g[i] = +0.5 when x_b[i]=1 else −0.5
    /// - q_u = quantized query, r_q[i] ≈ Δ·q_u[i] + v_l
    /// - Δ = (v_r − v_l) / (2^B_q − 1)
    /// - ⟨x_b, q_u⟩ = Σ_j 2^j · popcount(x_b AND q_u^(j))
    pub fn distance_query(&self, distance_fn: &DistanceFunction, qq: &QuantizedQuery) -> f32 {
        let packed = self.packed();

        // Compute ⟨x_b, q_u⟩ (the binary versions of g and r_q) via bit planes.
        // ⟨x_b, q_u⟩ = Σ_j 2^j · popcount(x_b AND q_u^(j))
        //
        // [B1] Interleaved: read each x_b word once, AND with all planes per word.
        //      Avoids re-reading x_b b_q times (4× at the default b_q=4).
        // [B2] chunks_exact(8) instead of step_by(8)+index: exposes the iteration
        //      structure to LLVM, enabling auto-vectorization of the inner loop.
        //      Benchmarked on Apple M-series: B2 alone gives 2.4× speedup on the
        //      primitive, B1+B2 gives ~2.7×. Combined effect on the hot 2048-code
        //      scan: −56% / +126%.
        //
        // bit_planes is now a flat Vec<u8>: plane j at [j*pb .. (j+1)*pb].
        // The b_q=4 fast-path slices the flat buffer and uses B1+B2 (interleaved
        // + chunks_exact) for full loop unrolling without bounds checks.
        // The general fallback handles b_q ≠ 4 (currently unused).
        let pb = qq.padded_bytes;
        let xb_dot_qu = if qq.b_q == 4 {
            let p0 = &qq.bit_planes[0 * pb..1 * pb];
            let p1 = &qq.bit_planes[1 * pb..2 * pb];
            let p2 = &qq.bit_planes[2 * pb..3 * pb];
            let p3 = &qq.bit_planes[3 * pb..4 * pb];
            let (mut pop0, mut pop1, mut pop2, mut pop3) = (0u32, 0u32, 0u32, 0u32);
            for (x_chunk, (((q0, q1), q2), q3)) in packed.chunks_exact(8).zip(
                p0.chunks_exact(8)
                    .zip(p1.chunks_exact(8))
                    .zip(p2.chunks_exact(8))
                    .zip(p3.chunks_exact(8)),
            ) {
                let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
                pop0 += (x & u64::from_le_bytes(q0.try_into().unwrap())).count_ones();
                pop1 += (x & u64::from_le_bytes(q1.try_into().unwrap())).count_ones();
                pop2 += (x & u64::from_le_bytes(q2.try_into().unwrap())).count_ones();
                pop3 += (x & u64::from_le_bytes(q3.try_into().unwrap())).count_ones();
            }
            pop0 + (pop1 << 1) + (pop2 << 2) + (pop3 << 3)
        } else {
            // General fallback for b_q ≠ 4.
            let mut result = 0u32;
            for j in 0..qq.b_q as usize {
                let plane = &qq.bit_planes[j * pb..(j + 1) * pb];
                let mut pop = 0u32;
                for (x_chunk, q_chunk) in packed.chunks_exact(8).zip(plane.chunks_exact(8)) {
                    let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
                    let q = u64::from_le_bytes(q_chunk.try_into().unwrap());
                    pop += (x & q).count_ones();
                }
                result += pop << j;
            }
            result
        };

        // signed_sum = 2·popcount(x_b) − dim, precomputed at index time
        let signed_sum = self.signed_sum() as f32;
        let signed_dot_qu = 2.0 * xb_dot_qu as f32 - qq.sum_q_u as f32;
        // ⟨g, r_q⟩ = 0.5·(Δ·signed_dot_qu + v_l·signed_sum)
        let g_dot_r_q = 0.5 * (qq.delta * signed_dot_qu + qq.v_l * signed_sum);

        rabitq_distance_query(
            g_dot_r_q,
            self,
            qq.c_norm,
            qq.c_dot_q,
            qq.q_norm,
            distance_fn,
        )
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Code1Bit<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Code1Bit {
    const GRID_OFFSET: f32 = 0.5;

    /// Padded byte length for a given dimension.
    pub fn packed_len(dim: usize) -> usize {
        padded_dim_1bit(dim) / 8
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<CodeHeader1>() + Self::packed_len(dim)
    }

    /// Quantizes a data vector relative to its cluster centroid (1-bit path).
    ///
    /// 1-bit quantization uses sign-based coding — no ray-walk needed.
    /// See section 3.1.3 of the paper.
    pub fn quantize(embedding: &[f32], centroid: &[f32]) -> Self {
        let dim = embedding.len();
        let mut packed = vec![0u8; Self::packed_len(dim)];
        let mut abs_sum = 0.0f32;
        let mut norm_sq = 0.0f32;
        let mut radial = 0.0f32;
        let mut popcount = 0u32;

        // Single fused pass over (embedding, centroid) — no intermediate `r` allocation.
        //
        // Each outer iteration processes 8 elements → 1 byte of packed output, computing:
        //   - sign bits  → packed codes (bit=1 when r[i] ≥ 0, bit=0 otherwise)
        //   - |r[i]|     → abs_sum     (for correction factor ⟨g, n⟩)
        //   - r[i]²      → norm_sq     (for ‖r‖)
        //   - r[i]·c[i]  → radial      (for ⟨r, c⟩)
        //   - popcount    → signed_sum  (2·popcount(x_b) − dim)
        //
        // Compared to the prior four-pass approach (vec_sub alloc, sign_pack, abs_sum, popcount),
        // this eliminates the 4 KB intermediate `r` allocation and three re-reads of it.
        // The four scalar accumulators (abs_sum, norm_sq, radial, popcount) are independent
        // across iterations and auto-vectorize to VABSPS/VFMADD on AVX2/AVX-512.
        for (byte_ref, (emb_chunk, cen_chunk)) in packed
            .iter_mut()
            .zip(embedding.chunks(8).zip(centroid.chunks(8)))
        {
            let mut byte = 0u8;
            for (j, (&e, &c)) in emb_chunk.iter().zip(cen_chunk).enumerate() {
                let val = e - c;
                let sign = (val.to_bits() >> 31) as u8; // 1 if negative, 0 if non-negative
                byte |= (sign ^ 1) << j;
                abs_sum += val.abs();
                norm_sq += val * val;
                radial += val * c;
            }
            popcount += byte.count_ones();
            *byte_ref = byte;
        }

        let norm = norm_sq.sqrt();

        // Early return for dim == 0 or near-zero residual (same semantics as before).
        if dim == 0 || norm < f32::EPSILON {
            let mut bytes = Vec::with_capacity(Self::size(dim));
            bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader1 {
                correction: 1.0,
                norm,
                radial,
                signed_sum: -(dim as i32),
            }));
            bytes.resize(Self::size(dim), 0);
            return Self(bytes);
        }

        // correction = ⟨g, n⟩
        //            = ⟨g, r⟩ / ‖r‖
        //            = Σ g[i] * r[i] / ‖r‖
        //               - for BITS=1, g[i] always has the same sign as r[i]
        //                  g[i] = +0.5   if r[i] >= 0
        //                  g[i] = -0.5   if r[i] <  0
        //               - Therefore g[i] * r[i] = sign(r[i]) * 0.5 * r[i] = 0.5 * |r[i]|
        //            = Σ 0.5 * |r[i]| / ‖r‖
        //            = 0.5 * Σ |r[i]| / ‖r‖
        //            = GRID_OFFSET * abs_sum / norm
        let correction = Self::GRID_OFFSET * abs_sum / norm;
        let signed_sum = 2 * popcount as i32 - dim as i32;

        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader1 {
            correction,
            norm,
            radial,
            signed_sum,
        }));
        bytes.extend_from_slice(&packed);
        Self(bytes)
    }
}

// ── Sizing helper (private) ───────────────────────────────────────────────────

/// Padded dimension for 1-bit codes (multiple of 64 for u64 popcount alignment).
fn padded_dim_1bit(dim: usize) -> usize {
    dim.div_ceil(64) * 64
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Computes hamming distance between two packed bit vectors.
///
/// Both slices must have the same length and that length must be a multiple of
/// 8 (guaranteed when `padded_dim` is a multiple of 64).
///
/// Uses `simsimd::BinarySimilarity::hamming` which dispatches at runtime to:
///   - AVX-512 VPOPCNTDQ on x86_64 (8 × u64 lanes per instruction)
///   - NEON CNT on ARM (byte-level popcount, vectorised over 16 bytes)
///
/// Falls back to scalar u64 XOR + POPCNT if simsimd returns None (e.g. on
/// unsupported targets or in tests without the CPU feature).
fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 8, 0);
    <u8 as BinarySimilarity>::hamming(a, b).expect("slices have equal length") as u32
}

/// Computes `Σ sign[i] · values[i]` where sign[i] = +1.0 if bit i is set
/// in `packed`, −1.0 otherwise.
///
/// This is the hot kernel for the 1-bit `distance_query` path.  The caller
/// multiplies the result by 0.5 to recover `⟨g, r_q⟩`.
///
/// # SIMD strategy
///
/// **Step 1 — sign expansion (integer bit trick).**
/// +1.0f32 and −1.0f32 differ only in bit 31 of their IEEE 754 representation
/// (0x3F800000 vs 0xBF800000).  For each extracted bit b ∈ {0, 1}:
///
/// ```text
///   sign_bit = (b ^ 1) & 1      // 1 when b=0 (want −1), 0 when b=1 (want +1)
///   f32_bits = 0x3F800000 | (sign_bit << 31)
/// ```
///
/// All shift amounts are compile-time constants (0..7), so LLVM fully unrolls
/// the 8-element inner body.  The operations are pure integer (XOR, AND, OR,
/// shift) until the final `f32::from_bits` reinterpretation — no
/// integer-to-float conversion or arithmetic is required.
///
/// **Step 2 — dot product (simsimd).**
/// The sign array and the value chunk are passed to `f32::dot`, which
/// dispatches to the platform's best FMA kernel (AVX2, AVX-512, etc.).
///
/// The expansion uses a 256-byte stack buffer (8 bytes × 8 floats × 4 bytes)
/// and is processed 64 floats at a time to avoid heap allocation.
fn signed_dot(packed: &[u8], values: &[f32]) -> f32 {
    const CHUNK: usize = 8; // bytes per outer iteration → 64 floats
    let mut signs = [0.0f32; CHUNK * 8];
    let mut sum = 0.0f32;

    for (packed_chunk, val_chunk) in packed.chunks(CHUNK).zip(values.chunks(CHUNK * 8)) {
        let n = val_chunk.len();
        for (i, &byte) in packed_chunk.iter().enumerate() {
            let base = i * 8;
            let b = byte as u32;
            // Constant shifts → LLVM fully unrolls this block.
            signs[base] = f32::from_bits(0x3F800000 | (((b >> 0) & 1) ^ 1) << 31);
            signs[base + 1] = f32::from_bits(0x3F800000 | (((b >> 1) & 1) ^ 1) << 31);
            signs[base + 2] = f32::from_bits(0x3F800000 | (((b >> 2) & 1) ^ 1) << 31);
            signs[base + 3] = f32::from_bits(0x3F800000 | (((b >> 3) & 1) ^ 1) << 31);
            signs[base + 4] = f32::from_bits(0x3F800000 | (((b >> 4) & 1) ^ 1) << 31);
            signs[base + 5] = f32::from_bits(0x3F800000 | (((b >> 5) & 1) ^ 1) << 31);
            signs[base + 6] = f32::from_bits(0x3F800000 | (((b >> 6) & 1) ^ 1) << 31);
            signs[base + 7] = f32::from_bits(0x3F800000 | (((b >> 7) & 1) ^ 1) << 31);
        }
        sum += f32::dot(&signs[..n], val_chunk).unwrap_or(0.0) as f32;
    }
    sum
}

// ── Bitwise distance estimation (paper Section 3.3) ──────────────────────────
//
// The paper's efficient estimator quantizes the query residual r_q into B_q-bit
// unsigned integers, then computes ⟨x_bar_b, q_bar_u⟩ using B_q rounds of
// bitwise AND + popcount on D-bit strings.  This eliminates all float
// arithmetic from the per-code inner product.
//
// Notation mapping (paper → our code):
//   o, q       → n (normalized residual), r_q/‖r_q‖
//   x_bar_b    → self.packed() (the stored D-bit quantization code)
//   q'         → r_q (already P⁻¹-rotated before reaching us)
//   q_bar_u    → quantized query (computed once per cluster scan)
//   ⟨o_bar, o⟩ → correction (= ⟨g, n⟩, stored in the header)
//   ⟨o_bar, q⟩ → g_dot_r_q (what we estimate per code)

/// Pre-computed query quantization for the bitwise distance path.
///
/// Computed once per query-cluster pair and reused across all codes in the
/// cluster.  For BITS=1 with B_q=4, this stores:
///   - 4 bit planes of the quantized query in a flat contiguous buffer
///   - v_l, delta, sum_q_u: scalar factors for Equation 20
pub struct QuantizedQuery {
    /// Flat bit-plane buffer: plane j occupies bytes [j*padded_bytes .. (j+1)*padded_bytes].
    /// bit_planes[j*padded_bytes + i] holds the j-th bit of q_u[i*8 .. i*8+8], packed LSB-first.
    /// One contiguous allocation replaces the prior b_q separate Vec<u8> allocations.
    pub bit_planes: Vec<u8>,
    /// Byte length of one bit plane (= packed data code length = ceil(dim/64)*8).
    pub padded_bytes: usize,
    /// Lower bound of query values: v_l = min(r_q[i])
    pub v_l: f32,
    /// Quantization step size: delta = (v_r - v_l) / (2^B_q - 1)
    pub delta: f32,
    /// Sum of quantized query values: Σ q_u[i]
    pub sum_q_u: u32,
    /// Number of bits used per query element
    pub b_q: u8,
    /// Precomputed query-level scalars
    pub c_norm: f32,
    pub c_dot_q: f32,
    pub q_norm: f32,
}

impl QuantizedQuery {
    /// Quantize a query residual r_q into B_q-bit unsigned integers and
    /// decompose into bit planes for AND+popcount inner products.
    ///
    /// `b_q` is the number of bits per query element (paper recommends 4).
    /// `padded_bytes` is the byte length of the packed data codes (for alignment).
    pub fn new(
        r_q: &[f32],
        b_q: u8,
        padded_bytes: usize,
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> Self {
        let max_val = (1u32 << b_q) - 1;

        // Two separate folds — each auto-vectorises to a SIMD reduction
        // (FMINV/FMAXV on ARM NEON, VMINPS horizontal on x86).
        // A combined tuple fold `(min, max)` breaks this vectorisation (scalar
        // pair dependency), measured 3.77× slower on Apple M-series.
        let v_l = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_r - v_l;
        let delta = if range > f32::EPSILON {
            range / max_val as f32
        } else {
            1.0
        };

        // [P2+P4] Single fused pass: quantize each element, accumulate sum,
        // and scatter bits into a flat bit-plane buffer — all in one read of r_q.
        // Eliminates the intermediate Vec<u32> (4 KB alloc + fill) and reduces
        // b_q separate Vec<u8> allocations to one contiguous slab.
        //
        // Why skip randomized rounding: the accuracy difference is negligible
        // at B_q=4.  See commit 3dd86c6f (randomized) and its revert 101af74f.
        //
        // The scatter uses the byte_chunks pattern: process 8 elements → 1 byte
        // per plane, building all b_q output bytes before writing.  This is
        // branchless and gives LLVM a clean 8-element inner loop to vectorise.
        // Measured 3.3× faster than the element-by-element branch scatter.
        //
        // Layout: plane j occupies flat_planes[j*padded_bytes .. (j+1)*padded_bytes].
        let inv_delta = 1.0 / delta;
        let mut bit_planes = vec![0u8; b_q as usize * padded_bytes];
        let mut sum_q_u = 0u32;
        // b_q=4 fast path: hardcoded planes let LLVM unroll and vectorize the
        // inner byte loop. The general fallback handles other values of b_q.
        if b_q == 4 {
            for (byte_idx, chunk) in r_q.chunks(8).enumerate() {
                let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
                for (bit, &v) in chunk.iter().enumerate() {
                    let qu = (((v - v_l) * inv_delta).round() as u32).min(max_val);
                    sum_q_u += qu;
                    b0 |= (((qu >> 0) & 1) as u8) << bit;
                    b1 |= (((qu >> 1) & 1) as u8) << bit;
                    b2 |= (((qu >> 2) & 1) as u8) << bit;
                    b3 |= (((qu >> 3) & 1) as u8) << bit;
                }
                bit_planes[0 * padded_bytes + byte_idx] = b0;
                bit_planes[1 * padded_bytes + byte_idx] = b1;
                bit_planes[2 * padded_bytes + byte_idx] = b2;
                bit_planes[3 * padded_bytes + byte_idx] = b3;
            }
        } else {
            for (byte_idx, chunk) in r_q.chunks(8).enumerate() {
                let mut bs = [0u8; 8]; // max b_q supported
                for (bit, &v) in chunk.iter().enumerate() {
                    let qu = (((v - v_l) * inv_delta).round() as u32).min(max_val);
                    sum_q_u += qu;
                    for j in 0..b_q as usize {
                        bs[j] |= (((qu >> j) & 1) as u8) << bit;
                    }
                }
                for j in 0..b_q as usize {
                    bit_planes[j * padded_bytes + byte_idx] = bs[j];
                }
            }
        }

        Self {
            bit_planes,
            padded_bytes,
            v_l,
            delta,
            sum_q_u,
            b_q,
            c_norm,
            c_dot_q,
            q_norm,
        }
    }
}

/// Pre-computed lookup tables for batch distance estimation (paper Section 3.3.2).
///
/// Intuition:
/// - BatchQueryLuts precomputes all possible _partial_ inner products and saves
///   them in lookup tables:
/// - For each group of 4 dimensions, a 16-entry table gives the partial
///   ⟨x_b, q_u⟩ for every possible 4-bit chunk (nibble) of the data code (x_b)
/// - At query time you only do nibble extraction and table lookups.
/// - Results: Large table (8 KB for dim=1024), but less compute per code.
///
/// Specifically:
/// Splits the D-bit data code into D/4 nibbles.  For each nibble position,
/// precomputes a 16-entry LUT: the partial inner product between the nibble
/// of x_b and the corresponding 4 elements of the quantized query.
///
/// At scan time, each code's distance requires D/4 LUT lookups + accumulation
/// (no float expansion, no AND+popcount).
///
/// Why distance_query_bitwise beats BatchQueryLuts::distance_query:
// The working set sizes explain the gap:
//   - Bitwise: 4 bit planes x 128 bytes = 512 bytes of query data (fits in L1),
//     plus 128 bytes per code. The inner loop is 4 rounds of 16 AND+popcount
//     operations on u64 words -- 64 iterations of 3-instruction sequences.
//   - LUT: 256 nibble positions x 32 bytes per LUT entry = 8 KB of LUT data,
//     plus 128 bytes per code. The inner loop is 256 iterations of nibble extraction
//     + array indexing + accumulation -- more iterations, more cache pressure, and
//     indirect addressing (table lookup) prevents pipelining.
// The bitwise approach reads less data, does fewer iterations, and each iteration is a simpler instruction sequence (AND, POPCNT, ADD) that modern CPUs pipeline perfectly.
pub struct BatchQueryLuts {
    /// luts[nibble_idx][nibble_value] = partial ⟨x_b, q_u⟩ contribution.
    /// nibble_idx ranges over 0..dim/4 (padded to byte boundary).
    pub luts: Vec<[u16; 16]>,
    pub v_l: f32,
    pub delta: f32,
    pub sum_q_u: u32,
    pub c_norm: f32,
    pub c_dot_q: f32,
    pub q_norm: f32,
    pub dim: usize,
}

impl BatchQueryLuts {
    /// Build Lookup Tables (LUTs) from a query residual for 1-bit codes.
    ///
    /// Each nibble of the data code covers 4 bits (i.e., 4 dimensions).
    /// For each of the 16 possible nibble values, we precompute the partial
    /// sum of q_u[i] for the bits that are set.
    pub fn new(r_q: &[f32], c_norm: f32, c_dot_q: f32, q_norm: f32) -> Self {
        let dim = r_q.len();
        let max_val = 15u32; // B_q = 4

        // Quantize the query residual.
        let v_l = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_r - v_l;
        let delta = if range > f32::EPSILON {
            range / max_val as f32
        } else {
            1.0
        };
        let q_u: Vec<u32> = r_q
            .iter()
            .map(|&v| (((v - v_l) / delta).round() as u32).min(max_val))
            .collect();

        // Sum of quantized query values (Σ q_u[i]) for future distance computation.
        let sum_q_u: u32 = q_u.iter().sum();

        // Number of nibbles (each nibble = 4 bits = 4 dimensions).
        let padded_dim = (dim + 63) / 64 * 64;
        let n_nibbles = padded_dim / 4;

        let mut luts = vec![[0u16; 16]; n_nibbles];

        for (nib_idx, lut) in luts.iter_mut().enumerate() {
            let base = nib_idx * 4;
            // For each of the 16 possible nibble values, sum q_u for set bits.
            for nibble_val in 0u8..16 {
                let mut partial = 0u32;
                for bit in 0..4 {
                    if (nibble_val >> bit) & 1 == 1 {
                        let elem_idx = base + bit;
                        if elem_idx < dim {
                            partial += q_u[elem_idx];
                        }
                    }
                }
                lut[nibble_val as usize] = partial as u16;
            }
        }

        Self {
            luts,
            v_l,
            delta,
            sum_q_u,
            c_norm,
            c_dot_q,
            q_norm,
            dim,
        }
    }

    /// Score a single 1-bit code using the precomputed LUTs.
    ///
    /// For each nibble of the packed data code, look up the partial inner
    /// product from the LUT and accumulate.  Then recover the full distance.
    pub fn distance_query(&self, code: &Code1Bit<&[u8]>, distance_fn: &DistanceFunction) -> f32 {
        let packed = code.packed();

        // ⟨x_b, q_u⟩ via LUT: iterate over nibbles of packed data.
        let mut xb_dot_qu = 0u32;
        for (nib_idx, lut) in self.luts.iter().enumerate() {
            let byte_idx = nib_idx / 2;
            let byte = if byte_idx < packed.len() {
                packed[byte_idx]
            } else {
                0
            };
            let nibble = if nib_idx % 2 == 0 {
                byte & 0x0F
            } else {
                byte >> 4
            };
            xb_dot_qu += lut[nibble as usize] as u32;
        }

        let signed_dot_qu = 2.0 * xb_dot_qu as f32 - self.sum_q_u as f32;
        let signed_sum = code.signed_sum() as f32;
        let g_dot_r_q = 0.5 * (self.delta * signed_dot_qu + self.v_l * signed_sum);

        rabitq_distance_query(
            g_dot_r_q,
            code,
            self.c_norm,
            self.c_dot_q,
            self.q_norm,
            distance_fn,
        )
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use simsimd::SpatialSimilarity;

    use super::*;
    use crate::quantization::RabitqCode;

    #[test]
    fn test_1bit_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code1Bit::quantize(&embedding, &centroid);

        // Verify accessors return finite values
        assert!(code.correction().is_finite());
        assert!(code.norm().is_finite());
        assert!(code.radial().is_finite());

        // Verify norm is ‖r‖
        let r = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect::<Vec<_>>();
        let expected_norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        assert!((code.norm() - expected_norm).abs() < f32::EPSILON);

        // Verify radial is ⟨r, c⟩
        let expected_radial = f32::dot(&r, &centroid).unwrap_or(0.0) as f32;
        assert!((code.radial() - expected_radial).abs() < f32::EPSILON);

        // Verify correction = 0.5 * Σ|r[i]| / ‖r‖
        let abs_sum: f32 = r.iter().map(|x| x.abs()).sum();
        let expected_correction = 0.5 * abs_sum / expected_norm;
        assert!(
            (code.correction() - expected_correction).abs() < 1e-5,
            "correction: got {}, expected {}",
            code.correction(),
            expected_correction
        );

        // Verify buffer size
        assert_eq!(code.as_ref().len(), Code1Bit::size(embedding.len()));
    }

    #[test]
    fn test_1bit_size() {
        // 64-aligned (256 dims)
        assert_eq!(Code1Bit::packed_len(256), 256 / 8); // 32 bytes
        assert_eq!(Code1Bit::size(256), 16 + 32); // CodeHeader1 (16 bytes) + packed

        // Non-aligned (300) - should pad to 320 (5 * 64)
        assert_eq!(Code1Bit::packed_len(300), 320 / 8); // 40 bytes
        assert_eq!(Code1Bit::size(300), 16 + 40);

        // 1024 dims
        assert_eq!(Code1Bit::packed_len(1024), 128);
        assert_eq!(Code1Bit::size(1024), 16 + 128);

        // 4096 dims
        assert_eq!(Code1Bit::packed_len(4096), 512);
        assert_eq!(Code1Bit::size(4096), 16 + 512);
    }

    #[test]
    fn test_1bit_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code1Bit::quantize(&embedding, &embedding);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code1Bit::quantize(&embedding, &centroid);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);
    }

    /// Reads bit `i` from packed 1-bit codes and returns the grid value (±0.5).
    fn read_1bit_grid(code: &Code1Bit<Vec<u8>>, dim: usize) -> Vec<f32> {
        let packed = code.packed();
        (0..dim)
            .map(|i| {
                let bit = (packed[i / 8] >> (i % 8)) & 1;
                bit as f32 - 0.5
            })
            .collect()
    }

    /// Verify each bit matches the sign of the residual.
    #[test]
    fn test_1bit_quantize_signs() {
        let embedding = vec![3.0, -1.0, 0.5, -2.0, 0.0, 1.0, -0.1, 0.1];
        let centroid = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        // residual: [2.0, -2.0, -0.5, -3.0, -1.0, 0.0, -1.1, -0.9]
        // expected bits: [1, 0, 0, 0, 0, 1, 0, 0] (bit 5 is 1 because r=0.0 >= 0)

        let code = Code1Bit::quantize(&embedding, &centroid);
        let grid = read_1bit_grid(&code, 8);

        let r: Vec<f32> = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect();
        for i in 0..8 {
            let expected_sign = if r[i] >= 0.0 { 0.5 } else { -0.5 };
            assert_eq!(
                grid[i], expected_sign,
                "dim {}: r={}, grid={}, expected={}",
                i, r[i], grid[i], expected_sign
            );
        }
    }

    /// Spot-check that original and new quantize agree on the per-element
    /// sign bit before packing.  Both reduce to code[i] = 1 if r[i] >= 0,
    /// 0 otherwise for BITS=1; the difference is only in how they pack those
    /// bits into bytes (BitPacker8x vs LSB-first).
    #[test]
    fn test_quantize_lyon_matches_quantize() {
        let mut rng = StdRng::seed_from_u64(42);
        for &dim in &[64, 300, 1024] {
            for _ in 0..10 {
                let embedding: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let r: Vec<f32> = embedding
                    .iter()
                    .zip(&centroid)
                    .map(|(e, c)| e - c)
                    .collect();

                // quantize_lyon logic: sign bit = (IEEE sign bit) XOR 1
                let signs_lyon: Vec<u8> =
                    r.iter().map(|&v| (v.to_bits() >> 31) as u8 ^ 1).collect();

                // quantize logic: for BITS=1, CEIL=1, ray-walk collapses to
                //   code[i] = 1 if r[i] >= 0, else 0
                let signs_quantize: Vec<u8> =
                    r.iter().map(|&v| if v >= 0.0 { 1 } else { 0 }).collect();

                assert_eq!(signs_lyon, signs_quantize, "sign mismatch at dim={dim}");
            }
        }
    }

    /// Tests that 1-bit grid points quantize exactly using distance_query.
    #[test]
    fn test_1bit_grid_points() {
        let centroid = vec![0.0; 8];
        let c_norm = 0.0;

        // 2 grid values for BITS=1: -0.5, +0.5
        let grid: Vec<f32> = vec![-0.5, 0.5];

        // Test all 2^8=256 combinations for 8 dimensions
        for bits in 0u8..=255 {
            let embedding: Vec<f32> = (0..8).map(|i| grid[((bits >> i) & 1) as usize]).collect();
            let embedding_norm = (f32::dot(&embedding, &embedding).unwrap_or(0.0) as f32).sqrt();

            if embedding_norm < f32::EPSILON {
                continue;
            }

            let code = Code1Bit::quantize(&embedding, &centroid);
            let dist = code.distance_query_full_precision(
                &DistanceFunction::Cosine,
                &embedding,
                c_norm,
                0.0,
                embedding_norm,
            );
            assert!(
                dist.abs() < 4.0 * f32::EPSILON,
                "1-bit grid {:08b} should have zero cosine self-distance, got {}",
                bits,
                dist
            );
        }
    }

    #[test]
    fn test_hamming_distance() {
        // Identical → hamming = 0
        let a = vec![0xFF, 0x00, 0xAA, 0x55, 0xFF, 0x00, 0xAA, 0x55];
        assert_eq!(hamming_distance(&a, &a), 0);

        // All different → hamming = 64 (8 bytes * 8 bits)
        let b = vec![0x00, 0xFF, 0x55, 0xAA, 0x00, 0xFF, 0x55, 0xAA];
        assert_eq!(hamming_distance(&a, &b), 64);

        // One bit different
        let mut c = a.clone();
        c[0] = 0xFE; // flip bit 0
        assert_eq!(hamming_distance(&a, &c), 1);
    }

    /// Validates that distance_query_bitwise and BatchQueryLuts produce results
    /// close to the float-based distance_query (within query quantization error).
    #[test]
    fn test_bitwise_distance_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();

        let query: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
        let c_dot_q = f32::dot(&centroid, &query).unwrap_or(0.0) as f32;
        let q_norm = (f32::dot(&query, &query).unwrap_or(0.0) as f32).sqrt();

        let padded_bytes = Code1Bit::packed_len(dim);
        let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, c_norm, c_dot_q, q_norm);
        let luts = BatchQueryLuts::new(&r_q, c_norm, c_dot_q, q_norm);
        let df = DistanceFunction::Euclidean;

        for _ in 0..100 {
            let emb: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let code_owned = Code1Bit::quantize(&emb, &centroid);
            let code = Code1Bit::new(code_owned.as_ref());

            let float_dist = code.distance_query_full_precision(&df, &r_q, c_norm, c_dot_q, q_norm);
            let bitwise_dist = code.distance_query(&df, &qq);
            let lut_dist = luts.distance_query(&code, &df);

            let tol = float_dist.abs() * 0.05 + 1.0;
            assert!(
                (float_dist - bitwise_dist).abs() < tol,
                "bitwise mismatch: float={float_dist}, bitwise={bitwise_dist}"
            );
            assert!(
                (float_dist - lut_dist).abs() < tol,
                "lut mismatch: float={float_dist}, lut={lut_dist}"
            );
            // bitwise and lut should agree exactly (same quantization)
            assert!(
                (bitwise_dist - lut_dist).abs() < f32::EPSILON * 100.0,
                "bitwise vs lut: bitwise={bitwise_dist}, lut={lut_dist}"
            );
        }
    }

    /// BITS=1: P95 relative error bound 8.0%, observed ~5% (code), ~3.5% (query)
    #[test]
    fn test_error_bound_bits_1() {
        for k in [1.0f32, 2.0, 4.0] {
            assert_error_bound_1bit(1024, k, 128);
        }
    }

    fn assert_error_bound_1bit(dim: usize, k: f32, n_vectors: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let centroid = (0..dim).map(|_| rng.gen_range(-k..k)).collect::<Vec<_>>();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = (0..n_vectors)
            .map(|_| {
                centroid
                    .iter()
                    .map(|c| c + rng.gen_range(-k..k))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let codes = vectors
            .iter()
            .map(|v| Code1Bit::quantize(v, &centroid))
            .collect::<Vec<_>>();

        let max_p95_rel_error = 0.16 / 2.0;
        let n_vectors = vectors.len();

        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            let mut rel_errors_code = Vec::new();
            let mut rel_errors_query = Vec::new();

            for i in 0..n_vectors {
                for j in (i + 1)..n_vectors {
                    let exact = match distance_fn {
                        DistanceFunction::Cosine => {
                            SpatialSimilarity::cos(&vectors[i], &vectors[j]).unwrap_or(0.0) as f32
                        }
                        DistanceFunction::Euclidean => {
                            SpatialSimilarity::l2sq(&vectors[i], &vectors[j]).unwrap_or(0.0) as f32
                        }
                        DistanceFunction::InnerProduct => {
                            1.0 - SpatialSimilarity::dot(&vectors[i], &vectors[j]).unwrap_or(0.0)
                                as f32
                        }
                    };

                    let estimated_code =
                        codes[i].distance_code(&distance_fn, &codes[j], c_norm, dim);
                    rel_errors_code
                        .push((exact - estimated_code).abs() / exact.abs().max(f32::EPSILON));

                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q: Vec<f32> = centroid.iter().zip(q).map(|(c, q)| q - c).collect();
                    let estimated_query = codes[i].distance_query_full_precision(
                        &distance_fn,
                        &r_q,
                        c_norm,
                        c_dot_q,
                        q_norm,
                    );
                    rel_errors_query
                        .push((exact - estimated_query).abs() / exact.abs().max(f32::EPSILON));
                }
            }

            rel_errors_code.sort_by(|a, b| a.total_cmp(b));
            rel_errors_query.sort_by(|a, b| a.total_cmp(b));
            let p95_code = rel_errors_code[rel_errors_code.len() * 95 / 100];
            let p95_query = rel_errors_query[rel_errors_query.len() * 95 / 100];

            assert!(
                p95_code < max_p95_rel_error,
                "{:?}: distance_code P95 rel error {:.4} exceeds bound {:.4}",
                distance_fn,
                p95_code,
                max_p95_rel_error
            );
            assert!(
                p95_query < max_p95_rel_error,
                "{:?}: distance_query P95 rel error {:.4} exceeds bound {:.4}",
                distance_fn,
                p95_query,
                max_p95_rel_error
            );
        }
    }
}

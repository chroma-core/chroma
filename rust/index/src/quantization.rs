//! Extended RaBitQ quantization for approximate nearest neighbor search.
//!
//! ## Assumptions
//!
//! Data, centroid, and query vectors have been transformed by the same random rotation.
//!
//! ## Notation
//!
//! | Symbol | Description |
//! |--------|-------------|
//! | `c` | Cluster centroid |
//! | `d` | Original data vector |
//! | `r` | Data residual (`d - c`) |
//! | `q` | Query vector |
//! | `r_q` | Query residual (`q - c`) |
//! | `n = r / ‖r‖` | Normalized data residual |
//! | `g` | Grid point (approximates direction of `n`) |
//!
//! ## Stored Values
//!
//! | Field | Value | Description |
//! |-------|-------|-------------|
//! | `bytes` | packed codes | Encodes grid point `g` |
//! | `correction` | `⟨g, n⟩` | For estimating `⟨n, r_q⟩` |
//! | `norm` | `‖r‖` | Data residual norm |
//! | `radial` | `⟨r, c⟩` | Residual dot centroid |
//!
//! ## Inner Product Estimation
//!
//! The key insight is that `⟨n, r_q⟩` can be estimated using the quantized vector:
//!
//! ```text
//! ⟨n, r_q⟩ ≈ ⟨g, r_q⟩ / ⟨g, n⟩
//! ```
//!
//! where `⟨g, r_q⟩` is computed at query time from packed codes, and `⟨g, n⟩`
//! is stored as the correction factor.
//!
//! ## Distance Estimation
//!
//! For original data vector `d = c + r` and query `q`:
//!
//! ```text
//! ⟨d, q⟩ = ⟨c + r, q⟩
//!        = ⟨c, q⟩ + ⟨r, q⟩
//!        = ⟨c, q⟩ + ‖r‖ * ⟨n, q⟩
//!        = ⟨c, q⟩ + ‖r‖ * ⟨n, c + r_q⟩
//!        = ⟨c, q⟩ + ⟨r, c⟩ + ‖r‖ * ⟨n, r_q⟩
//! ```
//!
//! For two data vectors `d_a = c + r_a` and `d_b = c + r_b` in the same cluster:
//!
//! ```text
//! ⟨d_a, d_b⟩ = ⟨c + r_a, c + r_b⟩
//!            = ⟨c, c⟩ + ⟨c, r_b⟩ + ⟨r_a, c⟩ + ⟨r_a, r_b⟩
//!            = ‖c‖² + ⟨r_a, c⟩ + ⟨r_b, c⟩ + ‖r_a‖ * ‖r_b‖ * ⟨n_a, n_b⟩
//!
//! ⟨n_a, n_b⟩ ≈ ⟨g_a, n_b⟩ / ⟨g_a, n_a⟩
//!            ≈ ⟨g_a, g_b⟩ / (⟨g_a, n_a⟩ * ⟨g_b, n_b⟩)
//!
//! ‖d‖² = ⟨d, d⟩
//!      = ‖c‖² + 2 * ⟨r, c⟩ + ‖r‖²
//! ```
//!
//! Distance formulas:
//!
//! ```text
//! Cosine:       1 - ⟨a, b⟩ / (‖a‖ * ‖b‖)
//! Euclidean:    ‖a‖² + ‖b‖² - 2 * ⟨a, b⟩
//! InnerProduct: 1 - ⟨a, b⟩
//! ```

use std::mem::size_of;

use bitpacking::{BitPacker, BitPacker8x};
use rand::Rng;
use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;

/// Header for quantized code containing metadata.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CodeHeader {
    correction: f32,
    norm: f32,
    radial: f32,
}

/// Quantized representation of a data residual.
///
/// Generic over the backing storage `T`, allowing both owned (`Vec<u8>`) and
/// borrowed (`&[u8]`) representations. The const parameter `BITS` controls
/// the number of bits per quantization code.
///
/// Byte layout:
/// - `[0..12]` CodeHeader (correction, norm, radial as f32)
/// - `[12..]` packed codes
pub struct Code<T, const BITS: u8 = 4>(T);

impl<T, const BITS: u8> Code<T, BITS> {
    /// Quantization range ceiling: `2^(BITS-1)`
    const CEIL: u8 = 1 << (BITS - 1);

    /// Grid offset for centering codes.
    const GRID_OFFSET: f32 = 0.5;

    /// Wraps existing bytes as a quantized code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }

    /// Returns the packed byte length for a given dimension.
    pub fn packed_len(dim: usize) -> usize {
        Self::padded_dim(dim) * BITS as usize / 8
    }

    /// Returns the padded dimension.
    ///
    /// For BITS=1, pads to u64 boundary (64) for efficient popcount.
    ///     We don't need to pad to 256 because we are using bit-level
    ///     operations instead of unpacking the grid. i.e. hamming_distance and
    ///     hamming_distance and signed_dot, not BitPacker8x decompression.
    /// For BITS≥2, pads to BitPacker8x block size (256).
    fn padded_dim(dim: usize) -> usize {
        if BITS == 1 {
            dim.div_ceil(64) * 64
        } else {
            dim.div_ceil(BitPacker8x::BLOCK_LEN) * BitPacker8x::BLOCK_LEN
        }
    }
}

impl<T: AsRef<[u8]>, const BITS: u8> Code<T, BITS> {
    /// Returns the correction factor `⟨g, n⟩`.
    pub fn correction(&self) -> f32 {
        self.header().correction
    }

    /// Estimates distance between two original data vectors `d_a` and `d_b`.
    ///
    /// See module-level documentation for the full derivation.
    pub fn distance_code<U: AsRef<[u8]>>(
        &self,
        distance_function: &DistanceFunction,
        code: &Code<U, BITS>,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let norm_a = self.norm();
        let norm_b = code.norm();
        let radial_a = self.radial();
        let radial_b = code.radial();
        let correction_a = self.correction();
        let correction_b = code.correction();

        // 3.2 Constructing an Unbiased Estimator for Distance Estimation
        // The key achievement is estimating the product of the data and query vectors:
        //     ⟨o, q⟩ in the paper and ⟨d_a, d_b⟩ in our code.
        // Theorem 3.2:
        //     ⟨o, q⟩ = E[⟨o¯, q⟩ / ⟨o¯, o⟩]
        //
        //     Where the error is bounded by O(1/√D) with high probability.
        //     Namely, as D → ∞, the error approaches 0.
        //     The constant factor of O depends on the norms of the data and query vectors.
        // Error sources
        // - Angular displacement caused by mapping o to the nearest hypercube vertex.
        // - Norm of the data and query vectors being non-unit.
        // How they are corrected:
        // - The division by ⟨ō, o⟩ corrects on average (in expectation) for
        //     the angular displacement caused by mapping o to the nearest hypercube vertex.
        //     - Without the division, ⟨ō, q⟩ underestimates ⟨o, q⟩ because ⟨ō, o⟩ is less than 1 — the quantization rotated ō away from o, attenuating the signal. Dividing by ⟨ō, o⟩ undoes that attenuation, recovering ⟨o, q⟩ from the signal term.
        // - The error that remains after the correction is the noise term ⟨ō, q⊥⟩ divided by ⟨ō, o⟩
        //     (which is bounded by O(1/√D))
        let g_a_dot_g_b = if BITS == 1 {
            // For BITS=1, each g[i] is either +0.5 or -0.5.
            // The dot product ⟨g_a, g_b⟩ counts where the two codes agree vs. disagree.
            //     When both bits match: (+0.5)(+0.5) or (-0.5)(-0.5) = +0.25
            //     When bits differ: (+0.5)(-0.5) or (-0.5)(+0.5) = -0.25
            // So ⟨g_a, g_b⟩ = 0.25 · (agreements − disagreements).
            //     And since agreements + disagreements = D, we get
            //     agreements − disagreements = D − 2·hamming.
            // Hence:
            //     ⟨g_a, g_b⟩ = 0.25 · (D − 2 · hamming(a, b))
            let hamming = hamming_distance(self.packed(), code.packed());
            0.25 * (dim as f32 - 2.0 * hamming as f32)
        } else {
            let g_a = self.unpack_grid(dim);
            let g_b = code.unpack_grid(dim);
            f32::dot(&g_a, &g_b).unwrap_or(0.0) as f32
        };

        // ⟨n_a, n_b⟩ ≈ ⟨g_a, g_b⟩ / (⟨g_a, n_a⟩ * ⟨g_b, n_b⟩)
        let n_a_dot_n_b = g_a_dot_g_b / (correction_a * correction_b);

        // ⟨d_a, d_b⟩ = ‖c‖² + ⟨r_a, c⟩ + ⟨r_b, c⟩ + ‖r_a‖ * ‖r_b‖ * ⟨n_a, n_b⟩
        let d_a_dot_d_b = c_norm * c_norm + radial_a + radial_b + norm_a * norm_b * n_a_dot_n_b;

        match distance_function {
            DistanceFunction::Cosine => {
                // ‖d‖² = ‖c‖² + 2⟨r, c⟩ + ‖r‖²
                let d_a_norm_sq = c_norm * c_norm + 2.0 * radial_a + norm_a * norm_a;
                let d_b_norm_sq = c_norm * c_norm + 2.0 * radial_b + norm_b * norm_b;
                1.0 - d_a_dot_d_b / (d_a_norm_sq.sqrt() * d_b_norm_sq.sqrt()).max(f32::EPSILON)
            }
            DistanceFunction::Euclidean => {
                // ‖d_a - d_b‖² = ‖d_a‖² + ‖d_b‖² - 2⟨d_a, d_b⟩
                let d_a_norm_sq = c_norm * c_norm + 2.0 * radial_a + norm_a * norm_a;
                let d_b_norm_sq = c_norm * c_norm + 2.0 * radial_b + norm_b * norm_b;
                d_a_norm_sq + d_b_norm_sq - 2.0 * d_a_dot_d_b
            }
            DistanceFunction::InnerProduct => 1.0 - d_a_dot_d_b,
        }
    }

    /// Estimates distance between original data vector `d` and query `q`.
    ///
    /// See module-level documentation for the full derivation.
    pub fn distance_query(
        &self,
        distance_function: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        let norm = self.norm();
        let radial = self.radial();
        let correction = self.correction();

        let g_dot_r_q = if BITS == 1 {
            // For BITS=1, g[i] is +0.5 when bit=1 and −0.5 when bit=0.
            // ⟨g, r_q⟩ = Σ g[i] · r_q[i]
            //           = 0.5 · Σ_{bit=1} r_q[i]  −  0.5 · Σ_{bit=0} r_q[i]
            //           = 0.5 · Σ sign(g[i]) · r_q[i]
            //
            // where
            // - sign(g[i]) = +1 when bit=1, −1 when bit=0.
            // - S₁ = Σ_{bit=1} r_q[i]
            // - S₀ = Σ_{bit=0} r_q[i]
            0.5 * signed_dot(self.packed(), r_q)
        } else {
            let g = self.unpack_grid(r_q.len());
            f32::dot(&g, r_q).unwrap_or(0.0) as f32
        };

        // ⟨r, r_q⟩ ≈ ‖r‖ * ⟨g, r_q⟩ / ⟨g, n⟩
        let r_dot_r_q = norm * g_dot_r_q / correction;

        // ⟨d, q⟩ = ⟨c, q⟩ + ⟨r, c⟩ + ⟨r, r_q⟩
        let d_dot_q = c_dot_q + radial + r_dot_r_q;

        match distance_function {
            DistanceFunction::Cosine => {
                // ‖d‖² = ‖c‖² + 2⟨r, c⟩ + ‖r‖²
                let d_norm_sq = c_norm * c_norm + 2.0 * radial + norm * norm;
                1.0 - d_dot_q / (d_norm_sq.sqrt() * q_norm).max(f32::EPSILON)
            }
            DistanceFunction::Euclidean => {
                // ‖d‖² = ‖c‖² + 2⟨r, c⟩ + ‖r‖²
                let d_norm_sq = c_norm * c_norm + 2.0 * radial + norm * norm;
                d_norm_sq + q_norm * q_norm - 2.0 * d_dot_q
            }
            DistanceFunction::InnerProduct => 1.0 - d_dot_q,
        }
    }

    /// Returns the header containing correction, norm, and radial.
    /// Uses unaligned read since Vec<u8> only guarantees 1-byte alignment.
    fn header(&self) -> CodeHeader {
        bytemuck::pod_read_unaligned(&self.0.as_ref()[..size_of::<CodeHeader>()])
    }

    /// Returns the data residual norm `‖r‖`.
    pub fn norm(&self) -> f32 {
        self.header().norm
    }

    /// Returns the packed codes portion of the buffer.
    fn packed(&self) -> &[u8] {
        &self.0.as_ref()[size_of::<CodeHeader>()..]
    }

    /// Returns the radial component `⟨r, c⟩`.
    pub fn radial(&self) -> f32 {
        self.header().radial
    }

    /// Returns the size of buffer in bytes.
    pub fn size(dim: usize) -> usize {
        size_of::<CodeHeader>() + Self::packed_len(dim)
    }

    /// Unpacks the grid point from the packed codes.
    fn unpack_grid(&self, dim: usize) -> Vec<f32> {
        let packed = self.packed();
        let bitpacker = BitPacker8x::new();
        let mut codes = vec![0u32; Self::padded_dim(dim)];

        for (i, chunk) in codes.chunks_mut(BitPacker8x::BLOCK_LEN).enumerate() {
            let offset = i * BitPacker8x::compressed_block_size(BITS);
            bitpacker.decompress(&packed[offset..], chunk, BITS);
        }

        let offset = f32::from(Self::CEIL) - Self::GRID_OFFSET;
        codes[..dim].iter().map(|&c| c as f32 - offset).collect()
    }
}

impl<T: AsRef<[u8]>, const BITS: u8> AsRef<[u8]> for Code<T, BITS> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<const BITS: u8> Code<Vec<u8>, BITS> {
    /// Quantizes a data vector relative to its cluster centroid.
    pub fn quantize(embedding: &[f32], centroid: &[f32]) -> Self {
        let r = embedding
            .iter()
            .zip(centroid)
            .map(|(e, c)| e - c)
            .collect::<Vec<_>>();
        let dim = r.len();
        let norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        let radial = f32::dot(&r, centroid).unwrap_or(0.0) as f32;

        // Early return for near-zero residual
        if dim == 0 || norm < f32::EPSILON {
            let header = CodeHeader {
                correction: 1.0,
                norm,
                radial,
            };
            let mut bytes = Vec::with_capacity(Self::size(dim));
            bytes.extend_from_slice(bytemuck::bytes_of(&header));
            bytes.resize(Self::size(dim), 0);
            return Self(bytes);
        }

        // 1-bit: sign-based quantization (no ray-walk needed). See section 3.1.3 of the paper.
        // The embedding is already rotated, so we only need to take the sign
        // of each bit.
        if BITS == 1 {
            // Build packed codes: [sign_bits]
            // Pack sign bits branchlessly, 8 floats → 1 byte at a time.
            // For each f32, the IEEE-754 sign bit is bit 31: 1 = negative, 0 = positive.
            // We want the packed bit to be 1 when val >= 0, so we invert the sign bit.
            // Processing a full chunk per byte eliminates all i/8 and i%8 index arithmetic
            // as used previously: `packed[i / 8] |= 1 << (i % 8);`
            let mut packed = vec![0u8; Self::packed_len(dim)];
            for (byte_ref, chunk) in packed.iter_mut().zip(r.chunks(8)) {
                let mut byte = 0u8;
                for (j, &val) in chunk.iter().enumerate() {
                    let sign = (val.to_bits() >> 31) as u8; // 1 if negative, 0 if non-negative
                    byte |= (sign ^ 1) << j;
                }
                *byte_ref = byte;
            }

            // abs_sum is computed in its own loop so rustc/LLVM can auto-vectorize it
            // with VABSPS + VADDPS (or equivalent).
            //
            // auto-vectorization can happen when a loop body has:
            // - No cross-iteration dependencies
            // - Pure float operations (abs + add)
            // - Sequential memory access over a contiguous slice
            //
            // So this line will get converted into:
            // - Load 8 floats at once into a 256-bit AVX register (YMM register)
            // - Apply VABSPS — "Vector ABSolute value Packed Single" — to all 8 lanes simultaneously (this is literally just masking off the sign bit on each f32 in parallel)
            // - Accumulate into a running sum register with VADDPS — "Vector ADD Packed Single"
            // - At the end, do a horizontal reduction across the 8 lanes to collapse into a single f32
            //
            // For a 1024-dimensional vector that's 128 iterations instead of 1024 scalar operations.
            let abs_sum: f32 = r.iter().map(|v| v.abs()).sum();

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
            let header = CodeHeader {
                correction,
                norm,
                radial,
            };
            // Build output: [header][packed_codes]
            let mut bytes = Vec::with_capacity(Self::size(dim));
            bytes.extend_from_slice(bytemuck::bytes_of(&header));
            bytes.extend_from_slice(&packed);
            return Self(bytes);
        }

        // Multi-bit ray-walk: find optimal grid point maximizing cosine similarity
        // max_t is when the largest magnitude component reaches max code
        let r_abs = r.iter().copied().map(f32::abs).collect::<Vec<_>>();
        let max_t = (f32::from(Self::CEIL) - 1.0 + f32::EPSILON)
            / r_abs.iter().copied().fold(f32::EPSILON, f32::max);

        // Collect critical t values: (t, dimension)
        let mut critical_ts = r_abs
            .iter()
            .enumerate()
            .flat_map(|(i, val)| (1..=(max_t * val) as u8).map(move |g| (f32::from(g) / val, i)))
            .collect::<Vec<_>>();
        critical_ts.sort_by(|(t1, _), (t2, _)| t1.total_cmp(t2));

        // Initialize grid point at t=0: all codes=0, grid values=sign(r[i])*0.5
        let mut code = vec![0u32; dim];
        let mut g_l2 = dim as f32 * Self::GRID_OFFSET * Self::GRID_OFFSET;
        let mut g_dot_r = r_abs.iter().sum::<f32>() * Self::GRID_OFFSET;

        // Walk along ray, track best cosine similarity
        let mut best_cosine = -1.0;
        let mut best_t = 0.0;

        for (t, i) in critical_ts {
            code[i] += 1;
            g_l2 += 2.0 * code[i] as f32;
            g_dot_r += r_abs[i];

            let cosine = g_dot_r / g_l2.sqrt() / norm;
            if cosine > best_cosine {
                best_cosine = cosine;
                best_t = t;
            }
        }

        // Reconstruct codes from best_t
        // At best_t, we crossed integer g = best_t * |r[i]| and entered cell g + 0.5
        // For positive r[i]: grid = g + 0.5, stored code = g + CEIL
        // For negative r[i]: grid = -(g + 0.5), stored code = CEIL - 1 - g
        for (code, val) in code.iter_mut().zip(&r) {
            let g = (best_t * val.abs()) as u32;
            *code = if *val >= 0.0 {
                g + Self::CEIL as u32
            } else {
                Self::CEIL as u32 - 1 - g
            };
        }

        // Compute correction factor: ⟨g, n⟩ = ⟨g, r⟩ / ‖r‖
        let offset = f32::from(Self::CEIL) - Self::GRID_OFFSET;
        let g = code.iter().map(|&c| c as f32 - offset).collect::<Vec<_>>();
        let correction = f32::dot(&g, &r).unwrap_or(0.0) as f32 / norm;

        // Pad to multiple of BLOCK_LEN
        code.resize(Self::padded_dim(dim), 0);

        // Pack using bitpacking
        let bitpacker = BitPacker8x::new();
        let mut packed = vec![0u8; Self::packed_len(dim)];

        for (i, chunk) in code.chunks(BitPacker8x::BLOCK_LEN).enumerate() {
            let offset = i * BitPacker8x::compressed_block_size(BITS);
            bitpacker.compress(chunk, &mut packed[offset..], BITS);
        }

        // Build output: [header][packed_codes]
        let header = CodeHeader {
            correction,
            norm,
            radial,
        };
        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(bytemuck::bytes_of(&header));
        bytes.extend_from_slice(&packed);

        Self(bytes)
    }
}

/// Computes hamming distance between two packed bit vectors.
///
/// Both slices must have the same length and that length must be a multiple of
/// 8 (guaranteed when `padded_dim` is a multiple of 64).
///
/// # Notes on SIMD optimization:
/// At the scalar level this is already near-optimal. Each iteration is three
/// instructions: load, XOR, POPCNT. On any modern x86 CPU with the popcnt
/// feature flag (which Rust can target with RUSTFLAGS="-C target-cpu=native"),
/// count_ones() on a u64 compiles directly to a single POPCNT instruction.
/// So for 1024-dim vectors we're doing 16 iterations of 64 bits and three
/// instructions each.
///
/// True SIMD speedup requires AVX-512 VPOPCNTDQ, which provides
/// _mm512_popcnt_epi64 — popcounting 8 u64 lanes simultaneously. That
/// processes 512 bits instead of 64 per iteration. So for 1024-dim vectors
/// we're doing 2 iterations.
// TODO use simsimd?
fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 8, 0);
    let mut count = 0u32;
    // Read 8 bytes at a time and count the number of ones in the XOR result.
    for i in (0..a.len()).step_by(8) {
        let a_word = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
        let b_word = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        count += (a_word ^ b_word).count_ones();
    }
    count
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
            signs[base]     = f32::from_bits(0x3F800000 | (((b >> 0) & 1) ^ 1) << 31);
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
///   - 4 bit planes of the quantized query (each ceil(dim/8) bytes)
///   - v_l, delta, sum_q_u, popcount_x_b: scalar factors for Equation 20
pub struct QuantizedQuery {
    /// Bit planes of the quantized query: bit_planes[j] is the j-th bit of
    /// each q_u[i], packed into bytes.  bit_planes[j] has ceil(dim/64)*8 bytes
    /// to match the padded data code layout.
    pub bit_planes: Vec<Vec<u8>>,
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
    /// Section 3.3.1 of the paper.
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
        Self::new_with_rng(r_q, b_q, padded_bytes, c_norm, c_dot_q, q_norm, &mut rand::thread_rng())
    }

    /// Same as `new` but uses the provided RNG for randomized rounding.
    /// Use this for reproducible tests.
    pub fn new_with_rng(
        r_q: &[f32],
        b_q: u8,
        padded_bytes: usize,
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
        rng: &mut impl Rng,
    ) -> Self {
        let max_val = (1u32 << b_q) - 1;

        let v_min = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_max = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_max - v_min;
        let delta = if range > f32::EPSILON { range / max_val as f32 } else { 1.0 };

        // Quantize each element to a B_q-bit unsigned integer using randomized
        // rounding (paper Eq. 18): q̄_u[i] := floor((q'[i] − v_l)/Δ + u_i) where
        // u_i ~ Uniform[0, 1]. This makes the quantization error unbiased in
        // expectation, preserving the theoretical guarantee.
        let q_u: Vec<u32> = r_q
            .iter()
            .map(|&v| {
                let x = (v - v_min) / delta;
                let u: f32 = rng.gen();
                ((x + u).floor() as u32).min(max_val)
            })
            .collect();

        let sum_q_u: u32 = q_u.iter().sum();

        // Decompose into bit planes.  bit_planes[j][byte] holds the j-th bit
        // of q_u[i] for i in [byte*8 .. byte*8+8], packed LSB-first.
        // TODO: justify this extra computation is worth the 2X space savings
        // (at B=4) for q.
        let mut bit_planes = vec![vec![0u8; padded_bytes]; b_q as usize];
        for (i, &qu) in q_u.iter().enumerate() {
            for j in 0..b_q as usize {
                if (qu >> j) & 1 == 1 {
                    bit_planes[j][i / 8] |= 1 << (i % 8);
                }
            }
        }

        // TODO need sum_x_bar_b? Equation 20.
        Self { bit_planes, v_l: v_min, delta, sum_q_u, b_q, c_norm, c_dot_q, q_norm }
    }
}

impl<T: AsRef<[u8]>> Code<T, 1> {
    /// Bitwise distance estimation using the paper's Section 3.3 approach.
    ///
    /// Instead of expanding packed bits to f32 signs and running a float dot
    /// product, this computes `⟨x_bar_b, q_bar_u⟩` using B_q rounds of
    /// AND + popcount on D-bit strings, then recovers the full distance
    /// estimate from the scalar factors.
    ///
    /// The inner product is derived from paper Equation 20:
    ///   ⟨x_bar, q_bar⟩ = (2Δ/√D) · ⟨x_b, q_u⟩
    ///                   + (2v_l/√D) · Σ x_b[i]
    ///                   - (Δ/√D) · Σ q_u[i]
    ///                   - √D · v_l
    ///
    /// But since we work with residuals (not unit vectors) and our g[i] values
    /// are ±0.5 (not ±1/√D), we adapt the derivation:
    ///
    ///   g[i] = sign(r[i]) × 0.5
    ///   g[i] · r_q[i] ≈ g[i] · (Δ · q_u[i] + v_l)
    ///
    /// ⟨g, r_q⟩ ≈ 0.5 · (2Δ · ⟨x_b, q_u⟩ + 2·v_l · popcount(x_b)
    ///                   - Δ · Σ q_u[i] - dim · v_l)
    ///
    /// where ⟨x_b, q_u⟩ = Σ_j 2^j · popcount(x_b AND q_u^(j))
    pub fn distance_query_bitwise(
        &self,
        distance_function: &DistanceFunction,
        qq: &QuantizedQuery,
        dim: usize,
    ) -> f32 {
        let norm = self.norm();
        let radial = self.radial();
        let correction = self.correction();
        let packed = self.packed();

        // ⟨x_b, q_u⟩ = Σ_j 2^j · popcount(x_b AND q_u^(j))
        // Each AND+popcount operates on the full D-bit string.
        let mut xb_dot_qu = 0u32;
        for (j, plane) in qq.bit_planes.iter().enumerate() {
            let mut plane_pop = 0u32;
            debug_assert!(packed.len() <= plane.len());
            for i in (0..packed.len()).step_by(8) {
                let x_word = u64::from_le_bytes(packed[i..i + 8].try_into().unwrap());
                let q_word = u64::from_le_bytes(plane[i..i + 8].try_into().unwrap());
                plane_pop += (x_word & q_word).count_ones();
            }
            xb_dot_qu += plane_pop << j;
        }

        // popcount(x_b) = number of set bits in the data code
        let popcount_xb = {
            let mut count = 0u32;
            for i in (0..packed.len()).step_by(8) {
                let word = u64::from_le_bytes(packed[i..i + 8].try_into().unwrap());
                count += word.count_ones();
            }
            count
        };

        // Recover ⟨g, r_q⟩ from the quantized inner product.
        //
        // g[i] = +0.5 when bit=1, −0.5 when bit=0
        // r_q[i] ≈ Δ · q_u[i] + v_l
        //
        // ⟨g, r_q⟩ = Σ g[i] · r_q[i]
        //           = 0.5 · Σ sign[i] · (Δ · q_u[i] + v_l)
        //           = 0.5 · (Δ · Σ sign[i] · q_u[i] + v_l · Σ sign[i])
        //
        // Σ sign[i] · q_u[i] = 2 · ⟨x_b, q_u⟩ − Σ q_u[i]
        //     (because sign[i] = 2·x_b[i] − 1, so sign[i]·q_u[i] = 2·x_b[i]·q_u[i] − q_u[i])
        //
        // Σ sign[i] = 2 · popcount(x_b) − dim
        let signed_dot_qu = 2.0 * xb_dot_qu as f32 - qq.sum_q_u as f32;
        let signed_sum = 2.0 * popcount_xb as f32 - dim as f32;

        let g_dot_r_q = 0.5 * (qq.delta * signed_dot_qu + qq.v_l * signed_sum);

        // From here on, same as the existing distance_query.
        let r_dot_r_q = norm * g_dot_r_q / correction;
        let d_dot_q = qq.c_dot_q + radial + r_dot_r_q;

        match distance_function {
            DistanceFunction::Cosine => {
                let d_norm_sq = qq.c_norm * qq.c_norm + 2.0 * radial + norm * norm;
                1.0 - d_dot_q / (d_norm_sq.sqrt() * qq.q_norm).max(f32::EPSILON)
            }
            DistanceFunction::Euclidean => {
                let d_norm_sq = qq.c_norm * qq.c_norm + 2.0 * radial + norm * norm;
                d_norm_sq + qq.q_norm * qq.q_norm - 2.0 * d_dot_q
            }
            DistanceFunction::InnerProduct => 1.0 - d_dot_q,
        }
    }
}

/// Pre-computed lookup tables for batch distance estimation (paper Section 3.3.2).
///
/// Splits the D-bit data code into D/4 nibbles.  For each nibble position,
/// precomputes a 16-entry LUT: the partial inner product between the nibble
/// of x_b and the corresponding 4 elements of the quantized query.
///
/// At scan time, each code's distance requires D/4 LUT lookups + accumulation
/// (no float expansion, no AND+popcount).
///
/// Why bitwise beats LUT:
// The working set sizes explain the gap:
//   - Bitwise: 4 bit planes x 128 bytes = 512 bytes of query data (fits in L1), plus 128 bytes per code. The inner loop is 4 rounds of 16 AND+popcount operations on u64 words -- 64 iterations of 3-instruction sequences.
//   - LUT: 256 nibble positions x 32 bytes per LUT entry = 8 KB of LUT data, plus 128 bytes per code. The inner loop is 256 iterations of nibble extraction + array indexing + accumulation -- more iterations, more cache pressure, and indirect addressing (table lookup) prevents pipelining.
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
        Self::new_with_rng(r_q, c_norm, c_dot_q, q_norm, &mut rand::thread_rng())
    }

    /// Same as `new` but uses the provided RNG for randomized rounding.
    pub fn new_with_rng(
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
        rng: &mut impl Rng,
    ) -> Self {
        let dim = r_q.len();
        let max_val = 15u32; // B_q = 4

        let v_l = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_r - v_l;
        let delta = if range > f32::EPSILON { range / max_val as f32 } else { 1.0 };

        let q_u: Vec<u32> = r_q
            .iter()
            .map(|&v| {
                let x = (v - v_l) / delta;
                let u: f32 = rng.gen();
                ((x + u).floor() as u32).min(max_val)
            })
            .collect();

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

        Self { luts, v_l, delta, sum_q_u, c_norm, c_dot_q, q_norm, dim }
    }

    /// Score a single 1-bit code using the precomputed LUTs.
    ///
    /// For each nibble of the packed data code, look up the partial inner
    /// product from the LUT and accumulate.  Then recover the full distance.
    pub fn distance_query(
        &self,
        code: &Code<&[u8], 1>,
        distance_function: &DistanceFunction,
    ) -> f32 {
        let norm = code.norm();
        let radial = code.radial();
        let correction = code.correction();
        let packed = code.packed();

        // ⟨x_b, q_u⟩ via LUT: iterate over nibbles of packed data.
        let mut xb_dot_qu = 0u32;
        let mut popcount_xb = 0u32;

        for (nib_idx, lut) in self.luts.iter().enumerate() {
            let byte_idx = nib_idx / 2;
            let byte = if byte_idx < packed.len() { packed[byte_idx] } else { 0 };
            let nibble = if nib_idx % 2 == 0 { byte & 0x0F } else { byte >> 4 };
            xb_dot_qu += lut[nibble as usize] as u32;
            popcount_xb += nibble.count_ones();
        }

        let signed_dot_qu = 2.0 * xb_dot_qu as f32 - self.sum_q_u as f32;
        let signed_sum = 2.0 * popcount_xb as f32 - self.dim as f32;

        let g_dot_r_q = 0.5 * (self.delta * signed_dot_qu + self.v_l * signed_sum);

        let r_dot_r_q = norm * g_dot_r_q / correction;
        let d_dot_q = self.c_dot_q + radial + r_dot_r_q;

        match distance_function {
            DistanceFunction::Cosine => {
                let d_norm_sq = self.c_norm * self.c_norm + 2.0 * radial + norm * norm;
                1.0 - d_dot_q / (d_norm_sq.sqrt() * self.q_norm).max(f32::EPSILON)
            }
            DistanceFunction::Euclidean => {
                let d_norm_sq = self.c_norm * self.c_norm + 2.0 * radial + norm * norm;
                d_norm_sq + self.q_norm * self.q_norm - 2.0 * d_dot_q
            }
            DistanceFunction::InnerProduct => 1.0 - d_dot_q,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code::<Vec<u8>>::quantize(&embedding, &centroid);

        // Verify accessors return finite values
        assert!(code.correction().is_finite());
        assert!(code.norm().is_finite());
        assert!(code.radial().is_finite());

        // Verify norm is ‖r‖ = ‖embedding - centroid‖
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

        // Verify buffer size
        assert_eq!(code.as_ref().len(), Code::<Vec<u8>>::size(embedding.len()));
    }

    #[test]
    fn test_size() {
        // Exactly one block (256)
        assert_eq!(Code::<Vec<u8>>::packed_len(256), 256 * 4 / 8); // 128 bytes
        assert_eq!(Code::<Vec<u8>>::size(256), 12 + 128); // 3 floats + packed

        // Non-aligned (300) - should pad to 512
        assert_eq!(Code::<Vec<u8>>::packed_len(300), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code::<Vec<u8>>::size(300), 12 + 256);

        // Two blocks (512)
        assert_eq!(Code::<Vec<u8>>::packed_len(512), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code::<Vec<u8>>::size(512), 12 + 256);
    }

    #[test]
    fn test_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code::<Vec<u8>>::quantize(&embedding, &embedding);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code::<Vec<u8>>::quantize(&embedding, &centroid);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);
    }

    /// Tests that grid points quantize exactly using distance_query.
    ///
    /// When an embedding lies exactly on a grid point and we query with itself,
    /// cosine distance should be zero (within floating point tolerance).
    #[test]
    fn test_grid_points() {
        let centroid = vec![0.0; 4];
        let c_norm = 0.0;

        // All 16 grid values for BITS=4: -7.5, -6.5, ..., 6.5, 7.5
        let grid: Vec<f32> = (0..16).map(|c| c as f32 - 7.5).collect();

        for &g0 in &grid {
            for &g1 in &grid {
                for &g2 in &grid {
                    for &g3 in &grid {
                        let embedding = vec![g0, g1, g2, g3];
                        let embedding_norm =
                            (f32::dot(&embedding, &embedding).unwrap_or(0.0) as f32).sqrt();

                        if embedding_norm < f32::EPSILON {
                            continue;
                        }

                        let code = Code::<Vec<u8>, 4>::quantize(&embedding, &centroid);
                        let dist = code.distance_query(
                            &DistanceFunction::Cosine,
                            &embedding,
                            c_norm,
                            0.0,
                            embedding_norm,
                        );
                        assert!(
                            dist.abs() < 4.0 * f32::EPSILON,
                            "Grid point {:?} should have zero cosine self-distance, got {}",
                            embedding,
                            dist
                        );
                    }
                }
            }
        }
    }

    // ==================== 1-bit tests ====================

    #[test]
    fn test_1bit_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code::<Vec<u8>, 1>::quantize(&embedding, &centroid);

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
        assert_eq!(
            code.as_ref().len(),
            Code::<Vec<u8>, 1>::size(embedding.len())
        );
    }

    #[test]
    fn test_1bit_size() {
        // 64-aligned (256 dims)
        assert_eq!(Code::<Vec<u8>, 1>::packed_len(256), 256 / 8); // 32 bytes
        assert_eq!(Code::<Vec<u8>, 1>::size(256), 12 + 32);

        // Non-aligned (300) - should pad to 320 (5 * 64)
        assert_eq!(Code::<Vec<u8>, 1>::packed_len(300), 320 / 8); // 40 bytes
        assert_eq!(Code::<Vec<u8>, 1>::size(300), 12 + 40);

        // 1024 dims
        assert_eq!(Code::<Vec<u8>, 1>::packed_len(1024), 128);
        assert_eq!(Code::<Vec<u8>, 1>::size(1024), 12 + 128);

        // 4096 dims
        assert_eq!(Code::<Vec<u8>, 1>::packed_len(4096), 512);
        assert_eq!(Code::<Vec<u8>, 1>::size(4096), 12 + 512);
    }

    #[test]
    fn test_1bit_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code::<Vec<u8>, 1>::quantize(&embedding, &embedding);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code::<Vec<u8>, 1>::quantize(&embedding, &centroid);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);
    }

    /// Reads bit `i` from packed 1-bit codes and returns the grid value (±0.5).
    fn read_1bit_grid(code: &Code<Vec<u8>, 1>, dim: usize) -> Vec<f32> {
        let packed = &code.as_ref()[size_of::<CodeHeader>()..];
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

        let code = Code::<Vec<u8>, 1>::quantize(&embedding, &centroid);
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
                let centroid: Vec<f32>  = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let r: Vec<f32> = embedding.iter().zip(&centroid).map(|(e, c)| e - c).collect();

                // quantize_lyon logic: sign bit = (IEEE sign bit) XOR 1
                let signs_lyon: Vec<u8> = r.iter()
                    .map(|&v| (v.to_bits() >> 31) as u8 ^ 1)
                    .collect();

                // quantize logic: for BITS=1, CEIL=1, ray-walk collapses to
                //   code[i] = 1 if r[i] >= 0, else 0
                let signs_quantize: Vec<u8> = r.iter()
                    .map(|&v| if v >= 0.0 { 1 } else { 0 })
                    .collect();

                assert_eq!(
                    signs_lyon, signs_quantize,
                    "sign mismatch at dim={dim}"
                );
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

            let code = Code::<Vec<u8>, 1>::quantize(&embedding, &centroid);
            let dist = code.distance_query(
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

        let padded_bytes = Code::<Vec<u8>, 1>::packed_len(dim);
        let qq = QuantizedQuery::new_with_rng(&r_q, 4, padded_bytes, c_norm, c_dot_q, q_norm, &mut rng);
        let luts = BatchQueryLuts::new_with_rng(&r_q, c_norm, c_dot_q, q_norm, &mut rng);
        let df = DistanceFunction::Euclidean;

        for _ in 0..100 {
            let emb: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let code_owned = Code::<Vec<u8>, 1>::quantize(&emb, &centroid);
            let code = Code::<&[u8], 1>::new(code_owned.as_ref());

            let float_dist = code.distance_query(&df, &r_q, c_norm, c_dot_q, q_norm);
            let bitwise_dist = code.distance_query_bitwise(&df, &qq, dim);
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
        }
    }

    /// BITS=1: P95 relative error bound 8.0%, observed ~5% (code), ~3.5% (query)
    #[test]
    fn test_error_bound_bits_1() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound::<1>(1024, k, 128);
        }
    }

    /// BITS=3: P95 relative error bound 2.0%, observed ~1.5% (code), ~1.0% (query)
    #[test]
    fn test_error_bound_bits_3() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound::<3>(1024, k, 128);
        }
    }

    /// BITS=4: P95 relative error bound 1.0%, observed ~0.65% (code), ~0.45% (query)
    #[test]
    fn test_error_bound_bits_4() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound::<4>(1024, k, 128);
        }
    }

    /// BITS=5: P95 relative error bound 0.5%, observed ~0.30% (code), ~0.21% (query)
    #[test]
    fn test_error_bound_bits_5() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound::<5>(1024, k, 128);
        }
    }

    /// BITS=6: P95 relative error bound 0.25%, observed ~0.14% (code), ~0.10% (query)
    #[test]
    fn test_error_bound_bits_6() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound::<6>(1024, k, 128);
        }
    }

    /// Asserts that quantization error is within expected bounds.
    ///
    /// Tests both `distance_code` and `distance_query` across all distance functions,
    /// verifying P95 relative error is below `0.16 / 2^BITS`.
    fn assert_error_bound<const BITS: u8>(dim: usize, k: f32, n_vectors: usize) {
        let mut rng = StdRng::seed_from_u64(42);

        // Generate centroid
        let centroid = (0..dim).map(|_| rng.gen_range(-k..k)).collect::<Vec<_>>();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();

        // Generate vectors shifted by centroid
        let vectors = (0..n_vectors)
            .map(|_| {
                centroid
                    .iter()
                    .map(|c| c + rng.gen_range(-k..k))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        // Quantize all vectors
        let codes = vectors
            .iter()
            .map(|v| Code::<Vec<u8>, BITS>::quantize(v, &centroid))
            .collect::<Vec<_>>();

        // Error bound: 0.16 / 2^BITS
        // BITS=2: 4.0%, BITS=3: 2.0%, BITS=4: 1.0%, BITS=5: 0.5%, BITS=6: 0.25%
        let max_p95_rel_error = 0.16 / (1 << BITS) as f32;

        // Test all distance functions
        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            let mut rel_errors_code = Vec::new();
            let mut rel_errors_query = Vec::new();

            // For each pair of vectors
            for i in 0..n_vectors {
                for j in (i + 1)..n_vectors {
                    // Exact distance using simsimd
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

                    // distance_code estimation
                    let estimated_code =
                        codes[i].distance_code(&distance_fn, &codes[j], c_norm, dim);
                    let abs_err_code = (exact - estimated_code).abs();
                    rel_errors_code.push(abs_err_code / exact.abs().max(f32::EPSILON));

                    // distance_query estimation (treat vectors[j] as query)
                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q = centroid
                        .iter()
                        .zip(q)
                        .map(|(c, q)| q - c)
                        .collect::<Vec<_>>();
                    let estimated_query =
                        codes[i].distance_query(&distance_fn, &r_q, c_norm, c_dot_q, q_norm);
                    let abs_err_query = (exact - estimated_query).abs();
                    rel_errors_query.push(abs_err_query / exact.abs().max(f32::EPSILON));
                }
            }

            // Calculate P95
            rel_errors_code.sort_by(|a, b| a.total_cmp(b));
            rel_errors_query.sort_by(|a, b| a.total_cmp(b));
            let p95_code = rel_errors_code[rel_errors_code.len() * 95 / 100];
            let p95_query = rel_errors_query[rel_errors_query.len() * 95 / 100];

            // Assert error bounds
            assert!(
                p95_code < max_p95_rel_error,
                "BITS={}, k={}, {:?}: distance_code P95 rel error {:.4} exceeds bound {:.4}",
                BITS,
                k,
                distance_fn,
                p95_code,
                max_p95_rel_error
            );
            assert!(
                p95_query < max_p95_rel_error,
                "BITS={}, k={}, {:?}: distance_query P95 rel error {:.4} exceeds bound {:.4}",
                BITS,
                k,
                distance_fn,
                p95_query,
                max_p95_rel_error
            );
        }
    }
}

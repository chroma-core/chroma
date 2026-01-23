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
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;

/// Quantized representation of a data residual.
///
/// Generic over the backing storage `T`, allowing both owned (`Vec<u8>`) and
/// borrowed (`&[u8]`) representations. The const parameter `BITS` controls
/// the number of bits per quantization code.
///
/// Byte layout:
/// - `[0..4]` correction (f32 LE)
/// - `[4..8]` norm (f32 LE)
/// - `[8..12]` radial (f32 LE)
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

    /// Returns the padded dimension (rounded up to BitPacker8x block size).
    fn padded_dim(dim: usize) -> usize {
        dim.div_ceil(BitPacker8x::BLOCK_LEN) * BitPacker8x::BLOCK_LEN
    }
}

impl<T: AsRef<[u8]>, const BITS: u8> Code<T, BITS> {
    /// Returns the correction factor `⟨g, n⟩`.
    pub fn correction(&self) -> f32 {
        *bytemuck::from_bytes(&self.0.as_ref()[0..4])
    }

    /// Returns the data residual norm `‖r‖`.
    pub fn norm(&self) -> f32 {
        *bytemuck::from_bytes(&self.0.as_ref()[4..8])
    }

    /// Returns the radial component `⟨r, c⟩`.
    pub fn radial(&self) -> f32 {
        *bytemuck::from_bytes(&self.0.as_ref()[8..12])
    }

    /// Returns the size of buffer in bytes.
    pub fn size(dim: usize) -> usize {
        3 * size_of::<f32>() + Self::packed_len(dim)
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

        let g_a = self.unpack_grid(dim);
        let g_b = code.unpack_grid(dim);
        let g_a_dot_g_b = f32::dot(&g_a, &g_b).unwrap_or(0.0) as f32;

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

        let g = self.unpack_grid(r_q.len());
        let g_dot_r_q = f32::dot(&g, r_q).unwrap_or(0.0) as f32;

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

    /// Unpacks the grid point from the packed codes.
    fn unpack_grid(&self, dim: usize) -> Vec<f32> {
        let Some(packed) = self.0.as_ref().get(3 * size_of::<f32>()..) else {
            return vec![0.0; dim];
        };

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
            let mut bytes = Vec::with_capacity(Self::size(dim));
            bytes.extend_from_slice(&1.0_f32.to_le_bytes()); // correction = 1.0
            bytes.extend_from_slice(&norm.to_le_bytes());
            bytes.extend_from_slice(&radial.to_le_bytes());
            bytes.resize(Self::size(dim), 0);
            return Self(bytes);
        }

        // Ray-walk: find optimal grid point maximizing cosine similarity
        // max_t is when the largest magnitude component reaches max code
        let r_abs = r.iter().copied().map(f32::abs).collect::<Vec<_>>();
        let max_t = f32::from(Self::CEIL) / r_abs.iter().copied().fold(f32::EPSILON, f32::max);

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

        // Reconstruct codes from best_t: signed code + CEIL -> stored [0, 2^BITS-1]
        for (code, val) in code.iter_mut().zip(&r) {
            let g_val = best_t * val + Self::CEIL as f32;
            *code = g_val as u32;
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

        // Build output: [correction][norm][radial][packed_codes]
        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(&correction.to_le_bytes());
        bytes.extend_from_slice(&norm.to_le_bytes());
        bytes.extend_from_slice(&radial.to_le_bytes());
        bytes.extend_from_slice(&packed);

        Self(bytes)
    }
}

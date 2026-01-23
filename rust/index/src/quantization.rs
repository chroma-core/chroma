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

    /// Returns the padded dimension (rounded up to BitPacker8x block size).
    fn padded_dim(dim: usize) -> usize {
        dim.div_ceil(BitPacker8x::BLOCK_LEN) * BitPacker8x::BLOCK_LEN
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

        // Ray-walk: find optimal grid point maximizing cosine similarity
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

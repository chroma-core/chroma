//! 4-bit RaBitQ quantization.
//!
//! Uses a ray-walk algorithm to find the optimal grid point in
//! {±0.5, ±1.5, ±2.5, ±3.5, ±4.5, ±5.5, ±6.5, ±7.5}^dim that maximises
//! cosine similarity with the data residual.

use std::mem::size_of;

use bitpacking::{BitPacker, BitPacker8x};
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;

use super::utils::{rabitq_distance_code, rabitq_distance_query, CodeHeader, RabitqCode};
use super::utils::padded_dim_4bit;

// ── Code4Bit ──────────────────────────────────────────────────────────────────

/// 4-bit RaBitQ quantized code.
///
/// Byte layout: `[CodeHeader (12 bytes)][packed 4-bit codes]`
///
/// Uses a ray-walk algorithm to find the optimal grid point in
/// {±0.5, ±1.5, ±2.5, ±3.5, ±4.5, ±5.5, ±6.5, ±7.5}^dim that maximises
/// cosine similarity with the data residual.
pub struct Code4Bit<T = Vec<u8>>(T);

impl<T: AsRef<[u8]>> RabitqCode for Code4Bit<T> {
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
        &self.0.as_ref()[size_of::<CodeHeader>()..]
    }
}

impl<T> Code4Bit<T> {
    /// Wraps existing bytes as a 4-bit code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }
}

impl<T: AsRef<[u8]>> Code4Bit<T> {
    /// Estimates distance between two original data vectors `d_a` and `d_b`.
    ///
    /// For 4-bit codes, `⟨g_a, g_b⟩` is computed by unpacking the grid vectors
    /// and taking their dot product.
    pub fn distance_code<U: AsRef<[u8]>>(
        &self,
        distance_fn: &DistanceFunction,
        other: &Code4Bit<U>,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let g_a = self.unpack_grid(dim);
        let g_b = other.unpack_grid(dim);
        let g_a_dot_g_b = f32::dot(&g_a, &g_b).unwrap_or(0.0) as f32;
        rabitq_distance_code(g_a_dot_g_b, self, other, c_norm, distance_fn)
    }

    /// Estimates distance from data vector `d` to query `q`.
    ///
    /// For 4-bit codes, `⟨g, r_q⟩` is computed by unpacking the grid vector
    /// and taking its dot product with the query residual.
    pub fn distance_query(
        &self,
        distance_fn: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        let g = self.unpack_grid(r_q.len());
        let g_dot_r_q = f32::dot(&g, r_q).unwrap_or(0.0) as f32;
        rabitq_distance_query(g_dot_r_q, self, c_norm, c_dot_q, q_norm, distance_fn)
    }

    /// Unpacks the grid point from the packed 4-bit codes.
    fn unpack_grid(&self, dim: usize) -> Vec<f32> {
        const BITS: u8 = 4;
        const CEIL: u8 = 8; // 1 << (BITS - 1)
        const GRID_OFFSET: f32 = 0.5;
        let packed = self.packed();
        let bitpacker = BitPacker8x::new();
        let mut codes = vec![0u32; padded_dim_4bit(dim)];

        for (i, chunk) in codes.chunks_mut(BitPacker8x::BLOCK_LEN).enumerate() {
            let offset = i * BitPacker8x::compressed_block_size(BITS);
            bitpacker.decompress(&packed[offset..], chunk, BITS);
        }

        let offset = f32::from(CEIL) - GRID_OFFSET;
        codes[..dim].iter().map(|&c| c as f32 - offset).collect()
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for Code4Bit<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Code4Bit {
    const BITS: u8 = 4;
    const CEIL: u8 = 8; // 1 << (BITS - 1)
    const GRID_OFFSET: f32 = 0.5;

    /// Packed byte length for a given dimension.
    pub fn packed_len(dim: usize) -> usize {
        padded_dim_4bit(dim) * Self::BITS as usize / 8
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<CodeHeader>() + Self::packed_len(dim)
    }

    /// Quantizes a data vector relative to its cluster centroid (4-bit ray-walk).
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
            bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader {
                correction: 1.0,
                norm,
                radial,
            }));
            bytes.resize(Self::size(dim), 0);
            return Self(bytes);
        }

        // Multi-bit ray-walk: find optimal grid point maximizing cosine similarity.
        // max_t is when the largest magnitude component reaches max code.
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

        // Reconstruct codes from best_t.
        // At best_t, we crossed integer g = best_t * |r[i]| and entered cell g + 0.5.
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

        // Pad to multiple of BLOCK_LEN and pack using bitpacking.
        code.resize(padded_dim_4bit(dim), 0);
        let bitpacker = BitPacker8x::new();
        let mut packed = vec![0u8; Self::packed_len(dim)];
        for (i, chunk) in code.chunks(BitPacker8x::BLOCK_LEN).enumerate() {
            let off = i * BitPacker8x::compressed_block_size(Self::BITS);
            bitpacker.compress(chunk, &mut packed[off..], Self::BITS);
        }

        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader {
            correction,
            norm,
            radial,
        }));
        bytes.extend_from_slice(&packed);
        Self(bytes)
    }
}

/// 4-bit code, for backward compatibility.
pub type Code<T> = Code4Bit<T>;

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use simsimd::SpatialSimilarity;

    use super::*;
    use crate::quantization::RabitqCode;

    #[test]
    fn test_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code4Bit::quantize(&embedding, &centroid);

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
        assert_eq!(code.as_ref().len(), Code4Bit::size(embedding.len()));
    }

    #[test]
    fn test_size() {
        // Exactly one block (256)
        assert_eq!(Code4Bit::packed_len(256), 256 * 4 / 8); // 128 bytes
        assert_eq!(Code4Bit::size(256), 12 + 128); // 3 floats + packed

        // Non-aligned (300) - should pad to 512
        assert_eq!(Code4Bit::packed_len(300), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code4Bit::size(300), 12 + 256);

        // Two blocks (512)
        assert_eq!(Code4Bit::packed_len(512), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code4Bit::size(512), 12 + 256);
    }

    #[test]
    fn test_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code4Bit::quantize(&embedding, &embedding);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code4Bit::quantize(&embedding, &centroid);
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

                        let code = Code4Bit::quantize(&embedding, &centroid);
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

    /// BITS=4: P95 relative error bound 1.0%, observed ~0.65% (code), ~0.45% (query)
    #[test]
    fn test_error_bound_bits_4() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound_4bit(1024, k, 128);
        }
    }

    fn assert_error_bound_4bit(dim: usize, k: f32, n_vectors: usize) {
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
            .map(|v| Code4Bit::quantize(v, &centroid))
            .collect::<Vec<_>>();

        let max_p95_rel_error = 0.16 / 16.0;
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
                    let estimated_query =
                        codes[i].distance_query(&distance_fn, &r_q, c_norm, c_dot_q, q_norm);
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

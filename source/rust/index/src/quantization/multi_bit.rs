//! 4-bit RaBitQ quantization.
//!
//! Uses a ray-walk algorithm to find the optimal grid point in
//! {±0.5, ±1.5, ±2.5, ±3.5, ±4.5, ±5.5, ±6.5, ±7.5}^dim that maximises
//! cosine similarity with the data residual.

use std::mem::size_of;

use bitpacking::{BitPacker, BitPacker8x};
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;

use bytemuck::{Pod, Zeroable};

use super::{rabitq_distance_code, rabitq_distance_query};
use super::{Code, QuantizedQuery};

/// Byte header for 4-bit quantized codes. 12 bytes.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub(super) struct CodeHeader4Bit {
    pub correction: f32,
    pub norm: f32,
    pub radial: f32,
}

// ── Code<4, T> ────────────────────────────────────────────────────────────────

impl<T> Code<4, T> {
    const BITS: u8 = 4;
    const CEIL: u8 = 8; // 1 << (BITS - 1)
    const GRID_OFFSET: f32 = 0.5;

    /// Wraps existing bytes as a 4-bit code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }

    /// Packed byte length for a given dimension.
    pub fn packed_len(dim: usize) -> usize {
        padded_dim_4bit(dim) * 4 / 8
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<CodeHeader4Bit>() + Self::packed_len(dim)
    }

    /// Unpacks 4-bit packed codes to grid values in
    /// {-7.5, ..., -0.5, +0.5, ..., +7.5}^dim.
    fn unpack_grid_from(packed: &[u8], dim: usize) -> Vec<f32> {
        let bitpacker = BitPacker8x::new();
        let mut codes = vec![0u32; padded_dim_4bit(dim)];
        for (i, chunk) in codes.chunks_mut(BitPacker8x::BLOCK_LEN).enumerate() {
            let offset = i * BitPacker8x::compressed_block_size(Self::BITS);
            bitpacker.decompress(&packed[offset..], chunk, Self::BITS);
        }
        let offset = f32::from(Self::CEIL) - Self::GRID_OFFSET;
        codes[..dim].iter().map(|&c| c as f32 - offset).collect()
    }
}

impl<T: AsRef<[u8]>> Code<4, T> {
    pub(super) fn header(&self) -> CodeHeader4Bit {
        bytemuck::pod_read_unaligned(&self.0.as_ref()[..size_of::<CodeHeader4Bit>()])
    }

    /// Packed quantization codes (excluding the header).
    pub(super) fn packed(&self) -> &[u8] {
        &self.0.as_ref()[size_of::<CodeHeader4Bit>()..]
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
        // Decode one fixed-size block on the stack rather than allocating a
        // float vector for every centroid scored during navigation.
        let bitpacker = BitPacker8x::new();
        let mut decoded = [0u32; BitPacker8x::BLOCK_LEN];
        let mut grid = [0.0f32; BitPacker8x::BLOCK_LEN];
        let mut g_dot_r_q = 0.0;
        for (block, query) in r_q.chunks(BitPacker8x::BLOCK_LEN).enumerate() {
            let offset = block * BitPacker8x::compressed_block_size(Self::BITS);
            bitpacker.decompress(&self.packed()[offset..], &mut decoded, Self::BITS);
            for (value, &code) in grid.iter_mut().zip(&decoded).take(query.len()) {
                *value = code as f32 - 7.5;
            }
            g_dot_r_q += f32::dot(&grid[..query.len()], query).unwrap_or(0.0) as f32;
        }
        let h = self.header();
        rabitq_distance_query(
            g_dot_r_q,
            h.correction,
            h.norm,
            h.radial,
            c_norm,
            c_dot_q,
            q_norm,
            distance_fn,
        )
    }

    /// Scores a four-bit stored code against the query's four-bit scalar codes.
    /// The query retains its original norm and residual correction factors,
    /// matching the one-bit scorer's distance convention.
    pub fn distance_quantized_query(
        &self,
        distance_fn: &DistanceFunction,
        query: &QuantizedQuery,
        dim: usize,
    ) -> f32 {
        assert!(query.padded_bytes * 8 >= dim);
        let bitpacker = BitPacker8x::new();
        let mut decoded = [0u32; BitPacker8x::BLOCK_LEN];
        let mut code_sum = 0u32;
        let mut integer_dot = 0u32;
        for start in (0..dim).step_by(BitPacker8x::BLOCK_LEN) {
            let offset =
                start / BitPacker8x::BLOCK_LEN * BitPacker8x::compressed_block_size(Self::BITS);
            bitpacker.decompress(&self.packed()[offset..], &mut decoded, Self::BITS);
            let count = (dim - start).min(BitPacker8x::BLOCK_LEN);
            // Each word represents 64 dimensions. Sixteen intersections give
            // the integer dot product of the two four-bit code vectors.
            for word_start in (0..count).step_by(64) {
                let word_len = (count - word_start).min(64);
                let mut data_planes = [0u64; 4];
                for (bit, &code) in decoded[word_start..word_start + word_len]
                    .iter()
                    .enumerate()
                {
                    code_sum += code;
                    for (plane, bits) in data_planes.iter_mut().enumerate() {
                        *bits |= u64::from((code >> plane) & 1) << bit;
                    }
                }
                let byte_start = (start + word_start) / 8;
                let mask = u64::MAX >> (64 - word_len);
                for (query_plane, bytes) in query
                    .bit_planes
                    .chunks_exact(query.padded_bytes)
                    .enumerate()
                {
                    let query_word =
                        u64::from_le_bytes(bytes[byte_start..byte_start + 8].try_into().unwrap())
                            & mask;
                    for (data_plane, &data_word) in data_planes.iter().enumerate() {
                        integer_dot +=
                            (data_word & query_word).count_ones() << (data_plane + query_plane);
                    }
                }
            }
        }
        // Stored grid coordinates are unsigned codes minus 7.5; query
        // coordinates are v_l + delta * unsigned query code.
        let grid_sum = code_sum as f32 - 7.5 * dim as f32;
        let grid_dot_query =
            query.v_l * grid_sum + query.delta * (integer_dot as f32 - 7.5 * query.sum_q_u as f32);
        let h = self.header();
        rabitq_distance_query(
            grid_dot_query,
            h.correction,
            h.norm,
            h.radial,
            query.c_norm,
            query.c_dot_q,
            query.q_norm,
            distance_fn,
        )
    }

    pub fn distance_code(
        &self,
        other: &Code<4, impl AsRef<[u8]>>,
        distance_fn: &DistanceFunction,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let g_a = Self::unpack_grid_from(self.packed(), dim);
        let g_b = Self::unpack_grid_from(other.packed(), dim);
        let g_a_dot_g_b = f32::dot(&g_a, &g_b).unwrap_or(0.0) as f32;
        let ha = self.header();
        let hb = other.header();
        rabitq_distance_code(
            g_a_dot_g_b,
            ha.correction,
            ha.norm,
            ha.radial,
            hb.correction,
            hb.norm,
            hb.radial,
            c_norm,
            distance_fn,
        )
    }
}

impl Code<4, Vec<u8>> {
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
            bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader4Bit {
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
        bytes.extend_from_slice(bytemuck::bytes_of(&CodeHeader4Bit {
            correction,
            norm,
            radial,
        }));
        bytes.extend_from_slice(&packed);
        Self(bytes)
    }
}

/// Padded dimension for 4-bit codes (multiple of BitPacker8x::BLOCK_LEN = 256).
fn padded_dim_4bit(dim: usize) -> usize {
    dim.div_ceil(BitPacker8x::BLOCK_LEN) * BitPacker8x::BLOCK_LEN
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use simsimd::SpatialSimilarity;

    use super::*;
    use crate::quantization::Code;

    #[test]
    fn four_bit_query_matches_scalar_reference_with_padding() {
        let mut rng = StdRng::seed_from_u64(7281);
        for dim in [1usize, 7, 64, 255, 256, 300, 1024] {
            let data: Vec<f32> = (0..dim).map(|_| rng.gen_range(-2.0..2.0)).collect();
            let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-0.5..0.5)).collect();
            let residual: Vec<f32> = (0..dim).map(|_| rng.gen_range(-2.0..2.0)).collect();
            let code = Code::<4>::quantize(&data, &centroid);
            let query = QuantizedQuery::new(&residual, dim.div_ceil(64) * 8, 1.2, 0.3, 2.7);
            let restored: Vec<f32> = (0..dim)
                .map(|i| {
                    let unsigned = (0..4)
                        .map(|plane| {
                            u32::from(
                                (query.bit_planes[plane * query.padded_bytes + i / 8] >> (i % 8))
                                    & 1,
                            ) << plane
                        })
                        .sum::<u32>();
                    query.v_l + query.delta * unsigned as f32
                })
                .collect();
            let grid = Code::<4, Vec<u8>>::unpack_grid_from(code.packed(), dim);
            for metric in [
                DistanceFunction::Euclidean,
                DistanceFunction::Cosine,
                DistanceFunction::InnerProduct,
            ] {
                let dot: f32 = grid.iter().zip(&restored).map(|(a, b)| a * b).sum();
                let h = code.header();
                let expected = rabitq_distance_query(
                    dot,
                    h.correction,
                    h.norm,
                    h.radial,
                    1.2,
                    0.3,
                    2.7,
                    &metric,
                );
                let actual = code.distance_quantized_query(&metric, &query, dim);
                assert!(
                    (actual - expected).abs() < 0.002 * expected.abs().max(1.0),
                    "dim={dim}: {actual} vs {expected}"
                );
                let expected_fp = code.distance_query(&metric, &residual, 1.2, 0.3, 2.7);
                let dot_fp: f32 = grid.iter().zip(&residual).map(|(a, b)| a * b).sum();
                let scalar_fp = rabitq_distance_query(
                    dot_fp,
                    h.correction,
                    h.norm,
                    h.radial,
                    1.2,
                    0.3,
                    2.7,
                    &metric,
                );
                assert!((expected_fp - scalar_fp).abs() < 0.002 * scalar_fp.abs().max(1.0));
            }
        }
    }

    #[test]
    fn test_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code::<4>::quantize(&embedding, &centroid);

        let h = code.header();
        assert!(h.correction.is_finite());
        assert!(h.norm.is_finite());
        assert!(h.radial.is_finite());

        // Verify norm is ‖r‖ = ‖embedding - centroid‖
        let r = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect::<Vec<_>>();
        let expected_norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        assert!((h.norm - expected_norm).abs() < f32::EPSILON);

        // Verify radial is ⟨r, c⟩
        let expected_radial = f32::dot(&r, &centroid).unwrap_or(0.0) as f32;
        assert!((h.radial - expected_radial).abs() < f32::EPSILON);

        // Verify buffer size
        assert_eq!(code.as_ref().len(), Code::<4>::size(embedding.len()));
    }

    #[test]
    fn test_size() {
        // Exactly one block (256)
        assert_eq!(Code::<4>::packed_len(256), 256 * 4 / 8); // 128 bytes
        assert_eq!(Code::<4>::size(256), 12 + 128); // 3 floats + packed

        // Non-aligned (300) - should pad to 512
        assert_eq!(Code::<4>::packed_len(300), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code::<4>::size(300), 12 + 256);

        // Two blocks (512)
        assert_eq!(Code::<4>::packed_len(512), 512 * 4 / 8); // 256 bytes
        assert_eq!(Code::<4>::size(512), 12 + 256);
    }

    #[test]
    fn test_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code::<4>::quantize(&embedding, &embedding);
        assert_eq!(code.header().correction, 1.0);
        assert!(code.header().norm < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code::<4>::quantize(&embedding, &centroid);
        assert_eq!(code.header().correction, 1.0);
        assert!(code.header().norm < f32::EPSILON);
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

                        let code = Code::<4>::quantize(&embedding, &centroid);
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
            .map(|v| Code::<4>::quantize(v, &centroid))
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
                        codes[i].distance_code(&codes[j], &distance_fn, c_norm, dim);
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

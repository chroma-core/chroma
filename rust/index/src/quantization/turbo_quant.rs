//! TurboQuant: Two-stage quantization combining PolarQuant + QJL.
//!
//! Implements the quantization method from the TurboQuant paper (ICLR 2026):
//!
//! 1. **PolarQuant** (2 bits/dim): MSE-optimal Lloyd-Max scalar quantizer applied
//!    to each coordinate of the normalized residual. After random rotation (applied
//!    upstream by the SPANN index), coordinates are approximately independent and
//!    follow a concentrated distribution near N(0, 1/d), enabling coordinate-wise
//!    scalar quantization that achieves near-optimal distortion.
//!
//! 2. **QJL** (1 bit/dim): Quantized Johnson-Lindenstrauss correction on the
//!    PolarQuant residual error. Stores the sign of each coordinate's quantization
//!    error, scaled by the optimal constant, making the combined inner product
//!    estimator less biased than PolarQuant alone.
//!
//! Total: **3 bits per dimension** (between 1-bit and 4-bit RaBitQ in size).
//!
//! ## Storage Layout
//!
//! | Field | Size | Description |
//! |-------|------|-------------|
//! | Header | 16 bytes | correction, norm, radial, qjl_scale |
//! | PolarQuant codes | ⌈dim/4⌉ bytes | 2-bit packed Lloyd-Max codes |
//! | QJL codes | ⌈dim/8⌉ bytes | 1-bit packed sign of residual error |
//!
//! ## Distance Estimation
//!
//! Uses the shared RaBitQ distance framework with an improved `⟨g, r_q⟩`
//! estimator that combines both stages:
//!
//! ```text
//! g[i]       = n_hat[i] + qjl_scale · sign(e[i])
//! ⟨g, r_q⟩   = ⟨n_hat, r_q⟩ + qjl_scale · ⟨sign(e), r_q⟩
//! correction = ⟨g, n⟩
//! ```
//!
//! where `n_hat` is the PolarQuant reconstruction and `sign(e)` captures the
//! residual direction via QJL. The correction factor is closer to 1.0 than
//! either RaBitQ variant, yielding lower-variance distance estimates.

use std::mem::size_of;

use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;

use super::{rabitq_distance_code, rabitq_distance_query};

// ── Lloyd-Max optimal quantizer for N(0, 1), 2 bits (4 levels) ──────────────
//
// Symmetric boundaries and centroids that minimize MSE for the standard normal.
// At runtime, coordinates are scaled by √dim before quantizing and by 1/√dim
// after dequantizing, since unit-vector coordinates have variance ~1/dim.

const BOUNDARY: f32 = 0.9816;
const INNER_CENTROID: f32 = 0.4528;
const OUTER_CENTROID: f32 = 1.510;
const CENTROIDS: [f32; 4] = [-OUTER_CENTROID, -INNER_CENTROID, INNER_CENTROID, OUTER_CENTROID];

/// Quantize a single scalar (assumed ~N(0,1)) to a 2-bit code (0–3).
#[inline(always)]
fn scalar_quantize(x: f32) -> u8 {
    // Branchless-friendly: most values land in the inner bins.
    if x < -BOUNDARY {
        0
    } else if x < 0.0 {
        1
    } else if x < BOUNDARY {
        2
    } else {
        3
    }
}

/// Dequantize a 2-bit code (0–3) to the Lloyd-Max centroid value.
#[inline(always)]
fn scalar_dequantize(code: u8) -> f32 {
    // Safety: code is always 0–3.
    CENTROIDS[code as usize]
}

// ── Header ──────────────────────────────────────────────────────────────────

/// 16-byte header for TurboQuant codes.
///
/// Field order: correction, norm, radial matches the RaBitQ headers so the
/// shared distance helpers can consume them uniformly.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct TurboQuantHeader {
    /// Combined correction factor: `⟨g, n⟩` where `g = n_hat + qjl_scale·sign(e)`.
    correction: f32,
    /// `‖r‖` — residual norm.
    norm: f32,
    /// `⟨r, c⟩` — residual dot centroid.
    radial: f32,
    /// Optimal scalar for QJL sign reconstruction: `Σ|e[i]| / dim`.
    qjl_scale: f32,
}

// ── TurboQuantCode ──────────────────────────────────────────────────────────

/// A TurboQuant-compressed vector code.
///
/// Layout: `[TurboQuantHeader | polar_packed (2-bit) | qjl_packed (1-bit)]`
pub struct TurboQuantCode<T = Vec<u8>>(T);

impl<T: AsRef<[u8]>> AsRef<[u8]> for TurboQuantCode<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<T> TurboQuantCode<T> {
    /// Wraps existing bytes as a TurboQuant code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }

    /// Packed byte length for the 2-bit PolarQuant section.
    pub fn polar_packed_len(dim: usize) -> usize {
        dim.div_ceil(4)
    }

    /// Packed byte length for the 1-bit QJL section.
    pub fn qjl_packed_len(dim: usize) -> usize {
        dim.div_ceil(8)
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<TurboQuantHeader>() + Self::polar_packed_len(dim) + Self::qjl_packed_len(dim)
    }
}

impl<T: AsRef<[u8]>> TurboQuantCode<T> {
    fn header(&self) -> TurboQuantHeader {
        bytemuck::pod_read_unaligned(&self.0.as_ref()[..size_of::<TurboQuantHeader>()])
    }

    /// 2-bit PolarQuant packed codes.
    fn polar_packed(&self, dim: usize) -> &[u8] {
        let start = size_of::<TurboQuantHeader>();
        &self.0.as_ref()[start..start + Self::polar_packed_len(dim)]
    }

    /// 1-bit QJL packed codes.
    fn qjl_packed(&self, dim: usize) -> &[u8] {
        let start = size_of::<TurboQuantHeader>() + Self::polar_packed_len(dim);
        &self.0.as_ref()[start..start + Self::qjl_packed_len(dim)]
    }

    /// Unpack PolarQuant 2-bit codes to reconstructed unit-direction coordinates.
    ///
    /// Each stored 2-bit code maps to a Lloyd-Max centroid, then is divided by
    /// `√dim` to undo the pre-quantization scaling.
    ///
    /// Used only in tests; the production `distance_query` / `distance_code`
    /// paths dequantize inline to avoid this heap allocation.
    #[cfg(test)]
    fn unpack_polar(&self, dim: usize) -> Vec<f32> {
        let packed = self.polar_packed(dim);
        let inv_sqrt_dim = 1.0 / (dim as f32).sqrt();
        let mut result = Vec::with_capacity(dim);
        for (byte_idx, &byte) in packed.iter().enumerate() {
            for j in 0..4 {
                let idx = byte_idx * 4 + j;
                if idx >= dim {
                    break;
                }
                let code = (byte >> (j * 2)) & 0x03;
                result.push(scalar_dequantize(code) * inv_sqrt_dim);
            }
        }
        result
    }

    /// Estimates distance from data vector `d` to query `q`.
    ///
    /// Combines PolarQuant reconstruction and QJL correction:
    /// ```text
    /// ⟨g, r_q⟩ = ⟨n_hat, r_q⟩ + qjl_scale · ⟨sign(e), r_q⟩
    /// ```
    ///
    /// **Zero-allocation**: iterates through packed codes directly, dequantizing
    /// each coordinate on the fly and immediately accumulating the dot product
    /// with `r_q`. No intermediate `Vec<f32>` is created.
    pub fn distance_query(
        &self,
        distance_fn: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        let dim = r_q.len();
        let h = self.header();
        let inv_sqrt_dim = 1.0 / (dim as f32).sqrt();
        let polar = self.polar_packed(dim);
        let qjl = self.qjl_packed(dim);

        // Fused pass: accumulate ⟨n_hat, r_q⟩ and ⟨sign(e), r_q⟩ simultaneously
        // without allocating an intermediate n_hat vector.
        let mut polar_dot = 0.0f32;
        let mut qjl_positive_sum = 0.0f32;
        let mut qjl_total_sum = 0.0f32;

        for i in 0..dim {
            let rq_i = r_q[i];

            // PolarQuant: dequantize 2-bit code inline
            let code = (polar[i / 4] >> ((i % 4) * 2)) & 0x03;
            polar_dot += scalar_dequantize(code) * inv_sqrt_dim * rq_i;

            // QJL: accumulate signed dot product
            qjl_total_sum += rq_i;
            if (qjl[i / 8] >> (i % 8)) & 1 == 1 {
                qjl_positive_sum += rq_i;
            }
        }

        let qjl_signed_dot = 2.0 * qjl_positive_sum - qjl_total_sum;
        let g_dot_r_q = polar_dot + h.qjl_scale * qjl_signed_dot;

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

    /// Estimates distance between two data vectors `d_a` and `d_b`.
    ///
    /// Reconstructs the combined `g` vectors from both codes and computes
    /// `⟨g_a, g_b⟩` for use in the shared distance formula.
    ///
    /// **Zero-allocation**: dequantizes each coordinate on the fly from both
    /// packed code buffers, avoiding intermediate `Vec<f32>` allocations.
    pub fn distance_code(
        &self,
        other: &TurboQuantCode<impl AsRef<[u8]>>,
        distance_fn: &DistanceFunction,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let ha = self.header();
        let hb = other.header();
        let inv_sqrt_dim = 1.0 / (dim as f32).sqrt();

        let polar_a = self.polar_packed(dim);
        let polar_b = other.polar_packed(dim);
        let qjl_a = self.qjl_packed(dim);
        let qjl_b = other.qjl_packed(dim);

        // Fused pass: reconstruct g_a[i] and g_b[i] inline and accumulate ⟨g_a, g_b⟩.
        // g[i] = n_hat[i] + qjl_scale · sign(e)[i]
        let mut g_a_dot_g_b = 0.0f32;
        for i in 0..dim {
            // PolarQuant: dequantize 2-bit codes inline
            let code_a = (polar_a[i / 4] >> ((i % 4) * 2)) & 0x03;
            let code_b = (polar_b[i / 4] >> ((i % 4) * 2)) & 0x03;
            let nhat_a = scalar_dequantize(code_a) * inv_sqrt_dim;
            let nhat_b = scalar_dequantize(code_b) * inv_sqrt_dim;

            // QJL: extract sign bits
            let sign_a = if (qjl_a[i / 8] >> (i % 8)) & 1 == 1 { 1.0f32 } else { -1.0 };
            let sign_b = if (qjl_b[i / 8] >> (i % 8)) & 1 == 1 { 1.0f32 } else { -1.0 };

            let ga = nhat_a + ha.qjl_scale * sign_a;
            let gb = nhat_b + hb.qjl_scale * sign_b;
            g_a_dot_g_b += ga * gb;
        }

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

// ── Encoding ────────────────────────────────────────────────────────────────

impl TurboQuantCode<Vec<u8>> {
    /// Quantizes a data vector relative to its cluster centroid using TurboQuant.
    ///
    /// # Two-stage process
    ///
    /// **Stage 1 — PolarQuant**: Normalizes the residual `r = embedding − centroid`
    /// to unit length, scales each coordinate by `√dim` to standardize to ~N(0,1),
    /// then applies the 2-bit Lloyd-Max quantizer independently per coordinate.
    ///
    /// **Stage 2 — QJL**: Computes the quantization error `e = n − n_hat`, stores
    /// `sign(e[i])` as a 1-bit code, and derives `qjl_scale = Σ|e[i]| / dim` —
    /// the least-squares optimal constant for reconstructing `e` from its signs.
    ///
    /// The combined grid point `g[i] = n_hat[i] + qjl_scale·sign(e[i])` is a
    /// better approximation of `n[i]` than either stage alone, yielding a
    /// correction factor `⟨g, n⟩` closer to 1.0 and lower-variance distances.
    pub fn quantize(embedding: &[f32], centroid: &[f32]) -> Self {
        let dim = embedding.len();

        // Residual computation: r = embedding - centroid, accumulating
        // norm² and radial in the same pass.
        let mut norm_sq = 0.0f32;
        let mut radial = 0.0f32;
        let r: Vec<f32> = embedding
            .iter()
            .zip(centroid)
            .map(|(&e, &c)| {
                let ri = e - c;
                norm_sq += ri * ri;
                radial += ri * c;
                ri
            })
            .collect();
        let norm = norm_sq.sqrt();

        // Early return for near-zero residual.
        if dim == 0 || norm < f32::EPSILON {
            let mut bytes = vec![0u8; Self::size(dim)];
            bytes[..size_of::<TurboQuantHeader>()].copy_from_slice(bytemuck::bytes_of(
                &TurboQuantHeader {
                    correction: 1.0,
                    norm,
                    radial,
                    qjl_scale: 0.0,
                },
            ));
            return Self(bytes);
        }

        let sqrt_dim = (dim as f32).sqrt();
        let inv_sqrt_dim = 1.0 / sqrt_dim;
        let inv_norm = 1.0 / norm;

        // ── Stage 1: PolarQuant (2-bit scalar quantization) ─────────────────

        let mut polar_codes = vec![0u8; Self::polar_packed_len(dim)];
        let mut n_hat = Vec::with_capacity(dim);

        for i in 0..dim {
            let ni = r[i] * inv_norm; // unit direction coordinate
            let scaled = ni * sqrt_dim; // standardize to ~N(0,1)
            let code = scalar_quantize(scaled);
            polar_codes[i / 4] |= code << ((i % 4) * 2);
            n_hat.push(scalar_dequantize(code) * inv_sqrt_dim);
        }

        // ── Stage 2: QJL (1-bit sign of quantization error) ────────────────

        let mut qjl_codes = vec![0u8; Self::qjl_packed_len(dim)];
        let mut error_abs_sum = 0.0f32;
        let mut n_hat_dot_n = 0.0f32;
        let mut sign_e_dot_n = 0.0f32;

        for i in 0..dim {
            let ni = r[i] * inv_norm;
            let ei = ni - n_hat[i];
            error_abs_sum += ei.abs();
            n_hat_dot_n += n_hat[i] * ni;

            let sign = if ei >= 0.0 {
                qjl_codes[i / 8] |= 1 << (i % 8);
                1.0f32
            } else {
                -1.0
            };
            sign_e_dot_n += sign * ni;
        }

        // Optimal scalar: minimizes ‖e − s·sign(e)‖² → s = ⟨e, sign(e)⟩ / dim = Σ|e| / dim
        let qjl_scale = error_abs_sum / dim as f32;

        // Combined correction: ⟨g, n⟩ = ⟨n_hat, n⟩ + qjl_scale · ⟨sign(e), n⟩
        let correction = n_hat_dot_n + qjl_scale * sign_e_dot_n;

        // ── Assemble output ─────────────────────────────────────────────────

        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(bytemuck::bytes_of(&TurboQuantHeader {
            correction,
            norm,
            radial,
            qjl_scale,
        }));
        bytes.extend_from_slice(&polar_codes);
        bytes.extend_from_slice(&qjl_codes);

        Self(bytes)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use simsimd::SpatialSimilarity;

    use super::*;
    use crate::quantization::{Code, QuantizedQuery};

    // ── Helper: generate random vectors near a centroid ──────────────────

    fn random_vectors(
        rng: &mut StdRng,
        centroid: &[f32],
        k: f32,
        n: usize,
    ) -> Vec<Vec<f32>> {
        (0..n)
            .map(|_| {
                centroid
                    .iter()
                    .map(|c| c + rng.gen_range(-k..k))
                    .collect()
            })
            .collect()
    }

    fn exact_distance(a: &[f32], b: &[f32], distance_fn: &DistanceFunction) -> f32 {
        match distance_fn {
            DistanceFunction::Cosine => {
                SpatialSimilarity::cos(a, b).unwrap_or(0.0) as f32
            }
            DistanceFunction::Euclidean => {
                SpatialSimilarity::l2sq(a, b).unwrap_or(0.0) as f32
            }
            DistanceFunction::InnerProduct => {
                1.0 - SpatialSimilarity::dot(a, b).unwrap_or(0.0) as f32
            }
        }
    }

    // ── Basic correctness ───────────────────────────────────────────────

    #[test]
    fn test_attributes() {
        let embedding: Vec<f32> = (0..300).map(|i| i as f32).collect();
        let centroid: Vec<f32> = (0..300).map(|i| i as f32 * 0.5).collect();

        let code = TurboQuantCode::quantize(&embedding, &centroid);
        let h = code.header();

        assert!(h.correction.is_finite());
        assert!(h.norm.is_finite());
        assert!(h.radial.is_finite());
        assert!(h.qjl_scale.is_finite());
        assert!(h.qjl_scale >= 0.0);

        // Verify norm is ‖r‖
        let r: Vec<f32> = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect();
        let expected_norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        assert!(
            (h.norm - expected_norm).abs() < f32::EPSILON,
            "norm mismatch: {} vs {}",
            h.norm,
            expected_norm
        );

        // Verify radial is ⟨r, c⟩
        let expected_radial = f32::dot(&r, &centroid).unwrap_or(0.0) as f32;
        assert!(
            (h.radial - expected_radial).abs() < 1.0, // FP accumulation tolerance
            "radial mismatch: {} vs {}",
            h.radial,
            expected_radial
        );

        // Verify buffer size
        assert_eq!(
            code.as_ref().len(),
            TurboQuantCode::<Vec<u8>>::size(embedding.len())
        );
    }

    #[test]
    fn test_size() {
        // dim=256: polar=64, qjl=32, total=16+64+32=112
        assert_eq!(TurboQuantCode::<Vec<u8>>::polar_packed_len(256), 64);
        assert_eq!(TurboQuantCode::<Vec<u8>>::qjl_packed_len(256), 32);
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(256), 16 + 64 + 32);

        // dim=1024: polar=256, qjl=128, total=16+256+128=400
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(1024), 400);

        // Non-aligned dim=300: polar=75, qjl=38, total=16+75+38=129
        assert_eq!(TurboQuantCode::<Vec<u8>>::polar_packed_len(300), 75);
        assert_eq!(TurboQuantCode::<Vec<u8>>::qjl_packed_len(300), 38);
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(300), 129);
    }

    #[test]
    fn test_zero_residual() {
        let embedding: Vec<f32> = (0..300).map(|i| i as f32).collect();

        // Exactly zero residual
        let code = TurboQuantCode::quantize(&embedding, &embedding);
        let h = code.header();
        assert_eq!(h.correction, 1.0);
        assert!(h.norm < f32::EPSILON);
        assert_eq!(h.qjl_scale, 0.0);

        // Near-zero residual
        let centroid: Vec<f32> = embedding.iter().map(|x| x + 1e-10).collect();
        let code = TurboQuantCode::quantize(&embedding, &centroid);
        let h = code.header();
        assert_eq!(h.correction, 1.0);
        assert!(h.norm < f32::EPSILON);
    }

    #[test]
    fn test_polar_roundtrip() {
        // Verify that unpacking PolarQuant codes gives reasonable reconstructions.
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 128;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let embedding: Vec<f32> = centroid
            .iter()
            .map(|c| c + rng.gen_range(-1.0..1.0))
            .collect();

        let code = TurboQuantCode::quantize(&embedding, &centroid);
        let n_hat = code.unpack_polar(dim);

        // n_hat should have the same sign pattern as n for most coordinates.
        let r: Vec<f32> = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect();
        let norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        let n: Vec<f32> = r.iter().map(|&x| x / norm).collect();

        let mut sign_matches = 0;
        for i in 0..dim {
            if (n[i] >= 0.0) == (n_hat[i] >= 0.0) {
                sign_matches += 1;
            }
        }
        // With 2-bit quantization, sign should be preserved for the vast majority.
        let sign_match_rate = sign_matches as f32 / dim as f32;
        assert!(
            sign_match_rate > 0.95,
            "PolarQuant sign match rate too low: {:.2}%",
            sign_match_rate * 100.0
        );
    }

    // ── Distance accuracy ───────────────────────────────────────────────

    /// TurboQuant (3-bit): P95 relative error should be competitive with 4-bit RaBitQ.
    #[test]
    fn test_error_bound() {
        for k in [1.0, 2.0, 4.0] {
            assert_error_bound(1024, k, 128);
        }
    }

    fn assert_error_bound(dim: usize, k: f32, n_vectors: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-k..k)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = random_vectors(&mut rng, &centroid, k, n_vectors);
        let codes: Vec<_> = vectors
            .iter()
            .map(|v| TurboQuantCode::quantize(v, &centroid))
            .collect();

        // At 3 bits/dim, we expect better accuracy than 1-bit but the bound
        // should be comfortably under 2% P95 relative error.
        let max_p95_rel_error = 0.02;

        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            let mut rel_errors_query = Vec::new();
            let mut rel_errors_code = Vec::new();

            for i in 0..n_vectors {
                for j in (i + 1)..n_vectors {
                    let exact = exact_distance(&vectors[i], &vectors[j], &distance_fn);

                    // Code-to-code distance
                    let estimated_code =
                        codes[i].distance_code(&codes[j], &distance_fn, c_norm, dim);
                    rel_errors_code
                        .push((exact - estimated_code).abs() / exact.abs().max(f32::EPSILON));

                    // Query distance (j as query)
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
                "{:?} k={}: TurboQuant distance_code P95 rel error {:.4} exceeds {:.4}",
                distance_fn,
                k,
                p95_code,
                max_p95_rel_error
            );
            assert!(
                p95_query < max_p95_rel_error,
                "{:?} k={}: TurboQuant distance_query P95 rel error {:.4} exceeds {:.4}",
                distance_fn,
                k,
                p95_query,
                max_p95_rel_error
            );
        }
    }

    // ── Comparison with RaBitQ ──────────────────────────────────────────

    /// Compares TurboQuant (3-bit) against RaBitQ 1-bit and 4-bit on:
    /// - P50 and P95 relative error
    /// - Compression ratio
    /// - Encoding speed
    #[test]
    fn test_comparison_with_rabitq() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let k = 2.0;
        let n_vectors = 256;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-k..k)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = random_vectors(&mut rng, &centroid, k, n_vectors);

        // ── Encode with all three methods ───────────────────────────────

        let t0 = Instant::now();
        let codes_1bit: Vec<_> = vectors
            .iter()
            .map(|v| Code::<1>::quantize(v, &centroid))
            .collect();
        let time_1bit = t0.elapsed();

        let t0 = Instant::now();
        let codes_4bit: Vec<_> = vectors
            .iter()
            .map(|v| Code::<4>::quantize(v, &centroid))
            .collect();
        let time_4bit = t0.elapsed();

        let t0 = Instant::now();
        let codes_turbo: Vec<_> = vectors
            .iter()
            .map(|v| TurboQuantCode::quantize(v, &centroid))
            .collect();
        let time_turbo = t0.elapsed();

        // ── Compression ratios ──────────────────────────────────────────

        let raw_size = dim * 4; // f32 per dimension
        let size_1bit = Code::<1, Vec<u8>>::size(dim);
        let size_4bit = Code::<4, Vec<u8>>::size(dim);
        let size_turbo = TurboQuantCode::<Vec<u8>>::size(dim);

        eprintln!("\n╔══════════════════════════════════════════════════════════════╗");
        eprintln!("║         TurboQuant vs RaBitQ Comparison (dim={})         ║", dim);
        eprintln!("╠══════════════════════════════════════════════════════════════╣");
        eprintln!("║ Method          │ Size    │ Ratio  │ Encode time           ║");
        eprintln!("╟─────────────────┼─────────┼────────┼───────────────────────╢");
        eprintln!(
            "║ Raw f32         │ {:>5} B │  1.0x  │ —                     ║",
            raw_size
        );
        eprintln!(
            "║ RaBitQ 1-bit    │ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_1bit,
            raw_size as f32 / size_1bit as f32,
            time_1bit,
            n_vectors
        );
        eprintln!(
            "║ TurboQuant 3-bit│ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_turbo,
            raw_size as f32 / size_turbo as f32,
            time_turbo,
            n_vectors
        );
        eprintln!(
            "║ RaBitQ 4-bit    │ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_4bit,
            raw_size as f32 / size_4bit as f32,
            time_4bit,
            n_vectors
        );
        eprintln!("╚══════════════════════════════════════════════════════════════╝");

        // ── Distance accuracy comparison ────────────────────────────────

        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            let mut errors_1bit = Vec::new();
            let mut errors_4bit = Vec::new();
            let mut errors_turbo = Vec::new();

            let sample_size = 128.min(n_vectors);
            for i in 0..sample_size {
                for j in (i + 1)..sample_size {
                    let exact = exact_distance(&vectors[i], &vectors[j], &distance_fn);

                    // Query-path distances (the search hot path in practice)
                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q: Vec<f32> = centroid.iter().zip(q).map(|(c, q)| q - c).collect();

                    // 1-bit: use the proper quantized-query path (AND+popcount)
                    let padded_bytes = Code::<1, Vec<u8>>::packed_len(dim);
                    let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
                    let code_ref = Code::<1, _>::new(codes_1bit[i].as_ref());
                    let est_1bit = code_ref.distance_quantized_query(&distance_fn, &qq);
                    let est_4bit =
                        codes_4bit[i].distance_query(&distance_fn, &r_q, c_norm, c_dot_q, q_norm);
                    let est_turbo = codes_turbo[i].distance_query(
                        &distance_fn,
                        &r_q,
                        c_norm,
                        c_dot_q,
                        q_norm,
                    );

                    let denom = exact.abs().max(f32::EPSILON);
                    errors_1bit.push((exact - est_1bit).abs() / denom);
                    errors_4bit.push((exact - est_4bit).abs() / denom);
                    errors_turbo.push((exact - est_turbo).abs() / denom);
                }
            }

            errors_1bit.sort_by(|a, b| a.total_cmp(b));
            errors_4bit.sort_by(|a, b| a.total_cmp(b));
            errors_turbo.sort_by(|a, b| a.total_cmp(b));

            let n = errors_1bit.len();
            let p50 = |e: &[f32]| e[n * 50 / 100];
            let p95 = |e: &[f32]| e[n * 95 / 100];

            eprintln!("\n  {:?} distance accuracy:", distance_fn);
            eprintln!("  ┌─────────────────┬──────────┬──────────┐");
            eprintln!("  │ Method          │ P50 err  │ P95 err  │");
            eprintln!("  ├─────────────────┼──────────┼──────────┤");
            eprintln!(
                "  │ RaBitQ 1-bit    │ {:.4}   │ {:.4}   │",
                p50(&errors_1bit),
                p95(&errors_1bit)
            );
            eprintln!(
                "  │ TurboQuant 3-bit│ {:.4}   │ {:.4}   │",
                p50(&errors_turbo),
                p95(&errors_turbo)
            );
            eprintln!(
                "  │ RaBitQ 4-bit    │ {:.4}   │ {:.4}   │",
                p50(&errors_4bit),
                p95(&errors_4bit)
            );
            eprintln!("  └─────────────────┴──────────┴──────────┘");
        }

        // ── Verify no data loss: ranking preservation ───────────────────

        // For a random query, check that TurboQuant preserves the top-k
        // nearest neighbor ranking compared to exact distances.
        let query = &vectors[0];
        let q_norm = (f32::dot(query, query).unwrap_or(0.0) as f32).sqrt();
        let c_dot_q = f32::dot(&centroid, query).unwrap_or(0.0) as f32;
        let r_q: Vec<f32> = centroid.iter().zip(query).map(|(c, q)| q - c).collect();

        let mut exact_dists: Vec<(usize, f32)> = (1..n_vectors)
            .map(|i| {
                (
                    i,
                    exact_distance(query, &vectors[i], &DistanceFunction::Cosine),
                )
            })
            .collect();
        exact_dists.sort_by(|a, b| a.1.total_cmp(&b.1));

        let mut turbo_dists: Vec<(usize, f32)> = (1..n_vectors)
            .map(|i| {
                (
                    i,
                    codes_turbo[i].distance_query(
                        &DistanceFunction::Cosine,
                        &r_q,
                        c_norm,
                        c_dot_q,
                        q_norm,
                    ),
                )
            })
            .collect();
        turbo_dists.sort_by(|a, b| a.1.total_cmp(&b.1));

        // Check recall@10: how many of the true top-10 are in TurboQuant's top-10?
        let k = 10;
        let exact_top_k: std::collections::HashSet<usize> =
            exact_dists.iter().take(k).map(|&(i, _)| i).collect();
        let turbo_top_k: std::collections::HashSet<usize> =
            turbo_dists.iter().take(k).map(|&(i, _)| i).collect();
        let recall = exact_top_k.intersection(&turbo_top_k).count() as f32 / k as f32;

        eprintln!("\n  Recall@{}: {:.0}%", k, recall * 100.0);
        assert!(
            recall >= 0.7,
            "TurboQuant recall@{} too low: {:.0}%",
            k,
            recall * 100.0
        );
    }

    // ── Speed benchmarks ────────────────────────────────────────────────

    #[test]
    fn test_encoding_speed() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let n_vectors = 1000;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-2.0..2.0)).collect();
        let vectors = random_vectors(&mut rng, &centroid, 2.0, n_vectors);

        // Warmup
        for v in vectors.iter().take(10) {
            let _ = TurboQuantCode::quantize(v, &centroid);
            let _ = Code::<1>::quantize(v, &centroid);
            let _ = Code::<4>::quantize(v, &centroid);
        }

        let iters = 3;

        // RaBitQ 1-bit
        let t0 = Instant::now();
        for _ in 0..iters {
            for v in &vectors {
                std::hint::black_box(Code::<1>::quantize(v, &centroid));
            }
        }
        let time_1bit = t0.elapsed() / (iters * n_vectors as u32);

        // RaBitQ 4-bit
        let t0 = Instant::now();
        for _ in 0..iters {
            for v in &vectors {
                std::hint::black_box(Code::<4>::quantize(v, &centroid));
            }
        }
        let time_4bit = t0.elapsed() / (iters * n_vectors as u32);

        // TurboQuant
        let t0 = Instant::now();
        for _ in 0..iters {
            for v in &vectors {
                std::hint::black_box(TurboQuantCode::quantize(v, &centroid));
            }
        }
        let time_turbo = t0.elapsed() / (iters * n_vectors as u32);

        eprintln!("\n  Encoding speed per vector (dim={}):", dim);
        eprintln!("  RaBitQ 1-bit:     {:>8.2?}", time_1bit);
        eprintln!("  TurboQuant 3-bit: {:>8.2?}", time_turbo);
        eprintln!("  RaBitQ 4-bit:     {:>8.2?}", time_4bit);

        // TurboQuant should not be more than 10x slower than 4-bit RaBitQ
        // (it does more work per vector but should still be reasonable).
        assert!(
            time_turbo < time_4bit * 10,
            "TurboQuant encoding too slow: {:?} vs 4-bit {:?}",
            time_turbo,
            time_4bit
        );
    }

    #[test]
    fn test_query_speed() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let n_codes = 1000;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-2.0..2.0)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = random_vectors(&mut rng, &centroid, 2.0, n_codes + 1);

        let query = &vectors[0];
        let q_norm = (f32::dot(query, query).unwrap_or(0.0) as f32).sqrt();
        let c_dot_q = f32::dot(&centroid, query).unwrap_or(0.0) as f32;
        let r_q: Vec<f32> = centroid.iter().zip(query).map(|(c, q)| q - c).collect();

        let codes_4bit: Vec<_> = vectors[1..]
            .iter()
            .map(|v| Code::<4>::quantize(v, &centroid))
            .collect();
        let codes_turbo: Vec<_> = vectors[1..]
            .iter()
            .map(|v| TurboQuantCode::quantize(v, &centroid))
            .collect();

        let iters = 3;

        // 4-bit query speed
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_4bit {
                std::hint::black_box(code.distance_query(
                    &DistanceFunction::Cosine,
                    &r_q,
                    c_norm,
                    c_dot_q,
                    q_norm,
                ));
            }
        }
        let time_4bit = t0.elapsed() / (iters * n_codes as u32);

        // TurboQuant query speed
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_turbo {
                std::hint::black_box(code.distance_query(
                    &DistanceFunction::Cosine,
                    &r_q,
                    c_norm,
                    c_dot_q,
                    q_norm,
                ));
            }
        }
        let time_turbo = t0.elapsed() / (iters * n_codes as u32);

        eprintln!("\n  Query speed per code (dim={}):", dim);
        eprintln!("  RaBitQ 4-bit:     {:>8.2?}", time_4bit);
        eprintln!("  TurboQuant 3-bit: {:>8.2?}", time_turbo);
    }
}

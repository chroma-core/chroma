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
//! ## Storage Layout (bit-plane format)
//!
//! Data is stored as 3 separate bit planes (not packed multi-bit), enabling
//! the AND+popcount bitwise kernel for fast distance estimation:
//!
//! | Field | Size | Description |
//! |-------|------|-------------|
//! | Header | 24 bytes | correction, norm, radial, qjl_scale, polar_vl_factor, signed_sum_qjl |
//! | Plane b0 | plane_len bytes | LSB of 2-bit PolarQuant code, 8 values per byte |
//! | Plane b1 | plane_len bytes | MSB of 2-bit PolarQuant code, 8 values per byte |
//! | Plane QJL | plane_len bytes | Sign of quantization error, 8 values per byte |
//!
//! where `plane_len = ⌈dim/64⌉ × 8` (64-bit aligned for u64 popcount).
//!
//! ## Bitwise Distance Kernel
//!
//! The 2-bit Lloyd-Max centroids decompose linearly into two bit planes:
//!
//! ```text
//! centroid(b1, b0) = ALPHA + BETA·b0 + GAMMA·b1
//!   where ALPHA = −1.510, BETA = 1.0572, GAMMA = 1.9628
//! ```
//!
//! This enables computing `⟨n_hat, r_q⟩` via AND+popcount on the quantized
//! query bit planes (same technique as 1-bit RaBitQ, but with 3 data planes):
//!
//! ```text
//! ⟨n_hat, r_q⟩ ≈ inv_sqrt_dim · δ · (α·Σq_u + β·⟨b0, q_u⟩ + γ·⟨b1, q_u⟩)
//!              + v_l · polar_vl_factor
//!
//! ⟨sign(e), r_q⟩ ≈ δ·(2·⟨qjl, q_u⟩ − Σq_u) + v_l·signed_sum_qjl
//! ```
//!
//! where `⟨b, q_u⟩ = Σ_j 2^j · popcount(b AND q_u^(j))` — the same bit-plane
//! expansion used by the 1-bit path.

use std::mem::size_of;

use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;

use super::rabitq_distance_query;
use super::single_bit::QuantizedQuery;
use super::{rabitq_distance_code};

// ── Lloyd-Max optimal quantizer for N(0, 1), 2 bits (4 levels) ──────────────
//
// Symmetric boundaries and centroids that minimize MSE for the standard normal.
// At runtime, coordinates are scaled by √dim before quantizing and by 1/√dim
// after dequantizing, since unit-vector coordinates have variance ~1/dim.

const BOUNDARY: f32 = 0.9816;
const INNER_CENTROID: f32 = 0.4528;
const OUTER_CENTROID: f32 = 1.510;
const CENTROIDS: [f32; 4] = [-OUTER_CENTROID, -INNER_CENTROID, INNER_CENTROID, OUTER_CENTROID];

// Linear decomposition of centroids into two bit planes:
//   centroid(b1, b0) = ALPHA + BETA·b0 + GAMMA·b1
//
// Derivation:
//   ALPHA = centroid(0,0) = −1.510
//   BETA  = centroid(0,1) − centroid(0,0) = −0.4528 − (−1.510) = 1.0572
//   GAMMA = centroid(1,0) − centroid(0,0) = 0.4528 − (−1.510) = 1.9628
//   Check: centroid(1,1) = −1.510 + 1.0572 + 1.9628 = 1.510 ✓ (no interaction term)
const ALPHA: f32 = -OUTER_CENTROID; // −1.510
const BETA: f32 = OUTER_CENTROID - INNER_CENTROID; // 1.0572
const GAMMA: f32 = OUTER_CENTROID + INNER_CENTROID; // 1.9628

/// Quantize a single scalar (assumed ~N(0,1)) to a 2-bit code (0–3).
#[inline(always)]
fn scalar_quantize(x: f32) -> u8 {
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
    CENTROIDS[code as usize]
}

// ── Header ──────────────────────────────────────────────────────────────────

/// 24-byte header for TurboQuant codes.
///
/// The first three fields (correction, norm, radial) match the RaBitQ header
/// layout so the shared distance helpers can consume them uniformly.
/// The additional fields enable the bitwise AND+popcount kernel.
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
    /// Precomputed v_l scaling: `inv_sqrt_dim · (ALPHA·dim + BETA·pop_b0 + GAMMA·pop_b1)`.
    /// Used by the bitwise kernel to avoid recomputing popcounts at query time.
    polar_vl_factor: f32,
    /// `2·popcount(qjl) − dim` — precomputed signed sum for QJL bit plane.
    signed_sum_qjl: i32,
}

// ── TurboQuantCode ──────────────────────────────────────────────────────────

/// A TurboQuant-compressed vector code.
///
/// Layout: `[TurboQuantHeader (24B) | plane_b0 | plane_b1 | plane_qjl]`
///
/// Each plane is `plane_len` bytes (⌈dim/64⌉ × 8, padded to 64-bit alignment
/// for u64 popcount). The bit-plane format enables the AND+popcount kernel
/// in [`distance_quantized_query`].
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

    /// Byte length of one bit plane (padded to 64-bit alignment for popcount).
    ///
    /// Same padding as 1-bit RaBitQ codes, so `QuantizedQuery` is directly reusable.
    pub fn plane_len(dim: usize) -> usize {
        dim.div_ceil(64) * 8
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<TurboQuantHeader>() + 3 * Self::plane_len(dim)
    }
}

impl<T: AsRef<[u8]>> TurboQuantCode<T> {
    fn header(&self) -> TurboQuantHeader {
        bytemuck::pod_read_unaligned(&self.0.as_ref()[..size_of::<TurboQuantHeader>()])
    }

    /// Bit plane for PolarQuant LSB (b0).
    fn plane_b0(&self, dim: usize) -> &[u8] {
        let pl = Self::plane_len(dim);
        let start = size_of::<TurboQuantHeader>();
        &self.0.as_ref()[start..start + pl]
    }

    /// Bit plane for PolarQuant MSB (b1).
    fn plane_b1(&self, dim: usize) -> &[u8] {
        let pl = Self::plane_len(dim);
        let start = size_of::<TurboQuantHeader>() + pl;
        &self.0.as_ref()[start..start + pl]
    }

    /// Bit plane for QJL sign codes.
    fn plane_qjl(&self, dim: usize) -> &[u8] {
        let pl = Self::plane_len(dim);
        let start = size_of::<TurboQuantHeader>() + 2 * pl;
        &self.0.as_ref()[start..start + pl]
    }

    /// Unpack PolarQuant codes to reconstructed unit-direction coordinates (test only).
    #[cfg(test)]
    fn unpack_polar(&self, dim: usize) -> Vec<f32> {
        let b0 = self.plane_b0(dim);
        let b1 = self.plane_b1(dim);
        let inv_sqrt_dim = 1.0 / (dim as f32).sqrt();
        (0..dim)
            .map(|i| {
                let bit0 = (b0[i / 8] >> (i % 8)) & 1;
                let bit1 = (b1[i / 8] >> (i % 8)) & 1;
                let code = bit0 | (bit1 << 1);
                scalar_dequantize(code) * inv_sqrt_dim
            })
            .collect()
    }

    // ── Scalar distance paths (zero-allocation) ─────────────────────────

    /// Estimates distance from data vector `d` to query `q` (scalar path).
    ///
    /// **Zero-allocation**: iterates through bit planes directly, dequantizing
    /// each coordinate on the fly. Use [`distance_quantized_query`] for the
    /// faster bitwise kernel when scanning many codes.
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
        let b0 = self.plane_b0(dim);
        let b1 = self.plane_b1(dim);
        let qjl = self.plane_qjl(dim);

        let mut polar_dot = 0.0f32;
        let mut qjl_positive_sum = 0.0f32;
        let mut qjl_total_sum = 0.0f32;

        for i in 0..dim {
            let rq_i = r_q[i];

            // PolarQuant: reconstruct centroid from two bit planes
            let bit0 = (b0[i / 8] >> (i % 8)) & 1;
            let bit1 = (b1[i / 8] >> (i % 8)) & 1;
            let code = bit0 | (bit1 << 1);
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

    // ── Bitwise distance kernel ─────────────────────────────────────────

    /// Estimates distance using the AND+popcount bitwise kernel.
    ///
    /// This is the **fast path** for scanning many codes against the same query.
    /// The query is pre-quantized once into a [`QuantizedQuery`], then reused
    /// across all codes in the cluster — eliminating all float arithmetic from
    /// the per-code inner product.
    ///
    /// # Derivation
    ///
    /// The PolarQuant centroids decompose linearly into two bit planes:
    /// ```text
    /// centroid(b1, b0) = α + β·b0 + γ·b1
    /// ```
    ///
    /// Combined with query quantization `r_q[i] ≈ δ·q_u[i] + v_l`:
    /// ```text
    /// ⟨n_hat, r_q⟩ ≈ inv_sqrt_dim · δ · (α·Σq_u + β·⟨b0, q_u⟩ + γ·⟨b1, q_u⟩)
    ///              + v_l · polar_vl_factor
    ///
    /// ⟨sign(e), r_q⟩ ≈ δ·(2·⟨qjl, q_u⟩ − Σq_u) + v_l · signed_sum_qjl
    /// ```
    ///
    /// Each `⟨plane, q_u⟩ = Σ_j 2^j · popcount(plane AND q_u^(j))` is computed
    /// via B_q=4 rounds of AND+popcount per data plane — 12 total per code.
    pub fn distance_quantized_query(
        &self,
        distance_fn: &DistanceFunction,
        qq: &QuantizedQuery,
        dim: usize,
    ) -> f32 {
        let pb = qq.padded_bytes;
        let b0 = self.plane_b0(dim);
        let b1 = self.plane_b1(dim);
        let qjl = self.plane_qjl(dim);

        // 3 × AND+popcount across 4 query bit planes.
        let b0_dot_qu = bitwise_dot_qu(b0, &qq.bit_planes, pb);
        let b1_dot_qu = bitwise_dot_qu(b1, &qq.bit_planes, pb);
        let qjl_dot_qu = bitwise_dot_qu(qjl, &qq.bit_planes, pb);

        let h = self.header();
        let inv_sqrt_dim = 1.0 / (dim as f32).sqrt();

        // PolarQuant: ⟨n_hat, r_q⟩
        let polar_delta_term = inv_sqrt_dim
            * (ALPHA * qq.sum_q_u as f32
                + BETA * b0_dot_qu as f32
                + GAMMA * b1_dot_qu as f32);
        let polar_dot = qq.delta * polar_delta_term + qq.v_l * h.polar_vl_factor;

        // QJL: qjl_scale · ⟨sign(e), r_q⟩
        let signed_dot_qu_qjl = 2.0 * qjl_dot_qu as f32 - qq.sum_q_u as f32;
        let qjl_correction = h.qjl_scale
            * (qq.delta * signed_dot_qu_qjl + qq.v_l * h.signed_sum_qjl as f32);

        let g_dot_r_q = polar_dot + qjl_correction;

        rabitq_distance_query(
            g_dot_r_q,
            h.correction,
            h.norm,
            h.radial,
            qq.c_norm,
            qq.c_dot_q,
            qq.q_norm,
            distance_fn,
        )
    }

    /// Estimates distance between two data vectors (scalar path, zero-allocation).
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

        let b0_a = self.plane_b0(dim);
        let b1_a = self.plane_b1(dim);
        let qjl_a = self.plane_qjl(dim);
        let b0_b = other.plane_b0(dim);
        let b1_b = other.plane_b1(dim);
        let qjl_b = other.plane_qjl(dim);

        let mut g_a_dot_g_b = 0.0f32;
        for i in 0..dim {
            let byte_idx = i / 8;
            let bit_idx = i % 8;

            let code_a = ((b0_a[byte_idx] >> bit_idx) & 1) | (((b1_a[byte_idx] >> bit_idx) & 1) << 1);
            let code_b = ((b0_b[byte_idx] >> bit_idx) & 1) | (((b1_b[byte_idx] >> bit_idx) & 1) << 1);
            let nhat_a = scalar_dequantize(code_a) * inv_sqrt_dim;
            let nhat_b = scalar_dequantize(code_b) * inv_sqrt_dim;

            let sign_a = if (qjl_a[byte_idx] >> bit_idx) & 1 == 1 { 1.0f32 } else { -1.0 };
            let sign_b = if (qjl_b[byte_idx] >> bit_idx) & 1 == 1 { 1.0f32 } else { -1.0 };

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

// ── Bitwise helper ──────────────────────────────────────────────────────────

/// Computes `⟨data_bits, q_u⟩` via B_q=4 rounds of AND+popcount.
///
/// ```text
/// ⟨data, q_u⟩ = Σ_j 2^j · popcount(data AND q_u^(j))
/// ```
///
/// Same interleaved-planes technique as the 1-bit RaBitQ path: reads the data
/// plane once while ANDing against all four query planes, using independent
/// accumulators that the OoO core can pipeline.
fn bitwise_dot_qu(data: &[u8], bit_planes: &[u8], pb: usize) -> u32 {
    let p0 = &bit_planes[0..pb];
    let p1 = &bit_planes[pb..2 * pb];
    let p2 = &bit_planes[2 * pb..3 * pb];
    let p3 = &bit_planes[3 * pb..4 * pb];
    let (mut pop0, mut pop1, mut pop2, mut pop3) = (0u32, 0u32, 0u32, 0u32);

    for (x_chunk, (((q0, q1), q2), q3)) in data.chunks_exact(8).zip(
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
}

// ── Encoding ────────────────────────────────────────────────────────────────

impl TurboQuantCode<Vec<u8>> {
    /// Quantizes a data vector relative to its cluster centroid using TurboQuant.
    ///
    /// # Two-stage process
    ///
    /// **Stage 1 — PolarQuant**: Normalizes the residual `r = embedding − centroid`
    /// to unit length, scales each coordinate by `√dim` to standardize to ~N(0,1),
    /// then applies the 2-bit Lloyd-Max quantizer. The two code bits are stored in
    /// separate bit planes (b0, b1) for the AND+popcount kernel.
    ///
    /// **Stage 2 — QJL**: Computes the quantization error `e = n − n_hat`, stores
    /// `sign(e[i])` as a third bit plane, and derives `qjl_scale = Σ|e[i]| / dim`.
    ///
    /// The header precomputes `polar_vl_factor` and `signed_sum_qjl` so that the
    /// bitwise kernel avoids per-code popcount recomputation at query time.
    pub fn quantize(embedding: &[f32], centroid: &[f32]) -> Self {
        let dim = embedding.len();
        let pl = Self::plane_len(dim);

        // Residual computation: fused r, norm², radial in one pass.
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
                    polar_vl_factor: 0.0,
                    signed_sum_qjl: 0,
                },
            ));
            return Self(bytes);
        }

        let sqrt_dim = (dim as f32).sqrt();
        let inv_sqrt_dim = 1.0 / sqrt_dim;
        let inv_norm = 1.0 / norm;

        // ── Stage 1: PolarQuant (2-bit → two bit planes) ───────────────────

        let mut plane_b0 = vec![0u8; pl];
        let mut plane_b1 = vec![0u8; pl];
        let mut pop_b0 = 0u32;
        let mut pop_b1 = 0u32;

        // Also accumulate n_hat[i] inline for Stage 2 (no separate Vec needed
        // if we recompute n[i] in Stage 2's loop — but we need n_hat[i] for the
        // error. Store n_hat values temporarily to avoid recomputing the quantizer.)
        let mut n_hat = Vec::with_capacity(dim);

        for i in 0..dim {
            let ni = r[i] * inv_norm;
            let scaled = ni * sqrt_dim;
            let code = scalar_quantize(scaled);

            let bit0 = code & 1;
            let bit1 = (code >> 1) & 1;
            if bit0 == 1 {
                plane_b0[i / 8] |= 1 << (i % 8);
                pop_b0 += 1;
            }
            if bit1 == 1 {
                plane_b1[i / 8] |= 1 << (i % 8);
                pop_b1 += 1;
            }

            n_hat.push(scalar_dequantize(code) * inv_sqrt_dim);
        }

        // Precompute polar_vl_factor for the bitwise kernel.
        let polar_vl_factor =
            inv_sqrt_dim * (ALPHA * dim as f32 + BETA * pop_b0 as f32 + GAMMA * pop_b1 as f32);

        // ── Stage 2: QJL (1-bit sign of quantization error) ────────────────

        let mut plane_qjl = vec![0u8; pl];
        let mut error_abs_sum = 0.0f32;
        let mut n_hat_dot_n = 0.0f32;
        let mut sign_e_dot_n = 0.0f32;
        let mut pop_qjl = 0u32;

        for i in 0..dim {
            let ni = r[i] * inv_norm;
            let ei = ni - n_hat[i];
            error_abs_sum += ei.abs();
            n_hat_dot_n += n_hat[i] * ni;

            let sign = if ei >= 0.0 {
                plane_qjl[i / 8] |= 1 << (i % 8);
                pop_qjl += 1;
                1.0f32
            } else {
                -1.0
            };
            sign_e_dot_n += sign * ni;
        }

        let qjl_scale = error_abs_sum / dim as f32;
        let signed_sum_qjl = 2 * pop_qjl as i32 - dim as i32;
        let correction = n_hat_dot_n + qjl_scale * sign_e_dot_n;

        // ── Assemble output ─────────────────────────────────────────────────

        let mut bytes = Vec::with_capacity(Self::size(dim));
        bytes.extend_from_slice(bytemuck::bytes_of(&TurboQuantHeader {
            correction,
            norm,
            radial,
            qjl_scale,
            polar_vl_factor,
            signed_sum_qjl,
        }));
        bytes.extend_from_slice(&plane_b0);
        bytes.extend_from_slice(&plane_b1);
        bytes.extend_from_slice(&plane_qjl);

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

    // ── Helpers ─────────────────────────────────────────────────────────

    fn random_vectors(rng: &mut StdRng, centroid: &[f32], k: f32, n: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|_| centroid.iter().map(|c| c + rng.gen_range(-k..k)).collect())
            .collect()
    }

    fn exact_distance(a: &[f32], b: &[f32], distance_fn: &DistanceFunction) -> f32 {
        match distance_fn {
            DistanceFunction::Cosine => SpatialSimilarity::cos(a, b).unwrap_or(0.0) as f32,
            DistanceFunction::Euclidean => SpatialSimilarity::l2sq(a, b).unwrap_or(0.0) as f32,
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
        assert!(h.polar_vl_factor.is_finite());

        // Verify norm is ‖r‖
        let r: Vec<f32> = embedding.iter().zip(&centroid).map(|(e, c)| e - c).collect();
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
            (h.radial - expected_radial).abs() < 1.0,
            "radial mismatch: {} vs {}",
            h.radial,
            expected_radial
        );

        // Verify buffer size
        assert_eq!(code.as_ref().len(), TurboQuantCode::<Vec<u8>>::size(300));
    }

    #[test]
    fn test_size() {
        // dim=256: plane_len=32, total=24+3*32=120
        assert_eq!(TurboQuantCode::<Vec<u8>>::plane_len(256), 32);
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(256), 24 + 3 * 32);

        // dim=1024: plane_len=128, total=24+3*128=408
        assert_eq!(TurboQuantCode::<Vec<u8>>::plane_len(1024), 128);
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(1024), 408);

        // dim=300: padded to ceil(300/64)*8 = 5*8 = 40, total=24+3*40=144
        assert_eq!(TurboQuantCode::<Vec<u8>>::plane_len(300), 40);
        assert_eq!(TurboQuantCode::<Vec<u8>>::size(300), 144);
    }

    #[test]
    fn test_zero_residual() {
        let embedding: Vec<f32> = (0..300).map(|i| i as f32).collect();

        let code = TurboQuantCode::quantize(&embedding, &embedding);
        let h = code.header();
        assert_eq!(h.correction, 1.0);
        assert!(h.norm < f32::EPSILON);
        assert_eq!(h.qjl_scale, 0.0);

        let centroid: Vec<f32> = embedding.iter().map(|x| x + 1e-10).collect();
        let code = TurboQuantCode::quantize(&embedding, &centroid);
        let h = code.header();
        assert_eq!(h.correction, 1.0);
        assert!(h.norm < f32::EPSILON);
    }

    #[test]
    fn test_polar_roundtrip() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 128;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let embedding: Vec<f32> = centroid.iter().map(|c| c + rng.gen_range(-1.0..1.0)).collect();

        let code = TurboQuantCode::quantize(&embedding, &centroid);
        let n_hat = code.unpack_polar(dim);

        let r: Vec<f32> = embedding.iter().zip(&centroid).map(|(e, c)| e - c).collect();
        let norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        let n: Vec<f32> = r.iter().map(|&x| x / norm).collect();

        let sign_matches = (0..dim).filter(|&i| (n[i] >= 0.0) == (n_hat[i] >= 0.0)).count();
        let rate = sign_matches as f32 / dim as f32;
        assert!(rate > 0.95, "PolarQuant sign match rate too low: {:.2}%", rate * 100.0);
    }

    // ── Bitwise kernel correctness ──────────────────────────────────────

    /// Verify that the bitwise kernel produces the same distances as the scalar path.
    #[test]
    fn test_bitwise_matches_scalar() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let k = 2.0;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-k..k)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = random_vectors(&mut rng, &centroid, k, 64);
        let codes: Vec<_> = vectors.iter().map(|v| TurboQuantCode::quantize(v, &centroid)).collect();

        let pl = TurboQuantCode::<Vec<u8>>::plane_len(dim);

        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            for i in 0..codes.len() {
                for j in (i + 1)..codes.len().min(i + 8) {
                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q: Vec<f32> = centroid.iter().zip(q).map(|(c, q)| q - c).collect();

                    let scalar = codes[i].distance_query(
                        &distance_fn, &r_q, c_norm, c_dot_q, q_norm,
                    );
                    let qq = QuantizedQuery::new(&r_q, pl, c_norm, c_dot_q, q_norm);
                    let bitwise = codes[i].distance_quantized_query(&distance_fn, &qq, dim);

                    // Allow small divergence from query quantization rounding.
                    let rel_err = (scalar - bitwise).abs() / scalar.abs().max(f32::EPSILON);
                    assert!(
                        rel_err < 0.05,
                        "{:?}: scalar={} bitwise={} rel_err={:.4}",
                        distance_fn, scalar, bitwise, rel_err,
                    );
                }
            }
        }
    }

    // ── Distance accuracy ───────────────────────────────────────────────

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
        let codes: Vec<_> = vectors.iter().map(|v| TurboQuantCode::quantize(v, &centroid)).collect();

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
                "{:?} k={}: distance_code P95 rel error {:.4} exceeds {:.4}",
                distance_fn, k, p95_code, max_p95_rel_error
            );
            assert!(
                p95_query < max_p95_rel_error,
                "{:?} k={}: distance_query P95 rel error {:.4} exceeds {:.4}",
                distance_fn, k, p95_query, max_p95_rel_error
            );
        }
    }

    // ── Comparison with RaBitQ ──────────────────────────────────────────

    #[test]
    fn test_comparison_with_rabitq() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let k = 2.0;
        let n_vectors = 256;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-k..k)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = random_vectors(&mut rng, &centroid, k, n_vectors);

        // ── Encode ──────────────────────────────────────────────────────

        let t0 = Instant::now();
        let codes_1bit: Vec<_> = vectors.iter().map(|v| Code::<1>::quantize(v, &centroid)).collect();
        let time_1bit = t0.elapsed();

        let t0 = Instant::now();
        let codes_4bit: Vec<_> = vectors.iter().map(|v| Code::<4>::quantize(v, &centroid)).collect();
        let time_4bit = t0.elapsed();

        let t0 = Instant::now();
        let codes_turbo: Vec<_> = vectors.iter().map(|v| TurboQuantCode::quantize(v, &centroid)).collect();
        let time_turbo = t0.elapsed();

        // ── Compression ratios ──────────────────────────────────────────

        let raw_size = dim * 4;
        let size_1bit = Code::<1, Vec<u8>>::size(dim);
        let size_4bit = Code::<4, Vec<u8>>::size(dim);
        let size_turbo = TurboQuantCode::<Vec<u8>>::size(dim);

        eprintln!("\n╔══════════════════════════════════════════════════════════════╗");
        eprintln!("║         TurboQuant vs RaBitQ Comparison (dim={})         ║", dim);
        eprintln!("╠══════════════════════════════════════════════════════════════╣");
        eprintln!("║ Method          │ Size    │ Ratio  │ Encode time           ║");
        eprintln!("╟─────────────────┼─────────┼────────┼───────────────────────╢");
        eprintln!("║ Raw f32         │ {:>5} B │  1.0x  │ —                     ║", raw_size);
        eprintln!("║ RaBitQ 1-bit    │ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_1bit, raw_size as f32 / size_1bit as f32, time_1bit, n_vectors);
        eprintln!("║ TurboQuant 3-bit│ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_turbo, raw_size as f32 / size_turbo as f32, time_turbo, n_vectors);
        eprintln!("║ RaBitQ 4-bit    │ {:>5} B │ {:>4.1}x  │ {:>10.2?} ({} vecs) ║",
            size_4bit, raw_size as f32 / size_4bit as f32, time_4bit, n_vectors);
        eprintln!("╚══════════════════════════════════════════════════════════════╝");

        // ── Distance accuracy (all using query-path) ────────────────────

        let padded_bytes_1bit = Code::<1, Vec<u8>>::packed_len(dim);
        let pl_turbo = TurboQuantCode::<Vec<u8>>::plane_len(dim);

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

                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q: Vec<f32> = centroid.iter().zip(q).map(|(c, q)| q - c).collect();

                    // 1-bit: quantized-query path (AND+popcount)
                    let qq_1bit = QuantizedQuery::new(&r_q, padded_bytes_1bit, c_norm, c_dot_q, q_norm);
                    let code_ref = Code::<1, _>::new(codes_1bit[i].as_ref());
                    let est_1bit = code_ref.distance_quantized_query(&distance_fn, &qq_1bit);

                    // 4-bit: float query path
                    let est_4bit = codes_4bit[i].distance_query(&distance_fn, &r_q, c_norm, c_dot_q, q_norm);

                    // TurboQuant: bitwise kernel (the fast path we're benchmarking)
                    let qq_turbo = QuantizedQuery::new(&r_q, pl_turbo, c_norm, c_dot_q, q_norm);
                    let est_turbo = codes_turbo[i].distance_quantized_query(&distance_fn, &qq_turbo, dim);

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
            eprintln!("  │ RaBitQ 1-bit    │ {:.4}   │ {:.4}   │", p50(&errors_1bit), p95(&errors_1bit));
            eprintln!("  │ TurboQuant 3-bit│ {:.4}   │ {:.4}   │", p50(&errors_turbo), p95(&errors_turbo));
            eprintln!("  │ RaBitQ 4-bit    │ {:.4}   │ {:.4}   │", p50(&errors_4bit), p95(&errors_4bit));
            eprintln!("  └─────────────────┴──────────┴──────────┘");
        }

        // ── Recall@10 ───────────────────────────────────────────────────

        let query = &vectors[0];
        let q_norm = (f32::dot(query, query).unwrap_or(0.0) as f32).sqrt();
        let c_dot_q = f32::dot(&centroid, query).unwrap_or(0.0) as f32;
        let r_q: Vec<f32> = centroid.iter().zip(query).map(|(c, q)| q - c).collect();
        let qq = QuantizedQuery::new(&r_q, pl_turbo, c_norm, c_dot_q, q_norm);

        let mut exact_dists: Vec<(usize, f32)> = (1..n_vectors)
            .map(|i| (i, exact_distance(query, &vectors[i], &DistanceFunction::Cosine)))
            .collect();
        exact_dists.sort_by(|a, b| a.1.total_cmp(&b.1));

        let mut turbo_dists: Vec<(usize, f32)> = (1..n_vectors)
            .map(|i| (i, codes_turbo[i].distance_quantized_query(&DistanceFunction::Cosine, &qq, dim)))
            .collect();
        turbo_dists.sort_by(|a, b| a.1.total_cmp(&b.1));

        let k_recall = 10;
        let exact_top: std::collections::HashSet<usize> =
            exact_dists.iter().take(k_recall).map(|&(i, _)| i).collect();
        let turbo_top: std::collections::HashSet<usize> =
            turbo_dists.iter().take(k_recall).map(|&(i, _)| i).collect();
        let recall = exact_top.intersection(&turbo_top).count() as f32 / k_recall as f32;

        eprintln!("\n  Recall@{}: {:.0}%", k_recall, recall * 100.0);
        assert!(recall >= 0.7, "TurboQuant recall@{} too low: {:.0}%", k_recall, recall * 100.0);
    }

    // ── Speed benchmarks ────────────────────────────────────────────────

    #[test]
    fn test_encoding_speed() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let n_vectors = 1000;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-2.0..2.0)).collect();
        let vectors = random_vectors(&mut rng, &centroid, 2.0, n_vectors);

        for v in vectors.iter().take(10) {
            let _ = TurboQuantCode::quantize(v, &centroid);
            let _ = Code::<1>::quantize(v, &centroid);
            let _ = Code::<4>::quantize(v, &centroid);
        }

        let iters = 3;

        let t0 = Instant::now();
        for _ in 0..iters { for v in &vectors { std::hint::black_box(Code::<1>::quantize(v, &centroid)); } }
        let time_1bit = t0.elapsed() / (iters * n_vectors as u32);

        let t0 = Instant::now();
        for _ in 0..iters { for v in &vectors { std::hint::black_box(Code::<4>::quantize(v, &centroid)); } }
        let time_4bit = t0.elapsed() / (iters * n_vectors as u32);

        let t0 = Instant::now();
        for _ in 0..iters { for v in &vectors { std::hint::black_box(TurboQuantCode::quantize(v, &centroid)); } }
        let time_turbo = t0.elapsed() / (iters * n_vectors as u32);

        eprintln!("\n  Encoding speed per vector (dim={}):", dim);
        eprintln!("  RaBitQ 1-bit:     {:>8.2?}", time_1bit);
        eprintln!("  TurboQuant 3-bit: {:>8.2?}", time_turbo);
        eprintln!("  RaBitQ 4-bit:     {:>8.2?}", time_4bit);

        assert!(time_turbo < time_4bit * 10);
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

        let codes_1bit: Vec<_> = vectors[1..].iter().map(|v| Code::<1>::quantize(v, &centroid)).collect();
        let codes_4bit: Vec<_> = vectors[1..].iter().map(|v| Code::<4>::quantize(v, &centroid)).collect();
        let codes_turbo: Vec<_> = vectors[1..].iter().map(|v| TurboQuantCode::quantize(v, &centroid)).collect();

        let padded_1bit = Code::<1, Vec<u8>>::packed_len(dim);
        let pl_turbo = TurboQuantCode::<Vec<u8>>::plane_len(dim);

        // Pre-quantize queries (amortized cost, computed once per cluster scan)
        let qq_1bit = QuantizedQuery::new(&r_q, padded_1bit, c_norm, c_dot_q, q_norm);
        let qq_turbo = QuantizedQuery::new(&r_q, pl_turbo, c_norm, c_dot_q, q_norm);

        let iters = 3;

        // 1-bit: AND+popcount kernel
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_1bit {
                let code_ref = Code::<1, _>::new(code.as_ref());
                std::hint::black_box(code_ref.distance_quantized_query(&DistanceFunction::Cosine, &qq_1bit));
            }
        }
        let time_1bit = t0.elapsed() / (iters * n_codes as u32);

        // 4-bit: float unpack + simsimd::dot
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_4bit {
                std::hint::black_box(code.distance_query(&DistanceFunction::Cosine, &r_q, c_norm, c_dot_q, q_norm));
            }
        }
        let time_4bit = t0.elapsed() / (iters * n_codes as u32);

        // TurboQuant: scalar path
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_turbo {
                std::hint::black_box(code.distance_query(&DistanceFunction::Cosine, &r_q, c_norm, c_dot_q, q_norm));
            }
        }
        let time_turbo_scalar = t0.elapsed() / (iters * n_codes as u32);

        // TurboQuant: bitwise kernel
        let t0 = Instant::now();
        for _ in 0..iters {
            for code in &codes_turbo {
                std::hint::black_box(code.distance_quantized_query(&DistanceFunction::Cosine, &qq_turbo, dim));
            }
        }
        let time_turbo_bitwise = t0.elapsed() / (iters * n_codes as u32);

        eprintln!("\n  Query speed per code (dim={}):", dim);
        eprintln!("  RaBitQ 1-bit  (bitwise):    {:>8.2?}", time_1bit);
        eprintln!("  TurboQuant    (bitwise):    {:>8.2?}", time_turbo_bitwise);
        eprintln!("  TurboQuant    (scalar):     {:>8.2?}", time_turbo_scalar);
        eprintln!("  RaBitQ 4-bit  (float dot):  {:>8.2?}", time_4bit);
        eprintln!(
            "\n  Bitwise speedup over scalar: {:.1}x",
            time_turbo_scalar.as_nanos() as f64 / time_turbo_bitwise.as_nanos().max(1) as f64
        );
    }
}

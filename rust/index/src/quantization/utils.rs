//! Shared infrastructure for RaBitQ code types.
//!
//! Contains the [`RabitqCode`] trait, the [`CodeHeader`] byte layout,
//! and the distance math helpers used by both [`super::quantization1bit`]
//! and [`super::quantization4bit`].

use bitpacking::{BitPacker, BitPacker8x};
use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;

// ── Header ────────────────────────────────────────────────────────────────────

/// Byte header for 4-bit quantized codes. 12 bytes.
///
/// Read and written via `bytemuck::pod_read_unaligned` / `bytemuck::bytes_of`
/// on the raw code buffer, so the layout is `#[repr(C)]` and `Pod`.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct CodeHeader {
    pub correction: f32,
    pub norm: f32,
    pub radial: f32,
}

// ── Shared interface ──────────────────────────────────────────────────────────

/// Shared accessors for RaBitQ code types.
///
/// Implemented by both [`super::quantization1bit::Code1Bit`] and
/// [`super::quantization4bit::Code4Bit`], allowing the shared distance helpers
/// ([`rabitq_distance_query`], [`rabitq_distance_code`]) to read header fields
/// without knowing the concrete code type.
pub trait RabitqCode {
    /// Correction factor `⟨g, n⟩`.
    fn correction(&self) -> f32;
    /// Data residual norm `‖r‖`.
    fn norm(&self) -> f32;
    /// Radial component `⟨r, c⟩`.
    fn radial(&self) -> f32;
    /// Packed quantization codes (excluding the header).
    fn packed(&self) -> &[u8];
}

// ── Shared math helpers ───────────────────────────────────────────────────────
//
// Called by both Code1Bit and Code4Bit after computing their type-specific
// inner product kernel. Factoring out the shared formulas keeps the
// DistanceFunction match arms in one place.

/// Estimates distance from data vector `d` to query `q`.
///
/// `g_dot_r_q` is the inner product `⟨g, r_q⟩`, computed differently by
/// each code type before calling this helper.
///
/// See module-level documentation for the derivation.
pub fn rabitq_distance_query(
    g_dot_r_q: f32,
    code: &impl RabitqCode,
    c_norm: f32,
    c_dot_q: f32,
    q_norm: f32,
    distance_fn: &DistanceFunction,
) -> f32 {
    let norm = code.norm();
    let radial = code.radial();
    let correction = code.correction();

    // ⟨r, r_q⟩ ≈ ‖r‖ · ⟨g, r_q⟩ / ⟨g, n⟩
    let r_dot_r_q = norm * g_dot_r_q / correction;
    // ⟨d, q⟩ = ⟨c, q⟩ + ⟨r, c⟩ + ⟨r, r_q⟩
    let d_dot_q = c_dot_q + radial + r_dot_r_q;
    // ‖d‖² = ‖c‖² + 2⟨r, c⟩ + ‖r‖²
    let d_norm_sq = c_norm * c_norm + 2.0 * radial + norm * norm;

    match distance_fn {
        DistanceFunction::Cosine => 1.0 - d_dot_q / (d_norm_sq.sqrt() * q_norm).max(f32::EPSILON),
        DistanceFunction::Euclidean => d_norm_sq + q_norm * q_norm - 2.0 * d_dot_q,
        DistanceFunction::InnerProduct => 1.0 - d_dot_q,
    }
}

/// Estimates distance between two data vectors `d_a` and `d_b`.
///
/// `g_a_dot_g_b` is the inner product `⟨g_a, g_b⟩`, computed differently by
/// each code type before calling this helper.
///
/// See module-level documentation for the derivation.
pub fn rabitq_distance_code(
    g_a_dot_g_b: f32,
    code_a: &impl RabitqCode,
    code_b: &impl RabitqCode,
    c_norm: f32,
    distance_fn: &DistanceFunction,
) -> f32 {
    let norm_a = code_a.norm();
    let norm_b = code_b.norm();
    let radial_a = code_a.radial();
    let radial_b = code_b.radial();
    let correction_a = code_a.correction();
    let correction_b = code_b.correction();

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

    // ⟨n_a, n_b⟩ ≈ ⟨g_a, g_b⟩ / (⟨g_a, n_a⟩ · ⟨g_b, n_b⟩)
    let n_a_dot_n_b = g_a_dot_g_b / (correction_a * correction_b);
    // ⟨d_a, d_b⟩ = ‖c‖² + ⟨r_a, c⟩ + ⟨r_b, c⟩ + ‖r_a‖·‖r_b‖·⟨n_a, n_b⟩
    let d_a_dot_d_b = c_norm * c_norm + radial_a + radial_b + norm_a * norm_b * n_a_dot_n_b;

    // ‖d‖² = ‖c‖² + 2⟨r, c⟩ + ‖r‖²
    let d_a_norm_sq = c_norm * c_norm + 2.0 * radial_a + norm_a * norm_a;
    let d_b_norm_sq = c_norm * c_norm + 2.0 * radial_b + norm_b * norm_b;

    match distance_fn {
        DistanceFunction::Cosine => {
            1.0 - d_a_dot_d_b / (d_a_norm_sq.sqrt() * d_b_norm_sq.sqrt()).max(f32::EPSILON)
        }
        DistanceFunction::Euclidean => d_a_norm_sq + d_b_norm_sq - 2.0 * d_a_dot_d_b,
        DistanceFunction::InnerProduct => 1.0 - d_a_dot_d_b,
    }
}

// ── Sizing helper ─────────────────────────────────────────────────────────────
//
// Module-level function rather than a method to avoid Rust's "can't infer T"
// error when calling Code4Bit::size(dim) without a concrete T annotation.

/// Padded dimension for 4-bit codes (multiple of BitPacker8x::BLOCK_LEN = 256).
pub fn padded_dim_4bit(dim: usize) -> usize {
    dim.div_ceil(BitPacker8x::BLOCK_LEN) * BitPacker8x::BLOCK_LEN
}

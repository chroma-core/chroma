//! Shared infrastructure for RaBitQ code types.
//!
//! Contains the [`CodeHeader`] byte layout and the distance math helpers
//! used by both [`super::single_bit`] and [`super::multi_bit`].

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

// ── Shared math helpers ───────────────────────────────────────────────────────
//
// Called by both Code::<1> and Code::<4> after computing their type-specific
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
    correction: f32,
    norm: f32,
    radial: f32,
    c_norm: f32,
    c_dot_q: f32,
    q_norm: f32,
    distance_fn: &DistanceFunction,
) -> f32 {
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
    correction_a: f32,
    norm_a: f32,
    radial_a: f32,
    correction_b: f32,
    norm_b: f32,
    radial_b: f32,
    c_norm: f32,
    distance_fn: &DistanceFunction,
) -> f32 {
    // Constructing an Unbiased Estimator for Distance Estimation
    // Section 3.2 of the paper
    //
    // The key achievement is estimating the product of the data and query vectors:
    //     ⟨o, q⟩ in the paper and ⟨d_a, d_b⟩ in our code.
    // Theorem 3.2:
    //     ⟨o, q⟩ = E[⟨o¯, q⟩ / ⟨o¯, o⟩]
    //
    //     Where the error is bounded by O(1/√D) with high probability.
    //     Namely, as D → ∞, the error approaches 0.
    //     The constant factor of O depends on the norms of the data and query vectors.

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

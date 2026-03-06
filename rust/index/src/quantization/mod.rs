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
//!
//! ## Code types
//!
//! Two concrete implementations cover the supported bit widths:
//!
//! | Type | Header | Quantization |
//! |------|--------|-------------|
//! | [`Code<1>`] | 16 bytes (includes `signed_sum`) | Sign of residual |
//! | [`Code<4>`] | 12 bytes | Ray-walk |
//!
//! The shared distance math lives in `utils::rabitq_distance_query` and
//! `utils::rabitq_distance_code`; each concrete type supplies only the
//! type-specific inner product kernel before calling the helper.

mod multi_bit;
mod single_bit;

use chroma_distance::DistanceFunction;

pub struct Code<const BITS: u8, T = Vec<u8>>(T);

impl<const BITS: u8, T: AsRef<[u8]>> AsRef<[u8]> for Code<BITS, T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub use single_bit::{BatchQueryLuts, QuantizedQuery};

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
pub(crate) fn rabitq_distance_query(
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
pub(crate) fn rabitq_distance_code(
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

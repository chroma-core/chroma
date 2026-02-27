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
//! | [`Code1Bit`] | 16 bytes (includes `signed_sum`) | Sign of residual |
//! | [`Code4Bit`] | 12 bytes | Ray-walk |
//!
//! Both implement [`RabitqCode`] for shared accessor access. The shared distance
//! math lives in `utils::rabitq_distance_query` and `utils::rabitq_distance_code`,
//! which take any `impl RabitqCode` — each concrete type supplies only the
//! type-specific inner product kernel before calling the helper.
//!
//! [`Code<T>`] is a type alias for [`Code4Bit<T>`] for backward compatibility.

mod quantization1bit;
mod quantization4bit;
mod utils;

use chroma_distance::DistanceFunction;
use std::sync::Arc;

pub use quantization1bit::{BatchQueryLuts, Code1Bit, QuantizedQuery};
pub use quantization4bit::{Code, Code4Bit};
pub use utils::RabitqCode;

/// Runtime selection of code format (1-bit or 4-bit). Create from config bits, then call
/// `quantize`, `distance_query`, and `size` without dispatching at each call site.
#[derive(Clone, Copy, Debug)]
pub enum GenericCode {
    OneBit,
    FourBit,
}

impl GenericCode {
    pub fn new(bits: u8) -> Self {
        match bits {
            1 => GenericCode::OneBit,
            _ => GenericCode::FourBit,
        }
    }

    pub fn size(&self, dim: usize) -> usize {
        match self {
            GenericCode::OneBit => Code1Bit::size(dim),
            GenericCode::FourBit => Code4Bit::size(dim),
        }
    }

    pub fn quantize(&self, embedding: &[f32], centroid: &[f32]) -> Arc<[u8]> {
        match self {
            GenericCode::OneBit => Code1Bit::quantize(embedding, centroid).as_ref().into(),
            GenericCode::FourBit => Code4Bit::quantize(embedding, centroid).as_ref().into(),
        }
    }

    pub fn distance_query(
        &self,
        code_bytes: &[u8],
        distance_fn: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        match self {
            GenericCode::OneBit => {
                Code1Bit::new(code_bytes).distance_query(distance_fn, r_q, c_norm, c_dot_q, q_norm)
            }
            GenericCode::FourBit => {
                Code4Bit::new(code_bytes).distance_query(distance_fn, r_q, c_norm, c_dot_q, q_norm)
            }
        }
    }
}

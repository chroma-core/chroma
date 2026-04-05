//! Sparse vector value quantization to u8.
//!
//! Provides log-scale quantization for sparse vector values, which is
//! significantly more accurate than naive linear quantization for the
//! power-law distributions typical of BM25 and SPLADE sparse vectors.
//!
//! # Encoding
//!
//! ```text
//! u8_code = round(log(1 + value) / log(1 + dim_max) * 255)
//! ```
//!
//! # Decoding
//!
//! ```text
//! value = exp(u8_code / 255 * log(1 + dim_max)) - 1
//! ```
//!
//! # Properties
//!
//! - **Monotonic**: preserves value ordering within a dimension, so block-max
//!   WAND upper bounds remain valid.
//! - **Single parameter**: requires only one `f32` scale factor per dimension
//!   (the dimension max), same as naive linear quantization.
//! - **Skew-aware**: allocates more u8 codes to the dense lower range of values,
//!   where most postings concentrate. This reduces RMSE by 20-46% on BM25/SPLADE
//!   distributions compared to linear quantization (see `quantization_research.py`).
//! - **Cheap**: `ln`/`exp` are hardware-accelerated on modern CPUs.

/// Quantize a single f32 value to u8 using log scaling.
///
/// `dim_max` is the maximum value observed in this dimension (the scale factor).
/// Returns 0 if `dim_max <= 0` or `value <= 0`.
#[inline]
pub fn quantize_log(value: f32, dim_max: f32) -> u8 {
    if dim_max <= 0.0 || value <= 0.0 {
        return 0;
    }
    let log_max = (1.0 + dim_max).ln();
    let code = ((1.0 + value).ln() / log_max * 255.0).round();
    code.clamp(0.0, 255.0) as u8
}

/// Dequantize a u8 code back to an approximate f32 value using log scaling.
///
/// `dim_max` is the same scale factor used during quantization.
#[inline]
pub fn dequantize_log(code: u8, dim_max: f32) -> f32 {
    if code == 0 || dim_max <= 0.0 {
        return 0.0;
    }
    let log_max = (1.0 + dim_max).ln();
    let t = code as f32 / 255.0;
    (t * log_max).exp() - 1.0
}

/// Quantize an entire posting list (slice of values) for a single dimension.
///
/// Returns a `Vec<u8>` of the same length.
pub fn quantize_posting_list(values: &[f32], dim_max: f32) -> Vec<u8> {
    // Pre-compute log(1 + dim_max) once for the whole list
    if dim_max <= 0.0 {
        return vec![0u8; values.len()];
    }
    let log_max = (1.0 + dim_max).ln();
    let inv_log_max = 255.0 / log_max;

    values
        .iter()
        .map(|&v| {
            if v <= 0.0 {
                0u8
            } else {
                ((1.0 + v).ln() * inv_log_max).round().clamp(0.0, 255.0) as u8
            }
        })
        .collect()
}

/// Dequantize an entire posting list back to approximate f32 values.
pub fn dequantize_posting_list(codes: &[u8], dim_max: f32) -> Vec<f32> {
    if dim_max <= 0.0 {
        return vec![0.0; codes.len()];
    }
    let log_max = (1.0 + dim_max).ln();
    let inv_255 = log_max / 255.0;

    codes
        .iter()
        .map(|&c| {
            if c == 0 {
                0.0
            } else {
                (c as f32 * inv_255).exp() - 1.0
            }
        })
        .collect()
}

/// Compute the conservative (upper-bound) dequantized value for a u8 code.
///
/// Because quantization rounds to the nearest code, the true value could be
/// anywhere in the interval that maps to this code. For block-max WAND
/// correctness, we need an upper bound on the true value.
///
/// Returns the dequantized value of `code + 0.5` (the upper edge of the
/// rounding interval), clamped to `dim_max`.
#[inline]
pub fn dequantize_log_upper_bound(code: u8, dim_max: f32) -> f32 {
    if code == 0 || dim_max <= 0.0 {
        return 0.0;
    }
    if code == 255 {
        return dim_max;
    }
    let log_max = (1.0 + dim_max).ln();
    let t = (code as f32 + 0.5) / 255.0;
    ((t * log_max).exp() - 1.0).min(dim_max)
}

/// Quantize a block maximum value conservatively.
///
/// For WAND correctness, the quantized block max must be >= the true block max.
/// We quantize to the next higher code to ensure the upper bound property.
#[inline]
pub fn quantize_block_max(value: f32, dim_max: f32) -> u8 {
    let code = quantize_log(value, dim_max);
    // If the dequantized value is less than the original, bump up by 1
    if code < 255 && dequantize_log(code, dim_max) < value {
        code + 1
    } else {
        code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_zero() {
        assert_eq!(quantize_log(0.0, 10.0), 0);
        assert_eq!(dequantize_log(0, 10.0), 0.0);
    }

    #[test]
    fn test_roundtrip_max() {
        let dim_max = 10.0;
        let code = quantize_log(dim_max, dim_max);
        assert_eq!(code, 255);
        let recon = dequantize_log(255, dim_max);
        assert!((recon - dim_max).abs() < 0.01, "max should roundtrip: {recon}");
    }

    #[test]
    fn test_monotonic() {
        let dim_max = 50.0;
        let values = [0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0];
        let codes: Vec<u8> = values.iter().map(|&v| quantize_log(v, dim_max)).collect();
        for i in 1..codes.len() {
            assert!(
                codes[i] >= codes[i - 1],
                "monotonicity violated: codes[{}]={} < codes[{}]={}",
                i,
                codes[i],
                i - 1,
                codes[i - 1]
            );
        }
    }

    #[test]
    fn test_better_resolution_for_small_values() {
        // Log quantization should allocate more codes to the lower range.
        // For dim_max=100, the value 1.0 should get a higher code than
        // naive linear (which would give round(1/100*255) = 3).
        let dim_max = 100.0;
        let code = quantize_log(1.0, dim_max);
        let naive_code = (1.0 / dim_max * 255.0).round() as u8;
        assert!(
            code > naive_code,
            "log code {} should be > naive code {} for small value",
            code,
            naive_code
        );
    }

    #[test]
    fn test_reconstruction_error_bm25_like() {
        // Simulate a BM25-like dimension: most values small, few large
        let dim_max = 15.0;
        let values = [0.3, 0.5, 0.8, 1.2, 1.5, 2.0, 3.0, 5.0, 8.0, 15.0];

        let mut log_mse = 0.0;
        let mut naive_mse = 0.0;

        for &v in &values {
            // Log quantization error
            let log_code = quantize_log(v, dim_max);
            let log_recon = dequantize_log(log_code, dim_max);
            log_mse += (v - log_recon).powi(2);

            // Naive linear quantization error
            let naive_code = (v / dim_max * 255.0).round().clamp(0.0, 255.0) as u8;
            let naive_recon = naive_code as f32 / 255.0 * dim_max;
            naive_mse += (v - naive_recon).powi(2);
        }

        log_mse /= values.len() as f32;
        naive_mse /= values.len() as f32;

        assert!(
            log_mse < naive_mse,
            "log MSE ({}) should be less than naive MSE ({})",
            log_mse,
            naive_mse
        );
    }

    #[test]
    fn test_posting_list_roundtrip() {
        let dim_max = 20.0;
        let values = vec![0.1, 0.5, 1.0, 3.0, 7.0, 15.0, 20.0];
        let codes = quantize_posting_list(&values, dim_max);
        let recon = dequantize_posting_list(&codes, dim_max);

        assert_eq!(codes.len(), values.len());
        assert_eq!(recon.len(), values.len());

        for (i, (&orig, &rec)) in values.iter().zip(recon.iter()).enumerate() {
            let rel_err = if orig > 0.0 {
                (orig - rec).abs() / orig
            } else {
                rec.abs()
            };
            assert!(
                rel_err < 0.05,
                "value[{}]: orig={}, recon={}, rel_err={:.4}",
                i,
                orig,
                rec,
                rel_err
            );
        }
    }

    #[test]
    fn test_block_max_conservative() {
        // Block max quantization must never underestimate
        let dim_max = 50.0;
        let test_values = [0.1, 0.5, 1.0, 3.0, 10.0, 25.0, 49.9, 50.0];

        for &v in &test_values {
            let code = quantize_block_max(v, dim_max);
            let recon = dequantize_log(code, dim_max);
            assert!(
                recon >= v || (v - recon).abs() < 1e-5,
                "block max underestimates: value={}, recon={}",
                v,
                recon
            );
        }
    }

    #[test]
    fn test_upper_bound_conservative() {
        let dim_max = 30.0;
        for code in 0..=255u8 {
            let ub = dequantize_log_upper_bound(code, dim_max);
            let val = dequantize_log(code, dim_max);
            assert!(
                ub >= val,
                "upper bound {} < value {} for code {}",
                ub,
                val,
                code
            );
        }
    }

    #[test]
    fn test_edge_cases() {
        // Negative dim_max
        assert_eq!(quantize_log(1.0, -1.0), 0);
        assert_eq!(dequantize_log(128, -1.0), 0.0);

        // Negative value
        assert_eq!(quantize_log(-1.0, 10.0), 0);

        // Very small dim_max
        let code = quantize_log(0.001, 0.001);
        assert_eq!(code, 255);

        // Very large dim_max
        let code = quantize_log(0.001, 1_000_000.0);
        assert!(code < 10, "tiny value with huge max should get low code: {}", code);
    }

    #[test]
    fn test_dot_product_preservation() {
        // Verify that quantized dot products are close to original
        let dim_maxes = [5.0, 10.0, 20.0, 50.0];
        let doc_values = [0.3, 1.5, 0.8, 12.0];
        let query_values = [1.0, 0.5, 2.0, 0.1];

        let true_dot: f32 = doc_values
            .iter()
            .zip(query_values.iter())
            .map(|(d, q)| d * q)
            .sum();

        let quantized_dot: f32 = doc_values
            .iter()
            .zip(dim_maxes.iter())
            .zip(query_values.iter())
            .map(|((&v, &dm), &q)| {
                let code = quantize_log(v, dm);
                let recon = dequantize_log(code, dm);
                recon * q
            })
            .sum();

        let rel_err = (true_dot - quantized_dot).abs() / true_dot;
        assert!(
            rel_err < 0.02,
            "dot product relative error too high: {:.4} (true={}, quant={})",
            rel_err,
            true_dot,
            quantized_dot
        );
    }
}

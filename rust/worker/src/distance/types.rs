use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

/// The distance function enum.
/// # Description
/// This enum defines the distance functions supported by indices in Chroma.
/// # Variants
/// - `Euclidean` - The Euclidean or l2 norm.
/// - `Cosine` - The cosine distance. Specifically, 1 - cosine.
/// - `InnerProduct` - The inner product. Specifically, 1 - inner product.
/// # Notes
/// See https://docs.trychroma.com/usage-guide#changing-the-distance-function
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DistanceFunction {
    Euclidean,
    Cosine,
    InnerProduct,
}

impl DistanceFunction {
    // TOOD: Should we error if mismatched dimensions?
    pub(crate) fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        // TODO: implement this in SSE/AVX SIMD
        // For now we write these as loops since we suspect that will more likely
        // lead to the compiler vectorizing the code. (We saw this on
        // Apple Silicon Macs who didn't have hand-rolled SIMD instructions in our
        // C++ code).
        match self {
            DistanceFunction::Euclidean => {
                let mut sum = 0.0;
                for i in 0..a.len() {
                    sum += (a[i] - b[i]).powi(2);
                }
                sum
            }
            DistanceFunction::Cosine => {
                // For cosine we just assume the vectors have been normalized, since that
                // is what our indices expect.
                let mut sum = 0.0;
                for i in 0..a.len() {
                    sum += a[i] * b[i];
                }
                1.0_f32 - sum
            }
            DistanceFunction::InnerProduct => {
                let mut sum = 0.0;
                for i in 0..a.len() {
                    sum += a[i] * b[i];
                }
                1.0_f32 - sum
            }
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum DistanceFunctionError {
    #[error("Invalid distance function `{0}`")]
    InvalidDistanceFunction(String),
}

impl ChromaError for DistanceFunctionError {
    fn code(&self) -> ErrorCodes {
        match self {
            DistanceFunctionError::InvalidDistanceFunction(_) => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<&str> for DistanceFunction {
    type Error = DistanceFunctionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "l2" => Ok(DistanceFunction::Euclidean),
            "cosine" => Ok(DistanceFunction::Cosine),
            "ip" => Ok(DistanceFunction::InnerProduct),
            _ => Err(DistanceFunctionError::InvalidDistanceFunction(
                value.to_string(),
            )),
        }
    }
}

impl Into<String> for DistanceFunction {
    fn into(self) -> String {
        match self {
            DistanceFunction::Euclidean => "l2".to_string(),
            DistanceFunction::Cosine => "cosine".to_string(),
            DistanceFunction::InnerProduct => "ip".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_function_try_from() {
        let distance_function: DistanceFunction = "l2".try_into().unwrap();
        assert_eq!(distance_function, DistanceFunction::Euclidean);
        let distance_function: DistanceFunction = "cosine".try_into().unwrap();
        assert_eq!(distance_function, DistanceFunction::Cosine);
        let distance_function: DistanceFunction = "ip".try_into().unwrap();
        assert_eq!(distance_function, DistanceFunction::InnerProduct);
    }

    #[test]
    fn test_distance_function_into() {
        let distance_function: String = DistanceFunction::Euclidean.into();
        assert_eq!(distance_function, "l2");
        let distance_function: String = DistanceFunction::Cosine.into();
        assert_eq!(distance_function, "cosine");
        let distance_function: String = DistanceFunction::InnerProduct.into();
        assert_eq!(distance_function, "ip");
    }

    #[test]
    fn test_distance_function_l2sqr() {
        let a = vec![1.0, 2.0, 3.0];
        let a_mag = (1.0_f32.powi(2) + 2.0_f32.powi(2) + 3.0_f32.powi(2)).sqrt();
        let a_norm = vec![1.0 / a_mag, 2.0 / a_mag, 3.0 / a_mag];
        let b = vec![4.0, 5.0, 6.0];
        let b_mag = (4.0_f32.powi(2) + 5.0_f32.powi(2) + 6.0_f32.powi(2)).sqrt();
        let b_norm = vec![4.0 / b_mag, 5.0 / b_mag, 6.0 / b_mag];

        let l2_sqr = (1.0 - 4.0_f32).powi(2) + (2.0 - 5.0_f32).powi(2) + (3.0 - 6.0_f32).powi(2);
        let inner_product_sim = 1.0_f32
            - a_norm
                .iter()
                .zip(b_norm.iter())
                .map(|(a, b)| a * b)
                .sum::<f32>();

        let distance_function: DistanceFunction = "l2".try_into().unwrap();
        assert_eq!(distance_function.distance(&a, &b), l2_sqr);
        let distance_function: DistanceFunction = "ip".try_into().unwrap();
        assert_eq!(
            distance_function.distance(&a_norm, &b_norm),
            inner_product_sim
        );
    }
}

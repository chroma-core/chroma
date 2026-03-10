/// Distance functions for vector similarity search.
///
/// Scalar implementations based on chroma-distance, suitable for WASM
/// where SIMD intrinsics are not available.

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DistanceFunction {
    /// Squared L2 (Euclidean) distance.
    Euclidean,
    /// Cosine distance: 1 - cosine_similarity. Assumes normalized vectors.
    Cosine,
    /// Inner product distance: 1 - dot_product.
    InnerProduct,
}

impl DistanceFunction {
    /// Compute distance between two vectors.
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceFunction::Euclidean => {
                let mut sum = 0.0_f32;
                for i in 0..a.len() {
                    let diff = a[i] - b[i];
                    sum += diff * diff;
                }
                sum
            }
            DistanceFunction::Cosine => {
                // Assumes vectors are pre-normalized
                let mut dot = 0.0_f32;
                for i in 0..a.len() {
                    dot += a[i] * b[i];
                }
                1.0 - dot
            }
            DistanceFunction::InnerProduct => {
                let mut dot = 0.0_f32;
                for i in 0..a.len() {
                    dot += a[i] * b[i];
                }
                1.0 - dot
            }
        }
    }
}

/// Normalize a vector to unit length.
pub fn normalize(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    vector.iter().map(|x| x / (norm + 1e-32)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let dist = DistanceFunction::Euclidean.distance(&a, &b);
        assert!((dist - 27.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_identical() {
        let a = normalize(&[1.0, 0.0, 0.0]);
        let dist = DistanceFunction::Cosine.distance(&a, &a);
        assert!(dist.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = normalize(&[1.0, 0.0]);
        let b = normalize(&[0.0, 1.0]);
        let dist = DistanceFunction::Cosine.distance(&a, &b);
        assert!((dist - 1.0).abs() < 1e-6);
    }
}

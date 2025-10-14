pub mod distance;
pub mod distance_avx;
pub mod distance_avx512;
pub mod distance_neon;
pub mod distance_sse;
pub mod types;

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512dq",
    target_feature = "avx512bw",
    target_feature = "avx512vl",
    target_feature = "fma"
))]
pub use distance_avx512::*;

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub use distance_avx::*;

#[cfg(target_feature = "neon")]
pub use distance_neon::*;

#[cfg(target_feature = "sse")]
pub use distance_sse::*;
pub use types::*;

pub fn normalize(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    vector.iter().map(|x| x / (norm + 1e-32)).collect()
}

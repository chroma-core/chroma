pub mod distance_avx;
pub mod distance_neon;
pub mod distance_sse;
pub mod types;

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub use distance_avx::*;

#[cfg(target_feature = "neon")]
pub use distance_neon::*;

#[cfg(target_feature = "sse")]
pub use distance_sse::*;
pub use types::*;

#![deny(missing_docs)]
//! Persist generated trajectory JSON as structured Chroma records.
//!
//! A [`GenerateTrajectoryFile`] is a single generated trajectory together with
//! the execution metadata that makes the trajectory intelligible after the fact.
//! This crate preserves that JSON shape while splitting large values into
//! bounded Chroma documents whose keys and metadata remain queryable.
//!
//! # Examples
//!
//! ```rust
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let json = br#"{
//!   "trajectory": {
//!     "id": "00000000-0000-0000-0000-000000000001",
//!     "actions_and_observations": []
//!   }
//! }"#;
//!
//! let file = foundation_api::trajectories::parse_generate_trajectory_bytes(json)?;
//! assert_eq!(file.trajectory.actions_and_observations.len(), 0);
//! # Ok(())
//! # }
//! ```

mod chroma_store;
mod chunkset;
mod citations;
mod error;
mod ids;
mod limits;
mod metadata;
mod model;
mod record_format;
mod validate;

pub use chroma_store::{
    chroma_create_open_trajectory, chroma_extend_open_trajectory, chroma_extend_open_trajectory_at,
    chroma_finalize_open_trajectory, chroma_load_generate_trajectory,
    chroma_save_all_generate_trajectories, chroma_save_generate_trajectory,
};
pub use error::TrajectoryError;
pub use ids::{sha256_base36, tid_to_uuid, uuid_to_tid};
pub use limits::{
    CHUNKSET_BASE_MAX_BYTES, KEY_MAX_BYTES, ROOT_METADATA_MAX_BYTES, VALUE_MAX_BYTES,
};
pub use model::*;

#[cfg(test)]
mod tests;

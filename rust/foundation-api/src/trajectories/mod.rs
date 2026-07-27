#![deny(missing_docs)]
//! Persist user-facing reasoning projections as structured Chroma records.
//!
//! A [`ReasoningTrajectoryFile`] is the projection of generated trajectory JSON
//! that can be shown through reasoning and citation views. Producer metadata,
//! tools, parameters, observations, and reasoning signatures are discarded at
//! deserialization and never enter the storage format.
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
//! assert_eq!(file.trajectory.entries.len(), 0);
//! # Ok(())
//! # }
//! ```

mod api;
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

pub use api::{
    append_open_generate_trajectory, create_open_generate_trajectory,
    finalize_open_generate_trajectory, load_generate_trajectory, save_generate_trajectory,
    AppendTrajectoryEntriesRequest, TrajectoryWriteResponse,
};
pub use chroma_store::{
    chroma_create_open_trajectory, chroma_extend_open_trajectory, chroma_extend_open_trajectory_at,
    chroma_finalize_open_trajectory, chroma_load_generate_trajectory,
    chroma_save_all_generate_trajectories, chroma_save_generate_trajectory,
};
pub use error::TrajectoryError;
pub use ids::{sha256_base36, tid_to_uuid, uuid_to_tid};
pub use limits::{CHUNKSET_BASE_MAX_BYTES, KEY_MAX_BYTES, VALUE_MAX_BYTES};
pub use model::*;

#[cfg(test)]
mod tests;

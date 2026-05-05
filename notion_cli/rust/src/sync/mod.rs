//! Incremental-sync brain: previous-run state, dirty-container diffing, per-file
//! Merkle hashing, and the append-only `dump.changelog.jsonl` writer.
//!
//! On-disk layout that matters here (under `<output>/`):
//!
//! ```text
//! sidebar.jsonl                           current top-level containers
//! discovery.jsonl                         current per-page state baseline
//! dump.summary.jsonl                      append-only history of exports
//! exports/<slug>__<id>/                   one dir per container
//!     export.zip
//!     unzipped/                           extracted markdown + assets
//! _state/file-hashes/<container_id>.json  per-file SHA256 leaves +
//!                                         per-container Merkle root
//! _state/last_run.json                    latest sync_run_id, started/ended
//! dump.changelog.jsonl                    append-only per-file change log
//! .tombstones/<id>_<ts>/                  removed-then-archived containers
//! ```
//!
//! The changelog is the contract for downstream consumers (the Chroma
//! upserter): one JSON object per line, fields documented in `changelog.rs`.

pub mod changelog;
pub mod diff;
pub mod merkle;
pub mod runner;
pub mod state;
pub mod stitch;

#[allow(unused_imports)]
pub use changelog::{append_changelog_entries, ChangelogEntry};
#[allow(unused_imports)]
pub use diff::{compute_dirty_containers, DirtyPlan};
#[allow(unused_imports)]
pub use merkle::{
    container_merkle_root_hex, diff_hash_maps, hash_directory, load_hash_map, save_hash_map,
    ContainerHashMap, FileLeaf,
};
#[allow(unused_imports)]
pub use runner::{run_incremental_dump, IncrementalParams, IncrementalRunSummary};
#[allow(unused_imports)]
pub use state::{load_prev_state, ContainerHistory, PrevState};

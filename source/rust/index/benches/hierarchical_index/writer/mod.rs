#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, ReentrantMutex, RwLock};

use super::common::{NodeId, TreeNode};
use super::config::HierarchicalSpannConfig;

mod diagnostics;
pub mod persistence;
// The implementation module shares the public component name.
pub mod upgrade;
#[allow(clippy::module_inception)]
mod writer;

pub use super::instrumentation::*;
#[allow(unused_imports)]
pub use diagnostics::WriterMemoryUsage;
// pub use diagnostics::WriterStats;
pub use persistence::HierarchicalSpannIds;

/// Maximum tree depth tracked by per-level navigate counters. Deeper
/// expansions get bucketed into the last slot. 8 covers anything we'd
/// realistically build (current 113M run is depth 4).
pub const MAX_NAV_LEVELS: usize = 8;

// =============================================================================
// Writer (thread-safe)
// =============================================================================

/// 1-bit quantized hierarchical SPANN index (thread-safe).
///
/// Stores data vectors as 1-bit RaBitQ codes in leaf nodes (posting lists).
/// Navigation mode is configurable: fp (f32) or 4bit (QuantizedQuery). Search scores data vectors with quantized codes
/// and optionally reranks with f32 embeddings.
///
/// Public append, balance, and commit phases require exclusive access. Parallel
/// balance workers reserve leaf lifetimes and postings with short claims, and
/// the tree lock protects parent links. Queries open committed reader snapshots.
pub struct HierarchicalSpannWriter {
    // Tree structure fields
    pub(super) nodes: DashMap<NodeId, TreeNode>,
    pub(super) root_id: AtomicU32,
    /// Reused during stable add phases and replaced at the start of each balance round.
    pub(super) policy_widths: RwLock<Option<Vec<usize>>>,
    /// Whether opened navigation has been packed for the first add batch.
    navigation_ready: AtomicBool,
    pub(super) embeddings: DashMap<u32, Arc<[f32]>>,
    /// Dataset "center" (a pre-allocated zero vector) for non-relative centroid code computation.
    zero_centroid: Vec<f32>,

    // Config fields
    pub(super) dim: usize,
    pub(super) distance_fn: DistanceFunction,
    pub(super) config: HierarchicalSpannConfig,

    // Writer specific fields
    pub(super) next_node_id: AtomicU32,
    /// Serializes tree structure modifications (replace_child, remove_child_locked,
    /// create_root_above, split_internal) to prevent races when concurrent splits
    /// modify the same parent. Reentrant because these functions are mutually recursive.
    tree_lock: ReentrantMutex<()>,
    /// This contains the set of cluster ids in the balance (split/merge) routine.
    /// It is used to prevent concurrent balancing attempts on the same clusters.
    balancing: DashSet<NodeId>,
    /// Short reservations protect leaf postings, centroids, and lifetime.
    leaf_claims: Mutex<std::collections::HashSet<NodeId>>,
    pending_transfers: Mutex<Vec<writer::Reassignment>>,
    poisoned: AtomicBool,
    #[cfg(test)]
    fail_transfer_after_append: AtomicBool,
    max_inserted_id: Option<u32>,

    /// Node ids removed from `nodes` since the last commit. Used by `commit()` to
    /// emit `delete` calls against forked blockfiles so phantom nodes don't
    /// resurface on subsequent `open()`.
    pub(super) tombstones: DashSet<NodeId>,

    /// Node ids modified (inserted or in-place mutated) since the last commit.
    /// Commit only re-writes per-node metadata for ids in this set; clean
    /// "lazy shells" inherited from the forked parent are skipped, which keeps
    /// the per-checkpoint memory spike proportional to mutation rate rather
    /// than to total tree size. See `docs/README.md` -> "Commit-time memory".
    pub(super) dirty_nodes: DashSet<NodeId>,
    /// Vector ids whose `embeddings` entry was inserted since the last commit.
    pub(super) dirty_embeddings: DashSet<u32>,

    pub stats: WriterStats,

    // Blockfile readers for lazy loading from persisted state.
    pub(super) max_persisted_id: Option<u32>,
    pub(super) scalar_metadata_reader:
        Option<chroma_blockstore::BlockfileReader<'static, u32, u32>>,
    pub(super) posting_list_reader: Option<
        chroma_blockstore::BlockfileReader<
            'static,
            u32,
            chroma_types::hierarchical_spann::HierarchicalSpannPostingList<'static>,
        >,
    >,
    pub(super) vector_data_reader:
        Option<chroma_blockstore::BlockfileReader<'static, u32, &'static [f32]>>,
}

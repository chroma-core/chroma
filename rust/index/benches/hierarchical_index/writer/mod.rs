#![allow(dead_code)]

use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use dashmap::{DashMap, DashSet};
use parking_lot::ReentrantMutex;

use super::common::{NodeId, TreeNode};
use super::config::HierarchicalSpannConfig;

mod diagnostics;
pub mod persistence;
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

pub const DELETED_BIT: u8 = 0x80;

// =============================================================================
// Writer (thread-safe)
// =============================================================================

/// 1-bit quantized hierarchical SPANN index (thread-safe).
///
/// Stores data vectors as 1-bit RaBitQ codes in leaf nodes (posting lists).
/// Navigation mode is configurable: fp (f32), 1bit (code-to-code), or 4bit (QuantizedQuery). Search scores data vectors with quantized codes
/// and optionally reranks with f32 embeddings.
///
/// Thread safety:
/// - `nodes` in `DashMap`: per-shard locks serialize concurrent access to the same node
/// - split/merge atomically remove nodes first, so concurrent register_in_leaf fails and add() retries
/// - `balancing`: DashSet guard to prevent duplicate balance work on the same cluster
/// - `embeddings`/`versions` in `DashMap` for concurrent access
/// - `root_id`/`next_node_id` are atomic
/// - Stats use `AtomicU64`
pub struct HierarchicalSpannWriter {
    // Tree structure fields
    pub(super) nodes: DashMap<NodeId, TreeNode>,
    pub(super) root_id: AtomicU32,
    pub(super) embeddings: DashMap<u32, Arc<[f32]>>,
    pub(super) versions: DashMap<u32, u8>,
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
    /// This contains the set of cluster ids in the balance (scrub/split/merge) routine.
    /// It is used to prevent concurrent balancing attempts on the same clusters.
    balancing: DashSet<NodeId>,

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
    /// Vector ids whose `versions` entry was bumped since the last commit.
    pub(super) dirty_versions: DashSet<u32>,
    /// Vector ids whose `embeddings` entry was inserted since the last commit.
    pub(super) dirty_embeddings: DashSet<u32>,
    /// Vector ids whose embedding should be deleted from the vector_data
    /// blockfile at the next commit. Populated by `delete()`.
    pub(super) dirty_deleted_embeddings: DashSet<u32>,

    pub stats: WriterStats,

    // Blockfile readers for lazy loading from persisted state.
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

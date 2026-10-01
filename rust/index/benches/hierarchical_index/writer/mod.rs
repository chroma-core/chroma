#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use dashmap::{DashMap, DashSet};
use parking_lot::{Mutex, ReentrantMutex, RwLock};

use super::common::{NodeId, TreeNode};
use super::config::HierarchicalSpannConfig;

mod diagnostics;
pub mod persistence;
mod writer;
use writer::NavigationIndex;

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

/// Unchanged checkpoint versions are cheap to reread and must not grow the
/// mutable version overlay as balancing touches more of the index.
const VERSION_CACHE_ENTRIES: usize = 65_536;

#[derive(Default)]
struct VersionCache {
    entries: HashMap<u32, Option<u8>>,
    insertion_order: VecDeque<u32>,
}

impl VersionCache {
    fn get(&self, id: u32) -> Option<Option<u8>> {
        self.entries.get(&id).copied()
    }

    fn insert(&mut self, id: u32, version: Option<u8>) {
        if self.entries.contains_key(&id) {
            return;
        }
        if self.entries.len() == VERSION_CACHE_ENTRIES {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(id, version);
        self.insertion_order.push_back(id);
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

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
/// - immutable navigation objects are shared by add workers through `navigation_index`
/// - `nodes` in `DashMap`: per-shard locks serialize posting updates and balance mutations
/// - split/merge atomically remove nodes first, so concurrent register_in_leaf fails and add() retries
/// - `balancing`: DashSet guard to prevent duplicate balance work on the same cluster
/// - `embeddings`/`versions` in `DashMap` for concurrent access
/// - `root_id`/`next_node_id` are atomic
/// - Stats use `AtomicU64`
pub struct HierarchicalSpannWriter {
    // Tree structure fields
    pub(super) nodes: DashMap<NodeId, TreeNode>,
    pub(super) root_id: AtomicU32,
    /// Reused during stable add phases and replaced at the start of each balance round.
    pub(super) policy_widths: RwLock<Option<Vec<usize>>>,
    /// Adjacent child references used while add workers leave the tree unchanged.
    navigation_index: RwLock<Option<NavigationIndex>>,
    pub(super) embeddings: DashMap<u32, Arc<[f32]>>,
    /// New or changed versions in this writer session. Unchanged checkpoint
    /// versions stay in scalar metadata and in the bounded read cache.
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
    pub(super) max_persisted_id: Option<u32>,
    version_cache: Mutex<VersionCache>,
    version_reader_lock: tokio::sync::Mutex<()>,
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

#[cfg(test)]
mod version_cache_tests {
    #[allow(unused_imports)]
    // Cargo checks bench modules with cfg(test) but without the test harness.
    use super::{VersionCache, VERSION_CACHE_ENTRIES};

    #[test]
    fn checkpoint_version_cache_evicts_old_entries() {
        let mut cache = VersionCache::default();
        cache.insert(0, Some(0));
        for id in 1..=VERSION_CACHE_ENTRIES as u32 {
            cache.insert(id, Some(1));
        }
        assert_eq!(cache.len(), VERSION_CACHE_ENTRIES);
        assert_eq!(cache.get(0), None);
        assert_eq!(cache.get(VERSION_CACHE_ENTRIES as u32), Some(Some(1)));
        cache.insert(42, None);
        assert_eq!(cache.get(42), Some(Some(1)));
    }
}

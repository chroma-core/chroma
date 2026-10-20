#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use chroma_distance::DistanceFunction;
use dashmap::{DashMap, DashSet};
use parking_lot::Mutex;

use super::common::{NodeId, TreeNode, VectorKey};
use super::config::HierarchicalSpannConfig;

mod diagnostics;
pub mod distributed;
pub mod dynamic_global;
mod eligibility;
pub mod owner_scan;
pub mod live_owner;
pub mod authoritative_owner;
#[cfg(test)]
mod authoritative_owner_tests;
#[cfg(test)]
pub(crate) mod live_owner_tests;
pub mod lazy_route;
pub mod persistence;
mod writer;
pub mod reference_owner_adapter;
pub mod owner_vector_store;
pub mod owner_work_queue;
pub mod owner_node_access;
pub mod owner_placement_catalog;
pub mod partition_authority;
pub mod posting64;

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
struct VersionCache<I:VectorKey=u32> {
    entries: HashMap<I, Option<u8>>,
    insertion_order: VecDeque<I>,
}

impl<I:VectorKey> VersionCache<I> {
    fn get(&self, id: I) -> Option<Option<u8>> {
        self.entries.get(&id).copied()
    }

    fn insert(&mut self, id: I, version: Option<u8>) {
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
/// - `nodes` in `DashMap`: per-shard locks serialize concurrent access to the same node
/// - split/merge atomically remove nodes first, so concurrent register_in_leaf fails and add() retries
/// - `balancing`: DashSet guard to prevent duplicate balance work on the same cluster
/// - `embeddings`/`versions` in `DashMap` for concurrent access
/// - `root_id`/`next_node_id` are atomic
/// - Stats use `AtomicU64`
pub struct HierarchicalSpannWriter<I:VectorKey=u32> {
    // Tree structure fields
    pub(super) nodes: DashMap<NodeId, TreeNode<I>>,
    pub(super) prepared_split: writer::PreparedSplitState,
    pub(super) owner_backend: Option<Arc<reference_owner_adapter::Backend<I>>>,
    pub(super) node_store:Option<Arc<owner_node_access::Store>>,
    pub(super) owner_placement:Option<Arc<owner_placement_catalog::Catalog>>,
    pub(super) owner_work:Option<Arc<owner_work_queue::Queue>>,
    pub(super) topology_visibility:Option<Arc<super::topology_visibility::Visibility>>,
    pub(super) live_topology:Option<Arc<super::live_topology::LiveTopology>>,
    pub(super) topology_dirty:DashSet<NodeId>,
    pub(super) vector_base_root: Option<uuid::Uuid>,
    pub(super) root_id: AtomicU32,
    pub(super) embeddings: DashMap<I, Arc<[f32]>>,
    /// New or changed versions in this writer session. Unchanged checkpoint
    /// versions stay in scalar metadata and in the bounded read cache.
    pub(super) versions: DashMap<I, u8>,
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
    tree_lock: structural_timing::StructuralMutex,
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
    pub(super) dirty_versions: DashSet<I>,
    /// Vector ids whose `embeddings` entry was inserted since the last commit.
    pub(super) dirty_embeddings: DashSet<I>,
    /// Vector ids whose embedding should be deleted from the vector_data
    /// blockfile at the next commit. Populated by `delete()`.
    pub(super) dirty_deleted_embeddings: DashSet<I>,

    pub stats: WriterStats,
    pub(super) live_owner_hook: Mutex<Option<Arc<live_owner::LiveOwnerHook>>>,

    // Blockfile readers for lazy loading from persisted state.
    pub(super) max_persisted_id: Option<u32>,
    version_cache: Mutex<VersionCache<I>>,
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
        let mut cache:VersionCache = VersionCache::default();
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

pub mod owner_vector_metadata;

pub mod structural_timing;

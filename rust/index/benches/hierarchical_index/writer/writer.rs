use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_index::spann::utils::{self, EmbeddingPoint};
use dashmap::{DashMap, DashSet};
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::{Mutex, ReentrantMutex, RwLock};
use simsimd::SpatialSimilarity;

use super::super::common::{
    code_slice, effective_beam, ChildNavigation, InternalNode, LeafNode, NavigationNode, NodeId,
    ReadBeamPolicy, TreeNode,
};
use super::super::config::{HierarchicalSpannConfig, NavigationMode};
use super::super::instrumentation::WriterStats;
use super::{HierarchicalSpannWriter, DELETED_BIT, MAX_NAV_LEVELS};

const MAX_BALANCE_DEPTH: u32 = 4;

/// Follow-up balancing waits until the current posting scans finish. Each worker
/// owns its queue, so publishing a destination does not interrupt its source.
/// Follow each newly generated cascade before older pending destinations, as
/// recursion does. Keep its depth to preserve the neighbor reassignment limit.
#[derive(Default)]
struct DeferredBalance {
    stack: Vec<(NodeId, u32)>,
    pending: HashSet<(NodeId, u32)>,
}

impl DeferredBalance {
    fn push(&mut self, leaf: NodeId, depth: u32) {
        if depth <= MAX_BALANCE_DEPTH && self.pending.insert((leaf, depth)) {
            self.stack.push((leaf, depth));
        }
    }

    fn pop(&mut self) -> Option<(NodeId, u32)> {
        let task = self.stack.pop()?;
        self.pending.remove(&task);
        Some(task)
    }
}

/// Reserve a source and destination together. Independent pairs transfer in
/// parallel; an overlapping merge leaves its source unchanged so a later
/// balancing pass can retry.
struct MergeReservation<'a> {
    active: &'a Mutex<HashSet<NodeId>>,
    source: NodeId,
    destination: NodeId,
}

impl<'a> MergeReservation<'a> {
    fn acquire(
        active: &'a Mutex<HashSet<NodeId>>,
        source: NodeId,
        destination: NodeId,
    ) -> Option<Self> {
        let mut reserved = active.lock();
        if reserved.contains(&source) || reserved.contains(&destination) {
            return None;
        }
        reserved.insert(source);
        reserved.insert(destination);
        Some(Self {
            active,
            source,
            destination,
        })
    }
}

impl Drop for MergeReservation<'_> {
    fn drop(&mut self) {
        let mut reserved = self.active.lock();
        reserved.remove(&self.source);
        reserved.remove(&self.destination);
    }
}

struct WidthSnapshotGuard<'a>(&'a RwLock<Option<Vec<usize>>>);

impl Drop for WidthSnapshotGuard<'_> {
    fn drop(&mut self) {
        *self.0.write() = None;
    }
}

// =============================================================================
// Compact u8 versions (SPFresh-style)
// =============================================================================
pub const VERSION_MASK: u8 = 0x7F;

fn bump_version(v: &mut u8) -> u8 {
    *v = (*v).wrapping_add(1) & VERSION_MASK;
    *v
}

fn push_code(codes: &mut Vec<u8>, code: &[u8]) {
    codes.extend_from_slice(code);
}

fn swap_remove_code(codes: &mut Vec<u8>, index: usize, code_size: usize) {
    let last_index = codes.len() / code_size - 1;
    if index != last_index {
        let dst = index * code_size;
        let src = last_index * code_size;
        codes.copy_within(src..src + code_size, dst);
    }
    codes.truncate(codes.len() - code_size);
}

/// Posting rows collected by one add worker while the tree is stable.
/// Embeddings remain in the writer's shared vector map, so these buffers hold
/// only the data needed by posting lists.
#[derive(Default)]
struct LeafDeltas {
    by_leaf: HashMap<NodeId, LeafDelta>,
}

pub struct BufferedAddTiming {
    pub initialization: Duration,
    pub collection: Duration,
    pub flush: Duration,
}

#[derive(Default)]
struct LeafDelta {
    ids: Vec<u32>,
    versions: Vec<u8>,
    codes: Vec<u8>,
}

impl HierarchicalSpannWriter {
    pub fn new(dim: usize, distance_fn: DistanceFunction, config: HierarchicalSpannConfig) -> Self {
        let zero_centroid = vec![0.0f32; dim];
        let initial_centroid = vec![0.0f32; dim];
        let initial_centroid_code = Code::<1>::quantize(&initial_centroid, &zero_centroid)
            .as_ref()
            .to_vec();
        let nodes = DashMap::new();
        nodes.insert(
            0,
            TreeNode::Leaf(LeafNode {
                navigation: Arc::new(NavigationNode::new(
                    initial_centroid,
                    initial_centroid_code,
                    Vec::new(),
                    None,
                )),
                ids: Vec::new(),
                versions: Vec::new(),
                codes: Vec::new(),
                length: 0,
            }),
        );
        let dirty_nodes = DashSet::new();
        dirty_nodes.insert(0);

        Self {
            dim,
            distance_fn,
            config,
            nodes,
            balancing: DashSet::new(),
            merge_reservations: Default::default(),
            tombstones: DashSet::new(),
            dirty_nodes,
            dirty_versions: DashSet::new(),
            dirty_embeddings: DashSet::new(),
            dirty_deleted_embeddings: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(0),
            navigation_ready: AtomicBool::new(true),
            policy_widths: RwLock::new(None),
            next_node_id: AtomicU32::new(1),
            embeddings: DashMap::new(),
            versions: DashMap::new(),
            stats: WriterStats::default(),
            zero_centroid,
            max_persisted_id: None,
            version_cache: Default::default(),
            version_reader_lock: Default::default(),
            scalar_metadata_reader: None,
            posting_list_reader: None,
            vector_data_reader: None,
        }
    }

    // =========================================================================
    // Dirty tracking
    // =========================================================================

    #[inline]
    pub(super) fn mark_node_dirty(&self, id: NodeId) {
        self.dirty_nodes.insert(id);
    }

    #[inline]
    pub(super) fn mark_version_dirty(&self, id: u32) {
        self.dirty_versions.insert(id);
    }

    #[inline]
    pub(super) fn mark_embedding_dirty(&self, id: u32) {
        self.dirty_embeddings.insert(id);
    }

    #[inline]
    pub(super) fn mark_embedding_deleted(&self, id: u32) {
        // If we deleted before the embedding was ever flushed, the embedding
        // never made it to disk; cancel the pending upsert so commit doesn't
        // try to write and then delete the same key.
        self.dirty_embeddings.remove(&id);
        self.dirty_deleted_embeddings.insert(id);
    }

    fn write_beam_policy(&self) -> ReadBeamPolicy {
        if self.config.write_level_taus.is_empty() && self.config.write_level_min_pcts.is_empty() {
            ReadBeamPolicy::uniform(
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
            )
        } else {
            // Tau overrides do not use level widths. Percentage floors do.
            let level_widths = if self
                .config
                .write_level_min_pcts
                .iter()
                .any(|&pct| pct > 0.0)
            {
                self.cached_level_widths()
            } else {
                Vec::new()
            };
            ReadBeamPolicy::with_level_overrides(
                Some(self.config.write_beam_tau),
                self.config.write_beam_min,
                self.config.write_beam_max,
                self.config.write_level_taus.clone(),
                self.config.write_level_min_pcts.clone(),
                level_widths,
            )
        }
    }

    fn cached_level_widths(&self) -> Vec<usize> {
        if let Some(widths) = &*self.policy_widths.read() {
            return widths.clone();
        }

        // Only one add computes the width of the stable tree. Balancing
        // publishes a new width before each round starts.
        let mut cache = self.policy_widths.write();
        if let Some(widths) = &*cache {
            return widths.clone();
        }
        let widths: Vec<usize> = self.level_node_counts().into_iter().skip(1).collect();
        *cache = Some(widths.clone());
        widths
    }

    fn alloc_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    pub(super) fn root_id(&self) -> NodeId {
        self.root_id.load(Ordering::Relaxed)
    }

    fn pack_child_navigation(&self, children: Vec<NodeId>) -> ChildNavigation {
        let mut packed = Vec::with_capacity(children.len() * self.dim);
        for &child_id in &children {
            if let Some(child) = self.nodes.get(&child_id) {
                if child.centroid().len() != self.dim {
                    return ChildNavigation::unpacked(children);
                }
                packed.extend_from_slice(child.centroid());
            } else {
                packed.extend(std::iter::repeat_n(f32::NAN, self.dim));
            }
        }
        ChildNavigation::packed(children, packed, self.dim)
    }

    /// Publish child IDs and their contiguous centroids as one validated payload.
    fn publish_child_navigation(&self, parent_id: NodeId) {
        let _guard = self.tree_lock.lock();
        let navigation = match self.nodes.get(&parent_id) {
            Some(node) => match node.value() {
                TreeNode::Internal(parent) => Arc::clone(&parent.navigation),
                TreeNode::Leaf(_) => return,
            },
            None => return,
        };
        let children = self.pack_child_navigation(navigation.children().to_vec());
        if let Some(mut node) = self.nodes.get_mut(&parent_id) {
            if let TreeNode::Internal(parent) = node.value_mut() {
                if Arc::ptr_eq(&parent.navigation, &navigation) {
                    parent.set_child_navigation(children);
                }
            }
        }
    }

    /// Every child-list edit publishes matching IDs and centroids in one map
    /// update. Callers never invalidate or repack the arrays themselves.
    fn update_children(
        &self,
        parent_id: NodeId,
        edit: impl FnOnce(&mut Vec<NodeId>),
    ) -> Option<(Vec<NodeId>, Option<NodeId>)> {
        let _guard = self.tree_lock.lock();
        let (mut children, parent_id_of_parent) = match self.nodes.get(&parent_id)?.value() {
            TreeNode::Internal(parent) => (parent.children().to_vec(), parent.parent_id),
            TreeNode::Leaf(_) => return None,
        };
        edit(&mut children);
        let payload = self.pack_child_navigation(children.clone());
        let mut node = self.nodes.get_mut(&parent_id)?;
        let TreeNode::Internal(parent) = node.value_mut() else {
            return None;
        };
        parent.set_child_navigation(payload);
        drop(node);
        self.mark_node_dirty(parent_id);
        Some((children, parent_id_of_parent))
    }

    /// Updating a parent's own centroid also refreshes its entry in the next
    /// parent's scoring payload, keeping that publication rule in one place.
    fn update_internal_centroid(&self, parent_id: NodeId, children: &[NodeId]) {
        let _guard = self.tree_lock.lock();
        let centroid = self.compute_centroid_of(children);
        let code = self.quantize_origin(&centroid);
        let grandparent = {
            let Some(mut node) = self.nodes.get_mut(&parent_id) else {
                return;
            };
            let TreeNode::Internal(parent) = node.value_mut() else {
                return;
            };
            parent.centroid = centroid;
            parent.centroid_code = code;
            parent.parent_id
        };
        self.mark_node_dirty(parent_id);
        if let Some(grandparent) = grandparent {
            self.publish_child_navigation(grandparent);
        }
    }

    /// A changed internal node needs a new child payload. Its parent also needs
    /// the node's new centroid. This method takes the tree lock itself, including
    /// when a leaf returns after a cancelled split or merge.
    fn publish_changed_navigation(&self, node_id: NodeId) {
        let _guard = self.tree_lock.lock();
        let (internal, parent_id) = match self.nodes.get(&node_id) {
            Some(node) => match node.value() {
                TreeNode::Internal(parent) => (true, parent.parent_id),
                TreeNode::Leaf(leaf) => (false, leaf.parent_id),
            },
            None => return,
        };
        if internal {
            self.publish_child_navigation(node_id);
        }
        if let Some(parent_id) = parent_id {
            self.publish_child_navigation(parent_id);
        }
    }

    /// Initialize reopened parents and refresh the stable tree after workers join.
    fn refresh_navigation(&self) {
        let parents: Vec<NodeId> = self
            .nodes
            .iter()
            .filter_map(|node| matches!(node.value(), TreeNode::Internal(_)).then_some(*node.key()))
            .collect();
        for parent_id in parents {
            self.publish_child_navigation(parent_id);
        }
        self.navigation_ready.store(true, Ordering::Release);
    }

    /// Initialize packed navigation once after loading an index. Subsequent add
    /// batches use the same live parent payload until balancing changes the tree.
    pub fn begin_add_batch(&self) {
        if !self.navigation_ready.load(Ordering::Acquire) {
            self.refresh_navigation();
        }
    }

    pub fn end_add_batch(&self) {}

    pub fn navigation_index_bytes(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| match node.value() {
                TreeNode::Internal(internal) => internal.child_navigation().centroid_bytes(),
                TreeNode::Leaf(_) => 0,
            })
            .sum()
    }

    /// Insert a raw embedding into the in-memory map (for populating after resume).
    pub fn insert_embedding(&self, id: u32, embedding: Arc<[f32]>) {
        self.embeddings.insert(id, embedding);
        self.mark_embedding_dirty(id);
    }

    // =========================================================================
    // Add
    // =========================================================================

    /// Add a data vector to the index.
    ///
    /// Thread-safe: multiple threads can call add() concurrently.
    /// No global lock -- DashMap per-shard atomicity + retry handles races
    /// with concurrent split/merge operations.
    pub fn add(&self, id: u32, embedding: &[f32]) {
        let add_start = Instant::now();
        let previous_version = self.current_version_sync(id);

        // Refuse to resurrect a deleted id. Re-adding after delete is not a
        // supported operation; silently drop. (Lifting this restriction would
        // require a separate "undelete" path that scrubs every leaf for the
        // stale tombstoned entries before bumping the version below the
        // DELETED_BIT.)
        if previous_version.is_some_and(|version| version & DELETED_BIT != 0) {
            return;
        }
        let mut version = {
            let mut v = self
                .versions
                .entry(id)
                .or_insert(previous_version.unwrap_or(0));
            if *v & DELETED_BIT != 0 {
                return;
            }
            bump_version(&mut v)
        };
        self.mark_version_dirty(id);

        let emb: Arc<[f32]> = Arc::from(embedding);
        self.embeddings.insert(id, emb);
        self.mark_embedding_dirty(id);
        self.stats.embeddings_added.fetch_add(1, Ordering::Relaxed);

        loop {
            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates =
                self.navigate_with_policy(embedding, self.config.write_navigation, &policy);
            let cluster_ids = self.rng_select(&candidates);
            self.stats
                .add_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let reg_start = Instant::now();
            let mut clusters_to_balance = Vec::new();
            for &cluster_id in &cluster_ids {
                if self.register_in_leaf(cluster_id, id, version, embedding) {
                    clusters_to_balance.push(cluster_id);
                }
            }
            self.stats
                .add_register_nanos
                .fetch_add(reg_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if clusters_to_balance.is_empty() {
                self.stats.add_missing_nodes.fetch_add(1, Ordering::Relaxed);
                version = {
                    let mut v = self.versions.entry(id).or_insert(0);
                    bump_version(&mut v)
                };
                self.mark_version_dirty(id);
                continue;
            }

            break;
        }

        self.stats.adds.fetch_add(1, Ordering::Relaxed);
        self.stats
            .add_nanos
            .fetch_add(add_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Collect one worker's adds without writing any leaf posting list.
    ///
    /// All workers must finish collecting and flush before balance, commit,
    /// or any other operation that changes the tree. This phase boundary keeps
    /// the chosen leaf ids and their centroids valid through the flush.
    fn collect_leaf_deltas<F: Fn() + Sync>(
        &self,
        vectors: &[(u32, Arc<[f32]>)],
        on_processed: &F,
    ) -> LeafDeltas {
        let mut deltas = LeafDeltas::default();
        for (id, embedding) in vectors {
            let add_start = Instant::now();
            let previous_version = self.current_version_sync(*id);
            if previous_version.is_some_and(|version| version & DELETED_BIT != 0) {
                on_processed();
                continue;
            }
            let version = {
                let mut v = self
                    .versions
                    .entry(*id)
                    .or_insert(previous_version.unwrap_or(0));
                if *v & DELETED_BIT != 0 {
                    on_processed();
                    continue;
                }
                let version = bump_version(&mut v);
                // Tie the shared embedding to its assigned version when an
                // id occurs in more than one worker chunk.
                self.embeddings.insert(*id, Arc::clone(embedding));
                version
            };
            self.mark_version_dirty(*id);
            self.mark_embedding_dirty(*id);
            self.stats.embeddings_added.fetch_add(1, Ordering::Relaxed);

            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates =
                self.navigate_with_policy(embedding, self.config.write_navigation, &policy);
            let cluster_ids = self.rng_select(&candidates);
            self.stats
                .add_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            assert!(
                !cluster_ids.is_empty(),
                "stable add tree has no destination leaf"
            );

            let collect_start = Instant::now();
            for leaf_id in cluster_ids {
                let node = self.nodes.get(&leaf_id).unwrap_or_else(|| {
                    panic!("leaf {leaf_id} disappeared during stable add phase")
                });
                let TreeNode::Leaf(leaf) = node.value() else {
                    panic!("node {leaf_id} changed type during stable add phase");
                };
                let q_start = Instant::now();
                let code = Code::<1>::quantize(embedding, &leaf.centroid);
                self.stats
                    .register_quantize_nanos
                    .fetch_add(q_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let delta = deltas.by_leaf.entry(leaf_id).or_default();
                delta.ids.push(*id);
                delta.versions.push(version);
                push_code(&mut delta.codes, code.as_ref());
            }
            self.stats
                .add_register_nanos
                .fetch_add(collect_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.stats
                .register_nanos
                .fetch_add(collect_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.stats.adds.fetch_add(1, Ordering::Relaxed);
            self.stats
                .add_nanos
                .fetch_add(add_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            on_processed();
        }
        deltas
    }

    /// Flush all worker buffers after their collection threads have joined.
    /// Each destination leaf takes one posting write lock, including leaves
    /// backed by a lazy persisted posting list.
    fn flush_leaf_deltas(&self, workers: Vec<LeafDeltas>) {
        let mut by_leaf: HashMap<NodeId, LeafDelta> = HashMap::new();
        for worker in workers {
            for (leaf_id, mut delta) in worker.by_leaf {
                let combined = by_leaf.entry(leaf_id).or_default();
                combined.ids.append(&mut delta.ids);
                combined.versions.append(&mut delta.versions);
                combined.codes.append(&mut delta.codes);
            }
        }
        for leaf_id in by_leaf.keys() {
            assert!(
                matches!(self.nodes.get(leaf_id).as_deref(), Some(TreeNode::Leaf(_))),
                "leaf {leaf_id} disappeared before buffered postings were flushed"
            );
        }
        for (leaf_id, mut delta) in by_leaf {
            let count = delta.ids.len();
            let lock_start = Instant::now();
            let mut node = self
                .nodes
                .get_mut(&leaf_id)
                .expect("prechecked leaf disappeared");
            self.stats
                .register_lock_wait_nanos
                .fetch_add(lock_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                panic!("prechecked leaf {leaf_id} changed type");
            };
            leaf.ids.append(&mut delta.ids);
            leaf.versions.append(&mut delta.versions);
            leaf.codes.append(&mut delta.codes);
            leaf.length += count;
            drop(node);
            self.mark_node_dirty(leaf_id);
            self.stats
                .registers
                .fetch_add(count as u64, Ordering::Relaxed);
            self.stats
                .register_nanos
                .fetch_add(lock_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Add one stable-tree batch, then make every posting visible before
    /// returning. Exclusive access prevents a caller from balancing or
    /// committing between routing and the flush.
    pub fn add_batch_buffered<F: Fn() + Sync>(
        &mut self,
        vectors: &[(u32, Arc<[f32]>)],
        num_threads: usize,
        on_processed: F,
    ) -> BufferedAddTiming {
        let start = Instant::now();
        self.begin_add_batch();
        let initialization = start.elapsed();

        let start = Instant::now();
        let worker_deltas = if num_threads <= 1 || vectors.is_empty() {
            vec![self.collect_leaf_deltas(vectors, &on_processed)]
        } else {
            let chunk_size = vectors.len().div_ceil(num_threads);
            let writer_ref: &HierarchicalSpannWriter = self;
            std::thread::scope(|scope| {
                let handles: Vec<_> = vectors
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let callback = &on_processed;
                        scope.spawn(move || writer_ref.collect_leaf_deltas(chunk, callback))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect()
            })
        };
        let collection = start.elapsed();

        let start = Instant::now();
        self.flush_leaf_deltas(worker_deltas);
        let flush = start.elapsed();
        self.end_add_batch();
        BufferedAddTiming {
            initialization,
            collection,
            flush,
        }
    }

    /// Mark a vector id as deleted.
    ///
    /// O(1) and idempotent. Sets `DELETED_BIT` on the global version map
    /// entry, which causes:
    ///   1. `is_valid()` / reassign to treat the id as gone immediately
    ///   2. `scrub()` and `split_leaf()` to drop the id's posting entries
    ///      (the in-memory and on-disk version no longer match the global)
    ///   3. `commit()` to delete the id's embedding from the vector_data
    ///      blockfile and persist the tombstoned version
    ///
    /// Posting list cleanup is lazy: tombstoned ids remain in untouched
    /// leaves on disk until those leaves are next mutated/scrubbed.
    pub fn delete(&self, id: u32) {
        let previous_version = self.current_version_sync(id);
        if previous_version.is_some_and(|version| version & DELETED_BIT != 0) {
            return;
        }
        let already = {
            let mut v = self
                .versions
                .entry(id)
                .or_insert(previous_version.unwrap_or(0));
            if *v & DELETED_BIT != 0 {
                true
            } else {
                *v |= DELETED_BIT;
                false
            }
        };
        if already {
            return;
        }
        self.mark_version_dirty(id);
        self.mark_embedding_deleted(id);
        // Drop the in-memory embedding eagerly so it can't be used by any
        // subsequent reassign/split for an id that's already tombstoned.
        self.embeddings.remove(&id);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Register a vector in a leaf. Uses per-leaf DashMap get_mut -- no global lock.
    /// Also computes and stores the 1-bit RaBitQ code of the vector residual.
    fn register_in_leaf(&self, leaf_id: NodeId, id: u32, version: u8, embedding: &[f32]) -> bool {
        let t0 = Instant::now();
        // A lazy leaf keeps new rows in these vectors until a scrub, split,
        // merge, or commit combines them with its persisted posting list.
        let lock_start = Instant::now();
        if let Some(mut node_ref) = self.nodes.get_mut(&leaf_id) {
            let lock_elapsed = lock_start.elapsed().as_nanos() as u64;
            self.stats
                .register_lock_wait_nanos
                .fetch_add(lock_elapsed, Ordering::Relaxed);
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                let q_start = Instant::now();
                let code = Code::<1>::quantize(embedding, &leaf.centroid);
                self.stats
                    .register_quantize_nanos
                    .fetch_add(q_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                leaf.ids.push(id);
                leaf.versions.push(version);
                push_code(&mut leaf.codes, code.as_ref());
                leaf.length += 1;
                drop(node_ref);
                self.dirty_nodes.insert(leaf_id);
                self.stats.registers.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .register_nanos
                    .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                return true;
            }
        }
        self.stats.registers.fetch_add(1, Ordering::Relaxed);
        self.stats
            .register_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        false
    }

    /// Publish a new version only after its first replacement posting is in
    /// a leaf. Holding the leaf and version locks in that order also keeps a
    /// concurrent scrub or split from discarding both the old and new posting.
    fn register_first_reassignment(
        &self,
        leaf_id: NodeId,
        id: u32,
        old_version: u8,
        embedding: &[f32],
    ) -> Option<u8> {
        let t0 = Instant::now();
        // A reopened vector can still have its version only in the checkpoint
        // cache. Resolve it before taking a leaf guard or the mutable overlay.
        let checkpoint_version = self.current_version_sync(id);
        self.load_posting_sync(leaf_id);
        let lock_start = Instant::now();
        let result = (|| {
            let mut node = self.nodes.get_mut(&leaf_id)?;
            self.stats
                .register_lock_wait_nanos
                .fetch_add(lock_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                return None;
            };
            let mut global_version = self.versions.entry(id).or_insert(checkpoint_version?);
            if *global_version != old_version || *global_version & DELETED_BIT != 0 {
                return None;
            }
            let version = bump_version(&mut global_version);
            let q_start = Instant::now();
            let code = Code::<1>::quantize(embedding, &leaf.centroid);
            self.stats
                .register_quantize_nanos
                .fetch_add(q_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            leaf.ids.push(id);
            leaf.versions.push(version);
            push_code(&mut leaf.codes, code.as_ref());
            leaf.length = leaf.ids.len();
            drop(global_version);
            drop(node);
            self.dirty_nodes.insert(leaf_id);
            self.mark_version_dirty(id);
            Some(version)
        })();
        self.stats.registers.fetch_add(1, Ordering::Relaxed);
        self.stats
            .register_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    // =========================================================================
    // Navigate (f32 -- used by write path and --fp-navigation)
    // =========================================================================

    /// Beam search the tree using f32 centroid distances.
    /// Used by the write path (always) and search path when navigation=Fp.
    pub(super) fn navigate_f32(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
    ) -> Vec<(NodeId, f32)> {
        self.navigate_f32_excluding(query, policy, None)
    }

    /// A merge keeps its source readable until every row has a replacement.
    /// Excluding that source prevents it from consuming the destination beam.
    fn navigate_f32_excluding(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
        excluded_leaf: Option<NodeId>,
    ) -> Vec<(NodeId, f32)> {
        let nav_t0 = Instant::now();
        let root = self.root_id();
        let Some(root_node) = self.nodes.get(&root) else {
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Vec::new();
        };
        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            let dist = self.dist(query, root_node.centroid());
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return if excluded_leaf == Some(root) {
                Vec::new()
            } else {
                vec![(root, dist)]
            };
        }
        drop(root_node);

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];
        let mut dist_nanos = 0u64;
        let mut sort_nanos = 0u64;
        let mut levels = 0u64;
        let mut dist_count = 0u64;
        // Per-level (level-1)-indexed sums; flushed to atomics at end.
        let mut nav_in_per_level = [0u64; MAX_NAV_LEVELS];
        let mut nav_dist_per_level = [0u64; MAX_NAV_LEVELS];
        let mut nav_out_per_level = [0u64; MAX_NAV_LEVELS];

        loop {
            let beam_in_len = beam.len() as u64;
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            let dist_start = Instant::now();
            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let navigation = Arc::clone(&internal.navigation);
                        drop(node_ref);
                        if let Some(rows) = navigation.child_navigation().scored_children() {
                            for (child_id, centroid) in rows {
                                if let Some(centroid) = centroid {
                                    child_scores.push((child_id, self.dist(query, centroid)));
                                } else {
                                    // A child missing at publication can return before
                                    // its parent's next refresh. Resolve only that row.
                                    #[cfg(test)]
                                    self.stats
                                        .navigation_child_lookups
                                        .fetch_add(1, Ordering::Relaxed);
                                    if let Some(child) = self.nodes.get(&child_id) {
                                        child_scores
                                            .push((child_id, self.dist(query, child.centroid())));
                                    } else {
                                        self.stats
                                            .navigate_missing_nodes
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        } else {
                            for &child_id in navigation.children() {
                                #[cfg(test)]
                                self.stats
                                    .navigation_child_lookups
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Some(child) = self.nodes.get(&child_id) {
                                    child_scores
                                        .push((child_id, self.dist(query, child.centroid())));
                                } else {
                                    self.stats
                                        .navigate_missing_nodes
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        };
                    }
                }
            }
            dist_nanos += dist_start.elapsed().as_nanos() as u64;

            if child_scores.is_empty() {
                break;
            }

            levels += 1;
            dist_count += child_scores.len() as u64;
            let li = (levels as usize - 1).min(MAX_NAV_LEVELS - 1);
            nav_in_per_level[li] = nav_in_per_level[li].saturating_add(beam_in_len);
            nav_dist_per_level[li] =
                nav_dist_per_level[li].saturating_add(child_scores.len() as u64);
            let params = policy.level_params(levels as usize);

            if let Some(excluded) = excluded_leaf {
                child_scores.retain(|&(id, _)| id != excluded);
            }
            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            sort_nanos += sort_start.elapsed().as_nanos() as u64;
            // Resolve only the candidates needed to fill the beam. Removed
            // children affect neither its best distance nor its size limits.
            let limit = if params.tau.is_some() {
                params.beam_max
            } else {
                params.beam_min
            };
            let floor = params.beam_min.min(limit);
            let mut best_live_distance: Option<f32> = None;
            let mut selected = 0usize;
            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, distance) in &child_scores {
                if selected == limit {
                    break;
                }
                if let (Some(best), Some(tau)) = (best_live_distance, params.tau) {
                    if selected >= floor
                        && !matches!(
                            distance.partial_cmp(&(best.max(1e-10_f32) * tau as f32)),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        )
                    {
                        break;
                    }
                }
                let Some(node) = self.nodes.get(&node_id) else {
                    self.stats
                        .navigate_missing_nodes
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let best = *best_live_distance.get_or_insert(distance);
                if let Some(tau) = params.tau {
                    if selected >= floor
                        && !matches!(
                            distance.partial_cmp(&(best.max(1e-10_f32) * tau as f32)),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        )
                    {
                        break;
                    }
                }
                match node.value() {
                    TreeNode::Leaf(_) => leaves.push((node_id, distance)),
                    TreeNode::Internal(_) => next_internals.push(node_id),
                }
                selected += 1;
            }
            nav_out_per_level[li] = nav_out_per_level[li].saturating_add(selected as u64);

            if next_internals.is_empty() {
                break;
            }
            beam = next_internals;
        }

        let sort_start = Instant::now();
        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        sort_nanos += sort_start.elapsed().as_nanos() as u64;

        self.stats
            .navigate_dist_nanos
            .fetch_add(dist_nanos, Ordering::Relaxed);
        self.stats
            .navigate_sort_nanos
            .fetch_add(sort_nanos, Ordering::Relaxed);
        self.stats
            .navigate_levels
            .fetch_add(levels, Ordering::Relaxed);
        self.stats
            .navigate_dist_count
            .fetch_add(dist_count, Ordering::Relaxed);
        for li in 0..(levels as usize).min(MAX_NAV_LEVELS) {
            self.stats.nav_in_per_level[li].fetch_add(nav_in_per_level[li], Ordering::Relaxed);
            self.stats.nav_dist_per_level[li].fetch_add(nav_dist_per_level[li], Ordering::Relaxed);
            self.stats.nav_out_per_level[li].fetch_add(nav_out_per_level[li], Ordering::Relaxed);
            self.stats.nav_calls_per_level[li].fetch_add(1, Ordering::Relaxed);
        }
        self.stats.navigates.fetch_add(1, Ordering::Relaxed);
        self.stats
            .navigate_nanos
            .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        leaves
    }

    pub(super) fn effective_beam(
        sorted: &[(NodeId, f32)],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
    ) -> usize {
        effective_beam(sorted, tau, beam_min, beam_max)
    }

    /// Dispatch to the configured navigate implementation.
    pub(super) fn navigate_with_policy(
        &self,
        query: &[f32],
        mode: NavigationMode,
        policy: &ReadBeamPolicy,
    ) -> Vec<(NodeId, f32)> {
        if mode != NavigationMode::Fp {
            static WARNED: Once = Once::new();
            WARNED.call_once(|| {
                eprintln!("Warning: only full precision f32 writer navigation is enabled.");
            });
        }
        // match mode {
        //     NavigationMode::Fp => self.navigate_f32(query, policy),
        //     NavigationMode::FourBit => self.navigate_4bit(query, policy),
        // }
        self.navigate_f32(query, policy)
    }

    pub(super) fn navigate(
        &self,
        query: &[f32],
        tau: Option<f64>,
        beam_min: usize,
        beam_max: usize,
        mode: NavigationMode,
    ) -> Vec<(NodeId, f32)> {
        let policy = ReadBeamPolicy::uniform(tau, beam_min, beam_max);
        self.navigate_with_policy(query, mode, &policy)
    }

    // =========================================================================
    // RNG select
    // =========================================================================

    /// Select clusters via RNG rule. Uses per-node DashMap gets -- no global lock.
    fn rng_select(&self, candidates: &[(NodeId, f32)]) -> Vec<NodeId> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let first_distance = candidates[0].1;
        let mut result = Vec::new();
        let mut selected_centroids: Vec<Vec<f32>> = Vec::new();

        if self.config.max_replicas == 1 {
            return vec![candidates[0].0];
        }

        for &(node_id, distance) in candidates {
            if (distance - first_distance).abs()
                > self.config.write_rng_epsilon * first_distance.abs().max(1e-10)
            {
                break;
            }

            let Some(node_ref) = self.nodes.get(&node_id) else {
                continue;
            };
            let centroid = node_ref.centroid().to_vec();
            drop(node_ref);

            // RNG filter
            // Don't replicate to clusters that are close to each other.
            // If the candidate cluster is farther away from the query than
            // from other already selected clusters, skip it.
            let blocked = selected_centroids
                .iter()
                .any(|sel| distance > self.dist(&centroid, sel) * self.config.write_rng_factor);
            if blocked {
                continue;
            }

            result.push(node_id);
            selected_centroids.push(centroid);

            if result.len() >= self.config.max_replicas {
                break;
            }
        }

        result
    }

    // =========================================================================
    // Balance / Scrub
    // =========================================================================

    /// Balance a cluster: scrub stale entries, then split or merge if needed.
    /// Scrub and size check use per-node DashMap access (no global lock).
    fn balance(&self, cluster_id: NodeId, depth: u32) {
        let mut deferred = DeferredBalance::default();
        self.balance_leaf(cluster_id, depth, &mut deferred);
        while let Some((leaf, depth)) = deferred.pop() {
            self.balance_leaf(leaf, depth, &mut deferred);
        }
    }

    fn balance_leaf(&self, cluster_id: NodeId, depth: u32, deferred: &mut DeferredBalance) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }

        self.scrub(cluster_id);

        let len = match self.nodes.get(&cluster_id) {
            Some(node_ref) => match node_ref.value() {
                TreeNode::Leaf(leaf) => leaf.length,
                _ => return,
            },
            None => return,
        };

        let needs_split = len > self.config.split_threshold;
        let needs_merge = len > 0 && len < self.config.merge_threshold;

        if needs_split || needs_merge {
            if !self.balancing.insert(cluster_id) {
                return;
            }

            if needs_split {
                self.split_leaf(cluster_id, depth, deferred);
            } else {
                self.merge_leaf(cluster_id, depth, deferred);
            }

            self.balancing.remove(&cluster_id);
        }
    }

    /// Collect all descendant leaf NodeIds under a given subtree root.
    fn collect_leaves_under(&self, node_id: NodeId) -> Vec<NodeId> {
        let mut leaves = Vec::new();
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            if let Some(node_ref) = self.nodes.get(&nid) {
                match node_ref.value() {
                    TreeNode::Leaf(_) => leaves.push(nid),
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children().iter().copied());
                    }
                }
            }
        }
        leaves
    }

    /// Count leaves needing balance under a subtree root.
    fn count_work_under(&self, node_id: NodeId) -> usize {
        let mut count = 0;
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            if let Some(node_ref) = self.nodes.get(&nid) {
                match node_ref.value() {
                    TreeNode::Leaf(leaf) => {
                        let len = leaf.length;
                        if len > self.config.split_threshold
                            || (len > 0 && len < self.config.merge_threshold)
                        {
                            count += 1;
                        }
                    }
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children().iter().copied());
                    }
                }
            }
        }
        count
    }

    /// Find subtree roots at the tree level that gives us >= num_threads partitions.
    /// Returns (subtree_root_id, estimated_work) pairs.
    fn find_partition_roots(&self, num_threads: usize) -> Vec<(NodeId, usize)> {
        let root = self.root_id();
        let mut frontier = vec![root];

        loop {
            if frontier.len() >= num_threads {
                break;
            }
            let mut next_frontier = Vec::new();
            let mut all_leaves = true;
            for &nid in &frontier {
                if let Some(node_ref) = self.nodes.get(&nid) {
                    match node_ref.value() {
                        TreeNode::Internal(internal) => {
                            all_leaves = false;
                            next_frontier.extend(internal.children().iter().copied());
                        }
                        TreeNode::Leaf(_) => {
                            next_frontier.push(nid);
                        }
                    }
                }
            }
            if all_leaves || next_frontier.len() <= frontier.len() {
                break;
            }
            frontier = next_frontier;
        }

        frontier
            .into_iter()
            .map(|nid| {
                let work = self.count_work_under(nid);
                (nid, work)
            })
            .collect()
    }

    /// Balance all leaves by partitioning the tree into subtrees and distributing
    /// them across workers, weighted by estimated work. One worker uses this loop too.
    pub fn balance_index_parallel(&self, num_threads: usize) {
        let num_threads = num_threads.max(1);
        // Each changed parent publishes its own payload during balancing.
        // Mark initialization incomplete so the joined tree is refreshed for add.
        self.navigation_ready.store(false, Ordering::Release);
        let _snapshot = WidthSnapshotGuard(&self.policy_widths);

        // Outer-loop cap so a bug or oscillation cannot spin forever.
        const MAX_PARALLEL_ROUNDS: u32 = 8;
        let mut round = 0u32;
        let mut balance_pb: Option<ProgressBar> = None;

        loop {
            let has_work = self.nodes.iter().any(|entry| match entry.value() {
                TreeNode::Leaf(leaf) => {
                    let len = leaf.length;
                    len > self.config.split_threshold
                        || (len > 0 && len < self.config.merge_threshold)
                }
                _ => false,
            });
            if !has_work {
                break;
            }

            if balance_pb.is_none() {
                let pb = ProgressBar::new(MAX_PARALLEL_ROUNDS as u64);
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[Balance] {wide_bar} {pos}/{len} [{elapsed_precise}]")
                        .unwrap(),
                );
                balance_pb = Some(pb);
            }

            round += 1;
            if round > MAX_PARALLEL_ROUNDS {
                if let Some(pb) = balance_pb.take() {
                    pb.finish_and_clear();
                }
                let (over, under) = self.nodes.iter().fold((0usize, 0usize), |acc, e| {
                    if let TreeNode::Leaf(leaf) = e.value() {
                        let len = leaf.length;
                        if len > self.config.split_threshold {
                            (acc.0 + 1, acc.1)
                        } else if len > 0 && len < self.config.merge_threshold {
                            (acc.0, acc.1 + 1)
                        } else {
                            acc
                        }
                    } else {
                        acc
                    }
                });
                eprintln!(
                    "[balance_index_parallel] note: stopped after {} rounds without full convergence ({} oversized, {} undersized leaves remain); continuing without crash",
                    MAX_PARALLEL_ROUNDS, over, under
                );
                break;
            }
            if let Some(ref pb) = balance_pb {
                pb.inc(1);
            }
            self.stats
                .balance_rounds
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Refresh beam widths before any worker mutates the tree. The next
            // round refreshes them after all workers from this round join.
            // A large rebalance can change level widths drastically within a
            // round. The stale width can then set a beam floor that is too
            // narrow, which may reduce recall for vectors placed in that round.
            if self
                .config
                .write_level_min_pcts
                .iter()
                .any(|&pct| pct > 0.0)
            {
                *self.policy_widths.write() =
                    Some(self.level_node_counts().into_iter().skip(1).collect());
            }

            let mut partitions = self.find_partition_roots(num_threads);
            partitions.sort_by_key(|a| std::cmp::Reverse(a.1));

            // Greedy assignment: assign each subtree to the thread with least work.
            let mut thread_work: Vec<usize> = vec![0; num_threads];
            let mut thread_subtrees: Vec<Vec<NodeId>> = vec![Vec::new(); num_threads];

            for (nid, work) in &partitions {
                let min_thread = thread_work
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, w)| **w)
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                thread_subtrees[min_thread].push(*nid);
                thread_work[min_thread] += work.max(&1);
            }

            std::thread::scope(|s| {
                for subtrees in &thread_subtrees {
                    if subtrees.is_empty() {
                        continue;
                    }
                    s.spawn(move || {
                        for &subtree_root in subtrees {
                            let leaves = self.collect_leaves_under(subtree_root);
                            for leaf_id in leaves {
                                self.balance(leaf_id, 0);
                            }
                        }
                    });
                }
            });
        }
        if let Some(pb) = balance_pb {
            pb.finish_and_clear();
        }
        self.refresh_navigation();
    }

    /// Scrub stale entries from a leaf. Uses per-leaf DashMap get_mut (no global lock).
    fn scrub(&self, cluster_id: NodeId) {
        let t0 = Instant::now();
        self.load_posting_sync(cluster_id);
        let Some(mut node_ref) = self.nodes.get_mut(&cluster_id) else {
            return;
        };
        let TreeNode::Leaf(leaf) = node_ref.value_mut() else {
            return;
        };

        let code_size = self.code_size();
        let mut removed = 0usize;
        let mut i = 0;
        while i < leaf.ids.len() {
            let id = leaf.ids[i];
            let version = leaf.versions[i];
            let current_version = self.current_version_sync(id).unwrap_or(0);
            if version != current_version {
                leaf.ids.swap_remove(i);
                leaf.versions.swap_remove(i);
                swap_remove_code(&mut leaf.codes, i, code_size);
                removed += 1;
            } else {
                i += 1;
            }
        }
        leaf.length = leaf.ids.len();

        drop(node_ref);
        if removed > 0 {
            self.mark_node_dirty(cluster_id);
        }

        self.stats.scrubs.fetch_add(1, Ordering::Relaxed);
        self.stats
            .scrub_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .scrub_removed
            .fetch_add(removed as u64, Ordering::Relaxed);
    }

    // =========================================================================
    // Split (leaf)
    // =========================================================================

    fn split_leaf(&self, leaf_id: NodeId, depth: u32, deferred: &mut DeferredBalance) {
        let t0 = Instant::now();
        let code_size = self.code_size();

        let (old_ids, old_versions, old_codes, parent_id, old_centroid) =
            match self.nodes.remove(&leaf_id) {
                Some((_, TreeNode::Leaf(leaf))) => {
                    let parent_id = leaf.parent_id;
                    let centroid = leaf.centroid.clone();
                    (leaf.ids, leaf.versions, leaf.codes, parent_id, centroid)
                }
                Some((_, node)) => {
                    self.nodes.insert(leaf_id, node);
                    self.mark_node_dirty(leaf_id);
                    return;
                }
                None => return,
            };
        self.tombstones.insert(leaf_id);
        self.dirty_nodes.remove(&leaf_id);

        self.load_embeddings_sync(&old_ids);
        let embeddings: Vec<EmbeddingPoint> = old_ids
            .iter()
            .zip(old_versions.iter())
            .filter_map(|(&id, &ver)| {
                let current_ver = self.current_version_sync(id).unwrap_or(0);
                if ver == current_ver {
                    self.embeddings
                        .get(&id)
                        .map(|e| (id, ver as u32, e.value().clone()))
                } else {
                    None
                }
            })
            .collect();

        if embeddings.len() <= self.config.split_threshold {
            // After filtering stale entries the leaf no longer needs splitting.
            // Re-insert it since we already removed it from the DashMap.
            let mut codes = Vec::with_capacity(embeddings.len() * code_size);
            for (_, _, emb) in &embeddings {
                let code = Code::<1>::quantize(emb, &old_centroid);
                push_code(&mut codes, code.as_ref());
            }
            let len = embeddings.len();
            let centroid_code = self.quantize_origin(&old_centroid);
            self.nodes.insert(
                leaf_id,
                TreeNode::Leaf(LeafNode {
                    navigation: Arc::new(NavigationNode::new(
                        old_centroid,
                        centroid_code,
                        Vec::new(),
                        parent_id,
                    )),
                    ids: embeddings.iter().map(|(id, _, _)| *id).collect(),
                    versions: embeddings.iter().map(|(_, ver, _)| *ver as u8).collect(),
                    codes,
                    length: len,
                }),
            );
            self.mark_node_dirty(leaf_id);
            self.tombstones.remove(&leaf_id);
            self.publish_changed_navigation(leaf_id);
            return;
        }

        let kmeans_start = Instant::now();
        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &self.distance_fn);
        self.stats
            .split_kmeans_nanos
            .fetch_add(kmeans_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();

        let left_centroid = left_center.to_vec();
        let right_centroid = right_center.to_vec();

        let quantize_start = Instant::now();
        let mut left_codes = Vec::with_capacity(left_group.len() * code_size);
        for (_, _, emb) in &left_group {
            let code = Code::<1>::quantize(emb, &left_centroid);
            push_code(&mut left_codes, code.as_ref());
        }
        let mut right_codes = Vec::with_capacity(right_group.len() * code_size);
        for (_, _, emb) in &right_group {
            let code = Code::<1>::quantize(emb, &right_centroid);
            push_code(&mut right_codes, code.as_ref());
        }

        self.stats.split_quantize_nanos.fetch_add(
            quantize_start.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );

        let left_len = left_group.len();
        let right_len = right_group.len();
        let left_centroid_code = self.quantize_origin(&left_centroid);
        let right_centroid_code = self.quantize_origin(&right_centroid);
        self.nodes.insert(
            left_id,
            TreeNode::Leaf(LeafNode {
                navigation: Arc::new(NavigationNode::new(
                    left_centroid,
                    left_centroid_code,
                    Vec::new(),
                    None,
                )),
                ids: left_group.iter().map(|(id, _, _)| *id).collect(),
                versions: left_group.iter().map(|(_, ver, _)| *ver as u8).collect(),
                codes: left_codes,
                length: left_len,
            }),
        );
        self.mark_node_dirty(left_id);
        self.nodes.insert(
            right_id,
            TreeNode::Leaf(LeafNode {
                navigation: Arc::new(NavigationNode::new(
                    right_centroid,
                    right_centroid_code,
                    Vec::new(),
                    None,
                )),
                ids: right_group.iter().map(|(id, _, _)| *id).collect(),
                versions: right_group.iter().map(|(_, ver, _)| *ver as u8).collect(),
                codes: right_codes,
                length: right_len,
            }),
        );
        self.mark_node_dirty(right_id);

        self.attach_split_children(leaf_id, parent_id, &[left_id, right_id]);

        if depth < MAX_BALANCE_DEPTH {
            let mut evaluated = HashSet::new();

            let npa_cluster_start = Instant::now();
            if self.config.fp_npa {
                self.apply_npa_to_cluster_f32(
                    left_id,
                    &left_group,
                    &old_centroid,
                    &left_center,
                    &mut evaluated,
                    depth,
                    deferred,
                );
                self.apply_npa_to_cluster_f32(
                    right_id,
                    &right_group,
                    &old_centroid,
                    &right_center,
                    &mut evaluated,
                    depth,
                    deferred,
                );
            } else {
                // Only quantized NPA needs a lookup from vector IDs to their old codes.
                let old_code_slots: HashMap<u32, usize> =
                    old_ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
                self.apply_npa_to_cluster_quantized(
                    left_id,
                    &left_group,
                    &old_centroid,
                    &left_center,
                    &old_codes,
                    &old_code_slots,
                    &mut evaluated,
                    depth,
                    deferred,
                );
                self.apply_npa_to_cluster_quantized(
                    right_id,
                    &right_group,
                    &old_centroid,
                    &right_center,
                    &old_codes,
                    &old_code_slots,
                    &mut evaluated,
                    depth,
                    deferred,
                );
            }
            self.stats.split_npa_cluster_nanos.fetch_add(
                npa_cluster_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );

            let npa_neighbor_start = Instant::now();
            let write_policy = self.write_beam_policy();
            self.apply_npa_to_neighbors(
                leaf_id,
                left_id,
                right_id,
                &old_centroid,
                &left_center,
                &right_center,
                &mut evaluated,
                depth,
                &write_policy,
                deferred,
            );
            self.stats.split_npa_neighbor_nanos.fetch_add(
                npa_neighbor_start.elapsed().as_nanos() as u64,
                Ordering::Relaxed,
            );
        }

        self.stats.split_sizes.lock().push(old_ids.len() as u32);
        self.stats.splits.fetch_add(1, Ordering::Relaxed);
        self.stats
            .split_depth_sum
            .fetch_add(depth as u64, Ordering::Relaxed);
        self.stats
            .split_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    // Keep the caller's independent split inputs explicit in this internal helper.
    #[allow(clippy::too_many_arguments)]
    fn apply_npa_to_cluster_quantized(
        &self,
        from_cluster_id: NodeId,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        old_codes: &[u8],
        old_code_slots: &HashMap<u32, usize>,
        evaluated: &mut HashSet<u32>,
        depth: u32,
        deferred: &mut DeferredBalance,
    ) {
        let padded_bytes = self.padded_bytes();
        let c_norm = Self::vec_norm(old_center);
        let code_size = self.code_size();

        let old_r_q = vec![0.0f32; old_center.len()];
        let old_c_dot_q = c_norm * c_norm;
        let old_q_norm = c_norm;
        let old_qq = QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);

        let new_r_q: Vec<f32> = new_center
            .iter()
            .zip(old_center.iter())
            .map(|(a, b)| a - b)
            .collect();
        let new_c_dot_q = f32::dot(old_center, new_center).unwrap_or(0.0) as f32;
        let new_q_norm = Self::vec_norm(new_center);
        let new_qq = QuantizedQuery::new(&new_r_q, padded_bytes, c_norm, new_c_dot_q, new_q_norm);

        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        for (id, version, _) in group {
            let current_ver = self.current_version_sync(*id).unwrap_or(0);
            if *version as u8 != current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            let Some(&code_slot) = old_code_slots.get(id) else {
                continue;
            };
            let code_bytes = code_slice(old_codes, code_slot, code_size);
            n_evaluated += 1;
            let code = Code::<1, _>::new(code_bytes);

            let old_dist = code.distance_quantized_query(&self.distance_fn, &old_qq);
            let new_dist = code.distance_quantized_query(&self.distance_fn, &new_qq);
            if new_dist > old_dist {
                n_reassigned += 1;
                self.reassign(from_cluster_id, *id, depth, deferred);
            }
        }
        self.stats
            .split_npa_self_total
            .fetch_add(group.len() as u64, Ordering::Relaxed);
        self.stats
            .split_npa_self_evaluated
            .fetch_add(n_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_self_reassigns
            .fetch_add(n_reassigned, Ordering::Relaxed);
    }

    fn apply_npa_to_cluster_f32(
        &self,
        from_cluster_id: NodeId,
        group: &[EmbeddingPoint],
        old_center: &[f32],
        new_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
        deferred: &mut DeferredBalance,
    ) {
        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        for (id, version, emb) in group {
            let current_ver = self.current_version_sync(*id).unwrap_or(0);
            if *version as u8 != current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            n_evaluated += 1;
            let old_dist = self.dist(emb, old_center);
            let new_dist = self.dist(emb, new_center);
            if new_dist > old_dist {
                n_reassigned += 1;
                self.reassign(from_cluster_id, *id, depth, deferred);
            }
        }
        self.stats
            .split_npa_self_total
            .fetch_add(group.len() as u64, Ordering::Relaxed);
        self.stats
            .split_npa_self_evaluated
            .fetch_add(n_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_self_reassigns
            .fetch_add(n_reassigned, Ordering::Relaxed);
    }

    /// NPA for neighbor points: check vectors in nearby clusters that might now
    /// be closer to the new left/right centroids than to their current cluster.
    /// Follows the LIRE protocol from SPFresh (Section 3.2).
    fn apply_npa_to_quantized_neighbor(
        &self,
        neighbor_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
        deferred: &mut DeferredBalance,
    ) -> Option<(usize, usize, usize)> {
        let node_ref = self.nodes.get(&neighbor_id)?;
        let TreeNode::Leaf(leaf) = node_ref.value() else {
            return None;
        };
        let n_centroid = &leaf.centroid;
        let n_total = leaf.ids.len();
        let mut to_reassign = Vec::new();
        let mut n_evaluated = 0usize;

        let code_size = self.code_size();
        let padded_bytes = self.padded_bytes();
        let old_q_norm = Self::vec_norm(old_center);
        let left_q_norm = Self::vec_norm(left_center);
        let right_q_norm = Self::vec_norm(right_center);
        let c_norm = Self::vec_norm(n_centroid);

        let old_r_q: Vec<f32> = old_center
            .iter()
            .zip(n_centroid.iter())
            .map(|(a, b)| a - b)
            .collect();
        let old_c_dot_q = f32::dot(n_centroid, old_center).unwrap_or(0.0) as f32;

        let left_r_q: Vec<f32> = left_center
            .iter()
            .zip(n_centroid.iter())
            .map(|(a, b)| a - b)
            .collect();
        let left_c_dot_q = f32::dot(n_centroid, left_center).unwrap_or(0.0) as f32;

        let right_r_q: Vec<f32> = right_center
            .iter()
            .zip(n_centroid.iter())
            .map(|(a, b)| a - b)
            .collect();
        let right_c_dot_q = f32::dot(n_centroid, right_center).unwrap_or(0.0) as f32;

        let neighbor_r_q = vec![0.0f32; n_centroid.len()];
        let neighbor_c_dot_q = c_norm * c_norm;
        let neighbor_q_norm = c_norm;

        let left_qq =
            QuantizedQuery::new(&left_r_q, padded_bytes, c_norm, left_c_dot_q, left_q_norm);
        let right_qq = QuantizedQuery::new(
            &right_r_q,
            padded_bytes,
            c_norm,
            right_c_dot_q,
            right_q_norm,
        );
        let neighbor_qq = QuantizedQuery::new(
            &neighbor_r_q,
            padded_bytes,
            c_norm,
            neighbor_c_dot_q,
            neighbor_q_norm,
        );
        let old_qq = QuantizedQuery::new(&old_r_q, padded_bytes, c_norm, old_c_dot_q, old_q_norm);

        for i in 0..leaf.ids.len() {
            let code_bytes = code_slice(&leaf.codes, i, code_size);
            let id = leaf.ids[i];
            let version = leaf.versions[i];

            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if version != current_ver {
                continue;
            }
            if !evaluated.insert(id) {
                continue;
            }

            n_evaluated += 1;
            let code = Code::<1, _>::new(code_bytes);

            let left_dist = code.distance_quantized_query(&self.distance_fn, &left_qq);
            let right_dist = code.distance_quantized_query(&self.distance_fn, &right_qq);
            let neighbor_dist = code.distance_quantized_query(&self.distance_fn, &neighbor_qq);

            if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                continue;
            }

            let old_dist = code.distance_quantized_query(&self.distance_fn, &old_qq);
            if old_dist <= left_dist && old_dist <= right_dist {
                continue;
            }

            to_reassign.push((id, version));
        }

        // Reassignment can mutate this leaf or trigger another split. Release
        // its map guard before performing any of those writes.
        drop(node_ref);
        let mut n_reassigned = 0;
        for &(id, version) in &to_reassign {
            // An earlier reassignment may have changed this posting's version.
            if self.current_version_sync(id).unwrap_or(0) == version {
                n_reassigned += 1;
                self.reassign(neighbor_id, id, depth, deferred);
            }
        }

        Some((n_total, n_evaluated, n_reassigned))
    }

    fn apply_npa_to_fp_neighbor(
        &self,
        neighbor_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
        deferred: &mut DeferredBalance,
    ) -> Option<(usize, usize, usize)> {
        // Load only embeddings absent from the in-memory map. Disk reads run
        // without a leaf guard so parallel balancing can keep making progress.
        let missing_ids = {
            let node_ref = self.nodes.get(&neighbor_id)?;
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                return None;
            };
            leaf.ids
                .iter()
                .copied()
                .filter(|id| !self.embeddings.contains_key(id))
                .collect::<Vec<_>>()
        };
        self.load_embeddings_sync(&missing_ids);

        let node_ref = self.nodes.get(&neighbor_id)?;
        let TreeNode::Leaf(leaf) = node_ref.value() else {
            return None;
        };
        let n_total = leaf.ids.len();
        let mut to_reassign = Vec::new();
        let mut n_evaluated = 0usize;

        for i in 0..leaf.ids.len() {
            let id = leaf.ids[i];
            let version = leaf.versions[i];

            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if version != current_ver {
                continue;
            }
            if !evaluated.insert(id) {
                continue;
            }

            let Some(emb) = self.embeddings.get(&id) else {
                continue;
            };
            n_evaluated += 1;

            let left_dist = self.dist(emb.value(), left_center);
            let right_dist = self.dist(emb.value(), right_center);
            let neighbor_dist = self.dist(emb.value(), &leaf.centroid);

            if neighbor_dist <= left_dist && neighbor_dist <= right_dist {
                continue;
            }

            let old_dist = self.dist(emb.value(), old_center);
            if old_dist <= left_dist && old_dist <= right_dist {
                continue;
            }

            to_reassign.push((id, version));
        }

        drop(node_ref);
        let mut n_reassigned = 0;
        for &(id, version) in &to_reassign {
            if self.current_version_sync(id).unwrap_or(0) == version {
                n_reassigned += 1;
                self.reassign(neighbor_id, id, depth, deferred);
            }
        }

        Some((n_total, n_evaluated, n_reassigned))
    }

    // The helper receives both split destinations and the old centroid for scoring.
    #[allow(clippy::too_many_arguments)]
    fn apply_npa_to_neighbors(
        &self,
        old_leaf_id: NodeId,
        left_id: NodeId,
        right_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        depth: u32,
        write_policy: &ReadBeamPolicy,
        deferred: &mut DeferredBalance,
    ) {
        let neighbors =
            self.navigate_with_policy(old_center, self.config.write_navigation, write_policy);

        let mut neighbors_visited = 0u64;
        let mut neighbors_active = 0u64;
        let mut total_evaluated = 0u64;
        let mut total_reassigned = 0u64;

        for &(neighbor_id, _) in neighbors.iter().take(self.config.reassign_neighbor_count) {
            if neighbor_id == old_leaf_id || neighbor_id == left_id || neighbor_id == right_id {
                continue;
            }

            self.scrub(neighbor_id);
            let Some((n_total, n_evaluated, n_reassigned)) = (if self.config.fp_npa {
                self.apply_npa_to_fp_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                    deferred,
                )
            } else {
                self.apply_npa_to_quantized_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                    deferred,
                )
            }) else {
                continue;
            };

            neighbors_visited += 1;
            total_evaluated += n_evaluated as u64;
            total_reassigned += n_reassigned as u64;
            if n_total > 0 && n_reassigned * 100 > n_total {
                neighbors_active += 1;
            }
        }

        self.stats
            .split_npa_neighbors_visited
            .fetch_add(neighbors_visited, Ordering::Relaxed);
        self.stats
            .split_npa_neighbors_active
            .fetch_add(neighbors_active, Ordering::Relaxed);
        self.stats
            .split_npa_neighbor_evaluated
            .fetch_add(total_evaluated, Ordering::Relaxed);
        self.stats
            .split_npa_neighbor_reassigns
            .fetch_add(total_reassigned, Ordering::Relaxed);
    }

    /// Reassign a vector to its best cluster(s).
    fn reassign(
        &self,
        from_cluster_id: NodeId,
        id: u32,
        depth: u32,
        deferred: &mut DeferredBalance,
    ) {
        self.reassign_excluding(from_cluster_id, id, depth, None, deferred);
    }

    fn reassign_excluding(
        &self,
        from_cluster_id: NodeId,
        id: u32,
        depth: u32,
        excluded_leaf: Option<NodeId>,
        deferred: &mut DeferredBalance,
    ) {
        let t0 = Instant::now();

        let current_ver = self.current_version_sync(id).unwrap_or(0);
        if !self.is_valid(id, current_ver) {
            return;
        }

        self.load_embeddings_sync(&[id]);
        let Some(embedding) = self.embeddings.get(&id).map(|e| e.value().clone()) else {
            return;
        };

        let mut failed_registrations = 0;
        loop {
            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates = self.navigate_f32_excluding(&embedding, &policy, excluded_leaf);
            let cluster_ids = self.rng_select(&candidates);
            self.stats
                .reassign_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if cluster_ids.contains(&from_cluster_id) {
                break;
            }
            if !self.is_valid(id, current_ver) {
                return;
            }

            let reg_start = Instant::now();
            let mut clusters_to_balance = Vec::new();
            let mut version = None;
            for &cluster_id in &cluster_ids {
                if let Some(new_version) = version {
                    if self.register_in_leaf(cluster_id, id, new_version, &embedding) {
                        clusters_to_balance.push(cluster_id);
                    }
                } else if let Some(new_version) =
                    self.register_first_reassignment(cluster_id, id, current_ver, &embedding)
                {
                    version = Some(new_version);
                    clusters_to_balance.push(cluster_id);
                }
            }
            self.stats
                .reassign_register_nanos
                .fetch_add(reg_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            if clusters_to_balance.is_empty() {
                self.stats.add_missing_nodes.fetch_add(1, Ordering::Relaxed);
                // All candidates can disappear while their leaves split. The
                // old version is still live, so another navigation can retry.
                failed_registrations += 1;
                if failed_registrations >= 3 {
                    return;
                }
                continue;
            }

            let balance_start = Instant::now();
            for cluster_id in clusters_to_balance {
                deferred.push(cluster_id, depth + 1);
            }
            self.stats
                .reassign_balance_nanos
                .fetch_add(balance_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            break;
        }

        self.stats.reassigns.fetch_add(1, Ordering::Relaxed);
        self.stats
            .reassign_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    pub(super) fn is_valid(&self, id: u32, version: u8) -> bool {
        self.current_version_sync(id).is_some_and(|current| {
            // Tombstoned ids are never valid, even if the low 7 bits match.
            current & DELETED_BIT == 0 && (current & VERSION_MASK) == (version & VERSION_MASK)
        })
    }

    // =========================================================================
    // Split (internal)
    // =========================================================================

    fn split_internal(&self, node_id: NodeId) {
        let _guard = self.tree_lock.lock();
        if !self.balancing.insert(node_id) {
            return;
        }

        let (children, parent_id, _old_centroid) = match self.nodes.remove(&node_id) {
            Some((_, TreeNode::Internal(internal))) => {
                if internal.children().len() <= self.config.branching_factor {
                    self.nodes.insert(node_id, TreeNode::Internal(internal));
                    self.balancing.remove(&node_id);
                    return;
                }
                (
                    internal.children().to_vec(),
                    internal.parent_id,
                    internal.centroid.clone(),
                )
            }
            Some((_, node)) => {
                self.nodes.insert(node_id, node);
                self.mark_node_dirty(node_id);
                self.balancing.remove(&node_id);
                return;
            }
            None => {
                self.balancing.remove(&node_id);
                return;
            }
        };
        self.tombstones.insert(node_id);
        self.dirty_nodes.remove(&node_id);

        let child_embeddings: Vec<EmbeddingPoint> = children
            .iter()
            .map(|&child_id| {
                let centroid = self
                    .nodes
                    .get(&child_id)
                    .map(|n| n.centroid().to_vec())
                    .unwrap_or_else(|| vec![0.0; self.dim]);
                (child_id, 0u32, Arc::from(centroid.as_slice()))
            })
            .collect();

        let (left_center, left_group, right_center, right_group) =
            utils::split(child_embeddings, &self.distance_fn);

        let left_children: Vec<NodeId> = left_group.iter().map(|(id, _, _)| *id).collect();
        let right_children: Vec<NodeId> = right_group.iter().map(|(id, _, _)| *id).collect();

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();

        let left_centroid = left_center.to_vec();
        let right_centroid = right_center.to_vec();

        let left_centroid_code = self.quantize_origin(&left_centroid);
        let right_centroid_code = self.quantize_origin(&right_centroid);
        self.nodes.insert(
            left_id,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    left_centroid.clone(),
                    left_centroid_code,
                    left_children.clone(),
                    None,
                )),
            }),
        );
        self.mark_node_dirty(left_id);
        self.publish_child_navigation(left_id);
        self.nodes.insert(
            right_id,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    right_centroid.clone(),
                    right_centroid_code,
                    right_children.clone(),
                    None,
                )),
            }),
        );
        self.mark_node_dirty(right_id);
        self.publish_child_navigation(right_id);

        // Recompute centroid_codes for children
        for &child_id in &left_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(left_id));
                self.recompute_centroid_code(&mut node_ref);
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }
        for &child_id in &right_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(right_id));
                self.recompute_centroid_code(&mut node_ref);
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }

        self.balancing.remove(&node_id);

        self.attach_split_children(node_id, parent_id, &[left_id, right_id]);
    }

    // =========================================================================
    // Merge
    // =========================================================================

    /// Remove one source posting only after its same-version destination row
    /// exists. Keeping its version preserves replicas in other leaves.
    fn remove_transferred_posting(&self, leaf_id: NodeId, id: u32, version: u8) {
        let Some(mut node) = self.nodes.get_mut(&leaf_id) else {
            return;
        };
        let TreeNode::Leaf(leaf) = node.value_mut() else {
            return;
        };
        let Some(slot) = leaf
            .ids
            .iter()
            .zip(&leaf.versions)
            .position(|(&stored_id, &stored_version)| stored_id == id && stored_version == version)
        else {
            return;
        };
        leaf.ids.swap_remove(slot);
        leaf.versions.swap_remove(slot);
        swap_remove_code(&mut leaf.codes, slot, self.code_size());
        leaf.length = leaf.ids.len();
        drop(node);
        self.mark_node_dirty(leaf_id);
    }

    fn merge_leaf(&self, leaf_id: NodeId, depth: u32, deferred: &mut DeferredBalance) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }
        let t0 = Instant::now();

        self.load_posting_sync(leaf_id);
        // Keep the source's postings and codes in the tree until a replacement
        // is published. Failed navigation leaves the old version readable.
        let (source_centroid, source_ids, source_versions) = {
            let Some(node) = self.nodes.get(&leaf_id) else {
                return;
            };
            let TreeNode::Leaf(leaf) = node.value() else {
                return;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
            )
        };

        let policy = self.write_beam_policy();
        let candidates = self.navigate_f32_excluding(&source_centroid, &policy, Some(leaf_id));
        let Some(&(target_id, _)) = candidates.first() else {
            return;
        };
        let target_centroid = match self.nodes.get(&target_id) {
            Some(node) => node.centroid().to_vec(),
            None => return,
        };

        let Some(reservation) =
            MergeReservation::acquire(&self.merge_reservations, leaf_id, target_id)
        else {
            return;
        };

        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        self.stats
            .merge_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        self.load_embeddings_sync(&source_ids);
        for (&id, &version) in source_ids.iter().zip(source_versions.iter()) {
            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if version != current_ver {
                continue;
            }
            let Some(embedding) = self.embeddings.get(&id).map(|e| e.value().clone()) else {
                continue;
            };

            let dist_to_target = self.dist(&embedding, &target_centroid);
            let dist_to_source = self.dist(&embedding, &source_centroid);

            if dist_to_target <= dist_to_source {
                if self.register_in_leaf(target_id, id, version, &embedding) {
                    self.remove_transferred_posting(leaf_id, id, version);
                } else {
                    self.stats
                        .register_missing_nodes
                        .fetch_add(1, Ordering::Relaxed);
                    self.reassign_excluding(leaf_id, id, depth, Some(leaf_id), deferred);
                }
            } else {
                self.reassign_excluding(leaf_id, id, depth, Some(leaf_id), deferred);
            }
        }

        // Reassignment publishes a new version before invalidating its source
        // row. Direct transfers remove just the accepted row. Failed transfers
        // and concurrent additions remain in this source leaf.
        self.scrub(leaf_id);
        {
            let _guard = self.tree_lock.lock();
            // Check emptiness and remove under the same map lock, so an append
            // cannot arrive between the check and removal.
            if let Some((_, TreeNode::Leaf(leaf))) = self.nodes.remove_if(
                &leaf_id,
                |_, node| matches!(node, TreeNode::Leaf(leaf) if leaf.length == 0),
            ) {
                self.tombstones.insert(leaf_id);
                self.dirty_nodes.remove(&leaf_id);
                self.remove_child_locked(leaf.parent_id.unwrap_or_else(|| self.root_id()), leaf_id);
            }
        }
        drop(reservation);
        deferred.push(target_id, depth + 1);
    }

    // =========================================================================
    // Tree structure helpers
    // =========================================================================

    /// Attach replacement children to the current tree. The parent saved at
    /// the start of a split may have moved, including when it was the root.
    fn attach_split_children(
        &self,
        old_node: NodeId,
        parent_id: Option<NodeId>,
        new_children: &[NodeId],
    ) {
        let _guard = self.tree_lock.lock();
        if self.root_id() == old_node {
            self.create_root_above(new_children);
        } else {
            self.replace_child(
                parent_id.unwrap_or_else(|| self.root_id()),
                old_node,
                new_children,
            );
        }
    }

    fn parent_reaches_root(&self, parent_id: NodeId) -> bool {
        let mut current = parent_id;
        for _ in 0..MAX_NAV_LEVELS {
            if current == self.root_id() {
                return true;
            }
            let Some(parent) = self
                .nodes
                .get(&current)
                .and_then(|node| match node.value() {
                    TreeNode::Internal(internal) => internal.parent_id,
                    TreeNode::Leaf(leaf) => leaf.parent_id,
                })
            else {
                return false;
            };
            let linked = self.nodes.get(&parent).is_some_and(|node| {
                matches!(node.value(), TreeNode::Internal(internal) if internal.children().contains(&current))
            });
            if !linked {
                return false;
            }
            current = parent;
        }
        false
    }

    fn reachable_parent_containing(&self, child: NodeId) -> Option<NodeId> {
        let mut stack = vec![self.root_id()];
        let mut visited = HashSet::new();
        while let Some(node_id) = stack.pop() {
            if !visited.insert(node_id) {
                continue;
            }
            if let Some(node) = self.nodes.get(&node_id) {
                if let TreeNode::Internal(internal) = node.value() {
                    if internal.children().contains(&child) {
                        return Some(node_id);
                    }
                    stack.extend(internal.children().iter().copied());
                }
            }
        }
        None
    }

    /// When a parent node has been removed by a concurrent split_internal,
    /// navigate from root to find the correct internal node and insert orphans.
    fn adopt_orphans(&self, orphan_ids: &[NodeId]) {
        for &orphan_id in orphan_ids {
            let centroid = match self.nodes.get(&orphan_id) {
                Some(node_ref) => node_ref.centroid().to_vec(),
                None => continue,
            };
            let is_leaf = matches!(
                self.nodes
                    .get(&orphan_id)
                    .map(|n| matches!(n.value(), TreeNode::Leaf(_))),
                Some(true)
            );

            // Navigate from root to find the internal node at the right depth.
            let mut current = self.root_id();
            while let Some(node_ref) = self.nodes.get(&current) {
                match node_ref.value() {
                    TreeNode::Internal(internal) => {
                        let children = internal.children().to_vec();
                        drop(node_ref);

                        // Check if this level's children match the orphan type.
                        // If orphan is a leaf, we want an internal node whose children are leaves.
                        // If orphan is internal, we want one level higher.
                        let child_is_leaf = children.iter().any(|&c| {
                            self.nodes
                                .get(&c)
                                .is_some_and(|n| matches!(n.value(), TreeNode::Leaf(_)))
                        });

                        if is_leaf && (child_is_leaf || children.is_empty()) {
                            // Insert orphan here
                            self.update_children(current, |children| {
                                if !children.contains(&orphan_id) {
                                    children.push(orphan_id);
                                }
                            });
                            if let Some(mut node_ref) = self.nodes.get_mut(&orphan_id) {
                                node_ref.set_parent_id(Some(current));
                            }
                            self.mark_node_dirty(orphan_id);
                            break;
                        }
                        if !is_leaf && !child_is_leaf {
                            self.update_children(current, |children| {
                                if !children.contains(&orphan_id) {
                                    children.push(orphan_id);
                                }
                            });
                            if let Some(mut node_ref) = self.nodes.get_mut(&orphan_id) {
                                node_ref.set_parent_id(Some(current));
                            }
                            self.mark_node_dirty(orphan_id);
                            break;
                        }

                        // Go deeper: pick closest child
                        let closest = children
                            .iter()
                            .filter_map(|&c| {
                                self.nodes
                                    .get(&c)
                                    .map(|n| (c, self.dist(&centroid, n.centroid())))
                            })
                            .min_by(|a, b| {
                                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .map(|(c, _)| c);

                        match closest {
                            Some(c) => current = c,
                            None => break,
                        }
                    }
                    TreeNode::Leaf(leaf) => {
                        let leaf_parent = leaf.parent_id;
                        drop(node_ref);
                        let root = self.root_id();
                        if !is_leaf || current == root {
                            // An internal orphan needs to sit beside the
                            // whole existing subtree, not one of its leaves.
                            self.create_root_above(&[root, orphan_id]);
                        } else if let Some(parent) = leaf_parent {
                            self.replace_child(parent, current, &[current, orphan_id]);
                        } else {
                            self.create_root_above(&[root, orphan_id]);
                        }
                        break;
                    }
                }
            }
        }
    }

    fn replace_child(&self, parent_id: NodeId, old_child: NodeId, new_children: &[NodeId]) {
        let _guard = self.tree_lock.lock();
        // A concurrent internal split can move old_child under a new parent
        // while its leaf split is computing centroids. Find that new parent
        // before replacing the reference, otherwise the removed leaf remains
        // in the tree and the replacement leaves become detached.
        let replacement_parent = if self.nodes.get(&parent_id).is_some_and(|node| {
            matches!(node.value(), TreeNode::Internal(parent) if parent.children().contains(&old_child))
        }) && self.parent_reaches_root(parent_id) {
            Some(parent_id)
        } else {
            self.reachable_parent_containing(old_child)
        };
        let Some(parent_id) = replacement_parent else {
            self.adopt_orphans(new_children);
            return;
        };
        let Some((children_clone, _)) = self.update_children(parent_id, |children| {
            children.retain(|&id| id != old_child);
            children.extend_from_slice(new_children);
        }) else {
            self.adopt_orphans(new_children);
            return;
        };
        self.update_internal_centroid(parent_id, &children_clone);

        for &child_id in new_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(parent_id));
                self.recompute_centroid_code(&mut node_ref);
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }

        if children_clone.len() > self.config.branching_factor {
            self.split_internal(parent_id);
        }
    }

    fn remove_child_locked(&self, parent_id: NodeId, child_id: NodeId) {
        let _guard = self.tree_lock.lock();
        let parent_id = if self.nodes.get(&parent_id).is_some_and(|node| {
            matches!(node.value(), TreeNode::Internal(parent) if parent.children().contains(&child_id))
        }) && self.parent_reaches_root(parent_id)
        {
            Some(parent_id)
        } else {
            self.reachable_parent_containing(child_id)
        };
        let Some(parent_id) = parent_id else {
            return;
        };
        let Some((children_clone, grandparent_id)) = self.update_children(parent_id, |children| {
            children.retain(|&id| id != child_id);
        }) else {
            return;
        };

        if children_clone.is_empty() {
            self.nodes.remove(&parent_id);
            self.tombstones.insert(parent_id);
            self.dirty_nodes.remove(&parent_id);
            if parent_id == self.root_id() {
                let new_root = self.alloc_node_id();
                let centroid = vec![0.0f32; self.dim];
                let centroid_code = self.quantize_origin(&centroid);
                self.nodes.insert(
                    new_root,
                    TreeNode::Leaf(LeafNode {
                        navigation: Arc::new(NavigationNode::new(
                            centroid,
                            centroid_code,
                            Vec::new(),
                            None,
                        )),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        length: 0,
                    }),
                );
                self.mark_node_dirty(new_root);
                self.root_id.store(new_root, Ordering::Relaxed);
            } else if let Some(gp_id) = grandparent_id {
                self.remove_child_locked(gp_id, parent_id);
            }
        } else if children_clone.len() == 1 && parent_id == self.root_id() {
            let only_child = children_clone[0];
            self.nodes.remove(&parent_id);
            self.tombstones.insert(parent_id);
            self.dirty_nodes.remove(&parent_id);
            if let Some(mut node_ref) = self.nodes.get_mut(&only_child) {
                node_ref.set_parent_id(None);
            }
            self.mark_node_dirty(only_child);
            self.root_id.store(only_child, Ordering::Relaxed);
        } else {
            self.update_internal_centroid(parent_id, &children_clone);
        }
    }

    fn create_root_above(&self, children: &[NodeId]) {
        let _guard = self.tree_lock.lock();
        let root_id = self.alloc_node_id();
        let centroid = self.compute_centroid_of(children);
        let centroid_code = self.quantize_origin(&centroid);

        self.nodes.insert(
            root_id,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    centroid.clone(),
                    centroid_code,
                    children.to_vec(),
                    None,
                )),
            }),
        );
        self.mark_node_dirty(root_id);

        for &child_id in children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(root_id));
                self.recompute_centroid_code(&mut node_ref);
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }

        self.publish_child_navigation(root_id);
        self.root_id.store(root_id, Ordering::Relaxed);
    }

    fn compute_centroid_of(&self, children: &[NodeId]) -> Vec<f32> {
        let mut mean = vec![0.0f32; self.dim];
        let mut count = 0;
        for &child_id in children {
            if let Some(node_ref) = self.nodes.get(&child_id) {
                for (i, &v) in node_ref.centroid().iter().enumerate() {
                    mean[i] += v;
                }
                count += 1;
            }
        }
        if count > 0 {
            let scale = 1.0 / count as f32;
            for v in &mut mean {
                *v *= scale;
            }
        }
        mean
    }

    /// Quantize an f32 centroid against the origin (zero vector). Used to
    /// populate `centroid_code` at every node-construction or centroid-mutation
    /// site so that `centroid_code` is never empty and never stale relative to
    /// `centroid`.
    pub(super) fn quantize_origin(&self, centroid: &[f32]) -> Vec<u8> {
        Code::<1>::quantize(centroid, &self.zero_centroid)
            .as_ref()
            .to_vec()
    }

    /// Recompute a node's centroid_code in place after its centroid has been
    /// mutated. Prefer constructing nodes with `quantize_origin` directly when
    /// possible.
    fn recompute_centroid_code(
        &self,
        node_ref: &mut dashmap::mapref::one::RefMut<'_, NodeId, TreeNode>,
    ) {
        let centroid = node_ref.centroid().to_vec();
        let code = Code::<1>::quantize(&centroid, &self.zero_centroid);
        let code_bytes = code.as_ref().to_vec();
        match node_ref.value_mut() {
            TreeNode::Leaf(leaf) => {
                leaf.centroid_code = code_bytes;
            }
            TreeNode::Internal(internal) => {
                internal.centroid_code = code_bytes;
            }
        }
    }

    /// Compute the padded byte length for 1-bit codes at this dimension.
    pub(super) fn padded_bytes(&self) -> usize {
        Code::<1, Vec<u8>>::packed_len(self.dim)
    }

    pub(super) fn code_size(&self) -> usize {
        Code::<1, Vec<u8>>::size(self.dim)
    }

    /// Compute ||v||.
    pub(super) fn vec_norm(v: &[f32]) -> f32 {
        (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt()
    }
}

// Search, diagnostics, and tree info methods are in diagnostics.rs.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_destinations_preserve_depth_and_deduplicate_pending_work() {
        let mut work = DeferredBalance::default();
        work.push(7, 2);
        work.push(7, 2);
        work.push(7, 3);
        work.push(8, MAX_BALANCE_DEPTH + 1);
        assert_eq!(work.pop(), Some((7, 3)));
        // A completed task can become dirty again before the queue drains.
        work.push(7, 3);
        assert_eq!(work.pop(), Some((7, 3)));
        assert_eq!(work.pop(), Some((7, 2)));
        assert_eq!(work.pop(), None);
        assert!(work.pending.is_empty());
    }

    #[test]
    fn reassignment_publishes_rows_before_deferred_destination_split() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                split_threshold: 4,
                merge_threshold: 0,
                write_beam_min: 1,
                write_beam_max: 1,
                ..Default::default()
            },
        );
        for id in 0..8 {
            writer.add(id, &[id as f32; 8]);
        }
        writer.nodes.insert(1, empty_leaf());
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        writer.navigation_ready.store(false, Ordering::Release);
        let mut work = DeferredBalance::default();
        for id in 0..8 {
            writer.reassign_excluding(0, id, 3, Some(0), &mut work);
        }
        // The whole scan finishes before its oversized destination is split.
        assert!(
            matches!(writer.nodes.get(&1).unwrap().value(), TreeNode::Leaf(l) if l.length == 8)
        );
        assert_eq!(writer.stats.splits.load(Ordering::Relaxed), 0);
        assert_eq!(work.stack.len(), 1);
        assert_eq!(writer.root_reachable_valid_ids().unwrap(), (0..8).collect());
        // Another worker may remove a queued destination before it runs.
        work.push(u32::MAX, MAX_BALANCE_DEPTH);
        while let Some((leaf, depth)) = work.pop() {
            assert_eq!(depth, MAX_BALANCE_DEPTH);
            writer.balance_leaf(leaf, depth, &mut work);
        }
        assert_eq!(writer.stats.splits.load(Ordering::Relaxed), 1);
        assert_eq!(writer.stats.split_depth_sum.load(Ordering::Relaxed), 4);
        assert_eq!(
            writer
                .stats
                .split_npa_self_evaluated
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(writer.root_reachable_valid_ids().unwrap(), (0..8).collect());
        assert!(work.pending.is_empty());
    }

    #[test]
    fn cross_worker_duplicate_keeps_embedding_and_posting_version_together() {
        let mut writer = HierarchicalSpannWriter::new(
            32,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let code_size = Code::<1>::size(32);
        for round in 0..32 {
            let first: Arc<[f32]> = Arc::from(
                (0..32)
                    .map(|coordinate| if coordinate % 2 == 0 { 1.0 } else { -1.0 })
                    .collect::<Vec<_>>(),
            );
            let second: Arc<[f32]> = Arc::from(
                (0..32)
                    .map(|coordinate| if coordinate % 2 == 0 { -2.0 } else { 2.0 })
                    .collect::<Vec<_>>(),
            );
            writer.add_batch_buffered(&[(7, first), (7, second)], 2, || {});
            let version = ((round + 1) * 2) as u8;
            assert_eq!(writer.current_version_sync(7), Some(version));
            let embedding = writer.embeddings.get(&7).unwrap();
            let node = writer.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected root leaf")
            };
            let offset = leaf
                .ids
                .iter()
                .zip(&leaf.versions)
                .position(|(&id, &row_version)| id == 7 && row_version == version)
                .unwrap();
            let expected = Code::<1>::quantize(&embedding, &leaf.centroid);
            assert_eq!(
                &leaf.codes[offset * code_size..(offset + 1) * code_size],
                expected.as_ref()
            );
        }
    }

    #[test]
    fn worker_deltas_flush_versions_codes_and_replicas_after_join() {
        let config = HierarchicalSpannConfig {
            max_replicas: 2,
            merge_threshold: 0,
            write_rng_epsilon: 1.0,
            write_rng_factor: 1.0,
            ..HierarchicalSpannConfig::default()
        };
        let writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, config);
        if let Some(mut node) = writer.nodes.get_mut(&0) {
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                unreachable!()
            };
            leaf.centroid[0] = -1.0;
        }
        let sibling = writer.alloc_node_id();
        let mut other = match empty_leaf() {
            TreeNode::Leaf(leaf) => leaf,
            _ => unreachable!(),
        };
        other.centroid[0] = 1.0;
        writer.nodes.insert(sibling, TreeNode::Leaf(other));
        writer.create_root_above(&[0, sibling]);
        writer.delete(9);
        writer.begin_add_batch();

        let embedding: Arc<[f32]> = Arc::from([0.0f32; 8]);
        let first = vec![(7, Arc::clone(&embedding)), (7, Arc::clone(&embedding))];
        let second = vec![(8, Arc::clone(&embedding)), (9, embedding)];
        let (left, right) = std::thread::scope(|scope| {
            let a = scope.spawn(|| writer.collect_leaf_deltas(&first, &|| {}));
            let b = scope.spawn(|| writer.collect_leaf_deltas(&second, &|| {}));
            (a.join().unwrap(), b.join().unwrap())
        });
        for leaf_id in [0, sibling] {
            let node = writer.nodes.get(&leaf_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected leaf")
            };
            assert!(
                leaf.ids.is_empty(),
                "collection wrote a posting before the flush"
            );
        }

        writer.flush_leaf_deltas(vec![left, right]);
        writer.end_add_batch();
        let code_size = Code::<1>::quantize(&[0.0; 8], &[0.0; 8]).as_ref().len();
        for leaf_id in [0, sibling] {
            let node = writer.nodes.get(&leaf_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected leaf")
            };
            assert_eq!(leaf.ids, [7, 7, 8]);
            assert_eq!(leaf.versions, [1, 2, 1]);
            assert_eq!(leaf.length, 3);
            assert_eq!(leaf.codes.len(), 3 * code_size);
        }
        assert!(!writer.is_valid(7, 1));
        assert!(writer.is_valid(7, 2));
        assert!(writer.is_valid(8, 1));
        assert!(!writer.is_valid(9, 1));
        writer.balance_index_parallel(1);
    }

    fn empty_leaf() -> TreeNode {
        TreeNode::Leaf(LeafNode {
            navigation: Arc::new(NavigationNode::new(
                vec![0.0; 8],
                Vec::new(),
                Vec::new(),
                None,
            )),
            ids: Vec::new(),
            versions: Vec::new(),
            codes: Vec::new(),
            length: 0,
        })
    }

    #[test]
    fn merge_removed_source_does_not_consume_beam() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                write_beam_min: 1,
                write_beam_max: 1,
                merge_threshold: 0,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]);
        let mut leaf = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut leaf {
            leaf.centroid[0] = 2.0;
        }
        writer.nodes.insert(1, leaf);
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        writer.refresh_navigation();
        writer.navigation_ready.store(false, Ordering::Release);
        writer.merge_leaf(0, 0, &mut DeferredBalance::default());
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
    }

    #[test]
    fn reassign_removed_source_with_missing_closest_sibling() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                merge_threshold: 0,
                write_beam_min: 1,
                write_beam_max: 1,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]);
        for (id, coordinate) in [(1, 1.0), (2, 2.0)] {
            let mut leaf = empty_leaf();
            if let TreeNode::Leaf(leaf) = &mut leaf {
                leaf.centroid[0] = coordinate;
            }
            writer.nodes.insert(id, leaf);
        }
        writer.next_node_id.store(3, Ordering::Relaxed);
        writer.create_root_above(&[0, 1, 2]);
        let root = writer.root_id();
        writer.refresh_navigation();
        writer.navigation_ready.store(false, Ordering::Release);
        writer.nodes.remove(&0);
        writer.remove_child_locked(root, 0);
        writer.nodes.remove(&1);
        writer.reassign(0, 7, 0, &mut DeferredBalance::default());
        let cached: HashSet<_> = writer
            .nodes
            .iter()
            .filter_map(|n| match n.value() {
                TreeNode::Leaf(l) => Some(l.ids.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        for mut node in writer.nodes.iter_mut() {
            if let TreeNode::Internal(parent) = node.value_mut() {
                parent.clear_child_scoring();
            }
        }
        writer.reassign(0, 7, 0, &mut DeferredBalance::default());
        let live: HashSet<_> = writer
            .nodes
            .iter()
            .filter_map(|n| match n.value() {
                TreeNode::Leaf(l) => Some(l.ids.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(cached, live);
    }

    #[test]
    fn missing_child_does_not_consume_beam() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        for (id, coordinate) in [(1, 1.0), (2, 2.0)] {
            let mut leaf = empty_leaf();
            if let TreeNode::Leaf(leaf) = &mut leaf {
                leaf.centroid[0] = coordinate;
            }
            writer.nodes.insert(id, leaf);
        }
        writer.next_node_id.store(3, Ordering::Relaxed);
        writer.create_root_above(&[1, 2]);
        writer.refresh_navigation();
        writer.navigation_ready.store(false, Ordering::Release);
        writer.nodes.remove(&1);
        let policy = ReadBeamPolicy::uniform(None, 1, 1);
        let cached = writer.navigate_f32(&[0.0; 8], &policy);
        for mut node in writer.nodes.iter_mut() {
            if let TreeNode::Internal(parent) = node.value_mut() {
                parent.clear_child_scoring();
            }
        }
        let live = writer.navigate_f32(&[0.0; 8], &policy);
        assert_eq!(cached, live);
    }

    #[test]
    fn merging_leaf_before_parent_assignment_removes_reachable_link() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                write_beam_min: 1,
                write_beam_max: 1,
                merge_threshold: 0,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]);
        let mut sibling = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut sibling {
            leaf.centroid[0] = 2.0;
        }
        writer.nodes.insert(1, sibling);
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        // A concurrent merge can observe a replacement leaf after its parent
        // publishes the child list but before its parent pointer is assigned.
        writer.nodes.get_mut(&0).unwrap().set_parent_id(None);
        writer.navigation_ready.store(false, Ordering::Release);
        writer.merge_leaf(0, 0, &mut DeferredBalance::default());
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(writer.root_id(), 1);
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
    }

    #[test]
    fn merge_keeps_postings_when_destinations_disappear_during_transfer() {
        for remove_root in [true, false] {
            let writer = HierarchicalSpannWriter::new(
                8,
                DistanceFunction::Euclidean,
                HierarchicalSpannConfig {
                    merge_threshold: 0,
                    ..Default::default()
                },
            );
            writer.add(7, &[0.0; 8]);
            for id in 1..=2 {
                let mut sibling = empty_leaf();
                if let TreeNode::Leaf(leaf) = &mut sibling {
                    leaf.centroid[0] = id as f32;
                }
                writer.nodes.insert(id, sibling);
            }
            writer.next_node_id.store(3, Ordering::Relaxed);
            writer.create_root_above(&[0, 1, 2]);
            let root = writer.root_id();
            writer.versions.insert(8, 1);
            writer.embeddings.insert(8, Arc::from([0.0; 8]));
            // Block the first row's version read after merge chooses its target.
            let version_guard = writer.versions.get_mut(&7).unwrap();
            std::thread::scope(|scope| {
                let merge =
                    scope.spawn(|| writer.merge_leaf(0, 0, &mut DeferredBalance::default()));
                let deadline = Instant::now() + std::time::Duration::from_secs(5);
                while writer.stats.merges.load(Ordering::Relaxed) == 0 {
                    assert!(
                        Instant::now() < deadline,
                        "merge never reached row transfer"
                    );
                    std::thread::yield_now();
                }
                // A concurrent registration must survive the source cleanup.
                assert!(writer.register_in_leaf(0, 8, 1, &[0.0; 8]));
                let removed: Vec<_> = if remove_root { vec![root] } else { vec![1, 2] }
                    .into_iter()
                    .map(|id| writer.nodes.remove(&id).unwrap())
                    .collect();
                drop(version_guard);
                merge.join().unwrap();
                for (id, node) in removed {
                    writer.nodes.insert(id, node);
                }
            });
            assert_eq!(
                writer.root_reachable_valid_ids().unwrap(),
                HashSet::from([7, 8])
            );
            assert_eq!(writer.current_version_sync(7), Some(1));
            assert_eq!(writer.stats.add_missing_nodes.load(Ordering::Relaxed), 3);
        }
    }

    #[test]
    fn merge_without_target_preserves_postings_and_codes() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        writer.add(7, &[0.0; 8]);
        writer.merge_leaf(0, 0, &mut DeferredBalance::default());
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
        assert_eq!(writer.current_version_sync(7), Some(1));
    }

    #[test]
    fn overlapping_merges_defer_without_swapping_rows() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                merge_threshold: 2,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]);
        // Find a second version in the same shard to pause both transfer loops.
        for id in 1000..2000 {
            writer.versions.insert(id, 1);
        }
        let blocked = writer.versions.get_mut(&7).unwrap();
        let other = (1000..2000)
            .find(|id| {
                matches!(
                    writer.versions.try_get(id),
                    dashmap::try_result::TryResult::Locked
                )
            })
            .unwrap();
        drop(blocked);
        let mut sibling = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut sibling {
            leaf.centroid = vec![2.0; 8];
        }
        writer.nodes.insert(1, sibling);
        writer.embeddings.insert(other, Arc::from([2.0; 8]));
        assert!(writer.register_in_leaf(1, other, 1, &[2.0; 8]));
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        let blocked = writer.versions.get_mut(&7).unwrap();
        std::thread::scope(|scope| {
            let first = scope.spawn(|| writer.merge_leaf(0, 0, &mut DeferredBalance::default()));
            let deadline = Instant::now() + Duration::from_secs(5);
            while writer.stats.merges.load(Ordering::Relaxed) == 0 {
                assert!(
                    Instant::now() < deadline,
                    "first merge never reserved its pair"
                );
                std::thread::yield_now();
            }
            let (done, completed) = std::sync::mpsc::channel();
            let writer_ref = &writer;
            let second = scope.spawn(move || {
                writer_ref.merge_leaf(1, 0, &mut DeferredBalance::default());
                done.send(()).unwrap();
            });
            let deferred = completed.recv_timeout(Duration::from_secs(2));
            drop(blocked);
            first.join().unwrap();
            second.join().unwrap();
            assert!(
                deferred.is_ok(),
                "overlapping merge entered its transfer loop"
            );
        });
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(writer.root_id(), 1);
        assert_eq!(writer.current_version_sync(other), Some(1));
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7, other])
        );
        assert!(writer.merge_reservations.lock().is_empty());
    }

    #[test]
    fn successful_merge_preserves_other_replicas() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                merge_threshold: 0,
                max_replicas: 2,
                ..Default::default()
            },
        );
        writer.add(7, &[1.0; 8]);
        let mut sibling = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut sibling {
            leaf.centroid = vec![1.0; 8];
        }
        writer.nodes.insert(1, sibling);
        let mut replica = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut replica {
            leaf.centroid = vec![3.0; 8];
        }
        writer.nodes.insert(2, replica);
        assert!(writer.register_in_leaf(2, 7, 1, &[1.0; 8]));
        writer.next_node_id.store(3, Ordering::Relaxed);
        writer.create_root_above(&[0, 1, 2]);
        writer.merge_leaf(0, 0, &mut DeferredBalance::default());
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(writer.current_version_sync(7), Some(1));
        let replica = writer.nodes.get(&2).unwrap();
        let TreeNode::Leaf(replica) = replica.value() else {
            panic!("replica must remain a leaf");
        };
        assert!(writer.is_valid(7, replica.versions[0]));
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
    }

    #[test]
    fn live_candidate_selection_preserves_tau_and_beam_limits() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        for id in 1..=3 {
            let mut leaf = empty_leaf();
            if let TreeNode::Leaf(leaf) = &mut leaf {
                leaf.centroid[0] = id as f32;
            }
            writer.nodes.insert(id, leaf);
        }
        writer.next_node_id.store(4, Ordering::Relaxed);
        writer.create_root_above(&[1, 2, 3]);
        writer.nodes.remove(&1);
        let live = vec![(2, 4.0), (3, 9.0)];
        for tau in [None, Some(0.5), Some(1.0), Some(1.5), Some(2.5)] {
            for minimum in 0..=4 {
                for maximum in 0..=4 {
                    let policy = ReadBeamPolicy::uniform(tau, minimum, maximum);
                    let params = policy.level_params(1);
                    let expected_count = HierarchicalSpannWriter::effective_beam(
                        &live,
                        params.tau,
                        params.beam_min,
                        params.beam_max,
                    );
                    assert_eq!(
                        writer.navigate_f32(&[0.0; 8], &policy),
                        live[..expected_count],
                        "tau={tau:?} minimum={minimum} maximum={maximum}"
                    );
                }
            }
        }
    }

    #[test]
    fn child_absent_during_publication_can_return_before_refresh() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let mut leaf = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut leaf {
            leaf.centroid[0] = 1.0;
        }
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[1]);
        writer.nodes.insert(1, leaf);
        assert_eq!(
            writer.navigate_f32(&[0.0; 8], &ReadBeamPolicy::uniform(None, 1, 1)),
            vec![(1, 1.0)]
        );
    }

    #[test]
    fn balancing_navigation_scores_shared_children_without_point_lookups() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        for (id, coordinate) in [(1, 1.0), (2, 2.0), (3, 3.0)] {
            let mut leaf = empty_leaf();
            if let TreeNode::Leaf(leaf) = &mut leaf {
                leaf.centroid[0] = coordinate;
                leaf.parent_id = Some(4);
            }
            writer.nodes.insert(id, leaf);
        }
        writer.next_node_id.store(4, Ordering::Relaxed);
        writer.create_root_above(&[1, 2]);
        let root = writer.root_id();
        writer.refresh_navigation();
        let before = match writer.nodes.get(&root).unwrap().value() {
            TreeNode::Internal(parent) => Arc::clone(&parent.navigation),
            _ => unreachable!(),
        };
        writer.navigation_ready.store(false, Ordering::Release);
        let policy = ReadBeamPolicy::uniform(None, 3, 3);
        assert_eq!(
            writer.navigate_f32(&[0.0; 8], &policy),
            vec![(1, 1.0), (2, 4.0)]
        );
        assert_eq!(
            writer
                .stats
                .navigation_child_lookups
                .load(Ordering::Relaxed),
            0
        );
        writer.replace_child(root, 1, &[3]);
        assert_eq!(
            writer.navigate_f32(&[0.0; 8], &policy),
            vec![(2, 4.0), (3, 9.0)]
        );
        assert_eq!(
            writer
                .stats
                .navigation_child_lookups
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(before.children(), vec![1, 2]);
        assert_eq!(
            before
                .child_navigation()
                .scored_children()
                .unwrap()
                .next()
                .unwrap()
                .1
                .unwrap()[0],
            1.0
        );
        assert_eq!(
            before
                .child_navigation()
                .scored_children()
                .unwrap()
                .nth(1)
                .unwrap()
                .1
                .unwrap()[0],
            2.0
        );
        writer.remove_child_locked(root, 3);
        assert_eq!(writer.root_id(), 2);
        assert_eq!(writer.navigate_f32(&[0.0; 8], &policy), vec![(2, 4.0)]);
    }

    #[test]
    fn cascading_internal_splits_publish_payloads_before_joined_refresh() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                branching_factor: 2,
                ..Default::default()
            },
        );
        for id in 1..=6 {
            let mut leaf = empty_leaf();
            if let TreeNode::Leaf(leaf) = &mut leaf {
                leaf.centroid[0] = id as f32;
            }
            writer.nodes.insert(id, leaf);
        }
        writer.next_node_id.store(7, Ordering::Relaxed);
        writer.create_root_above(&[1, 2]);
        let parent = writer.root_id();
        writer.create_root_above(&[parent, 6]);
        let root = writer.root_id();
        let old_payload = match writer.nodes.get(&root).unwrap().value() {
            TreeNode::Internal(root) => Arc::clone(&root.navigation),
            _ => unreachable!(),
        };
        writer.navigation_ready.store(false, Ordering::Release);
        writer.replace_child(parent, 1, &[3, 4]);
        assert_ne!(writer.root_id(), root);
        assert_eq!(
            writer.navigate_f32(&[0.0; 8], &ReadBeamPolicy::uniform(None, 1024, 1024)),
            vec![(2, 4.0), (3, 9.0), (4, 16.0), (6, 36.0)],
        );
        assert_eq!(
            writer
                .stats
                .navigation_child_lookups
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(old_payload.children(), vec![parent, 6]);
        assert_eq!(
            old_payload
                .child_navigation()
                .scored_children()
                .unwrap()
                .next()
                .unwrap()
                .1
                .unwrap()[0],
            1.5
        );
        assert_eq!(
            old_payload
                .child_navigation()
                .scored_children()
                .unwrap()
                .nth(1)
                .unwrap()
                .1
                .unwrap()[0],
            6.0
        );
    }

    #[test]
    fn parallel_splits_publish_matching_child_centroids_and_keep_all_ids() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                split_threshold: 24,
                merge_threshold: 0,
                branching_factor: 4,
                fp_npa: true,
                ..Default::default()
            },
        );
        for batch in 0..3 {
            let points: Vec<_> = (batch * 128..(batch + 1) * 128)
                .map(|id| {
                    let embedding: Vec<f32> = (0..8)
                        .map(|d| ((id * 31 + d * 17) as f32 * 0.13).sin())
                        .collect();
                    (id, Arc::from(embedding))
                })
                .collect();
            writer.add_batch_buffered(&points, 4, || {});
            writer.balance_index_parallel(4);
            let expected: HashSet<_> = (0..(batch + 1) * 128).collect();
            assert_eq!(writer.root_reachable_valid_ids().unwrap(), expected);
            let parents: Vec<_> = writer
                .nodes
                .iter()
                .filter_map(|node| match node.value() {
                    TreeNode::Internal(parent) => Some(Arc::clone(&parent.navigation)),
                    _ => None,
                })
                .collect();
            for parent in parents {
                for (id, centroid) in parent.child_navigation().scored_children().unwrap() {
                    assert_eq!(centroid.unwrap(), writer.nodes.get(&id).unwrap().centroid());
                }
            }
            // Exercise balancing's per-parent validity, independently of the
            // stable-add initialization flag.
            writer.navigation_ready.store(false, Ordering::Release);
            writer
                .stats
                .navigation_child_lookups
                .store(0, Ordering::Relaxed);
            let policy = ReadBeamPolicy::uniform(None, 1024, 1024);
            let cached = writer.navigate_f32(&points[0].1, &policy);
            assert_eq!(
                writer
                    .stats
                    .navigation_child_lookups
                    .load(Ordering::Relaxed),
                0
            );
            for mut node in writer.nodes.iter_mut() {
                if let TreeNode::Internal(parent) = node.value_mut() {
                    parent.clear_child_scoring();
                }
            }
            assert_eq!(writer.navigate_f32(&points[0].1, &policy), cached);
            writer.refresh_navigation();
        }
        assert!(writer.level_node_counts().len() > 2);
    }

    #[test]
    fn packed_navigation_matches_live_tree_and_refreshes_after_balance() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let query = [0.0; 8];
        let policy = ReadBeamPolicy::uniform(None, 3, 3);
        let root_leaf = writer.navigate_f32(&query, &policy);
        writer.begin_add_batch();
        assert_eq!(writer.navigate_f32(&query, &policy), root_leaf);
        writer.end_add_batch();

        for (id, coordinate) in [(0, 1.0), (1, 2.0), (2, 3.0)] {
            let mut leaf = match empty_leaf() {
                TreeNode::Leaf(leaf) => leaf,
                _ => unreachable!(),
            };
            leaf.centroid[0] = coordinate;
            writer.nodes.insert(id, TreeNode::Leaf(leaf));
        }
        let internal = 3;
        let mut internal_centroid = vec![0.0; 8];
        internal_centroid[0] = 2.5;
        writer.nodes.insert(
            internal,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    internal_centroid,
                    Vec::new(),
                    vec![1, 2],
                    Some(4),
                )),
            }),
        );
        let root = 4;
        writer.nodes.insert(
            root,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    vec![0.0; 8],
                    Vec::new(),
                    vec![0, internal, 999],
                    None,
                )),
            }),
        );
        writer.root_id.store(root, Ordering::Relaxed);

        let live = writer.navigate_f32(&query, &policy);
        let level_policy = ReadBeamPolicy::with_level_overrides(
            Some(1.5),
            1,
            2,
            vec![Some(3.0), Some(1.25)],
            vec![0.0, 0.0],
            vec![3, 3],
        );
        let live_level_policy = writer.navigate_f32(&query, &level_policy);
        let missing_before = writer.stats.navigate_missing_nodes.load(Ordering::Relaxed);
        writer.navigation_ready.store(false, Ordering::Release);
        writer.begin_add_batch();
        assert_eq!(writer.navigate_f32(&query, &policy), live);
        assert_eq!(
            writer.navigate_f32(&query, &level_policy),
            live_level_policy
        );
        assert_eq!(
            writer.stats.navigate_missing_nodes.load(Ordering::Relaxed),
            missing_before + 2
        );
        assert!(writer.memory_usage().navigation_index_bytes > 0);
        let packed_before = {
            let node = writer.nodes.get(&root).unwrap();
            let TreeNode::Internal(internal) = node.value() else {
                unreachable!()
            };
            Arc::clone(internal.child_navigation())
        };
        writer.end_add_batch();
        writer.begin_add_batch();
        let packed_after = {
            let node = writer.nodes.get(&root).unwrap();
            let TreeNode::Internal(internal) = node.value() else {
                unreachable!()
            };
            Arc::clone(internal.child_navigation())
        };
        assert!(Arc::ptr_eq(&packed_before, &packed_after));

        if let Some(mut node) = writer.nodes.get_mut(&2) {
            if let TreeNode::Leaf(leaf) = node.value_mut() {
                leaf.centroid[0] = 0.5;
            }
        }
        writer.mark_node_dirty(2);
        writer.balance_index_parallel(2);
        writer.begin_add_batch();
        assert_eq!(writer.navigate_f32(&query, &policy)[0].0, 2);
        writer.end_add_batch();
    }

    #[test]
    fn add_shares_navigation_while_postings_change() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        writer.begin_add_batch();
        let navigation = {
            let node = writer.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                unreachable!()
            };
            Arc::clone(&leaf.navigation)
        };
        {
            let node = writer.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                unreachable!()
            };
            assert!(Arc::ptr_eq(&leaf.navigation, &navigation));
        }
        writer.add(7, &[1.0; 8]);
        {
            let node = writer.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("root must remain a leaf");
            };
            assert_eq!(leaf.ids, vec![7]);
            assert!(Arc::ptr_eq(&leaf.navigation, &navigation));
        }
        writer.end_add_batch();
    }

    #[test]
    fn policy_width_snapshot_reuses_add_widths_and_clears_after_balancing() {
        let config = HierarchicalSpannConfig {
            write_beam_min: 0,
            write_beam_max: 100,
            write_level_min_pcts: vec![50.0],
            ..Default::default()
        };
        let writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, config);
        assert!(writer.cached_level_widths().is_empty());

        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let root = writer.root_id();
        // End the setup phase before observing widths from the new tree.
        writer.balance_index_parallel(1);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        assert_eq!(writer.write_beam_policy().level_params(1).beam_min, 1);

        let left = writer.alloc_node_id();
        let right = writer.alloc_node_id();
        writer.nodes.insert(left, empty_leaf());
        writer.nodes.insert(right, empty_leaf());
        writer.replace_child(root, sibling, &[left, right]);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.balance_index_parallel(1);
        assert_eq!(writer.cached_level_widths(), vec![3]);
        assert_eq!(writer.write_beam_policy().level_params(1).beam_min, 2);

        writer.remove_child_locked(root, 0);
        assert_eq!(writer.cached_level_widths(), vec![3]);
        writer.balance_index_parallel(1);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.remove_child_locked(root, left);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.balance_index_parallel(1);
        assert!(writer.cached_level_widths().is_empty());
        assert_eq!(writer.root_id(), right);
    }

    #[test]
    fn replacing_leaf_finds_parent_moved_by_internal_split() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let old_parent = writer.root_id();

        // The internal split has moved the old leaf to a new parent before
        // the leaf split finishes and calls replace_child with old_parent.
        // The old parent remains in the map, but is no longer reachable.
        let moved_parent = writer.alloc_node_id();
        let centroid = vec![0.0; 8];
        writer.nodes.insert(
            moved_parent,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    centroid.clone(),
                    writer.quantize_origin(&centroid),
                    vec![0, sibling],
                    None,
                )),
            }),
        );
        writer.root_id.store(moved_parent, Ordering::Relaxed);
        writer.nodes.remove(&0);
        let left = writer.alloc_node_id();
        let right = writer.alloc_node_id();
        writer.nodes.insert(left, empty_leaf());
        writer.nodes.insert(right, empty_leaf());

        writer.replace_child(old_parent, 0, &[left, right]);
        let parent = writer.nodes.get(&moved_parent).unwrap();
        let TreeNode::Internal(parent) = parent.value() else {
            panic!("moved parent must remain internal");
        };
        assert!(!parent.children().contains(&0));
        assert!(parent.children().contains(&left));
        assert!(parent.children().contains(&right));
    }

    #[test]
    fn stale_parentless_split_does_not_replace_current_root() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let current_root = writer.root_id();
        writer.nodes.remove(&0);
        let left = writer.alloc_node_id();
        let right = writer.alloc_node_id();
        writer.nodes.insert(left, empty_leaf());
        writer.nodes.insert(right, empty_leaf());

        writer.attach_split_children(0, None, &[left, right]);
        assert_eq!(writer.root_id(), current_root);
        let root = writer.nodes.get(&current_root).unwrap();
        let TreeNode::Internal(root) = root.value() else {
            panic!("current root must remain internal");
        };
        assert!(!root.children().contains(&0));
        assert!(root.children().contains(&sibling));
        assert!(root.children().contains(&left));
        assert!(root.children().contains(&right));
    }

    #[test]
    fn adopting_internal_orphan_keeps_entire_existing_root() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let old_root = writer.root_id();
        let left = writer.alloc_node_id();
        let right = writer.alloc_node_id();
        writer.nodes.insert(left, empty_leaf());
        writer.nodes.insert(right, empty_leaf());
        let orphan = writer.alloc_node_id();
        let centroid = vec![0.0; 8];
        writer.nodes.insert(
            orphan,
            TreeNode::Internal(InternalNode {
                navigation: Arc::new(NavigationNode::new(
                    centroid.clone(),
                    writer.quantize_origin(&centroid),
                    vec![left, right],
                    None,
                )),
            }),
        );

        writer.adopt_orphans(&[orphan]);
        let root = writer.nodes.get(&writer.root_id()).unwrap();
        let TreeNode::Internal(root) = root.value() else {
            panic!("new root must be internal");
        };
        assert!(root.children().contains(&old_root));
        assert!(root.children().contains(&orphan));
        assert!(writer.nodes.get(&sibling).is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reopened_reassignment_promotes_checkpoint_version_and_persists_it() {
        use crate::hierarchical_index::persistance::PREFIX_VERSION;
        use chroma_blockstore::{
            arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider,
        };
        use chroma_cache::new_cache_for_test;
        use chroma_storage::{local::LocalStorage, Storage};
        use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;

        let dir = tempfile::tempdir().unwrap();
        let provider = BlockfileProvider::new_arrow(
            Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),
            1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            4,
            0,
        );
        let config = HierarchicalSpannConfig {
            merge_threshold: 0,
            ..Default::default()
        };
        let writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, config.clone());
        let embedding = vec![1.0; 8];
        writer.add(7, &embedding);
        writer.add(8, &embedding);
        let first = Box::pin(
            Box::pin(writer.commit(&provider, None))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let reopened = HierarchicalSpannWriter::open(
            &provider,
            first.clone(),
            DistanceFunction::Euclidean,
            config,
        )
        .await
        .unwrap();
        assert!(!reopened.versions.contains_key(&7));
        assert_eq!(reopened.current_version_sync(7), Some(1));
        assert_eq!(
            reopened.register_first_reassignment(999, 7, 1, &embedding),
            None
        );
        assert!(reopened.is_valid(7, 1));

        // Two workers trying the same old version must publish one replacement.
        let results = std::thread::scope(|scope| {
            let a = scope.spawn(|| reopened.register_first_reassignment(0, 7, 1, &embedding));
            let b = scope.spawn(|| reopened.register_first_reassignment(0, 7, 1, &embedding));
            [a.join().unwrap(), b.join().unwrap()]
        });
        assert_eq!(results.iter().filter(|&&v| v == Some(2)).count(), 1);
        assert_eq!(results.iter().filter(|&&v| v.is_none()).count(), 1);
        assert!(!reopened.is_valid(7, 1));
        assert!(reopened.is_valid(7, 2));
        assert!(reopened.stats.posting_loads.load(Ordering::Relaxed) > 0);
        {
            let node = reopened.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected leaf")
            };
            assert_eq!(leaf.length, 3);
            assert_eq!(leaf.ids, vec![7, 8, 7]);
            assert_eq!(leaf.versions, vec![1, 1, 2]);
        }
        // Materialization merges the old base with the replacement delta.
        reopened.load_all_postings().await.unwrap();
        assert_eq!(reopened.total_leaf_entries(), 3);
        let second = Box::pin(
            Box::pin(reopened.commit(&provider, Some(&first)))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let postings = provider
            .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
                second.posting_list_id,
                "".to_string(),
            ))
            .await
            .unwrap();
        let posting = postings.get("", 0).await.unwrap().unwrap();
        assert_eq!(posting.ids.to_vec(), vec![7, 8, 7]);
        assert_eq!(posting.versions.to_vec(), vec![1, 1, 2]);
        assert_eq!(posting.codes.len(), 3 * Code::<1>::size(8));
        let versions = provider
            .read::<u32, u32>(BlockfileReaderOptions::new(
                second.scalar_metadata_id,
                "".to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(versions.get(PREFIX_VERSION, 7).await.unwrap(), Some(2));
    }

    #[test]
    fn reassignment_keeps_old_version_until_replacement_is_registered() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let embedding = vec![1.0; 8];
        writer.versions.insert(7, 1);
        assert!(writer.register_in_leaf(0, 7, 1, &embedding));

        assert_eq!(
            writer.register_first_reassignment(999, 7, 1, &embedding),
            None
        );
        assert!(writer.is_valid(7, 1));
        assert_eq!(
            writer.register_first_reassignment(0, 7, 1, &embedding),
            Some(2)
        );
        assert!(!writer.is_valid(7, 1));
        assert!(writer.is_valid(7, 2));
        let leaf = writer.nodes.get(&0).unwrap();
        let TreeNode::Leaf(leaf) = leaf.value() else {
            panic!("root must remain a leaf");
        };
        assert_eq!(leaf.versions, vec![1, 2]);
    }

    #[test]
    fn removing_child_finds_its_current_reachable_parent() {
        let writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let stale_parent = writer.alloc_node_id();
        writer.nodes.remove(&0);

        writer.remove_child_locked(stale_parent, 0);
        assert_eq!(writer.root_id(), sibling);
        assert!(writer.nodes.get(&sibling).is_some());
    }
}

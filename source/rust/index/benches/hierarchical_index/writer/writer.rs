use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
use super::super::instrumentation::{BalanceIterationTiming, WriterStats};
use super::{HierarchicalSpannWriter, MAX_NAV_LEVELS};

const MAX_BALANCE_DEPTH: u32 = 4;

/// Claims are acquired atomically without waiting. No node-map guard or tree
/// lock is held while claiming leaves. A claim owns lifetime and postings; map
/// guards remain short and are never held for two leaves at once.
struct LeafClaim<'a> {
    active: &'a Mutex<HashSet<NodeId>>,
    ids: Vec<NodeId>,
    stats: &'a WriterStats,
    start: Instant,
}

impl<'a> LeafClaim<'a> {
    fn acquire(writer: &'a HierarchicalSpannWriter, ids: &[NodeId]) -> Option<Self> {
        let wait = Instant::now();
        let mut active = writer.leaf_claims.lock();
        writer
            .stats
            .claim_mutex_wait_nanos
            .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if ids.iter().any(|id| active.contains(id)) {
            writer.stats.claim_conflicts.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        active.extend(ids.iter().copied());
        Some(Self {
            active: &writer.leaf_claims,
            ids: ids.to_vec(),
            stats: &writer.stats,
            start: Instant::now(),
        })
    }
}

impl<'a> LeafClaim<'a> {
    fn acquire_available(
        writer: &'a HierarchicalSpannWriter,
        source: NodeId,
        destinations: &[NodeId],
    ) -> Option<(Self, Vec<bool>)> {
        let mut ids = Vec::with_capacity(destinations.len() + 1);
        let mut available = Vec::with_capacity(destinations.len());
        let wait = Instant::now();
        let mut active = writer.leaf_claims.lock();
        writer
            .stats
            .claim_mutex_wait_nanos
            .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if active.contains(&source) {
            writer.stats.claim_conflicts.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        ids.push(source);
        for &destination in destinations {
            let free = !active.contains(&destination);
            available.push(free);
            if free {
                ids.push(destination);
            } else {
                writer.stats.claim_conflicts.fetch_add(1, Ordering::Relaxed);
            }
        }
        active.extend(ids.iter().copied());
        Some((
            Self {
                active: &writer.leaf_claims,
                ids,
                stats: &writer.stats,
                start: Instant::now(),
            },
            available,
        ))
    }
}

impl Drop for LeafClaim<'_> {
    fn drop(&mut self) {
        let wait = Instant::now();
        let mut active = self.active.lock();
        self.stats
            .claim_mutex_wait_nanos
            .fetch_add(wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
        for id in &self.ids {
            active.remove(id);
        }
        self.stats
            .claim_hold_nanos
            .fetch_add(self.start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

/// Merge one transfer call's accounting after its buffers and claims are used.
/// Byte counters describe explicit payload copies, not allocator traffic or I/O.
struct TransferObservation<'a> {
    stats: &'a WriterStats,
    started: Instant,
    target_groups: u64,
    membership_probes: u64,
    source_snapshot_bytes: u64,
    compaction_copy_bytes: u64,
    destination_append_bytes: u64,
}
impl Drop for TransferObservation<'_> {
    fn drop(&mut self) {
        self.stats
            .transfer_group_calls
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .transfer_target_groups
            .fetch_add(self.target_groups, Ordering::Relaxed);
        self.stats
            .transfer_membership_probes
            .fetch_add(self.membership_probes, Ordering::Relaxed);
        self.stats
            .transfer_source_snapshot_bytes
            .fetch_add(self.source_snapshot_bytes, Ordering::Relaxed);
        self.stats
            .transfer_compaction_copy_bytes
            .fetch_add(self.compaction_copy_bytes, Ordering::Relaxed);
        self.stats
            .transfer_destination_append_bytes
            .fetch_add(self.destination_append_bytes, Ordering::Relaxed);
        self.stats
            .transfer_elapsed_nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

/// Any unwind during a mutation makes this writer unusable for commit.
struct MutationGuard<'a>(&'a AtomicBool);
impl Drop for MutationGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone)]
pub(super) struct Reassignment {
    source: NodeId,
    ids: Vec<u32>,
    excluded: Option<NodeId>,
}

#[derive(Debug, PartialEq, Eq)]
enum TransferResult {
    Applied,
    Superseded,
    Contended,
    Renavigate,
}

struct RoutedTransfer {
    destination: NodeId,
    ids: Vec<u32>,
}
struct PreparedTransfer {
    centroid: Vec<f32>,
    query: Option<QuantizedQuery>,
    ids: Vec<u32>,
    codes: Vec<u8>,
}
enum TransferTarget {
    Contended,
    Missing,
    Ready(PreparedTransfer),
}
struct TransferOutcome {
    result: TransferResult,
    retry_ids: Vec<u32>,
}
impl TransferOutcome {
    fn superseded() -> Self {
        Self {
            result: TransferResult::Superseded,
            retry_ids: Vec::new(),
        }
    }
}

struct WidthSnapshotGuard<'a>(&'a RwLock<Option<Vec<usize>>>);

impl Drop for WidthSnapshotGuard<'_> {
    fn drop(&mut self) {
        *self.0.write() = None;
    }
}

fn push_code(codes: &mut Vec<u8>, code: &[u8]) {
    codes.extend_from_slice(code);
}

/// Posting rows collected by one add worker while the tree is stable.
/// Embeddings remain in the writer's shared vector map, so these buffers hold
/// only the data needed by posting lists.
#[derive(Default)]
struct LeafDeltas {
    by_leaf: HashMap<NodeId, LeafDelta>,
}

#[derive(Default, Debug)]
pub struct PreflightTiming {
    pub total: Duration,
    pub coordinates: Duration,
    pub ids: Duration,
    pub checked_ids: usize,
    pub persisted_probes: usize,
    pub checkpoint_fast_path_ids: usize,
}

#[derive(Debug)]
pub struct BufferedAddTiming {
    pub preflight: PreflightTiming,
    pub initialization: Duration,
    pub collection: Duration,
    pub flush: Duration,
}

/// One clock per operation, including early returns and unwinds.
struct ElapsedNanosGuard<'a> {
    counter: &'a AtomicU64,
    start: Instant,
}
impl Drop for ElapsedNanosGuard<'_> {
    fn drop(&mut self) {
        self.counter
            .fetch_add(self.start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct LeafDelta {
    ids: Vec<u32>,
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
            leaf_claims: Default::default(),
            pending_transfers: Default::default(),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_transfer_after_append: AtomicBool::new(false),
            max_inserted_id: None,
            tombstones: DashSet::new(),
            dirty_nodes,
            dirty_embeddings: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(0),
            navigation_ready: AtomicBool::new(true),
            policy_widths: RwLock::new(None),
            next_node_id: AtomicU32::new(1),
            embeddings: DashMap::new(),
            stats: WriterStats::default(),
            zero_centroid,
            max_persisted_id: None,
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
    pub(super) fn mark_embedding_dirty(&self, id: u32) {
        self.dirty_embeddings.insert(id);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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

    pub(super) fn ensure_healthy(&self) -> Result<(), String> {
        if self.poisoned.load(Ordering::Acquire) {
            Err("writer is poisoned; reopen the last committed checkpoint".into())
        } else {
            Ok(())
        }
    }

    /// Validate the entire batch before storing embeddings or postings. IDs
    /// above the known maximum avoid checkpoint reads; holes below it use
    /// the immutable embedding store as the existence authority.
    fn preflight(&self, vectors: &[(u32, Arc<[f32]>)]) -> Result<PreflightTiming, String> {
        let total = Instant::now();
        self.ensure_healthy()?;
        let start = Instant::now();
        self.validate_coordinates(vectors)?;
        let coordinates = start.elapsed();
        let start = Instant::now();
        let (persisted_probes, checkpoint_fast_path_ids) = self.validate_ids(vectors)?;
        let ids = start.elapsed();
        Ok(PreflightTiming {
            total: total.elapsed(),
            coordinates,
            ids,
            checked_ids: vectors.len(),
            persisted_probes,
            checkpoint_fast_path_ids,
        })
    }

    fn validate_coordinates(&self, vectors: &[(u32, Arc<[f32]>)]) -> Result<(), String> {
        for (id, embedding) in vectors {
            if embedding.len() != self.dim || embedding.iter().any(|x| !x.is_finite()) {
                return Err(format!(
                    "embedding {id} must have {} finite coordinates",
                    self.dim
                ));
            }
        }
        Ok(())
    }

    fn validate_ids(&self, vectors: &[(u32, Arc<[f32]>)]) -> Result<(usize, usize), String> {
        let mut batch = HashSet::with_capacity(vectors.len());
        let mut persisted_probes = 0;
        let mut checkpoint_fast_path_ids = 0;
        for (id, _) in vectors {
            if !batch.insert(*id) || self.embeddings.contains_key(id) {
                return Err(format!(
                    "duplicate vector ID {id}: embeddings are immutable"
                ));
            }
            if self.max_persisted_id.is_some_and(|max| *id <= max) {
                if let Some(reader) = &self.vector_data_reader {
                    persisted_probes += 1;
                    let exists = super::persistence::block_on_for_sync_writer(
                        reader.contains(super::super::persistance::PREFIX_EMBEDDING, *id),
                    )
                    .map_err(|err| format!("checking vector {id}: {err}"))?;
                    if exists {
                        return Err(format!(
                            "duplicate vector ID {id}: embeddings are immutable"
                        ));
                    }
                }
            } else {
                checkpoint_fast_path_ids += 1;
            }
        }
        Ok((persisted_probes, checkpoint_fast_path_ids))
    }

    /// Append one immutable embedding. Balancing and commit are separate
    /// exclusive phases; parallel append workers live inside the batch API.
    pub fn add(&mut self, id: u32, embedding: &[f32]) -> Result<(), String> {
        self.add_batch_buffered(&[(id, Arc::from(embedding))], 1, || {})
            .map(|_| ())
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
            self.embeddings.insert(*id, Arc::clone(embedding));
            self.mark_embedding_dirty(*id);
            self.stats.embeddings_added.fetch_add(1, Ordering::Relaxed);

            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates =
                self.navigate_with_policy(embedding, self.config.write_navigation, &policy);
            self.stats
                .add_navigate_nanos
                .fetch_add(nav_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let &(leaf_id, _) = candidates
                .first()
                .expect("stable add tree has no destination leaf");

            let collect_start = Instant::now();
            let node = self
                .nodes
                .get(&leaf_id)
                .unwrap_or_else(|| panic!("leaf {leaf_id} disappeared during stable add phase"));
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
            push_code(&mut delta.codes, code.as_ref());
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
    ) -> Result<BufferedAddTiming, String> {
        let preflight = self.preflight(vectors)?;
        let _mutation = MutationGuard(&self.poisoned);
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
        drop(_mutation);
        self.max_inserted_id = self
            .max_inserted_id
            .into_iter()
            .chain(vectors.iter().map(|(id, _)| *id))
            .max();
        Ok(BufferedAddTiming {
            preflight,
            initialization,
            collection,
            flush,
        })
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
    // Balance / Scrub
    // =========================================================================

    /// Load a cluster, then split or merge it if its size requires balancing.
    /// Scrub and size check use per-node DashMap access (no global lock).
    fn balance(&self, cluster_id: NodeId, depth: u32) {
        if depth > MAX_BALANCE_DEPTH {
            return;
        }

        self.load_posting_sync(cluster_id);

        let len = match self.nodes.get(&cluster_id) {
            Some(node_ref) => match node_ref.value() {
                TreeNode::Leaf(leaf) => leaf.length,
                _ => return,
            },
            None => return,
        };

        let needs_split = len > self.config.split_threshold;
        let needs_merge =
            cluster_id != self.root_id() && len > 0 && len < self.config.merge_threshold;

        if needs_split || needs_merge {
            if !self.balancing.insert(cluster_id) {
                return;
            }

            if needs_split {
                self.split_leaf(cluster_id, depth);
            } else {
                self.merge_leaf(cluster_id, depth);
            }

            self.balancing.remove(&cluster_id);
        }
    }

    /// Snapshot eligible leaves at the joined round boundary. Each leaf is one
    /// job, so an idle worker can help even when most work shares one subtree.
    /// Claims protect posting ownership and the tree lock protects parent edits;
    /// scheduling does not reserve ancestors. New or newly oversized leaves are
    /// discovered after this round joins.
    fn balance_jobs(&self) -> Vec<NodeId> {
        let root = self.root_id();
        let mut jobs: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|entry| {
                let TreeNode::Leaf(leaf) = entry.value() else {
                    return None;
                };
                let len = leaf.length;
                (len > self.config.split_threshold
                    || (*entry.key() != root && len > 0 && len < self.config.merge_threshold))
                    .then_some((*entry.key(), len))
            })
            .collect();
        // Start large postings first to reduce the tail left by expensive splits.
        jobs.sort_unstable_by_key(|&(id, len)| (std::cmp::Reverse(len), id));
        jobs.into_iter().map(|(id, _)| id).collect()
    }

    /// Balance eligible leaves through a shared queue, joining between rounds.
    pub fn balance_index_parallel(&mut self, num_threads: usize) -> Result<(), String> {
        self.ensure_healthy()?;
        let _mutation = MutationGuard(&self.poisoned);
        self.balance_parallel_inner(num_threads)
    }

    fn balance_parallel_inner(&self, num_threads: usize) -> Result<(), String> {
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
            let mut iteration = BalanceIterationTiming {
                round: round + 1,
                ..Default::default()
            };
            let start = Instant::now();
            let has_work = self.nodes.iter().any(|entry| match entry.value() {
                TreeNode::Leaf(leaf) => {
                    let len = leaf.length;
                    len > self.config.split_threshold
                        || (*entry.key() != self.root_id()
                            && len > 0
                            && len < self.config.merge_threshold)
                }
                _ => false,
            });
            iteration.detection_nanos = start.elapsed().as_nanos() as u64;
            let start = Instant::now();
            let pending = std::mem::take(&mut *self.pending_transfers.lock());
            iteration.pending_before = pending.len();
            iteration.pending_ids_before = pending.iter().map(|work| work.ids.len()).sum();
            iteration.retirements_before =
                pending.iter().filter(|work| work.ids.is_empty()).count();
            let had_pending = !pending.is_empty();
            // Compatible retry groups share one navigation/compaction batch.
            // Empty retirement work still creates an entry and is never lost.
            let mut retry_groups: HashMap<(NodeId, Option<NodeId>), Vec<u32>> = HashMap::new();
            for work in pending {
                retry_groups
                    .entry((work.source, work.excluded))
                    .or_default()
                    .extend(work.ids);
            }
            for ((source, excluded), mut ids) in retry_groups {
                ids.sort_unstable();
                ids.dedup();
                iteration.retry_coalesced_ids += ids.len();
                // Workers have joined. Read each source anew because an earlier
                // retry group may have moved rows. Materialize lazy postings
                // before treating their IDs as the current membership.
                self.load_posting_sync(source);
                if let Some(node) = self.nodes.get(&source) {
                    if let TreeNode::Leaf(leaf) = node.value() {
                        let current: HashSet<u32> = leaf.ids.iter().copied().collect();
                        ids.retain(|id| current.contains(id));
                    } else {
                        ids.clear();
                    }
                } else {
                    ids.clear();
                }
                iteration.retry_current_ids += ids.len();
                // Empty groups still carry retirement work for excluded sources.
                self.reassign_many(source, &ids, excluded);
            }
            iteration.retry_superseded_ids =
                iteration.retry_coalesced_ids - iteration.retry_current_ids;
            {
                let pending = self.pending_transfers.lock();
                iteration.pending_after = pending.len();
                iteration.pending_ids_after = pending.iter().map(|work| work.ids.len()).sum();
                iteration.retirements_after =
                    pending.iter().filter(|work| work.ids.is_empty()).count();
            }
            iteration.retry_nanos = start.elapsed().as_nanos() as u64;
            if !has_work && !had_pending && iteration.pending_after == 0 {
                self.stats.balance_iterations.lock().push(iteration);
                break;
            }

            let setup_start = Instant::now();
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
                let transfer_retries = self.pending_transfers.lock().len();
                iteration.setup_nanos = setup_start.elapsed().as_nanos() as u64;
                self.stats.balance_iterations.lock().push(iteration);
                if transfer_retries > 0 {
                    return Err(format!("balancing incomplete after {MAX_PARALLEL_ROUNDS} rounds: {over} oversized leaves, {under} undersized leaves, {transfer_retries} transfer retries"));
                }
                // Size thresholds guide placement quality. Each completed
                // transfer already preserves exactly one posting, so residual
                // leaf sizes alone do not prevent publishing a valid snapshot.
                eprintln!(
                    "[balance_index_parallel] note: stopped after {MAX_PARALLEL_ROUNDS} rounds with {over} oversized and {under} undersized leaves; all transfer retries are complete"
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

            let jobs = self.balance_jobs();
            // The existing diagnostic field now counts individual leaf jobs.
            iteration.partitions = jobs.len();
            iteration.workers = num_threads.min(jobs.len());
            iteration.setup_nanos = setup_start.elapsed().as_nanos() as u64;
            let next_job = AtomicUsize::new(0);
            let worker_start = Instant::now();
            let worker_times = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..iteration.workers)
                    .map(|_| {
                        let jobs = &jobs;
                        let next_job = &next_job;
                        scope.spawn(move || {
                            let start = Instant::now();
                            while let Some(&leaf_id) =
                                jobs.get(next_job.fetch_add(1, Ordering::Relaxed))
                            {
                                // Another job can retire this leaf or change its
                                // size. balance rechecks both before claiming it.
                                self.balance(leaf_id, 0);
                            }
                            start.elapsed().as_nanos() as u64
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>()
            });
            iteration.worker_wall_nanos = worker_start.elapsed().as_nanos() as u64;
            iteration.worker_sum_nanos = worker_times.iter().sum();
            iteration.worker_max_nanos = worker_times.into_iter().max().unwrap_or(0);
            self.stats.balance_iterations.lock().push(iteration);
        }
        if let Some(pb) = balance_pb {
            pb.finish_and_clear();
        }
        let refresh_start = Instant::now();
        self.refresh_navigation();
        self.stats
            .balance_refresh_nanos
            .fetch_add(refresh_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(())
    }

    // =========================================================================
    // Split (leaf)
    // =========================================================================

    fn split_leaf(&self, leaf_id: NodeId, depth: u32) {
        let t0 = Instant::now();
        let _mutation = MutationGuard(&self.poisoned);
        let code_size = self.code_size();
        let Some(source_claim) = LeafClaim::acquire(self, &[leaf_id]) else {
            return;
        };
        self.load_posting_sync(leaf_id);
        let (old_ids, old_codes, parent_id, old_centroid) =
            match self.nodes.get(&leaf_id).as_deref() {
                Some(TreeNode::Leaf(leaf)) if leaf.length > self.config.split_threshold => (
                    leaf.ids.clone(),
                    leaf.codes.clone(),
                    leaf.parent_id,
                    leaf.centroid.clone(),
                ),
                _ => return,
            };
        self.load_embeddings_sync(&old_ids);
        let embeddings: Vec<EmbeddingPoint> = old_ids
            .iter()
            .map(|&id| {
                let embedding = self
                    .embeddings
                    .get(&id)
                    .unwrap_or_else(|| panic!("missing immutable embedding {id}"));
                (id, 0, embedding.value().clone())
            })
            .collect();

        let kmeans_start = Instant::now();
        let (left_center, left_group, right_center, right_group) =
            utils::split(embeddings, &self.distance_fn);
        self.stats
            .split_kmeans_nanos
            .fetch_add(kmeans_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let left_id = self.alloc_node_id();
        let right_id = self.alloc_node_id();
        let replacement_claim = LeafClaim::acquire(self, &[left_id, right_id])
            .expect("fresh node IDs cannot be claimed");

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
                codes: right_codes,
                length: right_len,
            }),
        );
        self.mark_node_dirty(right_id);

        self.nodes.remove(&leaf_id);
        self.tombstones.insert(leaf_id);
        self.dirty_nodes.remove(&leaf_id);
        self.attach_split_children(leaf_id, parent_id, &[left_id, right_id]);
        drop(replacement_claim);
        drop(source_claim);

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
                );
                self.apply_npa_to_cluster_f32(
                    right_id,
                    &right_group,
                    &old_centroid,
                    &right_center,
                    &mut evaluated,
                    depth,
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
        _depth: u32,
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
        let mut proposals = Vec::new();
        for (id, _, _) in group {
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
                proposals.push(*id);
            }
        }
        self.reassign_many(from_cluster_id, &proposals, None);
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
        _depth: u32,
    ) {
        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        let mut proposals = Vec::new();
        for (id, _, emb) in group {
            if !evaluated.insert(*id) {
                continue;
            }
            n_evaluated += 1;
            let old_dist = self.dist(emb, old_center);
            let new_dist = self.dist(emb, new_center);
            if new_dist > old_dist {
                n_reassigned += 1;
                proposals.push(*id);
            }
        }
        self.reassign_many(from_cluster_id, &proposals, None);
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
        _depth: u32,
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

            to_reassign.push(id);
        }

        // Reassignment can mutate this leaf or trigger another split. Release
        // its map guard before performing any of those writes.
        drop(node_ref);
        let mut n_reassigned = 0;
        n_reassigned += to_reassign.len();
        self.reassign_many(neighbor_id, &to_reassign, None);

        Some((n_total, n_evaluated, n_reassigned))
    }

    fn apply_npa_to_fp_neighbor(
        &self,
        neighbor_id: NodeId,
        old_center: &[f32],
        left_center: &[f32],
        right_center: &[f32],
        evaluated: &mut HashSet<u32>,
        _depth: u32,
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

            to_reassign.push(id);
        }

        drop(node_ref);
        let mut n_reassigned = 0;
        n_reassigned += to_reassign.len();
        self.reassign_many(neighbor_id, &to_reassign, None);

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

            self.load_posting_sync(neighbor_id);
            let Some((n_total, n_evaluated, n_reassigned)) = (if self.config.fp_npa {
                self.apply_npa_to_fp_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                )
            } else {
                self.apply_npa_to_quantized_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
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

    /// Apply a pair proposal through the same engine used by routed batches.
    fn transfer_group(
        &self,
        source: NodeId,
        destination: NodeId,
        ids: &[u32],
        evacuate: bool,
    ) -> TransferResult {
        let _mutation = MutationGuard(&self.poisoned);
        if source == destination {
            return TransferResult::Superseded;
        }
        let Some(_claims) = LeafClaim::acquire(self, &[source, destination]) else {
            return TransferResult::Contended;
        };
        self.transfer_claimed(source, destination, ids, evacuate)
    }

    /// A merge already owns its pair; it must not acquire those claims again.
    fn transfer_claimed(
        &self,
        source: NodeId,
        destination: NodeId,
        ids: &[u32],
        evacuate: bool,
    ) -> TransferResult {
        let routes = [RoutedTransfer {
            destination,
            ids: ids.to_vec(),
        }];
        self.transfer_claimed_many(source, &routes, &[true], evacuate)
            .remove(0)
            .result
    }

    /// Reserve the source and all available targets together. Return only rows
    /// still in the source whose target is contended or missing. The caller
    /// reroutes those rows in a later round.
    fn transfer_routed(
        &self,
        source: NodeId,
        groups: HashMap<NodeId, Vec<u32>>,
        evacuate: bool,
    ) -> Vec<RoutedTransfer> {
        let _mutation = MutationGuard(&self.poisoned);
        let mut routes: Vec<_> = groups
            .into_iter()
            .filter(|(destination, _)| *destination != source)
            .map(|(destination, ids)| RoutedTransfer { destination, ids })
            .collect();
        routes.sort_unstable_by_key(|route| route.destination);
        // One deterministic target per candidate, including targets that cannot
        // be reserved. An accepted row can never also remain queued elsewhere.
        let mut seen = HashSet::new();
        for route in &mut routes {
            route.ids.retain(|id| seen.insert(*id));
        }
        routes.retain(|route| !route.ids.is_empty());
        if routes.is_empty() {
            return Vec::new();
        }
        let destinations: Vec<_> = routes.iter().map(|route| route.destination).collect();
        let Some((_claims, available)) = LeafClaim::acquire_available(self, source, &destinations)
        else {
            return routes;
        };
        let outcomes = self.transfer_claimed_many(source, &routes, &available, evacuate);
        routes
            .into_iter()
            .zip(outcomes)
            .filter_map(|(route, outcome)| {
                (!outcome.retry_ids.is_empty()).then_some(RoutedTransfer {
                    destination: route.destination,
                    ids: outcome.retry_ids,
                })
            })
            .collect()
    }

    /// Every available target and the source remain claimed through the final
    /// source replacement. Prepare all target buffers, reserves, and dirty marks
    /// before appending anywhere; use one node-map guard at a time.
    fn transfer_claimed_many(
        &self,
        source: NodeId,
        routes: &[RoutedTransfer],
        available: &[bool],
        evacuate: bool,
    ) -> Vec<TransferOutcome> {
        let mut observed = TransferObservation {
            stats: &self.stats,
            started: Instant::now(),
            target_groups: routes.len() as u64,
            membership_probes: 0,
            source_snapshot_bytes: 0,
            compaction_copy_bytes: 0,
            destination_append_bytes: 0,
        };
        self.load_posting_sync(source);
        let (source_ids, source_codes, source_centroid) = {
            let Some(node) = self.nodes.get(&source) else {
                return routes
                    .iter()
                    .map(|_| TransferOutcome::superseded())
                    .collect();
            };
            let TreeNode::Leaf(leaf) = node.value() else {
                return routes
                    .iter()
                    .map(|_| TransferOutcome::superseded())
                    .collect();
            };
            (leaf.ids.clone(), leaf.codes.clone(), leaf.centroid.clone())
        };
        observed.source_snapshot_bytes = (source_ids.len() * std::mem::size_of::<u32>()
            + source_codes.len()
            + source_centroid.len() * std::mem::size_of::<f32>())
            as u64;
        let code_size = self.code_size();
        assert_eq!(source_codes.len(), source_ids.len() * code_size);
        let proposed_ids: Vec<_> = routes
            .iter()
            .flat_map(|route| route.ids.iter().copied())
            .collect();
        self.load_embeddings_sync(&proposed_ids);
        let mut proposed = HashMap::with_capacity(proposed_ids.len());
        for (index, route) in routes.iter().enumerate() {
            for &id in &route.ids {
                proposed.entry(id).or_insert(index);
            }
        }
        // FP moves and forced evacuation do not construct quantized queries.
        let source_query = (!self.config.fp_npa && !evacuate).then(|| {
            let norm = Self::vec_norm(&source_centroid);
            QuantizedQuery::new(
                &vec![0.0; self.dim],
                self.padded_bytes(),
                norm,
                norm * norm,
                norm,
            )
        });
        let mut targets: Vec<_> = routes
            .iter()
            .zip(available)
            .map(|(route, &available)| {
                if !available {
                    return TransferTarget::Contended;
                }
                self.load_posting_sync(route.destination);
                let centroid = {
                    let Some(node) = self.nodes.get(&route.destination) else {
                        return TransferTarget::Missing;
                    };
                    let TreeNode::Leaf(leaf) = node.value() else {
                        return TransferTarget::Missing;
                    };
                    leaf.centroid.clone()
                };
                let query = source_query.as_ref().map(|source_query| {
                    let residual: Vec<_> = centroid
                        .iter()
                        .zip(&source_centroid)
                        .map(|(a, b)| a - b)
                        .collect();
                    QuantizedQuery::new(
                        &residual,
                        self.padded_bytes(),
                        source_query.c_norm,
                        f32::dot(&source_centroid, &centroid).unwrap_or(0.0) as f32,
                        Self::vec_norm(&centroid),
                    )
                });
                TransferTarget::Ready(PreparedTransfer {
                    centroid,
                    query,
                    ids: Vec::new(),
                    codes: Vec::new(),
                })
            })
            .collect();
        let mut retries: Vec<Vec<u32>> = routes.iter().map(|_| Vec::new()).collect();
        let mut remaining = Vec::with_capacity(source_ids.len());
        let mut remaining_codes = Vec::with_capacity(source_codes.len());
        let mut accepted_count = 0;
        for (slot, &id) in source_ids.iter().enumerate() {
            observed.membership_probes += 1;
            let code = code_slice(&source_codes, slot, code_size);
            let mut accepted = false;
            if let Some(&target) = proposed.get(&id) {
                match &mut targets[target] {
                    TransferTarget::Contended | TransferTarget::Missing => retries[target].push(id),
                    TransferTarget::Ready(prepared) => {
                        let embedding = self
                            .embeddings
                            .get(&id)
                            .unwrap_or_else(|| panic!("missing immutable embedding {id}"));
                        let eligible = evacuate
                            || if self.config.fp_npa {
                                self.dist(&embedding, &prepared.centroid)
                                    < self.dist(&embedding, &source_centroid)
                            } else {
                                let code = Code::<1, _>::new(code);
                                code.distance_quantized_query(
                                    &self.distance_fn,
                                    prepared.query.as_ref().unwrap(),
                                ) < code.distance_quantized_query(
                                    &self.distance_fn,
                                    source_query.as_ref().unwrap(),
                                )
                            };
                        if eligible {
                            prepared.ids.push(id);
                            push_code(
                                &mut prepared.codes,
                                Code::<1>::quantize(&embedding, &prepared.centroid).as_ref(),
                            );
                            accepted = true;
                            accepted_count += 1;
                        }
                    }
                }
            }
            if !accepted {
                observed.compaction_copy_bytes += (std::mem::size_of::<u32>() + code_size) as u64;
                remaining.push(id);
                remaining_codes.extend_from_slice(code);
            }
        }
        self.stats
            .transfer_source_entries_scanned
            .fetch_add(source_ids.len() as u64, Ordering::Relaxed);
        let outcomes = targets
            .iter()
            .zip(retries)
            .map(|(target, retry_ids)| TransferOutcome {
                result: match target {
                    TransferTarget::Contended if !retry_ids.is_empty() => TransferResult::Contended,
                    TransferTarget::Missing if !retry_ids.is_empty() => TransferResult::Renavigate,
                    TransferTarget::Ready(prepared) if !prepared.ids.is_empty() => {
                        TransferResult::Applied
                    }
                    _ => TransferResult::Superseded,
                },
                retry_ids,
            })
            .collect();
        if accepted_count == 0 {
            return outcomes;
        }
        for (route, target) in routes.iter().zip(&targets) {
            let TransferTarget::Ready(prepared) = target else {
                continue;
            };
            if prepared.ids.is_empty() {
                continue;
            }
            {
                let mut node = self
                    .nodes
                    .get_mut(&route.destination)
                    .expect("claimed destination disappeared");
                let TreeNode::Leaf(leaf) = node.value_mut() else {
                    panic!("claimed destination changed type");
                };
                leaf.ids
                    .try_reserve(prepared.ids.len())
                    .expect("reserving transferred IDs");
                leaf.codes
                    .try_reserve(prepared.codes.len())
                    .expect("reserving transferred codes");
            }
            self.mark_node_dirty(route.destination);
        }
        self.mark_node_dirty(source);
        for (route, target) in routes.iter().zip(&targets) {
            let TransferTarget::Ready(prepared) = target else {
                continue;
            };
            if prepared.ids.is_empty() {
                continue;
            }
            {
                let mut node = self
                    .nodes
                    .get_mut(&route.destination)
                    .expect("claimed destination disappeared");
                let TreeNode::Leaf(leaf) = node.value_mut() else {
                    unreachable!()
                };
                observed.destination_append_bytes +=
                    (prepared.ids.len() * std::mem::size_of::<u32>() + prepared.codes.len()) as u64;
                leaf.ids.extend_from_slice(&prepared.ids);
                leaf.codes.extend_from_slice(&prepared.codes);
                leaf.length = leaf.ids.len();
            }
            #[cfg(test)]
            if self
                .fail_transfer_after_append
                .swap(false, Ordering::Relaxed)
            {
                panic!("injected transfer failure after destination append");
            }
        }
        {
            let mut node = self
                .nodes
                .get_mut(&source)
                .expect("claimed source disappeared");
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                unreachable!()
            };
            leaf.ids = remaining;
            leaf.codes = remaining_codes;
            leaf.length = leaf.ids.len();
        }
        self.stats.transfer_batches.fetch_add(1, Ordering::Relaxed);
        self.stats
            .transferred_vectors
            .fetch_add(accepted_count as u64, Ordering::Relaxed);
        outcomes
    }

    /// Route all candidates first, then compact the source once across targets.
    /// Contention and removed destinations remain explicit work for later rounds.
    fn reassign_many(&self, source: NodeId, ids: &[u32], excluded: Option<NodeId>) {
        if !matches!(self.nodes.get(&source).as_deref(), Some(TreeNode::Leaf(_))) {
            return;
        }
        let start = Instant::now();
        self.stats
            .reassigns
            .fetch_add(ids.len() as u64, Ordering::Relaxed);
        self.load_embeddings_sync(ids);
        let mut groups: HashMap<NodeId, Vec<u32>> = HashMap::new();
        for &id in ids {
            let embedding = self
                .embeddings
                .get(&id)
                .unwrap_or_else(|| panic!("missing immutable embedding {id}"));
            let candidates =
                self.navigate_f32_excluding(&embedding, &self.write_beam_policy(), excluded);
            let Some(&(destination, _)) = candidates.first() else {
                self.pending_transfers.lock().push(Reassignment {
                    source,
                    ids: vec![id],
                    excluded,
                });
                continue;
            };
            if destination != source {
                groups.entry(destination).or_default().push(id);
            }
        }
        for retry in self.transfer_routed(source, groups, excluded == Some(source)) {
            self.stats.transfer_retries.fetch_add(1, Ordering::Relaxed);
            self.pending_transfers.lock().push(Reassignment {
                source,
                ids: retry.ids,
                excluded,
            });
        }
        if excluded == Some(source) && !self.retire_empty_leaf(source) {
            self.pending_transfers.lock().push(Reassignment {
                source,
                ids: Vec::new(),
                excluded,
            });
        }
        self.stats
            .reassign_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    fn reassign(&self, source: NodeId, id: u32, _depth: u32) {
        self.reassign_many(source, &[id], None);
    }
    fn reassign_excluding(&self, source: NodeId, id: u32, _depth: u32, excluded: Option<NodeId>) {
        self.reassign_many(source, &[id], excluded);
    }

    // =========================================================================
    // Split (internal)
    // =========================================================================

    fn split_internal(&self, node_id: NodeId) {
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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

        // Parent changes preserve each child's centroid and its existing code.
        for &child_id in &left_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(left_id));
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }
        for &child_id in &right_children {
            if let Some(mut node_ref) = self.nodes.get_mut(&child_id) {
                node_ref.set_parent_id(Some(right_id));
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

    /// Return false only for claim contention. An empty merge source must be
    /// retired even when no leaf is outside the configured size bounds.
    fn retire_empty_leaf(&self, source: NodeId) -> bool {
        let Some(_claim) = LeafClaim::acquire(self, &[source]) else {
            return false;
        };
        let empty = self
            .nodes
            .get(&source)
            .is_some_and(|node| matches!(node.value(), TreeNode::Leaf(leaf) if leaf.length == 0));
        if empty {
            let lock_wait = Instant::now();
            let _tree = self.tree_lock.lock();
            self.stats
                .tree_mutex_wait_nanos
                .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
            if source != self.root_id() {
                if let Some((_, TreeNode::Leaf(leaf))) = self.nodes.remove(&source) {
                    self.tombstones.insert(source);
                    self.dirty_nodes.remove(&source);
                    self.remove_child_locked(
                        leaf.parent_id.unwrap_or_else(|| self.root_id()),
                        source,
                    );
                }
            }
        }
        true
    }

    fn merge_leaf(&self, leaf_id: NodeId, _depth: u32) {
        let _mutation = MutationGuard(&self.poisoned);
        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        let _elapsed = ElapsedNanosGuard {
            counter: &self.stats.merge_nanos,
            start: Instant::now(),
        };
        let source_centroid = {
            let Some(node) = self.nodes.get(&leaf_id) else {
                return;
            };
            let TreeNode::Leaf(leaf) = node.value() else {
                return;
            };
            if leaf.length >= self.config.merge_threshold {
                return;
            }
            leaf.centroid.clone()
        };
        let candidates =
            self.navigate_f32_excluding(&source_centroid, &self.write_beam_policy(), Some(leaf_id));
        let Some(&(destination, _)) = candidates.first() else {
            return;
        };
        let Some(claims) = LeafClaim::acquire(self, &[leaf_id, destination]) else {
            return;
        };
        self.load_posting_sync(leaf_id);
        let ids = {
            let Some(node) = self.nodes.get(&leaf_id) else {
                return;
            };
            let TreeNode::Leaf(leaf) = node.value() else {
                return;
            };
            if leaf.length >= self.config.merge_threshold {
                return;
            }
            leaf.ids.clone()
        };
        // Move rows that prefer this target while the pair is reserved. Other
        // rows navigate after releasing the pair and can choose other leaves.
        self.transfer_claimed(leaf_id, destination, &ids, false);
        let remaining = {
            let Some(node) = self.nodes.get(&leaf_id) else {
                return;
            };
            let TreeNode::Leaf(leaf) = node.value() else {
                return;
            };
            leaf.ids.clone()
        };
        if remaining.is_empty() {
            let lock_wait = Instant::now();
            let _tree = self.tree_lock.lock();
            self.stats
                .tree_mutex_wait_nanos
                .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let (_, TreeNode::Leaf(leaf)) = self
                .nodes
                .remove(&leaf_id)
                .expect("claimed source disappeared")
            else {
                unreachable!()
            };
            self.tombstones.insert(leaf_id);
            self.dirty_nodes.remove(&leaf_id);
            self.remove_child_locked(leaf.parent_id.unwrap_or_else(|| self.root_id()), leaf_id);
        }
        drop(claims);
        if !remaining.is_empty() {
            self.reassign_many(leaf_id, &remaining, Some(leaf_id));
        }
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
                drop(node_ref);
                self.mark_node_dirty(child_id);
            }
        }

        if children_clone.len() > self.config.branching_factor {
            self.split_internal(parent_id);
        }
    }

    fn remove_child_locked(&self, parent_id: NodeId, child_id: NodeId) {
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        let lock_wait = Instant::now();
        let _guard = self.tree_lock.lock();
        self.stats
            .tree_mutex_wait_nanos
            .fetch_add(lock_wait.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
    fn balance_jobs_include_each_eligible_leaf_once() {
        let (mut writer, a, b) = three_leaves();
        writer.config.split_threshold = 2;
        writer.config.merge_threshold = 2;
        // Empty leaves and internal nodes are not work; the three-row source is.
        assert_eq!(writer.balance_jobs(), vec![0]);
        writer.transfer_group(0, a, &[7], true);
        let jobs = writer.balance_jobs();
        assert_eq!(jobs, vec![a]);
        assert!(!jobs.contains(&b));
        // The source becomes undersized on a subsequent move. A fresh round
        // discovers it without relying on the preceding snapshot.
        writer.transfer_group(0, b, &[8], true);
        let jobs = writer.balance_jobs();
        assert_eq!(jobs.len(), 3);
        assert_eq!(
            jobs.into_iter().collect::<HashSet<_>>(),
            HashSet::from([0, a, b])
        );
    }

    #[test]
    fn next_round_discovers_an_oversized_destination() {
        let (mut writer, a, _) = three_leaves();
        writer.config.split_threshold = 2;
        let before = writer.balance_jobs();
        assert_eq!(before, vec![0]);
        assert_eq!(
            writer.transfer_group(0, a, &[7, 8, 9], true),
            TransferResult::Applied
        );
        assert_eq!(writer.balance_jobs(), vec![a]);
        assert_codes_and_ids(&writer, &[7, 8, 9]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_leaf_jobs_preserve_rows_through_split_merge_and_reopen() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                split_threshold: 16,
                merge_threshold: 0,
                branching_factor: 2,
                fp_npa: true,
                ..Default::default()
            },
        );
        let expected: Vec<u32> = (0..256).collect();
        let points: Vec<_> = expected
            .iter()
            .map(|&id| {
                let emb: Vec<f32> = (0..8)
                    .map(|d| ((id * 31 + d * 17) as f32 * 0.13).sin())
                    .collect();
                (id, Arc::from(emb))
            })
            .collect();
        writer.add_batch_buffered(&points, 8, || {}).unwrap();
        writer.balance_index_parallel(8).unwrap();
        assert!(writer.level_node_counts().len() > 2);
        assert_codes_and_ids(&writer, &expected);
        // Raising the merge threshold and lowering the split threshold creates
        // sibling merge and split jobs while ancestors are promoted or retired.
        writer.config.merge_threshold = 8;
        writer.config.split_threshold = 12;
        for _ in 0..3 {
            writer.balance_index_parallel(8).unwrap();
            assert!(writer.pending_transfers.lock().is_empty());
            assert_codes_and_ids(&writer, &expected);
        }
        let dir = tempfile::tempdir().unwrap();
        let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
            chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                dir.path().to_str().unwrap(),
            )),
            1024 * 1024,
            chroma_cache::new_cache_for_test(),
            chroma_cache::new_cache_for_test(),
            4,
            0,
        );
        let ids = Box::pin(
            Box::pin(writer.commit(&provider, None))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let mut reopened = HierarchicalSpannWriter::open(
            &provider,
            ids,
            DistanceFunction::Euclidean,
            writer.config.clone(),
        )
        .await
        .unwrap();
        // Balancing materializes lazy postings. The joined tree must still have
        // exactly one copy of every input vector with matching quantized codes.
        reopened.balance_index_parallel(8).unwrap();
        let all_leaves: Vec<_> = reopened
            .nodes
            .iter()
            .filter_map(|entry| matches!(entry.value(), TreeNode::Leaf(_)).then_some(*entry.key()))
            .collect();
        for leaf in all_leaves {
            reopened.load_posting_sync(leaf);
        }
        reopened.load_embeddings_sync(&expected);
        assert_codes_and_ids(&reopened, &expected);
    }

    #[test]
    fn validation_rejects_invalid_coordinates_before_mutating_the_batch() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        for embedding in [vec![0.0; 7], vec![f32::NAN; 8], vec![f32::INFINITY; 8]] {
            assert!(writer
                .add_batch_buffered(
                    &[(1, Arc::from([0.0; 8])), (2, Arc::from(embedding))],
                    2,
                    || {}
                )
                .unwrap_err()
                .contains("finite coordinates"));
            assert!(writer.embeddings.is_empty());
            assert_eq!(writer.total_leaf_entries(), 0);
        }
    }

    fn three_leaves() -> (HierarchicalSpannWriter, NodeId, NodeId) {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                split_threshold: 100,
                merge_threshold: 0,
                ..Default::default()
            },
        );
        for (id, coordinate) in [(7, 0.0), (8, 2.0), (9, 10.0)] {
            writer.add(id, &[coordinate; 8]).unwrap();
        }
        {
            let mut node = writer.nodes.get_mut(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                unreachable!()
            };
            leaf.centroid = vec![10.0; 8];
            leaf.centroid_code = writer.quantize_origin(&leaf.centroid);
            leaf.codes = leaf
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&writer.embeddings.get(id).unwrap(), &leaf.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect();
        }
        let a = writer.alloc_node_id();
        let b = writer.alloc_node_id();
        for (id, coordinate) in [(a, 0.0), (b, 2.0)] {
            let mut node = empty_leaf();
            let TreeNode::Leaf(leaf) = &mut node else {
                unreachable!()
            };
            leaf.centroid = vec![coordinate; 8];
            leaf.centroid_code = writer.quantize_origin(&leaf.centroid);
            writer.nodes.insert(id, node);
            writer.mark_node_dirty(id);
        }
        writer.create_root_above(&[0, a, b]);
        (writer, a, b)
    }

    #[test]
    fn routed_transfers_compact_source_once_and_deduplicate_targets() {
        for fp_npa in [true, false] {
            let (mut writer, a, b) = three_leaves();
            writer.config.fp_npa = fp_npa;
            assert!(writer
                .transfer_routed(
                    0,
                    HashMap::from([(a, vec![7, 7, 9]), (b, vec![8, 7])]),
                    false
                )
                .is_empty());
            assert_codes_and_ids(&writer, &[7, 8, 9]);
            let node = writer.nodes.get(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                unreachable!()
            };
            assert_eq!(leaf.ids, [9]);
            assert_eq!(
                writer
                    .stats
                    .transfer_source_entries_scanned
                    .load(Ordering::Relaxed),
                3
            );
            assert_eq!(writer.stats.transfer_batches.load(Ordering::Relaxed), 1);
            assert_eq!(writer.stats.transferred_vectors.load(Ordering::Relaxed), 2);
            let observed = writer.stats.snapshot();
            assert_eq!(observed.transfer_group_calls, 1);
            assert_eq!(observed.transfer_target_groups, 2);
            assert_eq!(observed.transfer_membership_probes, 3);
            assert_eq!(
                observed.transfer_source_snapshot_bytes,
                (3 * (4 + writer.code_size()) + 8 * 4) as u64
            );
            assert_eq!(
                observed.transfer_compaction_copy_bytes,
                (4 + writer.code_size()) as u64
            );
            assert_eq!(
                observed.transfer_destination_append_bytes,
                (2 * (4 + writer.code_size())) as u64
            );
            assert!(observed.transfer_elapsed_nanos > 0);
        }
    }

    #[test]
    fn routed_transfers_defer_only_current_rows_with_unavailable_targets() {
        let (writer, a, b) = three_leaves();
        let claim = LeafClaim::acquire(&writer, &[a]).unwrap();
        let retries = writer.transfer_routed(0, HashMap::from([(a, vec![7]), (b, vec![8])]), false);
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].destination, a);
        assert_eq!(retries[0].ids, [7]);
        assert_codes_and_ids(&writer, &[7, 8, 9]);
        drop(claim);
        assert!(writer
            .transfer_routed(
                0,
                retries
                    .into_iter()
                    .map(|route| (route.destination, route.ids))
                    .collect(),
                false
            )
            .is_empty());
        assert_codes_and_ids(&writer, &[7, 8, 9]);

        let (writer, a, _) = three_leaves();
        let missing = writer.alloc_node_id();
        let retries = writer.transfer_routed(
            0,
            HashMap::from([(a, vec![7]), (missing, vec![8, 7])]),
            false,
        );
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].ids, [8]);
        // Row 7 has moved. A stale missing-target proposal for it is superseded.
        assert!(writer
            .transfer_routed(0, HashMap::from([(missing, vec![7])]), false)
            .is_empty());
        writer.nodes.remove(&0);
        assert!(writer
            .transfer_routed(0, HashMap::from([(missing, vec![8])]), false)
            .is_empty());
    }

    #[test]
    fn concurrent_routed_and_reverse_moves_preserve_exactly_one_copy() {
        for _ in 0..32 {
            let (writer, a, b) = three_leaves();
            let barrier = std::sync::Barrier::new(3);
            std::thread::scope(|scope| {
                let writer = &writer;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    writer.transfer_routed(0, HashMap::from([(a, vec![7]), (b, vec![8])]), false);
                });
                scope.spawn(move || {
                    barrier.wait();
                    writer.transfer_routed(a, HashMap::from([(0, vec![7])]), false);
                });
                barrier.wait();
            });
            writer.transfer_routed(0, HashMap::from([(a, vec![7]), (b, vec![8])]), false);
            assert_codes_and_ids(&writer, &[7, 8, 9]);
        }
    }

    #[test]
    fn retry_drain_coalesces_sources_and_preserves_empty_retirement_work() {
        let (mut writer, _, _) = three_leaves();
        for ids in [vec![7], vec![8], vec![7]] {
            writer.pending_transfers.lock().push(Reassignment {
                source: 0,
                ids,
                excluded: None,
            });
        }
        writer.balance_index_parallel(2).unwrap();
        assert_eq!(
            writer
                .stats
                .transfer_source_entries_scanned
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(writer.stats.transfer_batches.load(Ordering::Relaxed), 1);
        assert_codes_and_ids(&writer, &[7, 8, 9]);

        let (mut writer, a, b) = three_leaves();
        assert!(writer
            .transfer_routed(0, HashMap::from([(a, vec![7, 9]), (b, vec![8])]), true)
            .is_empty());
        for _ in 0..2 {
            writer.pending_transfers.lock().push(Reassignment {
                source: 0,
                ids: Vec::new(),
                excluded: Some(0),
            });
        }
        writer.balance_index_parallel(2).unwrap();
        assert!(writer.tombstones.contains(&0));
        assert!(!writer.nodes.contains_key(&0));
        assert_codes_and_ids(&writer, &[7, 8, 9]);
    }

    #[test]
    fn retry_drain_skips_moved_ids_before_navigation_and_still_retires() {
        let (mut writer, a, b) = three_leaves();
        assert!(writer
            .transfer_routed(0, HashMap::from([(a, vec![7, 9]), (b, vec![8])]), true)
            .is_empty());
        let navigates = writer.stats.navigates.load(Ordering::Relaxed);
        let reassigns = writer.stats.reassigns.load(Ordering::Relaxed);
        // Every queued ID has already moved; the source still needs retirement.
        for ids in [vec![7, 8, 9], vec![7], Vec::new()] {
            writer.pending_transfers.lock().push(Reassignment {
                source: 0,
                ids,
                excluded: Some(0),
            });
        }
        // A missing source and an internal source also supersede queued IDs.
        for source in [writer.alloc_node_id(), writer.root_id()] {
            writer.pending_transfers.lock().push(Reassignment {
                source,
                ids: vec![7],
                excluded: None,
            });
        }
        writer.balance_index_parallel(2).unwrap();
        assert_eq!(writer.stats.navigates.load(Ordering::Relaxed), navigates);
        assert_eq!(writer.stats.reassigns.load(Ordering::Relaxed), reassigns);
        let rounds = writer.stats.balance_iterations.lock();
        assert_eq!(rounds[0].pending_ids_before, 6);
        assert_eq!(rounds[0].retry_coalesced_ids, 5);
        assert_eq!(rounds[0].retry_current_ids, 0);
        assert_eq!(rounds[0].retry_superseded_ids, 5);
        assert_eq!(rounds[0].pending_after, 0);
        assert!(writer.tombstones.contains(&0));
        assert!(!writer.nodes.contains_key(&0));
        assert_codes_and_ids(&writer, &[7, 8, 9]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_drain_materializes_lazy_source_before_filtering() {
        let (mut writer, a, _) = three_leaves();
        let config = writer.config.clone();
        let dir = tempfile::tempdir().unwrap();
        let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
            chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                dir.path().to_str().unwrap(),
            )),
            1024 * 1024,
            chroma_cache::new_cache_for_test(),
            chroma_cache::new_cache_for_test(),
            4,
            0,
        );
        let ids = Box::pin(
            Box::pin(writer.commit(&provider, None))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let mut reopened =
            HierarchicalSpannWriter::open(&provider, ids, DistanceFunction::Euclidean, config)
                .await
                .unwrap();
        assert_eq!(reopened.memory_usage().posting_entries, 0);
        reopened.pending_transfers.lock().push(Reassignment {
            source: 0,
            ids: vec![7, 1234],
            excluded: None,
        });
        reopened.balance_index_parallel(2).unwrap();
        let rounds = reopened.stats.balance_iterations.lock();
        assert_eq!(rounds[0].retry_coalesced_ids, 2);
        assert_eq!(rounds[0].retry_current_ids, 1);
        assert_eq!(rounds[0].retry_superseded_ids, 1);
        assert_eq!(reopened.stats.reassigns.load(Ordering::Relaxed), 1);
        assert_eq!(reopened.stats.navigates.load(Ordering::Relaxed), 1);
        assert!(
            matches!(reopened.nodes.get(&a).as_deref(), Some(TreeNode::Leaf(leaf)) if leaf.ids == [7])
        );
        reopened.load_all_postings().await.unwrap();
        reopened.load_raw(&[7, 8, 9]).await.unwrap();
        assert_codes_and_ids(&reopened, &[7, 8, 9]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_target_preparation_and_partial_append_failures_prevent_commit() {
        for after_append in [false, true] {
            let (mut writer, a, b) = three_leaves();
            if after_append {
                writer
                    .fail_transfer_after_append
                    .store(true, Ordering::Relaxed);
            } else {
                writer.embeddings.remove(&8);
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.transfer_routed(0, HashMap::from([(a, vec![7]), (b, vec![8])]), false)
            }));
            assert!(result.is_err());
            assert!(writer.leaf_claims.lock().is_empty());
            for (id, expected) in [(0, 3), (a, usize::from(after_append)), (b, 0)] {
                let node = writer.nodes.get(&id).unwrap();
                let TreeNode::Leaf(leaf) = node.value() else {
                    unreachable!()
                };
                assert_eq!(leaf.length, expected);
            }
            let dir = tempfile::tempdir().unwrap();
            let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
                chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                    dir.path().to_str().unwrap(),
                )),
                1024 * 1024,
                chroma_cache::new_cache_for_test(),
                chroma_cache::new_cache_for_test(),
                4,
                0,
            );
            assert!(Box::pin(writer.commit(&provider, None))
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("poisoned"));
        }
    }

    fn two_leaves() -> (HierarchicalSpannWriter, NodeId) {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                split_threshold: 4,
                merge_threshold: 0,
                branching_factor: 4,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]).unwrap();
        {
            let mut node = writer.nodes.get_mut(&0).unwrap();
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                unreachable!()
            };
            leaf.centroid = vec![10.0; 8];
            leaf.codes = Code::<1>::quantize(&[0.0; 8], &leaf.centroid)
                .as_ref()
                .to_vec();
        }
        let destination = writer.alloc_node_id();
        writer.nodes.insert(destination, empty_leaf());
        writer.mark_node_dirty(destination);
        writer.create_root_above(&[0, destination]);
        (writer, destination)
    }

    fn assert_codes_and_ids(writer: &HierarchicalSpannWriter, expected: &[u32]) {
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            expected.iter().copied().collect()
        );
        for node in &writer.nodes {
            if let TreeNode::Leaf(leaf) = node.value() {
                assert_eq!(leaf.length, leaf.ids.len());
                for (slot, id) in leaf.ids.iter().enumerate() {
                    let embedding = writer.embeddings.get(id).unwrap();
                    assert_eq!(
                        code_slice(&leaf.codes, slot, writer.code_size()),
                        Code::<1>::quantize(&embedding, &leaf.centroid).as_ref()
                    );
                }
            }
        }
    }

    #[test]
    fn claims_reserve_pairs_atomically_and_release_on_unwind() {
        let (writer, dest) = two_leaves();
        let claim = LeafClaim::acquire(&writer, &[0]).unwrap();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(LeafClaim::acquire(&writer, &[dest, 0]).is_none());
                })
                .join()
                .unwrap();
        });
        assert!(
            LeafClaim::acquire(&writer, &[dest]).is_some(),
            "a failed pair claim reserved its free half"
        );
        drop(claim);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _claims = LeafClaim::acquire(&writer, &[dest, 0]).unwrap();
            panic!("release the claims");
        }));
        assert!(writer.leaf_claims.lock().is_empty());
    }

    #[test]
    fn conflicting_and_reverse_transfers_preserve_exactly_one_copy() {
        for _ in 0..32 {
            let (writer, dest) = two_leaves();
            let barrier = std::sync::Barrier::new(4);
            std::thread::scope(|scope| {
                for (source, target) in [(0, dest), (0, dest), (dest, 0)] {
                    let barrier = &barrier;
                    let writer = &writer;
                    scope.spawn(move || {
                        barrier.wait();
                        writer.transfer_group(source, target, &[7], false);
                    });
                }
                barrier.wait();
            });
            writer.reassign_many(0, &[7], None);
            assert_codes_and_ids(&writer, &[7]);
            assert_eq!(
                match writer.nodes.get(&dest).unwrap().value() {
                    TreeNode::Leaf(leaf) => leaf.ids.clone(),
                    _ => unreachable!(),
                },
                vec![7]
            );
        }
    }

    #[test]
    fn stale_transfer_rechecks_centroids_after_round_trip() {
        let (writer, dest) = two_leaves();
        assert_eq!(
            writer.transfer_group(0, dest, &[7], false),
            TransferResult::Applied
        );
        // The vector returns to its first leaf. The old proposal's source
        // membership is true again, so only current distance validation stops it.
        for (id, center) in [(0, 0.0), (dest, 10.0)] {
            let _claim = LeafClaim::acquire(&writer, &[id]).unwrap();
            let mut node = writer.nodes.get_mut(&id).unwrap();
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                unreachable!()
            };
            leaf.centroid = vec![center; 8];
            leaf.codes = leaf
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&writer.embeddings.get(id).unwrap(), &leaf.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect();
        }
        assert_eq!(
            writer.transfer_group(dest, 0, &[7], false),
            TransferResult::Applied
        );
        assert_eq!(
            writer.transfer_group(0, dest, &[7], false),
            TransferResult::Superseded
        );
        assert_codes_and_ids(&writer, &[7]);
    }

    #[test]
    fn transfers_between_leaves_in_one_map_shard_do_not_deadlock() {
        let (writer, original_dest) = two_leaves();
        let candidates: Vec<_> = (0..1024)
            .map(|_| {
                let id = writer.alloc_node_id();
                writer.nodes.insert(id, empty_leaf());
                id
            })
            .collect();
        let source_guard = writer.nodes.get_mut(&0).unwrap();
        let destination = *candidates
            .iter()
            .find(|id| {
                matches!(
                    writer.nodes.try_get(id),
                    dashmap::try_result::TryResult::Locked
                )
            })
            .unwrap();
        drop(source_guard);
        for id in candidates {
            if id != destination {
                writer.nodes.remove(&id);
            }
        }
        writer.nodes.remove(&original_dest);
        let root = writer.root_id();
        writer.update_children(root, |children| {
            *children = vec![0, destination];
        });
        writer
            .nodes
            .get_mut(&destination)
            .unwrap()
            .set_parent_id(Some(root));
        assert_eq!(
            writer.transfer_group(0, destination, &[7], false),
            TransferResult::Applied
        );
        assert_codes_and_ids(&writer, &[7]);
    }

    #[test]
    fn split_and_merge_race_with_arrivals_without_losing_rows() {
        for splitting in [true, false] {
            for _ in 0..16 {
                let (mut writer, dest) = two_leaves();
                for id in 8..16 {
                    writer.add(id, &[1.0; 8]).unwrap();
                }
                if !splitting {
                    writer.config.merge_threshold = 20;
                    writer.config.split_threshold = 30;
                }
                let barrier = std::sync::Barrier::new(3);
                std::thread::scope(|scope| {
                    let writer = &writer;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        if splitting {
                            writer.split_leaf(dest, 0);
                        } else {
                            writer.merge_leaf(dest, 0);
                        }
                    });
                    scope.spawn(move || {
                        barrier.wait();
                        writer.transfer_group(0, dest, &[7], true);
                    });
                    barrier.wait();
                });
                assert_codes_and_ids(&writer, &(7..16).collect::<Vec<_>>());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retry_queue_runs_for_leaves_within_size_limits() {
        let (mut writer, dest) = two_leaves();
        let claim = LeafClaim::acquire(&writer, &[dest]).unwrap();
        writer.reassign_many(0, &[7], None);
        assert_eq!(writer.pending_transfers.lock().len(), 1);
        drop(claim);
        writer.balance_index_parallel(2).unwrap();
        assert!(writer.pending_transfers.lock().is_empty());
        assert_codes_and_ids(&writer, &[7]);
        let (mut blocked, destination) = two_leaves();
        let claim = LeafClaim::acquire(&blocked, &[destination]).unwrap();
        blocked.reassign_many(0, &[7], None);
        let error = blocked.balance_parallel_inner(2).unwrap_err();
        assert!(error.contains("balancing incomplete"));
        assert!(!blocked.pending_transfers.lock().is_empty());
        drop(claim);
        let dir = tempfile::tempdir().unwrap();
        let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
            chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                dir.path().to_str().unwrap(),
            )),
            1024 * 1024,
            chroma_cache::new_cache_for_test(),
            chroma_cache::new_cache_for_test(),
            4,
            0,
        );
        assert!(Box::pin(blocked.commit(&provider, None))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("pending transfer retries"));
        blocked.balance_index_parallel(2).unwrap();
        assert!(blocked.pending_transfers.lock().is_empty());
        assert_codes_and_ids(&blocked, &[7]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn size_only_residuals_at_the_round_cap_can_commit_and_reopen() {
        for oversized in [true, false] {
            let (mut writer, destination) = two_leaves();
            let held_leaf = if oversized {
                let additions: Vec<_> = (8..14).map(|id| (id, Arc::from([1.0; 8]))).collect();
                writer.add_batch_buffered(&additions, 2, || {}).unwrap();
                destination
            } else {
                writer.config.merge_threshold = 4;
                0
            };
            let expected = if oversized {
                (7..14).collect::<HashSet<_>>()
            } else {
                HashSet::from([7])
            };
            // Keep the heuristic split or merge contended for every round.
            // No transfer has begun and no ownership work is queued.
            let claim = LeafClaim::acquire(&writer, &[held_leaf]).unwrap();
            writer.balance_parallel_inner(2).unwrap();
            assert_eq!(writer.stats.balance_rounds.load(Ordering::Relaxed), 8);
            assert!(writer.pending_transfers.lock().is_empty());
            assert!(writer.navigation_ready.load(Ordering::Acquire));
            assert_eq!(writer.root_reachable_valid_ids().unwrap(), expected);
            drop(claim);

            let dir = tempfile::tempdir().unwrap();
            let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
                chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                    dir.path().to_str().unwrap(),
                )),
                1024 * 1024,
                chroma_cache::new_cache_for_test(),
                chroma_cache::new_cache_for_test(),
                4,
                0,
            );
            let ids = Box::pin(
                Box::pin(writer.commit(&provider, None))
                    .await
                    .unwrap()
                    .flush(),
            )
            .await
            .unwrap();
            let reopened = HierarchicalSpannWriter::open(
                &provider,
                ids,
                DistanceFunction::Euclidean,
                writer.config.clone(),
            )
            .await
            .unwrap();
            reopened.load_all_postings().await.unwrap();
            assert_eq!(reopened.root_reachable_valid_ids().unwrap(), expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_transfer_failure_poisoning_prevents_commit() {
        let (mut writer, dest) = two_leaves();
        writer
            .fail_transfer_after_append
            .store(true, Ordering::Relaxed);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.transfer_group(0, dest, &[7], false)
        }));
        assert!(result.is_err());
        assert!(writer.leaf_claims.lock().is_empty());
        assert!(writer.add(8, &[0.0; 8]).unwrap_err().contains("poisoned"));
        let dir = tempfile::tempdir().unwrap();
        let provider = chroma_blockstore::provider::BlockfileProvider::new_arrow(
            chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
                dir.path().to_str().unwrap(),
            )),
            1024 * 1024,
            chroma_cache::new_cache_for_test(),
            chroma_cache::new_cache_for_test(),
            4,
            0,
        );
        assert!(Box::pin(writer.commit(&provider, None))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("poisoned"));
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
            codes: Vec::new(),
            length: 0,
        })
    }

    #[test]
    fn merge_removed_source_does_not_consume_beam() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                write_beam_min: 1,
                write_beam_max: 1,
                merge_threshold: 4,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]).unwrap();
        let mut leaf = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut leaf {
            leaf.centroid[0] = 2.0;
        }
        writer.nodes.insert(1, leaf);
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        writer.refresh_navigation();
        writer.navigation_ready.store(false, Ordering::Release);
        writer.merge_leaf(0, 0);
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
    }

    #[test]
    fn reassign_removed_source_with_missing_closest_sibling() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                merge_threshold: 0,
                write_beam_min: 1,
                write_beam_max: 1,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]).unwrap();
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
        writer.reassign(0, 7, 0);
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
        writer.reassign(0, 7, 0);
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
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig {
                write_beam_min: 1,
                write_beam_max: 1,
                merge_threshold: 4,
                ..Default::default()
            },
        );
        writer.add(7, &[0.0; 8]).unwrap();
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
        writer.merge_leaf(0, 0);
        assert!(!writer.nodes.contains_key(&0));
        assert_eq!(writer.root_id(), 1);
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([7])
        );
    }

    #[test]
    fn merge_without_target_preserves_postings_and_codes() {
        let mut writer = HierarchicalSpannWriter::new(
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        writer.add(7, &[0.0; 8]).unwrap();
        writer.merge_leaf(0, 0);
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
            writer.add_batch_buffered(&points, 4, || {}).unwrap();
            writer.balance_index_parallel(4).unwrap();
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
        let mut writer = HierarchicalSpannWriter::new(
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
        writer.balance_index_parallel(2).unwrap();
        writer.begin_add_batch();
        assert_eq!(writer.navigate_f32(&query, &policy)[0].0, 2);
        writer.end_add_batch();
    }

    #[test]
    fn add_shares_navigation_while_postings_change() {
        let mut writer = HierarchicalSpannWriter::new(
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
        writer.add(7, &[1.0; 8]).unwrap();
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
        let mut writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, config);
        assert!(writer.cached_level_widths().is_empty());

        let sibling = writer.alloc_node_id();
        writer.nodes.insert(sibling, empty_leaf());
        writer.create_root_above(&[0, sibling]);
        let root = writer.root_id();
        // End the setup phase before observing widths from the new tree.
        writer.balance_index_parallel(1).unwrap();
        assert_eq!(writer.cached_level_widths(), vec![2]);
        assert_eq!(writer.write_beam_policy().level_params(1).beam_min, 1);

        let left = writer.alloc_node_id();
        let right = writer.alloc_node_id();
        writer.nodes.insert(left, empty_leaf());
        writer.nodes.insert(right, empty_leaf());
        writer.replace_child(root, sibling, &[left, right]);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.balance_index_parallel(1).unwrap();
        assert_eq!(writer.cached_level_widths(), vec![3]);
        assert_eq!(writer.write_beam_policy().level_params(1).beam_min, 2);

        writer.remove_child_locked(root, 0);
        assert_eq!(writer.cached_level_widths(), vec![3]);
        writer.balance_index_parallel(1).unwrap();
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.remove_child_locked(root, left);
        assert_eq!(writer.cached_level_widths(), vec![2]);
        writer.balance_index_parallel(1).unwrap();
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

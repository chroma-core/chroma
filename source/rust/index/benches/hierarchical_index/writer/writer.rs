use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_index::spann::utils::{self, EmbeddingPoint};
use dashmap::{DashMap, DashSet};
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::ReentrantMutex;
use simsimd::SpatialSimilarity;

use super::super::common::{
    code_slice, effective_beam, InternalNode, LeafNode, NodeId, ReadBeamPolicy, TreeNode,
};
use super::super::config::{HierarchicalSpannConfig, NavigationMode};
use super::super::instrumentation::WriterStats;
use super::{HierarchicalSpannWriter, DELETED_BIT, MAX_NAV_LEVELS};

const MAX_BALANCE_DEPTH: u32 = 4;
// Both scheduling modes stop rather than spin if split and merge work oscillates.
const MAX_BALANCE_ROUNDS: u32 = 100;

static FOUR_BIT_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

fn navigation_fallback_warning(mode: NavigationMode, warned: &AtomicBool) -> Option<&'static str> {
    if mode == NavigationMode::FourBit && !warned.swap(true, Ordering::Relaxed) {
        Some("Warning: FourBit writer navigation is disabled; using full precision f32 navigation.")
    } else {
        None
    }
}

pub(super) fn warn_navigation_fallback(mode: NavigationMode) {
    if let Some(message) = navigation_fallback_warning(mode, &FOUR_BIT_FALLBACK_WARNED) {
        eprintln!("{message}");
    }
}

#[derive(Debug)]
enum ReassignmentRegistration {
    Retry,
    Obsolete,
    Registered(Vec<NodeId>),
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

impl HierarchicalSpannWriter {
    pub fn new(dim: usize, distance_fn: DistanceFunction, config: HierarchicalSpannConfig) -> Self {
        warn_navigation_fallback(config.write_navigation);
        let zero_centroid = vec![0.0f32; dim];
        let initial_centroid = vec![0.0f32; dim];
        let initial_centroid_code = Code::<1>::quantize(&initial_centroid, &zero_centroid)
            .as_ref()
            .to_vec();
        let nodes = DashMap::new();
        nodes.insert(
            0,
            TreeNode::Leaf(LeafNode {
                centroid: initial_centroid,
                centroid_code: initial_centroid_code,
                ids: Vec::new(),
                versions: Vec::new(),
                codes: Vec::new(),
                parent_id: None,
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
            owner_backend:None,
            vector_base_root:None,
            balancing: DashSet::new(),
            tombstones: DashSet::new(),
            dirty_nodes,
            dirty_versions: DashSet::new(),
            dirty_embeddings: DashSet::new(),
            dirty_deleted_embeddings: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(0),
            next_node_id: AtomicU32::new(1),
            embeddings: DashMap::new(),
            versions: DashMap::new(),
            stats: WriterStats::default(),
            zero_centroid,
            max_persisted_id: None,
            live_owner_hook: Default::default(),
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
            let level_widths: Vec<usize> = self.level_node_counts().into_iter().skip(1).collect();
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

    pub(super) fn alloc_node_id(&self) -> NodeId {
        self.next_node_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    pub(super) fn root_id(&self) -> NodeId {
        self.root_id.load(Ordering::Relaxed)
    }

    /// Insert a raw embedding into the in-memory map (for populating after resume).
    pub fn insert_embedding(&self, id: u32, embedding: Arc<[f32]>) {
        assert!(!self.bounded_vectors(), "bounded owner vectors reject replacement before mutation");
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
        if let Some(hook) = self.live_owner_hook.lock().clone() { hook.add_start(id, embedding); }
        let previous_version = self.current_version_sync(id);
        if self.bounded_vectors(){assert!(previous_version.is_none(),"bounded owner vectors require new IDs");self.owner_backend.as_ref().unwrap().staged_vector(id).expect("durable staging before navigation");}

        // Refuse to resurrect a deleted id. Re-adding after delete is not a
        // supported operation; silently drop. (Lifting this restriction would
        // require a separate "undelete" path that scrubs every leaf for the
        // stale tombstoned entries before bumping the version below the
        // DELETED_BIT.)
        if previous_version.is_some_and(|version| version & DELETED_BIT != 0) {
            return;
        }
        let mut version = if self.bounded_vectors(){1} else if let Some(backend)=&self.owner_backend {backend.bump(id,None).expect("new owner version")} else {
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

        if !self.bounded_vectors(){let emb: Arc<[f32]> = Arc::from(embedding);
        self.embeddings.insert(id, emb);
        self.mark_embedding_dirty(id);}
        self.stats.embeddings_added.fetch_add(1, Ordering::Relaxed);

        loop {
            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates = {
                let _navigation=self.owner_backend.as_ref().map(|b|b.navigation_span());
                self.navigate_with_policy(embedding, self.config.write_navigation, &policy)
            };
            let cluster_ids = self.rng_select(&candidates);
            if let Some(hook) = self.live_owner_hook.lock().clone() { hook.add_navigation(id, version, &candidates, &cluster_ids); }
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
                version = if self.bounded_vectors(){
                    // A failed first target has no membership yet. Advance the
                    // same local version while keeping authority unpublished.
                    assert!(self.owner_backend.as_ref().unwrap().current(id).is_none(),"fresh retry unexpectedly has authority");
                    bump_version(&mut version)
                }else if let Some(backend)=&self.owner_backend {backend.bump(id,None).expect("retry owner version")} else {
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
        assert!(self.owner_backend.is_none(),"owner adapter supports append-only construction");
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
    pub(super) fn register_in_leaf(
        &self,
        leaf_id: NodeId,
        id: u32,
        version: u8,
        embedding: &[f32],
    ) -> bool {
        let t0 = Instant::now();
        if let Some(backend)=&self.owner_backend {
            if let Some(mut node)=self.nodes.get_mut(&leaf_id) {if let TreeNode::Leaf(leaf)=node.value_mut() {if let Some(length)=backend.register(leaf_id,id,version,embedding) {leaf.length=length;drop(node);self.mark_node_dirty(leaf_id);self.stats.registers.fetch_add(1,Ordering::Relaxed);self.stats.register_nanos.fetch_add(t0.elapsed().as_nanos() as u64,Ordering::Relaxed);return true;}}}
            self.stats.register_missing_nodes.fetch_add(1,Ordering::Relaxed);return false;
        }
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
                self.mark_node_dirty(leaf_id);
                if let Some(hook) = self.live_owner_hook.lock().clone() { hook.leaf_registered(leaf_id, id, version, embedding, code.as_ref()); }
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
            drop(root_node);
            self.stats.navigates.fetch_add(1, Ordering::Relaxed);
            self.stats
                .navigate_nanos
                .fetch_add(nav_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return vec![(root, dist)];
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
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let dist = self.dist(query, child.centroid());
                                child_scores.push((child_id, dist));
                            } else {
                                self.stats
                                    .navigate_missing_nodes
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
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

            let sort_start = Instant::now();
            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let effective =
                Self::effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
            child_scores.truncate(effective);
            sort_nanos += sort_start.elapsed().as_nanos() as u64;
            nav_out_per_level[li] = nav_out_per_level[li].saturating_add(child_scores.len() as u64);

            let mut next_internals: Vec<NodeId> = Vec::new();
            for &(node_id, dist) in &child_scores {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    match node_ref.value() {
                        TreeNode::Leaf(_) => leaves.push((node_id, dist)),
                        TreeNode::Internal(_) => next_internals.push(node_id),
                    }
                }
            }

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
        _mode: NavigationMode,
        policy: &ReadBeamPolicy,
    ) -> Vec<(NodeId, f32)> {
        // Every admitted mode uses the same full precision implementation.
        // Unsupported configured modes are reported once when the writer opens.
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
        if self.stats.observe_current_versions.load(Ordering::Relaxed){self.stats.observed_balance_calls.fetch_add(1,Ordering::Relaxed);}
        let _work=self.owner_backend.as_ref().map(|b|b.balance_span());
        if depth > MAX_BALANCE_DEPTH {
            return;
        }

        self.scrub(cluster_id);

        let (len, has_parent) = match self.nodes.get(&cluster_id) {
            Some(node_ref) => match node_ref.value() {
                TreeNode::Leaf(leaf) => (leaf.length, leaf.parent_id.is_some()),
                _ => return,
            },
            None => return,
        };

        let needs_split = len > self.config.split_threshold;
        let needs_merge = has_parent && len > 0 && len < self.config.merge_threshold;

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

    /// Balance all leaves that exceed split_threshold or fall below merge_threshold.
    /// The root leaf cannot merge because it has no sibling. Scheduling excludes it.
    /// Repeats until convergence or the round limit, reporting remaining work on stderr.
    /// Count leaves that still require the existing split/merge balance rules.
    /// A caller uses this after balancing to report nonconvergence without changing it.
    pub fn pending_balance_leaves(&self) -> usize {
        self.nodes.iter().filter(|entry| match entry.value() {
            TreeNode::Leaf(leaf) => leaf.length > self.config.split_threshold
                || (leaf.parent_id.is_some() && leaf.length > 0
                    && leaf.length < self.config.merge_threshold),
            _ => false,
        }).count()
    }

    pub fn balance_index(&self) {
        let mut round = 0u32;
        loop {
            let leaf_ids: Vec<NodeId> = self
                .nodes
                .iter()
                .filter_map(|entry| match entry.value() {
                    TreeNode::Leaf(leaf) => {
                        let len = leaf.length;
                        if len > self.config.split_threshold
                            || (leaf.parent_id.is_some()
                                && len > 0
                                && len < self.config.merge_threshold)
                        {
                            Some(*entry.key())
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect();

            #[cfg(test)]
            super::live_owner_tests::record_balance_round(&leaf_ids);
            if leaf_ids.is_empty() {
                break;
            }
            if round >= MAX_BALANCE_ROUNDS {
                eprintln!("[balance_index] stopped after {MAX_BALANCE_ROUNDS} rounds without full convergence ({} leaves still require balance)",leaf_ids.len());
                break;
            }
            round += 1;

            self.stats
                .balance_rounds
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for leaf_id in leaf_ids {
                self.balance(leaf_id, 0);
            }
        }
    }

    /// Collect all descendant leaf NodeIds under a given subtree root.
    fn collect_leaves_under(&self, node_id: NodeId) -> Vec<NodeId> {
        let mut leaves = Vec::new();
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            match self.nodes.get(&nid) {
                Some(node_ref) => match node_ref.value() {
                    TreeNode::Leaf(_) => leaves.push(nid),
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children.iter().copied());
                    }
                },
                None => {}
            }
        }
        leaves
    }

    /// Count leaves needing balance under a subtree root.
    fn count_work_under(&self, node_id: NodeId) -> usize {
        let mut count = 0;
        let mut stack = vec![node_id];
        while let Some(nid) = stack.pop() {
            match self.nodes.get(&nid) {
                Some(node_ref) => match node_ref.value() {
                    TreeNode::Leaf(leaf) => {
                        let len = leaf.length;
                        if len > self.config.split_threshold
                            || (leaf.parent_id.is_some()
                                && len > 0
                                && len < self.config.merge_threshold)
                        {
                            count += 1;
                        }
                    }
                    TreeNode::Internal(internal) => {
                        stack.extend(internal.children.iter().copied());
                    }
                },
                None => {}
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
                match self.nodes.get(&nid) {
                    Some(node_ref) => match node_ref.value() {
                        TreeNode::Internal(internal) => {
                            all_leaves = false;
                            next_frontier.extend(internal.children.iter().copied());
                        }
                        TreeNode::Leaf(_) => {
                            next_frontier.push(nid);
                        }
                    },
                    None => {}
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

    /// Parallel version of balance_index. Partitions the tree into subtrees and
    /// distributes them across threads, weighted by estimated work.
    pub fn balance_index_parallel(&self, num_threads: usize) {
        if num_threads <= 1 {
            return self.balance_index();
        }

        // Outer-loop cap so a bug or oscillation cannot spin forever.
        let mut round = 0u32;
        let mut balance_pb: Option<ProgressBar> = None;

        loop {
            let has_work = self.nodes.iter().any(|entry| match entry.value() {
                TreeNode::Leaf(leaf) => {
                    let len = leaf.length;
                    len > self.config.split_threshold
                        || (leaf.parent_id.is_some()
                            && len > 0
                            && len < self.config.merge_threshold)
                }
                _ => false,
            });
            if !has_work {
                break;
            }

            if balance_pb.is_none() {
                let pb = ProgressBar::new(MAX_BALANCE_ROUNDS as u64);
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("[Balance] {wide_bar} {pos}/{len} [{elapsed_precise}]")
                        .unwrap(),
                );
                balance_pb = Some(pb);
            }

            round += 1;
            if round > MAX_BALANCE_ROUNDS {
                if let Some(pb) = balance_pb.take() {
                    pb.finish_and_clear();
                }
                let (over, under) = self.nodes.iter().fold((0usize, 0usize), |acc, e| {
                    if let TreeNode::Leaf(leaf) = e.value() {
                        let len = leaf.length;
                        if len > self.config.split_threshold {
                            (acc.0 + 1, acc.1)
                        } else if leaf.parent_id.is_some()
                            && len > 0
                            && len < self.config.merge_threshold
                        {
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
                    MAX_BALANCE_ROUNDS, over, under
                );
                break;
            }
            if let Some(ref pb) = balance_pb {
                pb.inc(1);
            }
            self.stats
                .balance_rounds
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let mut partitions = self.find_partition_roots(num_threads);
            partitions.sort_by(|a, b| b.1.cmp(&a.1));

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

            if let Some(backend)=&self.owner_backend {backend.record_schedule(&thread_subtrees);}
            std::thread::scope(|s| {
                for subtrees in &thread_subtrees {
                    if subtrees.is_empty() {
                        continue;
                    }
                    s.spawn(move || {
                        for &subtree_root in subtrees {
                            let _owner_subtree=self.owner_backend.as_ref().map(|b|b.subtree_span());
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
    }

    #[cfg(test)]
    pub(super) fn scrub_owner_fixture(&self, id:NodeId) { self.scrub(id); }

    /// Scrub stale entries from a leaf. Uses per-leaf DashMap get_mut (no global lock).
    fn scrub(&self, cluster_id: NodeId) {
        let t0 = Instant::now();
        if let Some(backend)=&self.owner_backend {
            // Retain this source-node guard through owner snapshot, validation
            // and ordered removal. Registration takes node access before IDs;
            // owner callbacks never acquire node or per-ID locks.
            if let Some(mut node)=self.nodes.get_mut(&cluster_id) {if let TreeNode::Leaf(leaf)=node.value_mut() {let old=leaf.length;if let Some(length)=backend.scrub(cluster_id,self.code_size()) {leaf.length=length;drop(node);if old!=length {self.mark_node_dirty(cluster_id);}self.stats.scrubs.fetch_add(1,Ordering::Relaxed);self.stats.scrub_removed.fetch_add(old.saturating_sub(length) as u64,Ordering::Relaxed);self.stats.scrub_nanos.fetch_add(t0.elapsed().as_nanos() as u64,Ordering::Relaxed);}}}return;
        }
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

    fn split_leaf(&self, leaf_id: NodeId, depth: u32) {
        let _owner_scope=self.owner_backend.as_ref().map(|b|b.scope(leaf_id));
        let t0 = Instant::now();
        // Keep removed children and their parent replacement in one transaction.
        // Parent splits must never copy a temporarily absent child into a new parent.
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let structure_guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        let code_size = self.code_size();

        if self.bounded_vectors(){
            if let Some(node)=self.nodes.get(&leaf_id){if let TreeNode::Leaf(leaf)=node.value(){assert!(leaf.length<=self.owner_backend.as_ref().unwrap().vector_store().unwrap().limits.split_rows,"bounded split tuple capacity before detach");}}
        }
        let (old_ids, old_versions, old_codes, parent_id, old_centroid) =
            match self.take_reference_node(leaf_id) {
                Some((_, TreeNode::Leaf(leaf))) => (
                    leaf.ids,
                    leaf.versions,
                    leaf.codes,
                    leaf.parent_id,
                    leaf.centroid,
                ),
                Some((_, node)) => {
                    self.install_reference_node(leaf_id, node);
                    self.mark_node_dirty(leaf_id);
                    return;
                }
                None => return,
            };
        self.tombstones.insert(leaf_id);
        self.dirty_nodes.remove(&leaf_id);

        if self.bounded_vectors(){assert!(old_ids.len()<=self.owner_backend.as_ref().unwrap().vector_store().unwrap().limits.split_rows,"bounded split tuple capacity");}else{self.load_embeddings_sync(&old_ids);}
        let embeddings: Vec<EmbeddingPoint> = if let Some(backend)=&self.owner_backend {
            // The source leaf is detached under tree_lock. Owner callbacks only
            // read versions; every ID guard drops before clustering/installation.
            let mut points=Vec::new();
            for (ids,versions) in old_ids.chunks(64).zip(old_versions.chunks(64)) {
                if self.stats.observe_current_versions.load(Ordering::Relaxed){self.stats.current_version_requests.fetch_add(ids.len() as u64,Ordering::Relaxed);}
                let flags=backend.split_input_flags(ids,versions);
                for ((id,version),valid) in ids.iter().zip(versions).zip(flags){
                    if valid {if let Some(e)=self.embedding_owned(*id){points.push((*id,*version as u32,e));}}
                }
            }
            points
        }else{
            old_ids
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
            .collect()
        };

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
            self.install_reference_node(
                leaf_id,
                TreeNode::Leaf(LeafNode {
                    centroid: old_centroid,
                    centroid_code,
                    ids: embeddings.iter().map(|(id, _, _)| *id).collect(),
                    versions: embeddings.iter().map(|(_, ver, _)| *ver as u8).collect(),
                    codes,
                    parent_id,
                    length: len,
                }),
            );
            self.mark_node_dirty(leaf_id);
            self.tombstones.remove(&leaf_id);
            return;
        }

        let kmeans_start = Instant::now();
        #[cfg(test)]
        let (left_center, left_group, right_center, right_group) =
            super::live_owner_tests::reference_split(embeddings, &self.distance_fn);
        #[cfg(not(test))]
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
        self.install_reference_node(
            left_id,
            TreeNode::Leaf(LeafNode {
                centroid: left_centroid,
                centroid_code: left_centroid_code,
                ids: left_group.iter().map(|(id, _, _)| *id).collect(),
                versions: left_group.iter().map(|(_, ver, _)| *ver as u8).collect(),
                codes: left_codes,
                parent_id: None,
                length: left_len,
            }),
        );
        self.mark_node_dirty(left_id);
        self.install_reference_node(
            right_id,
            TreeNode::Leaf(LeafNode {
                centroid: right_centroid,
                centroid_code: right_centroid_code,
                ids: right_group.iter().map(|(id, _, _)| *id).collect(),
                versions: right_group.iter().map(|(_, ver, _)| *ver as u8).collect(),
                codes: right_codes,
                parent_id: None,
                length: right_len,
            }),
        );
        self.mark_node_dirty(right_id);

        if let Some(pid) = parent_id {
            self.replace_child(pid, leaf_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }

        drop(_owner_critical);
        drop(structure_guard);
        let _owner_noncritical=self.owner_backend.as_ref().map(|b|b.noncritical_span());
        if let Some(hook) = self.live_owner_hook.lock().clone() {
            hook.split(leaf_id, left_id, right_id);
        }

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
                self.reassign(from_cluster_id, *id, depth);
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
    ) {
        let owner_predicate=self.owner_backend.as_ref().map(|b|b.prepare_predicate(self.distance_fn.clone(),&[],old_center,new_center,&[]));
        let mut n_evaluated = 0u64;
        let mut n_reassigned = 0u64;
        if let Some(backend)=self.owner_backend.as_ref().filter(|b|b.live_prefix_enabled()) {
            let mut index=0;
            while index<group.len() {
                let end=group.len().min(index+64);
                let predicates=backend.predicate_batch(from_cluster_id,group[index..end].iter().map(|v|Some(v.2.clone())).collect(),owner_predicate.as_ref().unwrap().clone(),true).expect("live prefix predicate policy");
                let rows:Vec<_>=group[index..end].iter().zip(predicates).map(|((id,version,_),predicate)|(*id,*version as u8,predicate)).collect();
                let step=backend.live_npa_prefix(&rows,evaluated);n_evaluated+=step.evaluated as u64;
                // The helper has dropped every ID guard and suffix observation.
                if let Some(slot)=step.action {n_reassigned+=1;self.reassign(from_cluster_id,rows[slot].0,depth);}
                index+=step.consumed;
            }
        } else {
        let mut predicate_batch = None;
        for (index, (id, version, emb)) in group.iter().enumerate() {
            if index % 64 == 0 {
                predicate_batch = self.owner_backend.as_ref().filter(|b|b.predicate_batching_enabled()).and_then(|b| b.predicate_batch(from_cluster_id, group[index..group.len().min(index+64)].iter().map(|v|Some(v.2.clone())).collect(), owner_predicate.as_ref().unwrap().clone(), true));
            }
            let current_ver = self.current_version_sync(*id).unwrap_or(0);
            if *version as u8 != current_ver {
                continue;
            }
            if !evaluated.insert(*id) {
                continue;
            }
            n_evaluated += 1;
            let eligible=if let Some(value)=predicate_batch.as_ref().and_then(|v|v[index%64]) {value} else if let Some(backend)=&self.owner_backend {backend.self_predicate(from_cluster_id,emb.clone(),owner_predicate.as_ref().unwrap().clone())} else {let old_dist = self.dist(emb, old_center);let new_dist = self.dist(emb, new_center);new_dist > old_dist};
            if eligible {
                n_reassigned += 1;
                self.reassign(from_cluster_id, *id, depth);
            }
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
        live_scope: Option<u64>,
    ) -> Option<(usize, usize, usize)> {
        let (n_centroid, n_ids, n_versions, n_codes) = if let Some(backend)=&self.owner_backend {
            let leaf=backend.snapshot(neighbor_id)?;(leaf.centroid,leaf.ids,leaf.versions,leaf.codes)
        } else {
            let Some(node_ref) = self.nodes.get(&neighbor_id) else {
                return None;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                return None;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                leaf.codes.clone(),
            )
        };

        let n_total = n_ids.len();
        let mut n_reassigned = 0usize;
        let mut n_evaluated = 0usize;

        let code_size = self.code_size();
        let eligibility = super::eligibility::QuantizedEligibility::new(
            &n_centroid, old_center, left_center, right_center, self.padded_bytes(),
        );

        let live = self.live_owner_hook.lock().clone();
        let session = live.as_ref().map(|hook| hook.begin(live_scope.expect("live discovery scope"), super::live_owner::Source {
            leaf:neighbor_id, centroid:n_centroid.clone(), ids:n_ids.clone(), versions:n_versions.clone(),
            codes:n_codes.clone(), vectors:vec![None; n_ids.len()], old:old_center.to_vec(), left:left_center.to_vec(), right:right_center.to_vec(), fp:false,
        }));

        for i in 0..n_ids.len() {
            let code_bytes = code_slice(&n_codes, i, code_size);
            let id = n_ids[i];
            let version = n_versions[i];

            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if let (Some(hook), Some(session)) = (&live, session) { hook.version(session, i, id, version, current_ver); }
            if version != current_ver {
                continue;
            }
            if !evaluated.insert(id) {
                continue;
            }

            n_evaluated += 1;
            let predicate = || eligibility.eligible(&self.distance_fn, code_bytes);
            let eligible = if let (Some(hook), Some(session)) = (&live, session) { hook.evaluate(session, i, id, current_ver, predicate) } else { predicate() };
            if !eligible { continue; }

            n_reassigned += 1;
            self.reassign(neighbor_id, id, depth);
        }

        if let (Some(hook), Some(session)) = (&live, session) { hook.end(session); }
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
        live_scope: Option<u64>,
    ) -> Option<(usize, usize, usize)> {
        let live = self.live_owner_hook.lock().clone();
        let (n_centroid, n_ids, n_versions, n_codes) = if let Some(backend)=&self.owner_backend {
            let leaf=backend.snapshot(neighbor_id)?;(leaf.centroid,leaf.ids,leaf.versions,leaf.codes)
        } else {
            let Some(node_ref) = self.nodes.get(&neighbor_id) else {
                return None;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                return None;
            };
            (
                leaf.centroid.clone(),
                leaf.ids.clone(),
                leaf.versions.clone(),
                if live.is_some() { leaf.codes.clone() } else { Vec::new() },
            )
        };

        let owner_predicate=self.owner_backend.as_ref().map(|b|b.prepare_predicate(self.distance_fn.clone(),&n_centroid,old_center,left_center,right_center));
        let n_total = n_ids.len();
        let mut n_reassigned = 0usize;
        let mut n_evaluated = 0usize;

        if !self.bounded_vectors(){self.load_embeddings_sync(&n_ids);}
        let mut n_embeddings: Vec<_> = n_ids
            .iter()
            .map(|id| if self.bounded_vectors(){None}else{self.embeddings.get(id).map(|e| e.value().clone())})
            .collect();


        let session = live.as_ref().map(|hook| hook.begin(live_scope.expect("live discovery scope"), super::live_owner::Source {
            leaf:neighbor_id, centroid:n_centroid.clone(), ids:n_ids.clone(), versions:n_versions.clone(),
            codes:n_codes.clone(), vectors:n_embeddings.iter().map(|v|v.as_ref().map(|v|v.to_vec())).collect(), old:old_center.to_vec(), left:left_center.to_vec(), right:right_center.to_vec(), fp:true,
        }));

        if let Some(backend)=self.owner_backend.as_ref().filter(|b|b.live_prefix_enabled() && live.is_none()) {
            let mut index=0;
            while index<n_ids.len() {
                let end=n_ids.len().min(index+64);
                if self.bounded_vectors(){for value in &mut n_embeddings{*value=None;}let fetched=self.owner_backend.as_ref().unwrap().fetch_vectors(&n_ids[index..end]).expect("immutable NPA vector fetch");for (slot,v) in n_embeddings[index..end].iter_mut().zip(fetched){*slot=Some(v);}}
                let predicates=backend.predicate_batch(neighbor_id,n_embeddings[index..end].to_vec(),owner_predicate.as_ref().unwrap().clone(),false).expect("live prefix predicate policy");
                let rows:Vec<_>=(index..end).zip(predicates).map(|(i,predicate)|(n_ids[i],n_versions[i],predicate)).collect();
                let step=backend.live_npa_prefix(&rows,evaluated);n_evaluated+=step.evaluated;
                if let Some(slot)=step.action {n_reassigned+=1;self.reassign(neighbor_id,rows[slot].0,depth);}
                index+=step.consumed;
            }
        } else {
        let mut predicate_batch = None;
        for i in 0..n_ids.len() {
            if i % 64 == 0 {
                if self.bounded_vectors(){for value in &mut n_embeddings{*value=None;}let end=n_ids.len().min(i+64);let fetched=self.owner_backend.as_ref().unwrap().fetch_vectors(&n_ids[i..end]).expect("immutable NPA vector fetch");for (slot,v) in n_embeddings[i..end].iter_mut().zip(fetched){*slot=Some(v);}}
                predicate_batch = self.owner_backend.as_ref().filter(|b|b.predicate_batching_enabled()).and_then(|b| b.predicate_batch(neighbor_id, n_embeddings[i..n_ids.len().min(i+64)].to_vec(), owner_predicate.as_ref().unwrap().clone(), false));
            }
            let id = n_ids[i];
            let version = n_versions[i];

            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if let (Some(hook), Some(session)) = (&live, session) { hook.version(session, i, id, version, current_ver); }
            if version != current_ver {
                continue;
            }
            if !evaluated.insert(id) {
                continue;
            }

            let Some(emb) = n_embeddings[i].as_deref() else {
                continue;
            };
            n_evaluated += 1;

            let predicate = || if let Some(value)=predicate_batch.as_ref().and_then(|v|v[i%64]) {value} else if let Some(backend)=&self.owner_backend {backend.fp_predicate(neighbor_id,n_embeddings[i].as_ref().unwrap().clone(),owner_predicate.as_ref().unwrap().clone())} else {super::eligibility::fp_eligible(
                &self.distance_fn, emb, &n_centroid, old_center, left_center, right_center,
            )};
            let eligible = if let (Some(hook), Some(session)) = (&live, session) { hook.evaluate(session, i, id, current_ver, predicate) } else { predicate() };
            if !eligible { continue; }

            n_reassigned += 1;
            self.reassign(neighbor_id, id, depth);
        }

        }
        if let (Some(hook), Some(session)) = (&live, session) { hook.end(session); }
        Some((n_total, n_evaluated, n_reassigned))
    }

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
        let live = self.live_owner_hook.lock().clone();
        let live_scope = live.as_ref().map(|hook|hook.discovery(old_leaf_id,left_id,right_id,&neighbors));

        let mut neighbors_visited = 0u64;
        let mut neighbors_active = 0u64;
        let mut total_evaluated = 0u64;
        let mut total_reassigned = 0u64;

        for &(neighbor_id, _) in neighbors.iter().take(self.config.reassign_neighbor_count) {
            if neighbor_id == old_leaf_id || neighbor_id == left_id || neighbor_id == right_id {
                continue;
            }

            if let (Some(hook), Some(scope)) = (&live, live_scope) { hook.before_scrub(self, scope, neighbor_id); }
            self.scrub(neighbor_id);
            let Some((n_total, n_evaluated, n_reassigned)) = (if self.config.fp_npa {
                self.apply_npa_to_fp_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                    live_scope,
                )
            } else {
                self.apply_npa_to_quantized_neighbor(
                    neighbor_id,
                    old_center,
                    left_center,
                    right_center,
                    evaluated,
                    depth,
                    live_scope,
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

        if let (Some(hook), Some(scope)) = (&live, live_scope) { hook.finish_scope(scope); }
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
    pub(super) fn reassign(&self, from_cluster_id: NodeId, id: u32, depth: u32) {
        let t0 = Instant::now();

        let current_ver = self.current_version_sync(id).unwrap_or(0);
        if !self.is_valid(id, current_ver) {
            return;
        }

        if !self.bounded_vectors(){self.load_embeddings_sync(&[id]);}
        let Some(embedding) = self.embedding_owned(id) else {
            return;
        };

        loop {
            let nav_start = Instant::now();
            let policy = self.write_beam_policy();
            let candidates =
                self.navigate_with_policy(&embedding, self.config.write_navigation, &policy);
            let cluster_ids = self.rng_select(&candidates);
            if let Some(hook) = self.live_owner_hook.lock().clone() { hook.navigation(id, from_cluster_id, current_ver, &candidates, &cluster_ids); }
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
            let clusters_to_balance =
                match self.register_reassignment(id, current_ver, &cluster_ids, &embedding) {
                    ReassignmentRegistration::Retry => {
                        self.stats.add_missing_nodes.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    ReassignmentRegistration::Obsolete => return,
                    ReassignmentRegistration::Registered(clusters) => clusters,
                };
            self.stats
                .reassign_register_nanos
                .fetch_add(reg_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let balance_start = Instant::now();
            for cluster_id in clusters_to_balance {
                self.balance(cluster_id, depth + 1);
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

    /// Publish a replacement version only when its target leaves cannot disappear.
    /// Node locks are never held together with the mutable version-entry guard.
    fn register_reassignment(
        &self,
        id: u32,
        expected: u8,
        clusters: &[NodeId],
        embedding: &[f32],
    ) -> ReassignmentRegistration {
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let _structure_guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        let mut seen = HashSet::new();
        let available: Vec<_> = clusters
            .iter()
            .copied()
            .filter(|cluster| {
                seen.insert(*cluster)
                    && self
                        .nodes
                        .get(cluster)
                        .is_some_and(|node| matches!(node.value(), TreeNode::Leaf(_)))
            })
            .collect();
        if available.is_empty() {
            return ReassignmentRegistration::Retry;
        }
        let version = if let Some(backend)=&self.owner_backend {let Some(v)=backend.bump(id,Some(expected)) else{return ReassignmentRegistration::Obsolete;};v} else {
            let mut current = self.versions.entry(id).or_insert(expected);
            if *current != expected || *current & DELETED_BIT != 0 {
                return ReassignmentRegistration::Obsolete;
            }
            bump_version(&mut current)
        };
        self.mark_version_dirty(id);
        for &cluster in &available {
            assert!(
                self.register_in_leaf(cluster, id, version, embedding),
                "reassignment target disappeared while the tree lock was held"
            );
        }
        if let Some(hook) = self.live_owner_hook.lock().clone() { hook.registered(self, id, &available); }
        ReassignmentRegistration::Registered(available)
    }

    fn is_valid(&self, id: u32, version: u8) -> bool {
        self.current_version_sync(id).is_some_and(|current| {
            // Tombstoned ids are never valid, even if the low 7 bits match.
            current & DELETED_BIT == 0 && (current & VERSION_MASK) == (version & VERSION_MASK)
        })
    }

    // =========================================================================
    // Split (internal)
    // =========================================================================

    fn split_internal(&self, node_id: NodeId) {
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let _structure_guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        if !self.balancing.insert(node_id) {
            return;
        }

        let (children, parent_id, _old_centroid) = match self.take_reference_node(node_id) {
            Some((_, TreeNode::Internal(internal))) => {
                if internal.children.len() <= self.config.branching_factor {
                    self.install_reference_node(node_id, TreeNode::Internal(internal));
                    self.balancing.remove(&node_id);
                    return;
                }
                (internal.children, internal.parent_id, internal.centroid)
            }
            Some((_, node)) => {
                self.install_reference_node(node_id, node);
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

        #[cfg(test)]
        let (left_center, left_group, right_center, right_group) = super::live_owner_tests::reference_split(child_embeddings,&self.distance_fn);
        #[cfg(not(test))]
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
        self.install_reference_node(
            left_id,
            TreeNode::Internal(InternalNode {
                centroid: left_centroid.clone(),
                centroid_code: left_centroid_code,
                children: left_children.clone(),
                parent_id: None,
            }),
        );
        self.mark_node_dirty(left_id);
        self.install_reference_node(
            right_id,
            TreeNode::Internal(InternalNode {
                centroid: right_centroid.clone(),
                centroid_code: right_centroid_code,
                children: right_children.clone(),
                parent_id: None,
            }),
        );
        self.mark_node_dirty(right_id);

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

        if let Some(pid) = parent_id {
            self.replace_child(pid, node_id, &[left_id, right_id]);
        } else {
            self.create_root_above(&[left_id, right_id]);
        }
    }

    // =========================================================================
    // Merge
    // =========================================================================

    fn merge_leaf(&self, leaf_id: NodeId, depth: u32) {
        let _owner_scope=self.owner_backend.as_ref().map(|b|b.scope(leaf_id));
        if depth > MAX_BALANCE_DEPTH {
            return;
        }
        let t0 = Instant::now();
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let structure_guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));

        let (source_centroid, source_ids, source_versions, source_codes, parent_id) =
            match self.take_reference_node(leaf_id) {
                Some((_, TreeNode::Leaf(leaf))) => {
                    self.tombstones.insert(leaf_id);
                    self.dirty_nodes.remove(&leaf_id);
                    (
                        leaf.centroid,
                        leaf.ids,
                        leaf.versions,
                        leaf.codes,
                        leaf.parent_id,
                    )
                }
                Some((_, node)) => {
                    self.install_reference_node(leaf_id, node);
                    self.mark_node_dirty(leaf_id);
                    return;
                }
                None => return,
            };

        let policy = self.write_beam_policy();
        let candidates =
            self.navigate_with_policy(&source_centroid, self.config.write_navigation, &policy);
        let target_id = match candidates.iter().find(|&&(nid, _)| nid != leaf_id) {
            Some(&(nid, _)) => nid,
            None => {
                // No merge target found, re-insert the leaf
                let len = source_ids.len();
                let centroid_code = self.quantize_origin(&source_centroid);
                self.install_reference_node(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code,
                        ids: source_ids,
                        versions: source_versions,
                        codes: source_codes,
                        parent_id,
                        length: len,
                    }),
                );
                self.mark_node_dirty(leaf_id);
                self.tombstones.remove(&leaf_id);
                return;
            }
        };

        let target_centroid = match self.nodes.get(&target_id) {
            Some(n) => n.centroid().to_vec(),
            None => {
                // Target gone, re-insert the leaf
                let len = source_ids.len();
                let centroid_code = self.quantize_origin(&source_centroid);
                self.install_reference_node(
                    leaf_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: source_centroid,
                        centroid_code,
                        ids: source_ids,
                        versions: source_versions,
                        codes: source_codes,
                        parent_id,
                        length: len,
                    }),
                );
                self.mark_node_dirty(leaf_id);
                self.tombstones.remove(&leaf_id);
                return;
            }
        };
        if let Some(pid) = parent_id {
            self.remove_child_locked(pid, leaf_id);
        }

        drop(_owner_critical);
        drop(structure_guard);
        let _owner_noncritical=self.owner_backend.as_ref().map(|b|b.noncritical_span());

        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        self.stats
            .merge_nanos
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

        if !self.bounded_vectors(){self.load_embeddings_sync(&source_ids);}
        for (&id, &version) in source_ids.iter().zip(source_versions.iter()) {
            let current_ver = self.current_version_sync(id).unwrap_or(0);
            if version != current_ver {
                continue;
            }
            let Some(embedding) = self.embedding_owned(id) else {
                continue;
            };

            let dist_to_target = self.dist(&embedding, &target_centroid);
            let dist_to_source = self.dist(&embedding, &source_centroid);

            if dist_to_target <= dist_to_source {
                if !self.register_in_leaf(target_id, id, version, &embedding) {
                    self.stats
                        .register_missing_nodes
                        .fetch_add(1, Ordering::Relaxed);
                    self.reassign(leaf_id, id, depth);
                }
            } else {
                self.reassign(leaf_id, id, depth);
            }
        }

        self.balance(target_id, depth + 1);
    }

    // =========================================================================
    // Tree structure helpers
    // =========================================================================

    /// When a parent node has been removed by a concurrent split_internal,
    /// navigate from root to find the correct internal node and insert orphans.
    fn adopt_orphans(&self, orphan_ids: &[NodeId]) {
        if let Some(backend)=&self.owner_backend {backend.record_orphans(orphan_ids.len());}
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
            loop {
                match self.nodes.get(&current) {
                    Some(node_ref) => match node_ref.value() {
                        TreeNode::Internal(internal) => {
                            let children = internal.children.clone();
                            drop(node_ref);

                            // Check if this level's children match the orphan type.
                            // If orphan is a leaf, we want an internal node whose children are leaves.
                            // If orphan is internal, we want one level higher.
                            let child_is_leaf = children.iter().any(|&c| {
                                self.nodes
                                    .get(&c)
                                    .map_or(false, |n| matches!(n.value(), TreeNode::Leaf(_)))
                            });

                            if (is_leaf && child_is_leaf) || (is_leaf && children.is_empty()) {
                                // Insert orphan here
                                if let Some(mut node_ref) = self.nodes.get_mut(&current) {
                                    if let TreeNode::Internal(parent) = node_ref.value_mut() {
                                        if !parent.children.contains(&orphan_id) {
                                            parent.children.push(orphan_id);
                                        }
                                    }
                                }
                                self.mark_node_dirty(current);
                                if let Some(mut node_ref) = self.nodes.get_mut(&orphan_id) {
                                    node_ref.set_parent_id(Some(current));
                                }
                                self.mark_node_dirty(orphan_id);
                                break;
                            }
                            if !is_leaf && !child_is_leaf {
                                if let Some(mut node_ref) = self.nodes.get_mut(&current) {
                                    if let TreeNode::Internal(parent) = node_ref.value_mut() {
                                        if !parent.children.contains(&orphan_id) {
                                            parent.children.push(orphan_id);
                                        }
                                    }
                                }
                                self.mark_node_dirty(current);
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
                        TreeNode::Leaf(_) => {
                            // Root is a leaf; create a new root above both.
                            self.create_root_above(&[current, orphan_id]);
                            break;
                        }
                    },
                    None => break,
                }
            }
        }
    }

    pub(super) fn replace_child(&self, parent_id: NodeId, old_child: NodeId, new_children: &[NodeId]) {
        if let Some(backend)=&self.owner_backend {backend.record_parent_edit();}
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let _guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        let children_clone = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else {
                // eprintln!("WARN: replace_child: parent {} gone, adopting orphans", parent_id);
                self.adopt_orphans(new_children);
                return;
            };
            let TreeNode::Internal(parent) = node_ref.value_mut() else {
                return;
            };
            parent.children.retain(|&c| c != old_child);
            parent.children.extend_from_slice(new_children);
            parent.children.clone()
        };
        self.mark_node_dirty(parent_id);

        let new_centroid = self.compute_centroid_of(&children_clone);
        let new_centroid_code = self.quantize_origin(&new_centroid);
        if let Some(mut node_ref) = self.nodes.get_mut(&parent_id) {
            if let TreeNode::Internal(parent) = node_ref.value_mut() {
                parent.centroid = new_centroid;
                parent.centroid_code = new_centroid_code;
            }
        }

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
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let _guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        let (children_clone, grandparent_id) = {
            let Some(mut node_ref) = self.nodes.get_mut(&parent_id) else {
                return;
            };
            let TreeNode::Internal(parent) = node_ref.value_mut() else {
                return;
            };
            parent.children.retain(|&c| c != child_id);
            (parent.children.clone(), parent.parent_id)
        };
        self.mark_node_dirty(parent_id);

        if children_clone.is_empty() {
            self.take_reference_node(parent_id);
            self.tombstones.insert(parent_id);
            self.dirty_nodes.remove(&parent_id);
            if parent_id == self.root_id() {
                let new_root = self.alloc_node_id();
                let centroid = vec![0.0f32; self.dim];
                let centroid_code = self.quantize_origin(&centroid);
                self.install_reference_node(
                    new_root,
                    TreeNode::Leaf(LeafNode {
                        centroid,
                        centroid_code,
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id: None,
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
            self.take_reference_node(parent_id);
            self.tombstones.insert(parent_id);
            self.dirty_nodes.remove(&parent_id);
            if let Some(mut node_ref) = self.nodes.get_mut(&only_child) {
                node_ref.set_parent_id(None);
            }
            self.mark_node_dirty(only_child);
            self.root_id.store(only_child, Ordering::Relaxed);
        } else {
            let new_centroid = self.compute_centroid_of(&children_clone);
            let new_centroid_code = self.quantize_origin(&new_centroid);
            if let Some(mut node_ref) = self.nodes.get_mut(&parent_id) {
                if let TreeNode::Internal(parent) = node_ref.value_mut() {
                    parent.centroid = new_centroid;
                    parent.centroid_code = new_centroid_code;
                }
            }
        }
    }

    fn create_root_above(&self, children: &[NodeId]) {
        let owner_lock_wait=self.owner_backend.as_ref().map(|_|Instant::now());
        let _guard = self.tree_lock.lock();
        let _owner_critical=self.owner_backend.as_ref().map(|b|b.critical_span(owner_lock_wait.unwrap()));
        let root_id = self.alloc_node_id();
        let centroid = self.compute_centroid_of(children);
        let centroid_code = self.quantize_origin(&centroid);

        self.install_reference_node(
            root_id,
            TreeNode::Internal(InternalNode {
                centroid: centroid.clone(),
                centroid_code,
                children: children.to_vec(),
                parent_id: None,
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
mod checkpoint_balance_tests {
    use super::*;
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    fn provider(path: &std::path::Path) -> BlockfileProvider {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(path.to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            16,
        ))
    }
    #[test]
    fn navigation_modes_keep_identical_fallback_results_and_bounded_warning() {
        let warned = AtomicBool::new(false);
        for _ in 0..10 {
            assert!(navigation_fallback_warning(NavigationMode::Fp, &warned).is_none());
        }
        assert!(!warned.load(Ordering::Relaxed));
        assert!(navigation_fallback_warning(NavigationMode::FourBit, &warned).is_some());
        for _ in 0..10 {
            assert!(navigation_fallback_warning(NavigationMode::FourBit, &warned).is_none());
        }
        let config = HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 2,
            branching_factor: 4,
            ..HierarchicalSpannConfig::default()
        };
        let writer = HierarchicalSpannWriter::new(2, DistanceFunction::Euclidean, config);
        for id in 0..16 {
            writer.add(id, &[id as f32 + 1.0, id as f32 * 0.5]);
        }
        writer.balance_index();
        assert!(writer.nodes.len() > 1);
        let policy = ReadBeamPolicy::uniform(None, 1, 16);
        let exact = writer.navigate_f32(&[2.0, 1.0], &policy);
        for mode in [NavigationMode::Fp, NavigationMode::FourBit] {
            assert_eq!(
                writer.navigate_with_policy(&[2.0, 1.0], mode, &policy),
                exact
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reassignment_advances_the_persisted_version_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let config = HierarchicalSpannConfig::default();
        let writer = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        for _ in 0..3 {
            writer.add(0, &[1.0, 2.0, 0.0, 0.0]);
        }
        assert_eq!(writer.current_version_sync(0), Some(3));
        let ids = writer
            .commit(&p, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let reopened = HierarchicalSpannWriter::open(
            &p,
            ids.clone(),
            DistanceFunction::Euclidean,
            config.clone(),
        )
        .await
        .unwrap();
        assert!(reopened.versions.is_empty());
        assert_eq!(reopened.current_version_sync(0), Some(3));
        assert!(reopened.versions.is_empty());
        // A merge removes the source before asking reassignment to find a target.
        reopened.reassign(u32::MAX, 0, 0);
        assert_eq!(reopened.current_version_sync(0), Some(4));
        reopened.load_all_postings().await.unwrap();
        let root = reopened.nodes.get(&reopened.root_id()).unwrap();
        let TreeNode::Leaf(leaf) = root.value() else {
            panic!("expected root leaf")
        };
        assert_eq!(leaf.ids, vec![0]);
        assert_eq!(leaf.versions, vec![4]);
        drop(root);
        let ids = reopened
            .commit(&p, Some(&ids))
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let again = HierarchicalSpannWriter::open(&p, ids, DistanceFunction::Euclidean, config)
            .await
            .unwrap();
        assert_eq!(again.current_version_sync(0), Some(4));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_merge_restores_posting_codes_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let config = HierarchicalSpannConfig::default();
        let writer = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        writer.add(7, &[1.0, 2.0, 0.0, 0.0]);
        let leaf_id = writer.root_id();
        let before = {
            let node = writer.nodes.get(&leaf_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected leaf")
            };
            leaf.codes.clone()
        };
        // The only lower leaf has no alternate merge target. Its data must survive.
        writer.create_root_above(&[leaf_id]);
        writer.merge_leaf(leaf_id, 0);
        {
            let node = writer.nodes.get(&leaf_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected restored leaf")
            };
            assert_eq!(leaf.ids, vec![7]);
            assert_eq!(leaf.codes, before);
            assert_eq!(leaf.codes.len(), writer.code_size());
        }
        let ids = writer
            .commit(&p, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let reader = super::super::super::reader::HierarchicalSpannReader::open(
            &p,
            ids,
            DistanceFunction::Euclidean,
            config,
        )
        .await
        .unwrap();
        let (hits, _, _, _) = reader
            .search(&[1.0, 2.0, 0.0, 0.0], 1, 2.0, 10, 256, 2)
            .await
            .unwrap();
        assert_eq!(hits, vec![(7, 0.0)]);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "read-only diagnostic; set HIERARCHICAL_BALANCE_CHECKPOINT to an archived checkpoint"]
    async fn diagnose_archived_balance_topology_and_coverage() {
        let dir = std::path::PathBuf::from(
            std::env::var("HIERARCHICAL_BALANCE_CHECKPOINT")
                .expect("checkpoint directory is required"),
        );
        let manifest: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(dir.join("manifest.json")).unwrap())
                .unwrap();
        let b = &manifest["block_ids"];
        let ids = super::super::HierarchicalSpannIds {
            posting_list_id: b["posting_list_id"].as_str().unwrap().parse().unwrap(),
            scalar_metadata_id: b["scalar_metadata_id"].as_str().unwrap().parse().unwrap(),
            vector_data_id: b["vector_data_id"].as_str().unwrap().parse().unwrap(),
            leaf_node_id: b["leaf_node_id"].as_str().unwrap().parse().unwrap(),
            internal_node_id: b["internal_node_id"].as_str().unwrap().parse().unwrap(),
        };
        let c = &manifest["construction"];
        let config = HierarchicalSpannConfig {
            branching_factor: c["branching_factor"].as_u64().unwrap() as usize,
            split_threshold: c["split_threshold"].as_u64().unwrap() as usize,
            merge_threshold: c["merge_threshold"].as_u64().unwrap() as usize,
            ..Default::default()
        };
        let p = provider(&dir.join("data"));
        let writer = HierarchicalSpannWriter::open(&p, ids, DistanceFunction::Euclidean, config)
            .await
            .unwrap();
        let mut reachable = HashSet::new();
        let mut stack = vec![writer.root_id()];
        let mut leaves = Vec::new();
        while let Some(id) = stack.pop() {
            assert!(reachable.insert(id), "repeated reachable node {id}");
            let node = writer.nodes.get(&id).expect("missing reachable node");
            match node.value() {
                TreeNode::Internal(internal) => stack.extend(internal.children.iter().copied()),
                TreeNode::Leaf(_) => leaves.push(id),
            }
        }
        let mut reachable_work = Vec::new();
        let mut orphan_work = Vec::new();
        for entry in writer.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                if leaf.length > writer.config.split_threshold
                    || (leaf.parent_id.is_some()
                        && leaf.length > 0
                        && leaf.length < writer.config.merge_threshold)
                {
                    let item = serde_json::json!({"node_id":*entry.key(),"length":leaf.length});
                    if reachable.contains(entry.key()) {
                        reachable_work.push(item)
                    } else {
                        orphan_work.push(item)
                    }
                }
            }
        }
        eprintln!(
            "BALANCE_TOPOLOGY {}",
            serde_json::json!({"stored_nodes":writer.nodes.len(),"reachable_nodes":reachable.len(),"reachable_work":reachable_work,"orphan_work":orphan_work})
        );
        for leaf in leaves {
            writer.load(leaf).await.unwrap();
        }
        let ids = writer
            .root_reachable_valid_ids()
            .expect("invalid reachable topology or posting data");
        let expected = manifest["vector_count"].as_u64().unwrap();
        let complete =
            ids.len() as u64 == expected && (0..expected).all(|id| ids.contains(&(id as u32)));
        eprintln!(
            "BALANCE_COVERAGE {}",
            serde_json::json!({"expected_vectors":expected,"reachable_valid_vectors":ids.len(),"complete":complete})
        );
        assert!(
            complete,
            "published snapshot has missing reachable logical vectors"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parallel_parent_child_splits_and_merges_preserve_committed_coverage() {
        use rand::{Rng, SeedableRng};
        for seed in 0..8 {
            let dir = tempfile::tempdir().unwrap();
            let p = provider(dir.path());
            let config = HierarchicalSpannConfig {
                branching_factor: 2,
                split_threshold: 8,
                merge_threshold: 2,
                ..Default::default()
            };
            let writer =
                HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
            let mut random = rand::rngs::StdRng::seed_from_u64(seed);
            for id in 0..128 {
                let vector: Vec<f32> = (0..4).map(|_| random.gen_range(-10.0..10.0)).collect();
                writer.add(id, &vector);
            }
            writer.balance_index_parallel(4);
            let live = writer.root_reachable_valid_ids().unwrap();
            if live.len() != 128 {
                let mut reachable_nodes = HashSet::new();
                let mut pending = vec![writer.root_id()];
                while let Some(node_id) = pending.pop() {
                    if !reachable_nodes.insert(node_id) {
                        continue;
                    }
                    if let Some(node) = writer.nodes.get(&node_id) {
                        if let TreeNode::Internal(internal) = node.value() {
                            pending.extend(internal.children.iter().copied());
                        }
                    }
                }
                for id in (0..128).filter(|id| !live.contains(id)) {
                    let current = writer.current_version_sync(id);
                    let copies:Vec<_>=writer.nodes.iter().filter_map(|node| {let TreeNode::Leaf(leaf)=node.value() else{return None};let versions:Vec<_>=leaf.ids.iter().zip(&leaf.versions).filter_map(|(&stored,&version)|(stored==id).then_some(version)).collect();(!versions.is_empty()).then(||serde_json::json!({"node_id":*node.key(),"parent_id":leaf.parent_id,"reachable":reachable_nodes.contains(node.key()),"versions":versions}))}).collect();
                    eprintln!(
                        "MISSING_LOGICAL_TRACE {}",
                        serde_json::json!({"seed":seed,"id":id,"current_version":current,"physical_copies":copies,"failed_registrations":writer.stats.add_missing_nodes.load(Ordering::Relaxed)})
                    );
                }
            }
            assert_eq!(live.len(), 128, "split coverage seed {seed}");
            for id in 0..112 {
                writer.delete(id);
            }
            let leaves: Vec<_> = writer
                .nodes
                .iter()
                .filter_map(|node| matches!(node.value(), TreeNode::Leaf(_)).then_some(*node.key()))
                .collect();
            for leaf in leaves {
                writer.scrub(leaf);
            }
            writer.balance_index_parallel(4);
            let expected: HashSet<u32> = (112..128).collect();
            assert_eq!(
                writer.root_reachable_valid_ids().unwrap(),
                expected,
                "merge coverage seed {seed}"
            );
            let ids = writer
                .commit(&p, None)
                .await
                .unwrap()
                .flush()
                .await
                .unwrap();
            let reopened =
                HierarchicalSpannWriter::open(&p, ids, DistanceFunction::Euclidean, config)
                    .await
                    .unwrap();
            reopened.load_all_postings().await.unwrap();
            assert_eq!(
                reopened.root_reachable_valid_ids().unwrap(),
                expected,
                "committed coverage seed {seed}"
            );
        }
    }
    #[test]
    fn disappeared_reassignment_target_cannot_invalidate_the_current_copy() {
        let writer = HierarchicalSpannWriter::new(
            4,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
        );
        let vector = [1.0, 2.0, 0.0, 0.0];
        writer.add(0, &vector);
        let target = writer.root_id();
        let removed = writer.nodes.remove(&target).unwrap();
        assert!(matches!(
            writer.register_reassignment(0, 1, &[target], &vector),
            ReassignmentRegistration::Retry
        ));
        assert_eq!(writer.current_version_sync(0), Some(1));
        writer.nodes.insert(removed.0, removed.1);
        assert!(matches!(
            writer.register_reassignment(0, 1, &[target], &vector),
            ReassignmentRegistration::Registered(_)
        ));
        writer.scrub(target);
        assert_eq!(writer.current_version_sync(0), Some(2));
        assert_eq!(
            writer.root_reachable_valid_ids().unwrap(),
            HashSet::from([0])
        );
        writer.add(0, &[3.0, 2.0, 0.0, 0.0]);
        assert!(matches!(
            writer.register_reassignment(0, 2, &[target], &vector),
            ReassignmentRegistration::Obsolete
        ));
        assert_eq!(writer.current_version_sync(0), Some(3));
        writer.delete(0);
        assert!(matches!(
            writer.register_reassignment(0, 3, &[target], &vector),
            ReassignmentRegistration::Obsolete
        ));
        assert_eq!(writer.current_version_sync(0), Some(3 | DELETED_BIT));
    }
}

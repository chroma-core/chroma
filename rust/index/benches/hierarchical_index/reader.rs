#![allow(dead_code)]

use std::sync::Arc;
use std::time::Instant;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use chroma_types::QuantizedCluster;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};
use simsimd::SpatialSimilarity;

/// Max in-flight async loads per query in `search_with_policy_lazy`.
/// Each rayon worker drives one query at a time, so total in-flight ops
/// across the rayon pool is roughly `num_rayon_threads * this`. NVMe SSDs
/// benefit from deep queues; foyer/RAM hits add per-future polling cost.
/// 32 is a balance that helps the cold pass meaningfully without hurting
/// the warm pass much.
const LAZY_RECALL_CONCURRENCY: usize = 32;

use super::writer::{
    code_slice, effective_beam,
    persistence::{
        HierarchicalSpannIds, NODE_TYPE_LEAF, NO_PARENT, PREFIX_CENTROID_CODE, PREFIX_CHILDREN,
        PREFIX_DIM, PREFIX_EMBEDDING, PREFIX_LENGTH, PREFIX_NODE_TYPE, PREFIX_PARENT, PREFIX_ROOT,
        SINGLETON_KEY,
    },
    unpack_u32s_to_bytes, HierarchicalSpannConfig, InternalNode, LeafNode, NodeId, ReadBeamPolicy,
    SearchTimings, TreeNode, WriterStats,
};

pub struct HierarchicalSpannReader {
    nodes: DashMap<NodeId, TreeNode>,
    root_id: u32,
    embeddings: DashMap<u32, Arc<[f32]>>,
    
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,
    pub stats: WriterStats,
    posting_list_reader:
        chroma_blockstore::BlockfileReader<'static, u32, QuantizedCluster<'static>>,
    vector_data_reader: chroma_blockstore::BlockfileReader<'static, u32, &'static [f32]>,
}

impl HierarchicalSpannReader {
    /// Open a persisted index for read-only search. Only loads centroid_codes
    /// and tree structure; f32 centroids and posting data are lazy-loaded.
    pub async fn open(
        blockfile_provider: &BlockfileProvider,
        ids: HierarchicalSpannIds,
        distance_fn: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // --- Step 1: Read scalar metadata ---
        let sm_reader = blockfile_provider
            .read::<u32, u32>(BlockfileReaderOptions::new(
                ids.scalar_metadata_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let root_id = sm_reader
            .get(PREFIX_ROOT, SINGLETON_KEY)
            .await?
            .expect("missing root_id");
        let dim = sm_reader
            .get(PREFIX_DIM, SINGLETON_KEY)
            .await?
            .expect("missing dim") as usize;

        let mut node_types: Vec<(u32, u32)> = Vec::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_NODE_TYPE..=PREFIX_NODE_TYPE, ..)
            .await?
        {
            node_types.push((key, value));
        }

        let mut parent_ids: std::collections::HashMap<u32, u32> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_PARENT..=PREFIX_PARENT, ..)
            .await?
        {
            parent_ids.insert(key, value);
        }

        let mut lengths: std::collections::HashMap<u32, usize> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_LENGTH..=PREFIX_LENGTH, ..)
            .await?
        {
            lengths.insert(key, value as usize);
        }

        // --- Step 2: Read children lists ---
        let ld_reader = blockfile_provider
            .read::<u32, &'static [u32]>(BlockfileReaderOptions::new(
                ids.list_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let mut children_map: std::collections::HashMap<u32, Vec<u32>> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in ld_reader
            .get_range(PREFIX_CHILDREN..=PREFIX_CHILDREN, ..)
            .await?
        {
            children_map.insert(key, value.to_vec());
        }

        // --- Step 3: Build the node tree (no f32 centroids loaded) ---
        let nodes: DashMap<NodeId, TreeNode> = DashMap::new();

        for &(node_id, ntype) in &node_types {
            let parent_id = parent_ids
                .get(&node_id)
                .copied()
                .map(|p| if p == NO_PARENT { None } else { Some(p) })
                .unwrap_or(None);

            if ntype == NODE_TYPE_LEAF {
                let length = lengths.get(&node_id).copied().unwrap_or(0);
                nodes.insert(
                    node_id,
                    TreeNode::Leaf(LeafNode {
                        centroid: Vec::new(),
                        centroid_code: Vec::new(),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id,
                        length,
                    }),
                );
            } else {
                let children = children_map.remove(&node_id).unwrap_or_default();
                nodes.insert(
                    node_id,
                    TreeNode::Internal(InternalNode {
                        centroid: Vec::new(),
                        centroid_code: Vec::new(),
                        children,
                        parent_id,
                    }),
                );
            }
        }

        // --- Step 4: Load centroid_codes from blockfile ---
        let code_byte_len = Code::<1, Vec<u8>>::size(dim);
        for (_prefix, key, value) in ld_reader
            .get_range(PREFIX_CENTROID_CODE..=PREFIX_CENTROID_CODE, ..)
            .await?
        {
            let code_bytes = unpack_u32s_to_bytes(&value, code_byte_len);
            if let Some(mut node_ref) = nodes.get_mut(&key) {
                match node_ref.value_mut() {
                    TreeNode::Leaf(leaf) => leaf.centroid_code = code_bytes,
                    TreeNode::Internal(internal) => internal.centroid_code = code_bytes,
                }
            }
        }

        // --- Step 5: Open blockfile readers for lazy loading ---
        let posting_list_reader = blockfile_provider
            .read::<u32, QuantizedCluster<'static>>(BlockfileReaderOptions::new(
                ids.posting_list_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let vector_data_reader = blockfile_provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        Ok(Self {
            dim,
            distance_fn,
            config,
            nodes,
            root_id,
            embeddings: DashMap::new(),
            stats: WriterStats::default(),
            posting_list_reader,
            vector_data_reader,
        })
    }

    // =========================================================================
    // Lazy loading
    // =========================================================================

    /// Lazily load a leaf's posting data (ids, codes, versions) and centroid.
    pub async fn load_node(&self, node_id: NodeId) -> Result<(), Box<dyn ChromaError>> {
        {
            let node_ref = self.nodes.get(&node_id);
            match node_ref.as_ref().map(|r| r.value()) {
                Some(TreeNode::Leaf(leaf)) if leaf.ids.len() >= leaf.length => return Ok(()),
                Some(TreeNode::Leaf(_)) => {}
                _ => return Ok(()),
            }
        }

        let Some(cluster) = self.posting_list_reader.get("", node_id).await? else {
            return Ok(());
        };

        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.centroid = cluster.center.to_vec();
                    leaf.ids = cluster.ids.to_vec();
                    leaf.versions = cluster.versions.iter().map(|&v| v as u8).collect();
                    leaf.codes = cluster.codes.to_vec();
                    leaf.length = leaf.ids.len();
                }
            }
        }

        Ok(())
    }

    /// Lazily load raw f32 embeddings for vector reranking.
    pub async fn load_embeddings(&self, ids: &[u32]) -> Result<(), Box<dyn ChromaError>> {
        let missing_ids: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect();

        for id in missing_ids {
            if let Some(embedding) = self.vector_data_reader.get(PREFIX_EMBEDDING, id).await? {
                self.embeddings.insert(id, Arc::from(embedding));
            }
        }

        Ok(())
    }

    /// Eagerly load all leaf posting data from the blockfile.
    pub async fn load_all_postings(&self) -> Result<(), Box<dyn ChromaError>> {
        let leaf_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|e| matches!(e.value(), TreeNode::Leaf(_)))
            .map(|e| *e.key())
            .collect();
        for id in leaf_ids {
            self.load_node(id).await?;
        }
        Ok(())
    }

    /// Eagerly load all raw f32 embeddings from the blockfile.
    pub async fn load_all_embeddings(&self) -> Result<(), Box<dyn ChromaError>> {
        let all_ids: Vec<u32> = self
            .nodes
            .iter()
            .filter_map(|e| {
                if let TreeNode::Leaf(leaf) = e.value() {
                    Some(leaf.ids.clone())
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        self.load_embeddings(&all_ids).await
    }

    // =========================================================================
    // Navigate (4-bit quantized, no centroid rerank)
    // =========================================================================

    fn navigate_4bit(&self, query: &[f32], policy: &ReadBeamPolicy) -> Vec<(NodeId, f32)> {
        let root = self.root_id;
        let Some(root_node) = self.nodes.get(&root) else {
            return Vec::new();
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            return vec![(root, 0.0)];
        }
        drop(root_node);

        let q_norm = Self::vec_norm(query);
        let padded_bytes = self.padded_bytes();

        let qq = QuantizedQuery::new(query, padded_bytes, 0.0, 0.0, q_norm);

        let mut leaves: Vec<(NodeId, f32)> = Vec::new();
        let mut beam: Vec<NodeId> = vec![root];
        let mut levels = 0u64;

        loop {
            let mut child_scores: Vec<(NodeId, f32)> = Vec::new();

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                let code_bytes = child.centroid_code();
                                let dist = if code_bytes.is_empty() {
                                    f32::MAX
                                } else {
                                    Code::<1, _>::new(code_bytes)
                                        .distance_quantized_query(&self.distance_fn, &qq)
                                };
                                child_scores.push((child_id, dist));
                            }
                        }
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            levels += 1;
            let params = policy.level_params(levels as usize);

            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let eff = effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
            child_scores.truncate(eff);

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

        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        leaves
    }

    // =========================================================================
    // Search (async -- lazy loads posting data)
    // =========================================================================

    pub async fn search(
        &self,
        query: &[f32],
        k: usize,
        tau: f64,
        beam_min: usize,
        beam_max: usize,
        rerank_vectors: usize,
    ) -> Result<(Vec<(u32, f32)>, usize, usize, SearchTimings), Box<dyn ChromaError>> {
        let policy = ReadBeamPolicy::uniform(Some(tau), beam_min, beam_max);
        self.search_with_policy(query, k, rerank_vectors, &policy)
            .await
    }

    pub async fn search_with_policy(
        &self,
        query: &[f32],
        k: usize,
        rerank_vectors: usize,
        policy: &ReadBeamPolicy,
    ) -> Result<(Vec<(u32, f32)>, usize, usize, SearchTimings), Box<dyn ChromaError>> {
        let nav_t0 = Instant::now();
        let leaves = self.navigate_4bit(query, policy);
        let navigate_nanos = nav_t0.elapsed().as_nanos() as u64;

        for &(leaf_id, _) in &leaves {
            self.load_node(leaf_id).await?;
        }

        let (deduped, scanned, leaves_scanned, timings) =
            self.score_leaves(query, k, rerank_vectors, &leaves, navigate_nanos);

        if rerank_vectors > 1 {
            let ids_to_load: Vec<u32> = deduped.iter().map(|(id, _)| *id).collect();
            self.load_embeddings(&ids_to_load).await?;
            Ok(self.rerank(query, k, deduped, scanned, leaves_scanned, timings))
        } else {
            Ok((deduped, scanned, leaves_scanned, timings))
        }
    }

    /// Synchronous search. Requires that posting data is already loaded
    /// (via `load_all_postings()`). Embeddings for vector reranking must
    /// also be pre-loaded if `rerank_vectors > 1`.
    pub fn search_with_policy_sync(
        &self,
        query: &[f32],
        k: usize,
        rerank_vectors: usize,
        policy: &ReadBeamPolicy,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let nav_t0 = Instant::now();
        let leaves = self.navigate_4bit(query, policy);
        let navigate_nanos = nav_t0.elapsed().as_nanos() as u64;

        let (deduped, scanned, leaves_scanned, timings) =
            self.score_leaves(query, k, rerank_vectors, &leaves, navigate_nanos);

        if rerank_vectors > 1 {
            self.rerank(query, k, deduped, scanned, leaves_scanned, timings)
        } else {
            (deduped, scanned, leaves_scanned, timings)
        }
    }

    /// Async search that lazy-loads embeddings into a per-query local map
    /// (never populates the shared `self.embeddings` cache), and lazy-loads
    /// each beam leaf's posting data on demand if it isn't already in
    /// `self.nodes` (no-op if `load_all_postings()` was called at setup).
    /// Use this path at large index sizes where `load_all_embeddings()` would
    /// exhaust RAM, and combine with `clear_loaded_postings()` between rounds
    /// to also bound posting-side memory growth.
    pub async fn search_with_policy_lazy(
        &self,
        query: &[f32],
        k: usize,
        rerank_vectors: usize,
        policy: &ReadBeamPolicy,
    ) -> Result<(Vec<(u32, f32)>, usize, usize, SearchTimings), Box<dyn ChromaError>> {
        let nav_t0 = Instant::now();
        let leaves = self.navigate_4bit(query, policy);
        let navigate_nanos = nav_t0.elapsed().as_nanos() as u64;

        // Drive up to LAZY_RECALL_CONCURRENCY posting `load_node` calls
        // concurrently. `load_node` is idempotent (early-returns once a
        // leaf is populated), so concurrent overlapping fetches across
        // queries are safe -- worst case one of them does redundant work.
        // Within a single query the leaves are unique, so no intra-query
        // duplication.
        stream::iter(leaves.iter().map(|&(leaf_id, _)| self.load_node(leaf_id)))
            .buffer_unordered(LAZY_RECALL_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;

        let (deduped, scanned, leaves_scanned, mut timings) =
            self.score_leaves(query, k, rerank_vectors, &leaves, navigate_nanos);

        if rerank_vectors > 1 {
            let rr_t0 = Instant::now();
            let ids_to_load: Vec<u32> = deduped.iter().map(|(id, _)| *id).collect();

            // Drive up to LAZY_RECALL_CONCURRENCY embedding fetches in
            // parallel. Each completes into a `(id, Vec<f32>)` pair (or
            // None if the id isn't in the blockfile). Collected into a
            // per-query local map, never the shared `self.embeddings`.
            let reader = &self.vector_data_reader;
            let pairs: Vec<Option<(u32, Vec<f32>)>> = stream::iter(ids_to_load.into_iter().map(
                |id| async move {
                    let emb = reader.get(PREFIX_EMBEDDING, id).await?;
                    Ok::<Option<(u32, Vec<f32>)>, Box<dyn ChromaError>>(
                        emb.map(|e| (id, e.to_vec())),
                    )
                },
            ))
            .buffer_unordered(LAZY_RECALL_CONCURRENCY)
            .try_collect()
            .await?;
            let local: std::collections::HashMap<u32, Vec<f32>> =
                pairs.into_iter().flatten().collect();

            let mut reranked: Vec<(u32, f32)> = deduped
                .into_iter()
                .map(|(id, approx_dist)| {
                    if let Some(emb) = local.get(&id) {
                        (id, self.dist(query, emb))
                    } else {
                        (id, approx_dist)
                    }
                })
                .collect();
            reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            reranked.truncate(k);
            timings.rerank_nanos = rr_t0.elapsed().as_nanos() as u64;
            Ok((reranked, scanned, leaves_scanned, timings))
        } else {
            Ok((deduped, scanned, leaves_scanned, timings))
        }
    }

    /// Aggregate block-pin stats across both internal blockfile readers.
    /// Returns ((posting_blocks, posting_bytes), (vector_data_blocks, vector_data_bytes)).
    /// See `docs/README.md` ("Reader-side block pinning") for why this matters.
    pub fn loaded_blocks_stats(&self) -> ((usize, u64), (usize, u64)) {
        (
            self.posting_list_reader.loaded_blocks_stats(),
            self.vector_data_reader.loaded_blocks_stats(),
        )
    }

    /// Clear block pins in both internal blockfile readers. Safe to call
    /// after any data a caller needed has been copied out (e.g. after
    /// `load_all_postings` / `load_all_embeddings`, or between tau rounds
    /// in lazy recall mode). Subsequent reads will refault through the
    /// provider's Foyer cache.
    pub fn clear_loaded_blocks(&self) {
        self.posting_list_reader.clear_loaded_blocks();
        self.vector_data_reader.clear_loaded_blocks();
    }

    /// Count of leaves whose posting data (ids/codes/versions/centroid) is
    /// currently materialized in `self.nodes`, and the owned-bytes those
    /// fields contribute. The `centroid_code` field is loaded once at
    /// `open()` and is NOT included here; only fields that `load_node` /
    /// `clear_loaded_postings` touch.
    pub fn loaded_postings_stats(&self) -> (usize, u64) {
        let mut leaves_loaded = 0usize;
        let mut bytes = 0u64;
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                if !leaf.ids.is_empty() {
                    leaves_loaded += 1;
                    bytes += (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes += leaf.codes.len() as u64;
                    bytes += leaf.versions.len() as u64;
                    bytes += (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                }
            }
        }
        (leaves_loaded, bytes)
    }

    /// Drop posting data (ids/codes/versions/centroid) from every leaf in
    /// `self.nodes`, so subsequent `load_node` calls re-fetch from the
    /// posting blockfile. The `length` field is preserved so `load_node`'s
    /// `ids.len() < length` check still triggers a refetch. Tree topology
    /// and quantized centroid_codes are untouched. Returns
    /// `(leaves_cleared, bytes_freed)`.
    pub fn clear_loaded_postings(&self) -> (usize, u64) {
        let mut leaves_cleared = 0usize;
        let mut bytes_freed = 0u64;
        for mut entry in self.nodes.iter_mut() {
            if let TreeNode::Leaf(leaf) = entry.value_mut() {
                if !leaf.ids.is_empty() {
                    bytes_freed +=
                        (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes_freed += leaf.codes.len() as u64;
                    bytes_freed += leaf.versions.len() as u64;
                    bytes_freed +=
                        (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                    leaf.ids = Vec::new();
                    leaf.codes = Vec::new();
                    leaf.versions = Vec::new();
                    leaf.centroid = Vec::new();
                    // Keep `length` intact so load_node detects the gap.
                    leaves_cleared += 1;
                }
            }
        }
        (leaves_cleared, bytes_freed)
    }

    fn score_leaves(
        &self,
        query: &[f32],
        k: usize,
        rerank_vectors: usize,
        leaves: &[(NodeId, f32)],
        navigate_nanos: u64,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let leaves_scanned = leaves.len();
        let padded_bytes = self.padded_bytes();
        let code_size = self.code_size();
        let q_norm = Self::vec_norm(query);
        let rerank_factor = rerank_vectors;

        let mut results: Vec<(u32, f32)> = Vec::new();
        let mut quantize_nanos = 0u64;
        let mut distance_nanos = 0u64;

        for &(leaf_id, _) in leaves {
            let Some(node_ref) = self.nodes.get(&leaf_id) else {
                continue;
            };
            let TreeNode::Leaf(leaf) = node_ref.value() else {
                continue;
            };

            if leaf.ids.is_empty() {
                continue;
            }

            let qt0 = Instant::now();
            let r_q: Vec<f32> = query
                .iter()
                .zip(leaf.centroid.iter())
                .map(|(q, c)| q - c)
                .collect();
            let c_norm = Self::vec_norm(&leaf.centroid);
            let c_dot_q = f32::dot(&leaf.centroid, query).unwrap_or(0.0) as f32;
            let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
            quantize_nanos += qt0.elapsed().as_nanos() as u64;

            results.reserve(leaf.ids.len());
            let dt0 = Instant::now();
            for (i, &id) in leaf.ids.iter().enumerate() {
                let dist = Code::<1, _>::new(code_slice(&leaf.codes, i, code_size))
                    .distance_quantized_query(&self.distance_fn, &qq);
                results.push((id, dist));
            }
            distance_nanos += dt0.elapsed().as_nanos() as u64;
        }

        let sort_t0 = Instant::now();
        let m = (k * rerank_factor).max(k);
        let scanned;
        let mut deduped: Vec<(u32, f32)> = if self.config.max_replicas == 1 {
            scanned = results.len();
            results
        } else {
            let mut best: std::collections::HashMap<u32, f32> =
                std::collections::HashMap::with_capacity(results.len());
            for (id, dist) in results {
                let entry = best.entry(id).or_insert(f32::MAX);
                if dist < *entry {
                    *entry = dist;
                }
            }
            scanned = best.len();
            best.into_iter().collect()
        };

        if deduped.len() > m {
            let nth = m - 1;
            deduped.select_nth_unstable_by(nth, |a, b| a.1.partial_cmp(&b.1).unwrap());
            deduped.truncate(m);
        }
        deduped.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let sort_dedup_nanos = sort_t0.elapsed().as_nanos() as u64;

        (
            deduped,
            scanned,
            leaves_scanned,
            SearchTimings {
                navigate_nanos,
                quantize_nanos,
                distance_nanos,
                sort_dedup_nanos,
                rerank_nanos: 0,
            },
        )
    }

    fn rerank(
        &self,
        query: &[f32],
        k: usize,
        deduped: Vec<(u32, f32)>,
        scanned: usize,
        leaves_scanned: usize,
        mut timings: SearchTimings,
    ) -> (Vec<(u32, f32)>, usize, usize, SearchTimings) {
        let rr_t0 = Instant::now();
        let mut reranked: Vec<(u32, f32)> = deduped
            .into_iter()
            .map(|(id, approx_dist)| {
                if let Some(emb) = self.embeddings.get(&id) {
                    let dist = self.dist(query, emb.value());
                    (id, dist)
                } else {
                    (id, approx_dist)
                }
            })
            .collect();
        reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        timings.rerank_nanos = rr_t0.elapsed().as_nanos() as u64;
        reranked.truncate(k);
        (reranked, scanned, leaves_scanned, timings)
    }

    // =========================================================================
    // Utility methods
    // =========================================================================

    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    fn padded_bytes(&self) -> usize {
        Code::<1, Vec<u8>>::packed_len(self.dim)
    }

    fn code_size(&self) -> usize {
        Code::<1, Vec<u8>>::size(self.dim)
    }

    fn vec_norm(v: &[f32]) -> f32 {
        (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt()
    }

    pub fn depth(&self) -> usize {
        self.depth_of(self.root_id)
    }

    fn depth_of(&self, node_id: NodeId) -> usize {
        let Some(node_ref) = self.nodes.get(&node_id) else {
            return 0;
        };
        match node_ref.value() {
            TreeNode::Leaf(_) => 1,
            TreeNode::Internal(internal) => {
                let children: Vec<NodeId> = internal.children.clone();
                drop(node_ref);
                1 + children.iter().map(|&c| self.depth_of(c)).max().unwrap_or(0)
            }
        }
    }

    pub fn level_node_counts(&self) -> Vec<usize> {
        let root = self.root_id;
        let depth = self.depth_of(root);
        let mut counts = vec![0usize; depth];
        let mut queue: Vec<(NodeId, usize)> = vec![(root, 0)];
        while let Some((node_id, level)) = queue.pop() {
            if level >= depth {
                continue;
            }
            counts[level] += 1;
            if let Some(node_ref) = self.nodes.get(&node_id) {
                if let TreeNode::Internal(internal) = node_ref.value() {
                    let children: Vec<NodeId> = internal.children.clone();
                    drop(node_ref);
                    for child_id in children {
                        queue.push((child_id, level + 1));
                    }
                }
            }
        }
        counts
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Estimated in-memory footprint of the reader's owned data structures.
    /// Only counts data the reader explicitly holds (tree nodes, posting
    /// data, and reranking embeddings). Excludes blockfile reader caches
    /// (which are managed separately by the blockfile provider).
    pub fn memory_usage(&self) -> ReaderMemoryUsage {
        let dim = self.dim;
        let code_byte_len = Code::<1, Vec<u8>>::size(dim) as u64;
        let f32_centroid_bytes = (dim as u64) * 4;

        let mut leaf_count: u64 = 0;
        let mut internal_count: u64 = 0;
        let mut tree_bytes: u64 = 0;
        let mut centroid_bytes: u64 = 0;
        let mut posting_entries: u64 = 0;
        let mut posting_bytes: u64 = 0;

        for entry in self.nodes.iter() {
            match entry.value() {
                TreeNode::Leaf(leaf) => {
                    leaf_count += 1;
                    tree_bytes += leaf.centroid_code.len() as u64;
                    if !leaf.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    let n = leaf.ids.len() as u64;
                    posting_entries += n;
                    // ids (u32) + per-vector codes + versions (u8)
                    posting_bytes += n.saturating_mul(4 + code_byte_len + 1);
                }
                TreeNode::Internal(internal) => {
                    internal_count += 1;
                    tree_bytes += internal.centroid_code.len() as u64;
                    if !internal.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    // children Vec<u32>
                    tree_bytes += (internal.children.len() as u64).saturating_mul(4);
                }
            }
        }

        let embedding_count = self.embeddings.len() as u64;
        let embedding_bytes = embedding_count.saturating_mul(f32_centroid_bytes);

        ReaderMemoryUsage {
            dim,
            leaf_count,
            internal_count,
            tree_bytes,
            centroid_bytes,
            posting_entries,
            posting_bytes,
            embedding_count,
            embedding_bytes,
        }
    }
}

/// In-memory footprint breakdown of a `HierarchicalSpannReader`.
/// All byte counts are estimates of the *payload* size of the owned
/// `Vec`/`DashMap` contents and exclude per-allocation overhead.
#[derive(Debug, Clone, Copy)]
pub struct ReaderMemoryUsage {
    pub dim: usize,
    pub leaf_count: u64,
    pub internal_count: u64,
    /// `centroid_code` (1-bit RaBitQ) for every node, plus `children` Vec
    /// payloads on internal nodes.
    pub tree_bytes: u64,
    /// f32 centroids loaded onto leaves/internals (only populated for
    /// leaves whose posting data has been materialized via `load_node`).
    pub centroid_bytes: u64,
    /// Sum of `leaf.ids.len()` across all materialized leaves.
    pub posting_entries: u64,
    /// `posting_entries * (4 [id] + code_size + 1 [version])`.
    pub posting_bytes: u64,
    /// Number of full-precision embeddings in the rerank cache.
    pub embedding_count: u64,
    /// `embedding_count * dim * 4`.
    pub embedding_bytes: u64,
}

impl ReaderMemoryUsage {
    pub fn total_bytes(&self) -> u64 {
        self.tree_bytes
            .saturating_add(self.centroid_bytes)
            .saturating_add(self.posting_bytes)
            .saturating_add(self.embedding_bytes)
    }
}

#![allow(dead_code)]

use std::time::Instant;

use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use futures::stream::{self, StreamExt, TryStreamExt};
use simsimd::SpatialSimilarity;

use super::super::writer::{
    code_slice, effective_beam, persistence::PREFIX_EMBEDDING, NodeId, ReadBeamPolicy,
    SearchTimings, TreeNode,
};
use super::HierarchicalSpannReader;

/// Max in-flight async loads per query in `search_with_policy_lazy`.
/// Each rayon worker drives one query at a time, so total in-flight ops
/// across the rayon pool is roughly `num_rayon_threads * this`. NVMe SSDs
/// benefit from deep queues; foyer/RAM hits add per-future polling cost.
/// 32 is a balance that helps the cold pass meaningfully without hurting
/// the warm pass much.
const LAZY_RECALL_CONCURRENCY: usize = 32;

impl HierarchicalSpannReader {
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
            self.load_node_posting_list(leaf_id).await?;
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
        stream::iter(
            leaves
                .iter()
                .map(|&(leaf_id, _)| self.load_node_posting_list(leaf_id)),
        )
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
            let pairs: Vec<Option<(u32, Vec<f32>)>> =
                stream::iter(ids_to_load.into_iter().map(|id| async move {
                    let emb = reader.get(PREFIX_EMBEDDING, id).await?;
                    Ok::<Option<(u32, Vec<f32>)>, Box<dyn ChromaError>>(
                        emb.map(|e| (id, e.to_vec())),
                    )
                }))
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

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

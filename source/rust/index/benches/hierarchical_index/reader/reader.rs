#![allow(dead_code)]

use std::time::Instant;

use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use futures::stream::{self, StreamExt, TryStreamExt};
use simsimd::SpatialSimilarity;

use super::super::common::{code_slice, effective_beam, NodeId, ReadBeamPolicy, TreeNode};
use super::super::instrumentation::SearchTimings;
use super::super::persistance::PREFIX_EMBEDDING;
use super::{HierarchicalSpannReader, ReaderIntegrityError};

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
                                let dist = Code::<1, _>::new(code_bytes)
                                    .distance_quantized_query(&self.distance_fn, &qq);
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
            if pairs.iter().any(Option::is_none) {
                return Err(Box::new(ReaderIntegrityError(
                    "missing exact embedding for live candidate".into(),
                )));
            }
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

    pub(super) fn score_leaves(
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
        // Reassignment can leave several physical copies even when the target
        // replica count is one. Budget distinct live IDs before exact reranking.
        let mut best: std::collections::HashMap<u32, f32> =
            std::collections::HashMap::with_capacity(results.len());
        for (id, dist) in results {
            let entry = best.entry(id).or_insert(dist);
            if dist < *entry {
                *entry = dist;
            }
        }
        let scanned = best.len();
        let mut deduped: Vec<(u32, f32)> = best.into_iter().collect();

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

    pub(super) fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.distance_fn.distance(a, b)
    }

    pub(super) fn padded_bytes(&self) -> usize {
        Code::<1, Vec<u8>>::packed_len(self.dim)
    }

    fn code_size(&self) -> usize {
        Code::<1, Vec<u8>>::size(self.dim)
    }

    pub(super) fn vec_norm(v: &[f32]) -> f32 {
        (f32::dot(v, v).unwrap_or(0.0) as f32).sqrt()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod reader_integrity_tests {
    use super::super::super::{config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter};
    use super::*;
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_filters_stale_and_deleted_postings_and_budgets_unique_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let provider = BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(directory.path().to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            16,
        ));
        let config = HierarchicalSpannConfig::default();
        let writer = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        for id in 0..8 {
            writer.add(id, &[id as f32, 1.0, 0.0, 0.0]);
        }
        // Leave the old version and deleted ID in the untouched root posting.
        writer.add(0, &[20.0, 1.0, 0.0, 0.0]);
        writer.delete(1);
        let ids = writer
            .commit(&provider, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let reader =
            HierarchicalSpannReader::open(&provider, ids, DistanceFunction::Euclidean, config)
                .await
                .unwrap();
        reader.load_node_posting_list(reader.root_id).await.unwrap();
        {
            let node = reader.nodes.get(&reader.root_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value() else {
                panic!("expected one root leaf")
            };
            assert_eq!(leaf.ids.iter().filter(|&&id| id == 0).count(), 1);
            assert!(!leaf.ids.contains(&1));
            assert_eq!(leaf.ids.len(), 7);
        }
        // Reproduce duplicate current physical copies even with max_replicas=1.
        // Distinct candidates must consume the budget, before exact reranking.
        {
            let mut node = reader.nodes.get_mut(&reader.root_id).unwrap();
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                panic!("expected root")
            };
            let position = leaf.ids.iter().position(|&id| id == 2).unwrap();
            let version = leaf.versions[position];
            let size = reader.code_size();
            let code = code_slice(&leaf.codes, position, size).to_vec();
            for _ in 0..32 {
                leaf.ids.push(2);
                leaf.versions.push(version);
                leaf.codes.extend_from_slice(&code);
            }
            leaf.length = leaf.ids.len();
        }
        let query = [2.0, 1.0, 0.0, 0.0];
        let (hits, scanned, _, _) = reader.search(&query, 4, 2.0, 10, 256, 2).await.unwrap();
        assert_eq!(scanned, 7);
        assert_eq!(
            hits.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(
            hits.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
            vec![0.0, 1.0, 4.0, 9.0]
        );
        let (lazy, scanned, _, _) = reader
            .search_with_policy_lazy(&query, 4, 2, &ReadBeamPolicy::uniform(Some(2.0), 10, 256))
            .await
            .unwrap();
        assert_eq!(scanned, 7);
        assert_eq!(lazy, hits);
    }
}

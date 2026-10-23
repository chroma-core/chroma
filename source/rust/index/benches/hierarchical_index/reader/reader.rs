#![allow(dead_code)]

use std::time::Instant;

use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery, OneBitScorer};
use futures::stream::{self, StreamExt, TryStreamExt};
use simsimd::SpatialSimilarity;

use super::super::common::{code_slice, effective_beam, NodeId, ReadBeamPolicy, TreeNode};
use super::super::instrumentation::{SearchTimings,QueryWork,QueryStageTrace,NavigationLevelTrace,NavigationLevelWork};
use super::super::persistance::PREFIX_EMBEDDING;
use super::{HierarchicalSpannReader, ReaderIntegrityError};

/// Max in-flight async loads per query in `search_with_policy_lazy`.
/// Each rayon worker drives one query at a time, so total in-flight ops
/// across the rayon pool is roughly `num_rayon_threads * this`. NVMe SSDs
/// benefit from deep queues; foyer/RAM hits add per-future polling cost.
/// 32 is a balance that helps the cold pass meaningfully without hurting
/// the warm pass much.
const LAZY_RECALL_CONCURRENCY: usize = 32;

// Observing the input must leave the ordinary distance-only partition and tie choices unchanged.
fn trim_posting_candidates(rows:&mut Vec<(u32,f32)>,m:usize,mut trace:Option<&mut QueryStageTrace>) {
    if let Some(t)=trace.as_deref_mut(){t.pretrim_candidate_score_bits=rows.iter().map(|(id,d)|(*id,d.to_bits())).collect();}
    if rows.len()>m {rows.select_nth_unstable_by(m-1,|a,b|a.1.partial_cmp(&b.1).unwrap());
        if let Some(t)=trace.as_deref_mut(){let boundary=rows[m-1].1;t.cutoff_score_bits=Some(boundary.to_bits());t.cutoff_tie_candidates=rows.iter().filter(|(_,d)|*d==boundary).count();t.cutoff_tie_retained=rows[..m].iter().filter(|(_,d)|*d==boundary).count();}
        rows.truncate(m);
    }
    rows.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());
}

#[cfg(test)]
#[test]
fn candidate_score_trace_preserves_partition_ties_and_bits() {
    for input in [vec![],vec![(9,1.0)],vec![(7,2.0),(3,0.0),(8,-0.0),(1,1.0),(5,1.0),(2,1.0),(6,-2.0)]] {
        for m in 1..=input.len()+1 {
            let mut expected=input.clone();if expected.len()>m {expected.select_nth_unstable_by(m-1,|a,b|a.1.partial_cmp(&b.1).unwrap());expected.truncate(m);}expected.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());
            let mut plain=input.clone();trim_posting_candidates(&mut plain,m,None);
            let mut trace=QueryStageTrace{complete:true,id_cap:16,selected_leaf_ids:vec![12],pretrim_candidate_ids:input.iter().map(|v|v.0).collect(),retained_candidate_ids:vec![],pretrim_candidate_score_bits:vec![],cutoff_score_bits:None,cutoff_tie_candidates:0,cutoff_tie_retained:0,copy_elapsed_ns:0};
            let mut observed=input.clone();trim_posting_candidates(&mut observed,m,Some(&mut trace));
            let bits=|v:&Vec<(u32,f32)>|v.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>();assert_eq!(bits(&expected),bits(&plain));assert_eq!(bits(&expected),bits(&observed));assert_eq!(trace.pretrim_candidate_score_bits,bits(&input));assert_eq!(trace.selected_leaf_ids,vec![12]);
            if input.len()>m {let cutoff=f32::from_bits(trace.cutoff_score_bits.unwrap());assert_eq!(trace.cutoff_tie_candidates,input.iter().filter(|v|v.1==cutoff).count());assert_eq!(trace.cutoff_tie_retained,observed.iter().filter(|v|v.1==cutoff).count());}else{assert!(trace.cutoff_score_bits.is_none());}
        }
    }
}

impl HierarchicalSpannReader {
    // =========================================================================
    // Navigate (one-bit centroid scoring by default; optional FP control)
    // =========================================================================

    fn navigate_4bit(&self, query: &[f32], policy: &ReadBeamPolicy) -> Vec<(NodeId, f32)> {
        self.navigate_with_centroids(query,policy,None).expect("legacy navigation has no FP cache validation").0
    }

    pub(super) fn navigate_with_centroids(&self,query:&[f32],policy:&ReadBeamPolicy,fp:Option<&super::FpNavigationCache>)
        ->Result<(Vec<(NodeId,f32)>,(usize,usize,usize,usize)),Box<dyn ChromaError>> {
        let (leaves,work,_)=self.navigate_with_centroids_observed(query,policy,fp,false)?;
        Ok((leaves,work))
    }

    pub(super) fn navigate_with_centroids_observed(&self,query:&[f32],policy:&ReadBeamPolicy,fp:Option<&super::FpNavigationCache>,observe:bool)
        ->Result<(Vec<(NodeId,f32)>,(usize,usize,usize,usize),Option<NavigationLevelTrace>),Box<dyn ChromaError>> {
        if let Some(cache)=fp {cache.check(self)?;}
        let mut trace=observe.then(NavigationLevelTrace::new);
        let mut expanded=0usize;let mut evaluations=0usize;let mut peak_frontier=0usize;
        let root = self.root_id;
        let Some(root_node) = self.nodes.get(&root) else {
            return Ok((Vec::new(),(0,0,0,0),trace));
        };

        if matches!(root_node.value(), TreeNode::Leaf(_)) {
            return Ok((vec![(root,0.0)],(0,0,0,0),trace));
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
            let expanded_before=expanded;

            for &node_id in &beam {
                if let Some(node_ref) = self.nodes.get(&node_id) {
                    if let TreeNode::Internal(internal) = node_ref.value() {
                        expanded+=1;
                        let children: Vec<NodeId> = internal.children.clone();
                        drop(node_ref);

                        for child_id in children {
                            if let Some(child) = self.nodes.get(&child_id) {
                                evaluations+=1;
                                let dist=if let Some(cache)=fp {
                                    cache.distance(self,query,child_id,q_norm)?
                                }else{let code_bytes=child.centroid_code();Code::<1,_>::new(code_bytes).distance_quantized_query(&self.distance_fn,&qq)};
                                child_scores.push((child_id, dist));
                            }
                        }
                    }
                }
            }

            if child_scores.is_empty() {
                break;
            }

            peak_frontier=peak_frontier.max(child_scores.len());
            levels += 1;
            let params = policy.level_params(levels as usize);

            child_scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let eff = effective_beam(&child_scores, params.tau, params.beam_min, params.beam_max);
            // Inspect the exact sorted frontier without changing its order or cutoff.
            if let Some(trace)=trace.as_mut() {
                let mut work=NavigationLevelWork{level:levels as usize,expanded_internal:expanded-expanded_before,..Default::default()};
                for (position,(id,_)) in child_scores.iter().enumerate() {
                    if let Some(node)=self.nodes.get(id) {
                        match node.value() {
                            TreeNode::Internal(_)=>{work.offered_internal+=1;if position<eff {work.retained_internal+=1;}else{work.discarded_internal+=1;}},
                            TreeNode::Leaf(_)=>{work.offered_leaf+=1;if position<eff {work.retained_leaf+=1;}else{work.discarded_leaf+=1;}},
                        }
                    }else{trace.complete=false;}
                }
                trace.push(work);
            }
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
        Ok((leaves,(expanded,evaluations,levels as usize,peak_frontier),trace))
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
        self.search_with_policy_lazy_inner(query,k,rerank_vectors,policy,None,None).await
    }

    /// Bounded IDs for stage attribution only; query decisions are unchanged.
    pub async fn search_with_policy_lazy_traced(&self,query:&[f32],k:usize,rerank_vectors:usize,policy:&ReadBeamPolicy)
        ->Result<(Vec<(u32,f32)>,usize,usize,SearchTimings),Box<dyn ChromaError>> {
        self.search_with_policy_lazy_inner(query,k,rerank_vectors,policy,Some(16384),None).await
    }

    pub async fn search_with_policy_lazy_fp(&self,query:&[f32],k:usize,rerank_vectors:usize,policy:&ReadBeamPolicy,cache:&super::FpNavigationCache,traced:bool)
        ->Result<(Vec<(u32,f32)>,usize,usize,SearchTimings),Box<dyn ChromaError>> {
        self.search_with_policy_lazy_inner(query,k,rerank_vectors,policy,if traced{Some(16384)}else{None},Some(cache)).await
    }

    async fn search_with_policy_lazy_inner(
        &self,
        query: &[f32],
        k: usize,
        rerank_vectors: usize,
        policy: &ReadBeamPolicy,
        trace_cap:Option<usize>,
        fp:Option<&super::FpNavigationCache>,
    ) -> Result<(Vec<(u32, f32)>, usize, usize, SearchTimings), Box<dyn ChromaError>> {
        let nav_t0 = Instant::now();
        let (leaves,nav_work,nav_trace)=self.navigate_with_centroids_observed(query,policy,fp,trace_cap.is_some())?;
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
            self.score_leaves_with_trace(query, k, rerank_vectors, &leaves, navigate_nanos,trace_cap)?;
        timings.work.navigation_level_trace=nav_trace;
        timings.work.expanded_internal_nodes=nav_work.0;timings.work.centroid_distance_evaluations=nav_work.1;timings.work.navigation_levels=nav_work.2;timings.work.peak_child_frontier=nav_work.3;

        if rerank_vectors > 1 {
            let rr_t0 = Instant::now();
            let ids_to_load: Vec<u32> = deduped.iter().map(|(id, _)| *id).collect();
            timings.work.exact_embedding_gets=ids_to_load.len();

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
            timings.work.exact_vectors_reranked=reranked.len();
            reranked.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            reranked.truncate(k);
            timings.rerank_nanos = rr_t0.elapsed().as_nanos() as u64;
            Ok((reranked, scanned, leaves_scanned, timings))
        } else {
            Ok((deduped, scanned, leaves_scanned, timings))
        }
    }

    pub(super) fn score_leaves(&self,query:&[f32],k:usize,rerank_vectors:usize,leaves:&[(NodeId,f32)],navigate_nanos:u64)
        ->(Vec<(u32,f32)>,usize,usize,SearchTimings) {
        self.score_leaves_with_trace(query,k,rerank_vectors,leaves,navigate_nanos,None).expect("untraced scoring has no trace cap")
    }

    fn score_leaves_with_trace(&self,query:&[f32],k:usize,rerank_vectors:usize,leaves:&[(NodeId,f32)],navigate_nanos:u64,trace_cap:Option<usize>)
        ->Result<(Vec<(u32,f32)>,usize,usize,SearchTimings),Box<dyn ChromaError>> {
        let (mut deduped,scanned,leaves_scanned,mut timings,_)=self.score_leaf_minima(query,leaves,navigate_nanos);
        let mut trace=if let Some(cap)=trace_cap {
            if leaves.len()>cap || deduped.len()>cap {return Err(Box::new(ReaderIntegrityError("query stage trace cap exceeded before ID copy".into())));}
            let t=Instant::now();let leaf_ids=leaves.iter().map(|v|v.0).collect();let candidate_ids=deduped.iter().map(|v|v.0).collect();
            Some(QueryStageTrace{complete:true,id_cap:cap,selected_leaf_ids:leaf_ids,pretrim_candidate_ids:candidate_ids,retained_candidate_ids:vec![],pretrim_candidate_score_bits:vec![],cutoff_score_bits:None,cutoff_tie_candidates:0,cutoff_tie_retained:0,copy_elapsed_ns:t.elapsed().as_nanos() as u64})
        }else{None};
        let sort_t0=Instant::now();let m=(k*rerank_vectors).max(k);
        trim_posting_candidates(&mut deduped,m,trace.as_mut());
        timings.work.candidates_after_trim=deduped.len();
        timings.sort_dedup_nanos+=sort_t0.elapsed().as_nanos() as u64;
        if let Some(trace)=trace.as_mut(){let t=Instant::now();trace.retained_candidate_ids=deduped.iter().map(|v|v.0).collect();trace.copy_elapsed_ns+=t.elapsed().as_nanos() as u64;}
        timings.query_stage_trace=trace;
        Ok((deduped,scanned,leaves_scanned,timings))
    }

    pub(super) fn score_leaf_minima(&self,query:&[f32],leaves:&[(NodeId,f32)],navigate_nanos:u64)
        ->(Vec<(u32,f32)>,usize,usize,SearchTimings,bool) {
        self.score_leaf_minima_with_scorer(query,leaves,navigate_nanos,None).expect("legacy scorer has no batch shape branch")
    }

    pub(super) fn score_leaf_minima_with_scorer(&self,query:&[f32],leaves:&[(NodeId,f32)],navigate_nanos:u64,scorer:Option<OneBitScorer>)
        ->Result<(Vec<(u32,f32)>,usize,usize,SearchTimings,bool), &'static str> {
        let leaves_scanned = leaves.len();
        let padded_bytes = self.padded_bytes();
        let code_size = self.code_size();
        let q_norm = Self::vec_norm(query);

        let mut results: Vec<(u32, f32)> = Vec::new();
        let mut quantize_nanos = 0u64;
        let mut distance_nanos = 0u64;
        let mut nonfinite=false;
        let mut nonempty_leaves_scored=0;

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

            nonempty_leaves_scored+=1;
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
            if let Some(overlay)=self.posting_overlay.as_ref() {
                for &id in &leaf.ids {let dist=overlay.distance(leaf_id,id,self.dim,&self.distance_fn,&r_q,c_norm,c_dot_q,q_norm)?;results.push((id,dist));}
            } else if let Some(scorer)=scorer {
                if leaf.ids.len().checked_mul(code_size)!=Some(leaf.codes.len()) {return Err("one-bit posting ID/code count mismatch");}
                scorer.for_each_distance(&leaf.codes,&qq,&self.distance_fn,|i,dist| {
                    nonfinite|=!dist.is_finite();results.push((leaf.ids[i],dist));
                })?;
            } else {
                for (i, &id) in leaf.ids.iter().enumerate() {
                    let dist = Code::<1, _>::new(code_slice(&leaf.codes, i, code_size))
                        .distance_quantized_query(&self.distance_fn, &qq);
                    nonfinite|=!dist.is_finite();
                    results.push((id, dist));
                }
            }
            distance_nanos += dt0.elapsed().as_nanos() as u64;
        }

        let sort_t0 = Instant::now();
        // Reassignment can leave several physical copies even when the target
        // replica count is one. Budget distinct live IDs before exact reranking.
        let mut best: std::collections::HashMap<u32, f32> =
            std::collections::HashMap::with_capacity(results.len());
        let posting_rows_scored_before_dedup=results.len();
        for (id, dist) in results {
            let entry = best.entry(id).or_insert(dist);
            if dist < *entry {
                *entry = dist;
            }
        }
        let scanned = best.len();
        let deduped: Vec<(u32, f32)> = best.into_iter().collect();

        Ok((deduped,scanned,leaves_scanned,SearchTimings{query_stage_trace:None,work:QueryWork{selected_leaves:leaves.len(),nonempty_leaves_scored,posting_rows_scored_before_dedup,distinct_candidates_before_trim:scanned,..Default::default()},navigate_nanos,quantize_nanos,distance_nanos,sort_dedup_nanos:sort_t0.elapsed().as_nanos() as u64,rerank_nanos:0},nonfinite))
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
        let (lazy, scanned, leaves, timings) = reader
            .search_with_policy_lazy(&query, 4, 2, &ReadBeamPolicy::uniform(Some(2.0), 10, 256))
            .await
            .unwrap();
        assert_eq!(scanned, 7);
        assert_eq!(lazy, hits);
        assert_eq!(leaves,1);
        assert_eq!(timings.work.selected_leaves,1);
        assert_eq!(timings.work.nonempty_leaves_scored,1);
        assert_eq!(timings.work.posting_rows_scored_before_dedup,39);
        assert_eq!(timings.work.distinct_candidates_before_trim,7);
        assert_eq!(timings.work.candidates_after_trim,7);
        assert_eq!(timings.work.exact_embedding_gets,7);
        assert_eq!(timings.work.exact_vectors_reranked,7);
        let (small,distinct,_,work)=reader.search_with_policy_lazy(&query,2,2,&ReadBeamPolicy::uniform(Some(2.0),10,256)).await.unwrap();
        assert_eq!(distinct,7);
        assert_eq!(work.work.posting_rows_scored_before_dedup,39);
        assert_eq!(work.work.distinct_candidates_before_trim,7);
        assert_eq!(work.work.candidates_after_trim,4);
        assert_eq!(work.work.exact_embedding_gets,4);
        assert_eq!(work.work.exact_vectors_reranked,4);
        assert_eq!(small.len(),2);
        let (traced,distinct,_,timings)=reader.search_with_policy_lazy_traced(&query,2,2,&ReadBeamPolicy::uniform(Some(2.0),10,256)).await.unwrap();
        assert_eq!(traced,small);assert_eq!(distinct,7);
        let trace=timings.query_stage_trace.unwrap();assert!(trace.complete);assert_eq!(trace.selected_leaf_ids,vec![reader.root_id]);
        assert_eq!(trace.pretrim_candidate_ids.len(),7);assert_eq!(trace.retained_candidate_ids.len(),4);
        assert_eq!(trace.pretrim_candidate_ids.iter().copied().collect::<std::collections::BTreeSet<_>>(),[0,2,3,4,5,6,7].into_iter().collect());
        assert!(reader.score_leaves_with_trace(&query,2,2,&[(reader.root_id,0.)],0,Some(2)).is_err());
    }
}

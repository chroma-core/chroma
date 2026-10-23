//! Serial borrowed-posting scoring with explicit owned-state limits.
use super::super::{
    common::{code_slice, NodeId, TreeNode},
    persistance::{PREFIX_CENTROID, PREFIX_VERSION},
    writer::DELETED_BIT,
};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use serde::{Deserialize, Serialize};
use simsimd::SpatialSimilarity;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamingLimits {
    pub max_leaf_entries: usize,
    pub max_total_entries: usize,
    pub max_total_posting_bytes: usize,
    pub max_unique_candidates: usize,
}
#[derive(Default, Debug, Serialize)]
pub struct StreamingMetrics {
    pub physical_entries: usize,
    pub physical_posting_payload_bytes: usize,
    pub current_entries_scored: usize,
    pub peak_borrowed_leaf_entries: usize,
    pub owned_posting_array_bytes: usize,
    pub peak_candidate_entries: usize,
    pub peak_candidate_capacity: usize,
    pub peak_candidate_capacity_payload_bytes: usize,
    pub residual_vector_capacity_bytes: usize,
}
fn invalid(message: &str) -> Box<dyn ChromaError> {
    Box::new(ReaderIntegrityError(message.into()))
}
fn add_candidate(
    best: &mut HashMap<u32, f32>,
    id: u32,
    distance: f32,
    limit: usize,
) -> Result<(), Box<dyn ChromaError>> {
    if !distance.is_finite() {
        return Err(invalid("non-finite streaming candidate distance"));
    }
    if let Some(old) = best.get_mut(&id) {
        if distance < *old {
            *old = distance;
        }
    } else {
        if best.len() >= limit {
            return Err(invalid("streaming unique-candidate budget exceeded"));
        }
        best.try_reserve(1)
            .map_err(|_| invalid("streaming candidate allocation failed"))?;
        best.insert(id, distance);
    }
    Ok(())
}
fn finish(best: HashMap<u32, f32>, budget: usize) -> Vec<(u32, f32)> {
    let mut hits: Vec<_> = best.into_iter().collect();
    hits.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    if hits.len() > budget {
        let cutoff = hits[budget - 1].1;
        let keep = hits.iter().take_while(|(_, d)| *d <= cutoff).count();
        hits.truncate(keep);
    }
    hits
}
impl HierarchicalSpannReader {
    /// Exclusive serial CLI operation. Provider decoding/cache and quantizer
    /// scratch are outside the posting-array/candidate payload counters.
    pub async fn score_selected_leaves_streaming(
        &mut self,
        query: &[f32],
        leaves: &[NodeId],
        budget: usize,
        local_id_limit: usize,
        limits: StreamingLimits,
    ) -> Result<(Vec<(u32, f32)>, usize, StreamingMetrics), Box<dyn ChromaError>> {
        // Drop previously retained arrays; this opt-in operation never uses them.
        for mut node in self.nodes.iter_mut() {
            if let TreeNode::Leaf(leaf) = node.value_mut() {
                leaf.ids = Vec::new();
                leaf.versions = Vec::new();
                leaf.codes = Vec::new();
                leaf.centroid = Vec::new();
            }
        }
        self.clear_loaded_blocks();
        let result = self
            .stream_inner(query, leaves, budget, local_id_limit, limits)
            .await;
        // All provider borrows in stream_inner have ended, including errors.
        self.clear_loaded_blocks();
        result
    }
    async fn stream_inner(
        &self,
        query: &[f32],
        leaves: &[NodeId],
        budget: usize,
        local_id_limit: usize,
        limits: StreamingLimits,
    ) -> Result<(Vec<(u32, f32)>, usize, StreamingMetrics), Box<dyn ChromaError>> {
        if query.len() != self.dim
            || query.iter().any(|x| !x.is_finite())
            || budget == 0
            || local_id_limit > 100_000
            || leaves.is_empty()
            || leaves.len() > 100_000
            || limits.max_leaf_entries == 0
            || limits.max_leaf_entries > 100_000
            || limits.max_total_entries == 0
            || limits.max_total_entries > 1_000_000
            || limits.max_total_posting_bytes == 0
            || limits.max_total_posting_bytes > 64 * 1024 * 1024
            || limits.max_unique_candidates == 0
            || limits.max_unique_candidates > 100_000
        {
            return Err(invalid("invalid bounded streaming parameters"));
        }
        let mut selected = HashSet::new();
        for id in leaves {
            if !selected.insert(*id)
                || !self
                    .nodes
                    .get(id)
                    .is_some_and(|n| matches!(n.value(), TreeNode::Leaf(_)))
            {
                return Err(invalid("invalid or duplicate streaming leaf"));
            }
        }
        let mut metrics = StreamingMetrics::default();
        let mut best = HashMap::new();
        let size = Code::<1, Vec<u8>>::size(self.dim);
        // One request owns scalar pin lifecycle. No cached leaf.length estimate
        // substitutes for the actual persisted physical posting lengths.
        let _versions = self.version_reader_lock.lock().await;
        for leaf in leaves {
            {
                let Some(posting) = self.posting_list_reader.get("", *leaf).await? else {
                    let empty = self.nodes.get(leaf).is_some_and(
                        |node| matches!(node.value(),TreeNode::Leaf(value) if value.length==0),
                    );
                    if !empty {
                        return Err(invalid("missing nonempty streaming posting"));
                    }
                    self.clear_loaded_blocks();
                    continue;
                };
                let count = posting.ids.len();
                let codes = count
                    .checked_mul(size)
                    .ok_or_else(|| invalid("streaming size overflow"))?;
                if posting.versions.len() != count || posting.codes.len() != codes {
                    return Err(invalid("malformed streaming posting lengths"));
                }
                let bytes = count
                    .checked_mul(5)
                    .and_then(|n| n.checked_add(codes))
                    .ok_or_else(|| invalid("streaming size overflow"))?;
                metrics.physical_entries = metrics
                    .physical_entries
                    .checked_add(count)
                    .ok_or_else(|| invalid("streaming count overflow"))?;
                metrics.physical_posting_payload_bytes = metrics
                    .physical_posting_payload_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("streaming byte overflow"))?;
                if count > limits.max_leaf_entries
                    || metrics.physical_entries > limits.max_total_entries
                    || metrics.physical_posting_payload_bytes > limits.max_total_posting_bytes
                {
                    return Err(invalid("streaming physical posting budget exceeded before owned scoring allocation"));
                }
                metrics.peak_borrowed_leaf_entries = metrics.peak_borrowed_leaf_entries.max(count);
                if count == 0 {
                    self.clear_loaded_blocks();
                    continue;
                }
                let centroid = self
                    .vector_data_reader
                    .get(PREFIX_CENTROID, *leaf)
                    .await?
                    .ok_or_else(|| invalid("missing streaming centroid"))?;
                if centroid.len() != self.dim || centroid.iter().any(|x| !x.is_finite()) {
                    return Err(invalid("invalid streaming centroid"));
                }
                let residual: Vec<f32> = query.iter().zip(centroid).map(|(q, c)| q - c).collect();
                metrics.residual_vector_capacity_bytes = metrics
                    .residual_vector_capacity_bytes
                    .max(residual.capacity() * 4);
                let qq = QuantizedQuery::new(
                    &residual,
                    self.padded_bytes(),
                    Self::vec_norm(centroid),
                    f32::dot(centroid, query).unwrap_or(0.0) as f32,
                    Self::vec_norm(query),
                );
                for (i, id) in posting.ids.iter().copied().enumerate() {
                    if id as usize >= local_id_limit {
                        return Err(invalid("streaming ID outside pinned local range"));
                    }
                    let current = self
                        .scalar_metadata_reader
                        .get(PREFIX_VERSION, id)
                        .await?
                        .ok_or_else(|| invalid("missing published streaming version"))?;
                    self.scalar_metadata_reader.clear_loaded_blocks();
                    if current > u8::MAX as u32 {
                        return Err(invalid("invalid published streaming version"));
                    }
                    if (current as u8) & DELETED_BIT != 0 || posting.versions[i] != current as u8 {
                        continue;
                    }
                    let distance = Code::<1, _>::new(code_slice(posting.codes, i, size))
                        .distance_quantized_query(&self.distance_fn, &qq);
                    add_candidate(&mut best, id, distance, limits.max_unique_candidates)?;
                    metrics.current_entries_scored += 1;
                    metrics.peak_candidate_entries = metrics.peak_candidate_entries.max(best.len());
                    metrics.peak_candidate_capacity =
                        metrics.peak_candidate_capacity.max(best.capacity());
                    metrics.peak_candidate_capacity_payload_bytes =
                        metrics.peak_candidate_capacity * std::mem::size_of::<(u32, f32)>();
                }
            }
            // A provider may retain decoded blocks in its independent cache.
            self.clear_loaded_blocks();
        }
        let unique = best.len();
        Ok((finish(best, budget), unique, metrics))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_minima_late_improvement_all_ties_and_overflow() {
        let mut best = HashMap::new();
        for (id, d) in [(0, 9.), (1, 2.), (2, 2.), (0, 1.), (1, 3.)] {
            add_candidate(&mut best, id, d, 3).unwrap();
        }
        assert!(add_candidate(&mut best, 3, 0., 3).is_err());
        assert!(add_candidate(&mut best, 0, f32::NAN, 3).is_err());
        assert_eq!(finish(best, 2), vec![(0, 1.), (1, 2.), (2, 2.)]);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_stale_deleted_duplicates_ties_and_budget_cleanup() {
        use super::super::super::{
            config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter,
        };
        use chroma_blockstore::{
            arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider,
        };
        use chroma_cache::new_cache_for_test;
        use chroma_distance::DistanceFunction;
        use chroma_storage::{local::LocalStorage, Storage};
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
            writer.add(id, &[2., 1., 0., 0.]);
        }
        writer.add(0, &[20., 1., 0., 0.]);
        writer.delete(1);
        let root = writer.root_id.load(std::sync::atomic::Ordering::Relaxed);
        {
            let mut node = writer.nodes.get_mut(&root).unwrap();
            let TreeNode::Leaf(leaf) = node.value_mut() else {
                panic!("root leaf");
            };
            let i = leaf.ids.iter().position(|id| *id == 2).unwrap();
            let stride = leaf.codes.len() / leaf.ids.len();
            let code = leaf.codes[i * stride..(i + 1) * stride].to_vec();
            leaf.ids.push(2);
            leaf.versions.push(leaf.versions[i]);
            leaf.codes.extend_from_slice(&code);
            leaf.length = leaf.ids.len();
        }
        let ids = writer
            .commit(&provider, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let mut reader =
            HierarchicalSpannReader::open(&provider, ids, DistanceFunction::Euclidean, config)
                .await
                .unwrap();
        let query = [2., 1., 0., 0.];
        let limits = StreamingLimits {
            max_leaf_entries: 100,
            max_total_entries: 100,
            max_total_posting_bytes: 4096,
            max_unique_candidates: 8,
        };
        let (mut oracle, unique) = reader
            .score_selected_leaves(&query, &[root], 1)
            .await
            .unwrap();
        oracle.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let (actual, count, metrics) = reader
            .score_selected_leaves_streaming(&query, &[root], 1, 8, limits)
            .await
            .unwrap();
        println!(
            "STREAMING_PERSISTED_PROOF {}",
            serde_json::json!({"actual":actual,"oracle":oracle,"unique":count,"metrics":metrics,"expected_deleted_id":1,"fixture":"eight original vectors, stale replacement of id0, deleted id1, duplicate-current id2"})
        );
        assert_eq!(actual, oracle);
        assert_eq!(unique, count);
        assert_eq!(count, 7);
        assert!(actual.len() > 1);
        assert!(!actual.iter().any(|(id, _)| *id == 1));
        assert!(metrics.physical_entries > metrics.peak_candidate_entries);
        assert_eq!(metrics.owned_posting_array_bytes, 0);
        assert_eq!(reader.loaded_postings_stats(), (0, 0));
        for limited in [
            StreamingLimits {
                max_leaf_entries: 1,
                ..limits
            },
            StreamingLimits {
                max_unique_candidates: 1,
                ..limits
            },
            StreamingLimits {
                max_total_posting_bytes: 1,
                ..limits
            },
        ] {
            assert!(reader
                .score_selected_leaves_streaming(&query, &[root], 1, 8, limited)
                .await
                .is_err());
            assert_eq!(reader.loaded_postings_stats(), (0, 0));
            assert_eq!(reader.loaded_blocks_stats(), ((0, 0), (0, 0)));
            assert_eq!(reader.scalar_metadata_reader.loaded_blocks_stats(), (0, 0));
        }
        assert!(reader
            .score_selected_leaves_streaming(&query, &[root, root], 1, 8, limits)
            .await
            .is_err());
        assert_eq!(
            reader
                .score_selected_leaves_streaming(&query, &[root], 1, 8, limits)
                .await
                .unwrap()
                .0,
            actual
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_persisted_leaf_without_centroid_matches_default() {
        use super::super::super::{
            config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter,
        };
        use chroma_blockstore::{
            arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider,
            BlockfileWriterOptions,
        };
        use chroma_cache::new_cache_for_test;
        use chroma_distance::DistanceFunction;
        use chroma_storage::{local::LocalStorage, Storage};
        for explicit_empty_posting in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let provider = BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
                Storage::Local(LocalStorage::new(directory.path().to_str().unwrap())),
                3 * 1024 * 1024,
                new_cache_for_test(),
                new_cache_for_test(),
                16,
            ));
            let config = HierarchicalSpannConfig::default();
            let writer =
                HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
            let root = writer.root_id.load(std::sync::atomic::Ordering::Relaxed);
            let mut ids = writer
                .commit(&provider, None)
                .await
                .unwrap()
                .flush()
                .await
                .unwrap();
            let mut data = provider
                .write::<u32, Vec<f32>>(
                    BlockfileWriterOptions::new(String::new()).fork_from(ids.vector_data_id),
                )
                .await
                .unwrap();
            data.delete::<_, Vec<f32>>(PREFIX_CENTROID, root)
                .await
                .unwrap();
            let flushed = data.commit::<u32, Vec<f32>>().await.unwrap();
            ids.vector_data_id = flushed.id();
            flushed.flush::<u32, Vec<f32>>().await.unwrap();
            if explicit_empty_posting {
                use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
                let mut postings = provider
                    .write::<u32, HierarchicalSpannPostingList<'_>>(
                        BlockfileWriterOptions::new(String::new()).fork_from(ids.posting_list_id),
                    )
                    .await
                    .unwrap();
                postings
                    .set(
                        "",
                        root,
                        HierarchicalSpannPostingList {
                            ids: &[],
                            versions: &[],
                            codes: &[],
                        },
                    )
                    .await
                    .unwrap();
                let flushed = postings
                    .commit::<u32, HierarchicalSpannPostingList<'_>>()
                    .await
                    .unwrap();
                ids.posting_list_id = flushed.id();
                flushed
                    .flush::<u32, HierarchicalSpannPostingList<'_>>()
                    .await
                    .unwrap();
            }
            let mut reader =
                HierarchicalSpannReader::open(&provider, ids, DistanceFunction::Euclidean, config)
                    .await
                    .unwrap();
            assert!(reader
                .vector_data_reader
                .get(PREFIX_CENTROID, root)
                .await
                .unwrap()
                .is_none());
            let oracle = reader
                .score_selected_leaves(&[0.; 4], &[root], 1)
                .await
                .unwrap();
            let limits = StreamingLimits {
                max_leaf_entries: 1,
                max_total_entries: 1,
                max_total_posting_bytes: 1,
                max_unique_candidates: 1,
            };
            let (actual, count, metrics) = reader
                .score_selected_leaves_streaming(&[0.; 4], &[root], 1, 0, limits)
                .await
                .unwrap();
            assert_eq!((actual, count), oracle);
            assert_eq!(count, 0);
            assert_eq!(metrics.physical_entries, 0);
            assert_eq!(reader.loaded_blocks_stats(), ((0, 0), (0, 0)));
            println!(
                "STREAMING_EMPTY_PROOF {}",
                serde_json::json!({"explicit_empty_posting":explicit_empty_posting,"centroid_absent":true,"unique":count,"metrics":metrics})
            );
        }
    }
}

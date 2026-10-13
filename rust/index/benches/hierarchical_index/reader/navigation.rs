use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::quantization::{Code, PackedFourBitCentroid};
use rayon::prelude::*;

use super::super::common::NodeId;
use super::super::persistance::PREFIX_CENTROID;
use super::HierarchicalSpannReader;

#[derive(Debug, thiserror::Error)]
enum NavigationCentroidError {
    #[error("node {0} has no persisted centroid")]
    Missing(NodeId),
    #[error("centroid preparation task failed: {0}")]
    Preparation(#[from] tokio::task::JoinError),
    #[error("node {node} centroid has {actual} dimensions; expected {expected}")]
    Dimension {
        node: NodeId,
        actual: usize,
        expected: usize,
    },
}

impl ChromaError for NavigationCentroidError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::Preparation(_) => ErrorCodes::Internal,
            _ => ErrorCodes::DataLoss,
        }
    }
}

impl HierarchicalSpannReader {
    /// Read original centroids once and prepare immutable scoring payloads.
    /// Preparing at open supports existing checkpoints. Persisting four-bit
    /// centroids can replace this step when the checkpoint format supports it.
    pub(super) async fn prepare_navigation_centroids(
        &mut self,
    ) -> Result<(), Box<dyn ChromaError>> {
        let centroids = self
            .vector_data_reader
            .get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..)
            .await?;
        let mut owned = Vec::with_capacity(self.nodes.len());
        for (_, id, centroid) in centroids {
            if !self.nodes.contains_key(&id) {
                continue;
            }
            if centroid.len() != self.dim {
                return Err(Box::new(NavigationCentroidError::Dimension {
                    node: id,
                    actual: centroid.len(),
                    expected: self.dim,
                }));
            }
            owned.push((id, centroid.to_vec()));
        }
        // Original centroids have been copied; release their block pins before
        // CPU preparation so the source blocks need not survive quantization.
        self.vector_data_reader.clear_loaded_blocks();
        let dim = self.dim;
        self.navigation_centroids = tokio::task::spawn_blocking(move || {
            let zero = vec![0.0; dim];
            owned
                .into_par_iter()
                .map(|(id, centroid)| {
                    let code = Code::<4>::quantize(&centroid, &zero);
                    (id, PackedFourBitCentroid::from_origin_code(&code, dim))
                })
                .collect()
        })
        .await
        .map_err(|e| Box::new(NavigationCentroidError::Preparation(e)) as Box<dyn ChromaError>)?;
        for node in self.nodes.iter() {
            if !self.navigation_centroids.contains_key(node.key()) {
                return Err(Box::new(NavigationCentroidError::Missing(*node.key())));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchical_index::common::{effective_beam, ReadBeamPolicy, TreeNode};
    use crate::hierarchical_index::config::HierarchicalSpannConfig;
    use crate::hierarchical_index::writer::{HierarchicalSpannIds, HierarchicalSpannWriter};
    use chroma_blockstore::provider::BlockfileProvider;
    use chroma_cache::new_cache_for_test;
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};
    use simsimd::SpatialSimilarity;

    async fn saved_reader(
        dim: usize,
        count: u32,
    ) -> (
        tempfile::TempDir,
        HierarchicalSpannReader,
        BlockfileProvider,
        HierarchicalSpannIds,
    ) {
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
            branching_factor: 4,
            split_threshold: 32,
            merge_threshold: 0,
            max_replicas: 1,
            write_rng_factor: 0.0,
            ..Default::default()
        };
        let writer = HierarchicalSpannWriter::new(dim, DistanceFunction::Euclidean, config.clone());
        for id in 0..count {
            let embedding: Vec<_> = (0..dim)
                .map(|d| ((id * 17 + d as u32 * 7) as f32 * 0.3).sin() + (id / 32) as f32)
                .collect();
            writer.add(id, &embedding);
        }
        writer.balance_index_parallel(1);
        let ids = Box::pin(
            Box::pin(writer.commit(&provider, None))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let reader = HierarchicalSpannReader::open(
            &provider,
            ids.clone(),
            DistanceFunction::Euclidean,
            config,
        )
        .await
        .unwrap();
        (dir, reader, provider, ids)
    }

    async fn scalar_navigation(
        reader: &HierarchicalSpannReader,
        query: &[f32],
        policy: &ReadBeamPolicy,
    ) -> Vec<NodeId> {
        if matches!(
            reader.nodes.get(&reader.root_id).unwrap().value(),
            TreeNode::Leaf(_)
        ) {
            return vec![reader.root_id];
        }
        let norm = (f32::dot(query, query).unwrap() as f32).sqrt();
        let mut beam = vec![reader.root_id];
        let mut leaves = Vec::new();
        let mut level = 0;
        loop {
            let mut scored = Vec::new();
            for &id in &beam {
                let children = match reader.nodes.get(&id).unwrap().value() {
                    TreeNode::Internal(node) => node.children().to_vec(),
                    _ => Vec::new(),
                };
                for child in children {
                    let centroid = reader
                        .vector_data_reader
                        .get(PREFIX_CENTROID, child)
                        .await
                        .unwrap()
                        .unwrap();
                    let code = Code::<4>::quantize(centroid, &vec![0.0; reader.dim]);
                    let distance = code.distance_query(&reader.distance_fn, query, 0.0, 0.0, norm);
                    scored.push((child, distance));
                }
            }
            if scored.is_empty() {
                break;
            }
            level += 1;
            let params = policy.level_params(level);
            scored.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
            scored.truncate(effective_beam(
                &scored,
                params.tau,
                params.beam_min,
                params.beam_max,
            ));
            beam.clear();
            for (id, distance) in scored {
                if matches!(reader.nodes.get(&id).unwrap().value(), TreeNode::Leaf(_)) {
                    leaves.push((id, distance));
                } else {
                    beam.push(id);
                }
            }
            if beam.is_empty() {
                break;
            }
        }
        leaves.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
        leaves.into_iter().map(|(id, _)| id).collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn existing_checkpoints_prepare_navigation_and_preserve_scalar_fp_choices() {
        let (_dir, reader, provider, ids) = saved_reader(17, 128).await;
        assert_eq!(reader.navigation_centroids.len(), reader.nodes.len());
        assert!(reader
            .nodes
            .iter()
            .all(|n| n.centroid_code().len() == Code::<1>::size(17)));
        assert!(reader.nodes.iter().all(|n| match n.value() {
            TreeNode::Leaf(leaf) => leaf.ids.is_empty(),
            _ => true,
        }));
        let query: Vec<_> = (0..17).map(|d| (d as f32 * 0.5).cos()).collect();
        for policy in [
            ReadBeamPolicy::uniform(None, 2, 2),
            ReadBeamPolicy::uniform(Some(0.7), 1, 16),
            ReadBeamPolicy::uniform(Some(2.0), 1, 32),
        ] {
            let expected = scalar_navigation(&reader, &query, &policy).await;
            assert_eq!(
                reader
                    .navigate(&query, &policy)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>(),
                expected
            );
            let (_, scanned, leaves, _) = reader
                .search_with_policy_lazy(&query, 10, 2, &policy)
                .await
                .unwrap();
            assert_eq!(leaves, expected.len());
            assert!(scanned > 0);
            reader.clear_loaded_postings();
            reader.clear_loaded_blocks();
            assert_eq!(
                reader
                    .navigate(&query, &policy)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let reopened = HierarchicalSpannReader::open(
            &provider,
            ids,
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        let policy = ReadBeamPolicy::uniform(None, 2, 2);
        assert_eq!(
            reopened.navigate(&query, &policy),
            reader.navigate(&query, &policy)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_leaf_and_missing_or_malformed_centroids_are_handled() {
        let (_dir, mut reader, _provider, _ids) = saved_reader(7, 8).await;
        assert_eq!(
            reader.navigate(&[0.0; 7], &ReadBeamPolicy::uniform(None, 1, 1)),
            vec![(reader.root_id, 0.0)]
        );
        reader.dim += 1;
        let error = reader.prepare_navigation_centroids().await.unwrap_err();
        assert_eq!(error.code(), ErrorCodes::DataLoss);
        assert!(error.to_string().contains("dimensions"));
        reader.dim -= 1;
        let node = reader.nodes.remove(&reader.root_id).unwrap().1;
        reader.nodes.insert(1234567, node);
        let error = reader.prepare_navigation_centroids().await.unwrap_err();
        assert_eq!(error.code(), ErrorCodes::DataLoss);
        assert!(error.to_string().contains("no persisted centroid"));
    }
}

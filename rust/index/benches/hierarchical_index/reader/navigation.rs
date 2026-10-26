use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::quantization::{
    Code, PackedCentroidQuery, PackedFourBitCentroid, PackedFourBitCentroidBatch,
    PackedFourBitScorer,
};
use rayon::prelude::*;
use std::collections::HashMap;

use super::super::common::{NodeId, TreeNode};
use super::super::persistance::PREFIX_CENTROID;
use super::HierarchicalSpannReader;

/// Immutable child order and contiguous codes prepared once at reader open.
pub(super) struct ChildCentroids {
    ids: Vec<NodeId>,
    codes: PackedFourBitCentroidBatch,
}

impl ChildCentroids {
    fn new(children: Vec<(NodeId, PackedFourBitCentroid)>, dim: usize) -> Self {
        let (ids, codes) = children.into_iter().unzip();
        Self {
            ids,
            codes: PackedFourBitCentroidBatch::new(codes, dim),
        }
    }

    pub(super) fn payload_bytes(&self) -> usize {
        self.codes.payload_bytes() + self.ids.len() * std::mem::size_of::<NodeId>()
    }

    pub(super) fn scores<'a>(
        &'a self,
        scorer: &'a PackedFourBitScorer,
        query: &'a PackedCentroidQuery<'_>,
        metric: &'a DistanceFunction,
    ) -> impl Iterator<Item = (NodeId, f32)> + 'a {
        self.ids
            .iter()
            .copied()
            .zip(self.codes.distances(scorer, query, metric))
    }
}

#[derive(Debug, thiserror::Error)]
enum NavigationCentroidError {
    #[error("node {0} has no persisted centroid")]
    Missing(NodeId),
    #[error("parent {parent} references missing child {child}")]
    MissingChild { parent: NodeId, child: NodeId },
    #[error("child {child} is referenced more than once by parents {first_parent} and {parent}")]
    DuplicateChild {
        child: NodeId,
        first_parent: NodeId,
        parent: NodeId,
    },
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

fn pack_children(
    parents: Vec<(NodeId, Vec<NodeId>)>,
    mut codes: HashMap<NodeId, PackedFourBitCentroid>,
    dim: usize,
) -> Result<HashMap<NodeId, ChildCentroids>, NavigationCentroidError> {
    let mut owners = HashMap::new();
    let mut prepared = HashMap::with_capacity(parents.len());
    for (parent, ids) in parents {
        let children = ids
            .into_iter()
            .map(|child| {
                if let Some(first_parent) = owners.insert(child, parent) {
                    return Err(NavigationCentroidError::DuplicateChild {
                        child,
                        first_parent,
                        parent,
                    });
                }
                let code = codes
                    .remove(&child)
                    .ok_or(NavigationCentroidError::MissingChild { parent, child })?;
                Ok((child, code))
            })
            .collect::<Result<Vec<_>, _>>()?;
        prepared.insert(parent, ChildCentroids::new(children, dim));
    }
    Ok(prepared)
}

impl HierarchicalSpannReader {
    /// Prepare legacy checkpoints from their original centroids.
    /// New checkpoints load the writer's packed codes directly.
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
        let prepared = tokio::task::spawn_blocking(move || {
            let zero = vec![0.0; dim];
            let codes: HashMap<_, _> = owned
                .into_par_iter()
                .map(|(id, centroid)| {
                    let code = Code::<4>::quantize(&centroid, &zero);
                    (id, PackedFourBitCentroid::from_origin_code(&code, dim))
                })
                .collect();
            codes
        })
        .await
        .map_err(|e| Box::new(NavigationCentroidError::Preparation(e)) as Box<dyn ChromaError>)?;
        self.install_navigation_centroids(prepared)
    }

    pub(super) fn install_navigation_centroids(
        &mut self,
        codes: HashMap<NodeId, PackedFourBitCentroid>,
    ) -> Result<(), Box<dyn ChromaError>> {
        let parents: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|node| match node.value() {
                TreeNode::Internal(internal) => Some((*node.key(), internal.children().to_vec())),
                TreeNode::Leaf(_) => None,
            })
            .collect();
        let node_ids: Vec<_> = self.nodes.iter().map(|node| *node.key()).collect();
        for id in node_ids {
            if !codes.contains_key(&id) {
                return Err(Box::new(NavigationCentroidError::Missing(id)));
            }
        }
        self.navigation_children = pack_children(parents, codes, self.dim)
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
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

    #[test]
    fn level_beam_maxes_cap_their_level_only() {
        let policy = ReadBeamPolicy::uniform(None, 512, 512).with_level_beam_maxes(vec![0, 64]);
        let caps: Vec<(usize, usize)> = (1..=3)
            .map(|l| {
                let p = policy.level_params(l);
                (p.beam_min, p.beam_max)
            })
            .collect();
        assert_eq!(caps, vec![(512, 512), (64, 64), (512, 512)]);
        let floors = ReadBeamPolicy::uniform(None, 512, 4096).with_level_beam_mins(vec![0, 16]);
        assert_eq!(floors.level_params(2).beam_min, 16);
        assert_eq!(floors.level_params(1).beam_min, 512);
    }

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

    // Build the exact legacy layout, including unmodified one-bit node values.
    async fn legacy_checkpoint(
        provider: &BlockfileProvider,
        ids: &HierarchicalSpannIds,
        dim: usize,
    ) -> HierarchicalSpannIds {
        use crate::hierarchical_index::persistance::{PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY};
        use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, BlockfileWriterOptions};
        use chroma_types::hierarchical_spann::{HierarchicalInternalNode, HierarchicalLeafNode};
        let original = provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(ids.vector_data_id, "".into()))
            .await
            .unwrap();
        let leaves = provider
            .read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
                ids.leaf_node_id,
                "".into(),
            ))
            .await
            .unwrap();
        let internals = provider
            .read::<u32, HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(
                ids.internal_node_id,
                "".into(),
            ))
            .await
            .unwrap();
        let metadata = provider
            .write::<u32, u32>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.scalar_metadata_id),
            )
            .await
            .unwrap();
        metadata
            .delete::<_, u32>(PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY)
            .await
            .unwrap();
        let leaf_writer = provider
            .write::<u32, HierarchicalLeafNode<'_>>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.leaf_node_id),
            )
            .await
            .unwrap();
        let internal_writer = provider
            .write::<u32, HierarchicalInternalNode<'_>>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.internal_node_id),
            )
            .await
            .unwrap();
        let zero = vec![0.0; dim];
        for (_, id, leaf) in leaves.get_range(""..="", ..).await.unwrap() {
            let centroid = original.get(PREFIX_CENTROID, id).await.unwrap().unwrap();
            let code = Code::<1>::quantize(centroid, &zero);
            leaf_writer
                .set(
                    "",
                    id,
                    HierarchicalLeafNode {
                        centroid_code: code.as_ref(),
                        ..leaf
                    },
                )
                .await
                .unwrap();
        }
        for (_, id, internal) in internals.get_range(""..="", ..).await.unwrap() {
            let centroid = original.get(PREFIX_CENTROID, id).await.unwrap().unwrap();
            let code = Code::<1>::quantize(centroid, &zero);
            internal_writer
                .set(
                    "",
                    id,
                    HierarchicalInternalNode {
                        centroid_code: code.as_ref(),
                        ..internal
                    },
                )
                .await
                .unwrap();
        }
        let metadata = metadata.commit::<u32, u32>().await.unwrap();
        let leaf = leaf_writer
            .commit::<u32, HierarchicalLeafNode<'_>>()
            .await
            .unwrap();
        let internal = internal_writer
            .commit::<u32, HierarchicalInternalNode<'_>>()
            .await
            .unwrap();
        let result = HierarchicalSpannIds {
            scalar_metadata_id: metadata.id(),
            leaf_node_id: leaf.id(),
            internal_node_id: internal.id(),
            ..ids.clone()
        };
        metadata.flush::<u32, u32>().await.unwrap();
        leaf.flush::<u32, HierarchicalLeafNode<'_>>().await.unwrap();
        internal
            .flush::<u32, HierarchicalInternalNode<'_>>()
            .await
            .unwrap();
        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn packed_checkpoint_navigation_needs_no_original_centroids() {
        use chroma_blockstore::BlockfileWriterOptions;
        let (_dir, reader, provider, mut ids) = saved_reader(17, 128).await;
        let vectors = provider
            .write::<u32, Vec<f32>>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.vector_data_id),
            )
            .await
            .unwrap();
        let mut node_ids: Vec<_> = reader.nodes.iter().map(|n| *n.key()).collect();
        node_ids.sort_unstable();
        for id in node_ids {
            vectors
                .delete::<_, Vec<f32>>(PREFIX_CENTROID, id)
                .await
                .unwrap();
        }
        let vectors = vectors.commit::<u32, Vec<f32>>().await.unwrap();
        ids.vector_data_id = vectors.id();
        vectors.flush::<u32, Vec<f32>>().await.unwrap();
        let reopened = HierarchicalSpannReader::open(
            &provider,
            ids,
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        assert_eq!(reopened.vector_data_reader.loaded_blocks_stats(), (0, 0));
        assert!(reopened.nodes.iter().all(|n| n.centroid_code().is_empty()));
        let policy = ReadBeamPolicy::uniform(None, 2, 2);
        assert_eq!(
            reopened.navigate(&[0.5; 17], &policy),
            reader.navigate(&[0.5; 17], &policy)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_upgrade_includes_unchanged_nodes_and_keeps_postings_lazy() {
        use crate::hierarchical_index::persistance::{
            PACKED_NAVIGATION_FORMAT, PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY,
        };
        let (_dir, reader, provider, ids) = saved_reader(17, 128).await;
        let legacy = Box::pin(legacy_checkpoint(&provider, &ids, 17)).await;
        let legacy_reader = HierarchicalSpannReader::open(
            &provider,
            legacy.clone(),
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        let policy = ReadBeamPolicy::uniform(Some(2.0), 1, 32);
        let query = vec![0.5; 17];
        assert_eq!(
            reader.navigate(&query, &policy),
            legacy_reader.navigate(&query, &policy)
        );
        let writer = HierarchicalSpannWriter::open(
            &provider,
            legacy.clone(),
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        assert!(writer.dirty_nodes.is_empty());
        assert!(writer
            .nodes
            .iter()
            .all(|n| n.centroid_code().len() == Code::<1>::size(17)));
        let upgraded = Box::pin(
            Box::pin(writer.commit(&provider, Some(&legacy)))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        assert_eq!(writer.reader_block_pin_stats().0, (0, 0));
        let metadata = provider
            .read::<u32, u32>(
                chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                    upgraded.scalar_metadata_id,
                    "".into(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            metadata
                .get(PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY)
                .await
                .unwrap(),
            Some(PACKED_NAVIGATION_FORMAT)
        );
        let reopened = HierarchicalSpannReader::open(
            &provider,
            upgraded.clone(),
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        assert_eq!(reopened.vector_data_reader.loaded_blocks_stats(), (0, 0));
        assert_eq!(
            reader.navigate(&query, &policy),
            reopened.navigate(&query, &policy)
        );
        assert_eq!(
            reader
                .search_with_policy_lazy(&query, 10, 2, &policy)
                .await
                .unwrap()
                .0,
            reopened
                .search_with_policy_lazy(&query, 10, 2, &policy)
                .await
                .unwrap()
                .0
        );
        // Advance the upgraded tree to exercise dirty nodes, new splits, and tombstones.
        let writer = HierarchicalSpannWriter::open(
            &provider,
            upgraded.clone(),
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        for id in 128..256 {
            writer.add(id, &[id as f32 * 0.01; 17]);
        }
        writer.balance_index_parallel(1);
        let next = Box::pin(
            Box::pin(writer.commit(&provider, Some(&upgraded)))
                .await
                .unwrap()
                .flush(),
        )
        .await
        .unwrap();
        let next = HierarchicalSpannReader::open(
            &provider,
            next,
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            next.navigate(&query, &policy)
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            scalar_navigation(&next, &query, &policy).await
        );
        next.load_all_postings().await.unwrap();
        let ids: std::collections::BTreeSet<_> = next
            .nodes
            .iter()
            .flat_map(|n| match n.value() {
                TreeNode::Leaf(l) => l.ids.clone(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(ids, (0..256).collect());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_persisted_code_returns_data_loss_without_quantizing() {
        use chroma_blockstore::BlockfileWriterOptions;
        use chroma_types::hierarchical_spann::HierarchicalLeafNode;
        let (_dir, reader, provider, mut ids) = saved_reader(7, 8).await;
        let leaf = provider
            .write::<u32, HierarchicalLeafNode<'_>>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.leaf_node_id),
            )
            .await
            .unwrap();
        leaf.set(
            "",
            reader.root_id,
            HierarchicalLeafNode {
                parent: crate::hierarchical_index::persistance::NO_PARENT,
                length: 8,
                centroid_code: &[0; 3],
            },
        )
        .await
        .unwrap();
        let leaf = leaf
            .commit::<u32, HierarchicalLeafNode<'_>>()
            .await
            .unwrap();
        ids.leaf_node_id = leaf.id();
        leaf.flush::<u32, HierarchicalLeafNode<'_>>().await.unwrap();
        let error = match HierarchicalSpannReader::open(
            &provider,
            ids,
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("truncated packed code accepted"),
        };
        assert_eq!(error.code(), ErrorCodes::DataLoss);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_navigation_formats_fail_before_reading_codes() {
        use crate::hierarchical_index::persistance::{PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY};
        use chroma_blockstore::BlockfileWriterOptions;
        let (_dir, reader, provider, mut ids) = saved_reader(7, 8).await;
        let metadata = provider
            .write::<u32, u32>(
                BlockfileWriterOptions::new("".into())
                    .ordered_mutations()
                    .fork(ids.scalar_metadata_id),
            )
            .await
            .unwrap();
        metadata
            .set(PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY, 999)
            .await
            .unwrap();
        let metadata = metadata.commit::<u32, u32>().await.unwrap();
        ids.scalar_metadata_id = metadata.id();
        metadata.flush::<u32, u32>().await.unwrap();
        let error = match HierarchicalSpannReader::open(
            &provider,
            ids.clone(),
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("unknown format accepted"),
        };
        assert_eq!(error.code(), ErrorCodes::DataLoss);
        let error = match HierarchicalSpannWriter::open(
            &provider,
            ids,
            DistanceFunction::Euclidean,
            reader.config.clone(),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("unknown format accepted"),
        };
        assert_eq!(error.code(), ErrorCodes::DataLoss);
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

    #[test]
    fn child_references_report_duplicates_and_missing_nodes() {
        let make_codes = || {
            let code = Code::<4>::quantize(&[1.0; 7], &[0.0; 7]);
            HashMap::from([(42, PackedFourBitCentroid::from_origin_code(&code, 7))])
        };
        for parents in [vec![(1, vec![42, 42])], vec![(1, vec![42]), (2, vec![42])]] {
            let error = match pack_children(parents, make_codes(), 7) {
                Err(error) => error,
                Ok(_) => panic!("duplicate child must be rejected"),
            };
            assert_eq!(error.code(), ErrorCodes::DataLoss);
            assert!(matches!(
                error,
                NavigationCentroidError::DuplicateChild {
                    child: 42,
                    first_parent: 1,
                    ..
                }
            ));
            assert!(error.to_string().contains("referenced more than once"));
        }
        let error = match pack_children(vec![(1, vec![99])], make_codes(), 7) {
            Err(error) => error,
            Ok(_) => panic!("missing child must be rejected"),
        };
        assert_eq!(error.code(), ErrorCodes::DataLoss);
        assert!(matches!(
            error,
            NavigationCentroidError::MissingChild {
                parent: 1,
                child: 99
            }
        ));
        assert_eq!(error.to_string(), "parent 1 references missing child 99");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn existing_checkpoints_prepare_navigation_and_preserve_scalar_fp_choices() {
        let (_dir, reader, provider, ids) = saved_reader(17, 128).await;
        assert_eq!(
            reader.navigation_children.len(),
            reader
                .nodes
                .iter()
                .filter(|n| matches!(n.value(), TreeNode::Internal(_)))
                .count()
        );
        for parent in reader.nodes.iter() {
            if let TreeNode::Internal(internal) = parent.value() {
                assert_eq!(
                    reader.navigation_children[parent.key()].ids,
                    internal.children()
                );
            }
        }
        assert!(reader.nodes.iter().all(|n| n.centroid_code().is_empty()));
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

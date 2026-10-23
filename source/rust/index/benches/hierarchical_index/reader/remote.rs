//! One-level traversal and candidate operations for a coordinator that owns the beam.
use super::super::{
    common::{NodeId, TreeNode},
    persistance::{PREFIX_CENTROID, PREFIX_EMBEDDING},
};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Serialize;
use std::{collections::HashSet, time::Instant};

#[derive(Debug, Serialize)]
pub struct RootDescriptor {
    pub id: NodeId,
    pub node_id: NodeId,
    pub kind: &'static str,
    pub is_leaf: bool,
    pub level: usize,
}

#[cfg(test)]
mod rerank_pin_contract {
    use super::*;
    use crate::hierarchical_index::{config::HierarchicalSpannConfig,writer::HierarchicalSpannWriter};
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider,provider::BlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage,Storage};

    async fn fixture() -> (tempfile::TempDir,HierarchicalSpannReader,HierarchicalSpannReader) {
        let directory=tempfile::tempdir().unwrap();
        let provider=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(directory.path().to_str().unwrap())),1024,
            new_cache_for_test(),new_cache_for_test(),2));
        let mut config=HierarchicalSpannConfig::default();
        config.branching_factor=2;config.split_threshold=4;config.merge_threshold=1;
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        for id in 0..16 {let mut vector=vec![0.;32];vector[0]=id as f32;vector[1]=1.;writer.add(id,&vector);}
        writer.balance_index_parallel(2);
        let ids=writer.commit(&provider,None).await.unwrap().flush().await.unwrap();
        // Fresh plain caches make vector data cold to the provider, not to the OS.
        let provider=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(directory.path().to_str().unwrap())),1024,
            new_cache_for_test(),new_cache_for_test(),2));
        let a=HierarchicalSpannReader::open(&provider,ids.clone(),DistanceFunction::Euclidean,config.clone()).await.unwrap();
        let b=HierarchicalSpannReader::open(&provider,ids,DistanceFunction::Euclidean,config).await.unwrap();
        (directory,a,b)
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn refinement_access_cold_contention_warm_reuse_and_default_parity() {
        let (_directory,mut serial,reference)=fixture().await;
        let ids:Vec<_>=(0..16).collect();let query=vec![0.;32];
        assert!(reference.vector_pin_observation().is_none());
        reference.start_vector_pin_observation();
        let mut expected=reference.rerank_selected(&query,&ids).await.unwrap();expected.sort_by_key(|v|v.0);
        let cold=reference.vector_pin_observation().unwrap();assert!(!cold.access_overflow);
        assert_eq!(cold.point_blocks.len(),16);assert!(cold.block_access.len()>1);
        let requests:u64=cold.block_access.values().map(|v|v.reader_requests).sum();assert_eq!(requests,16);
        assert_eq!(cold.block_access.values().map(|v|v.pin_hits+v.pin_misses).sum::<u64>(),requests);
        assert!(cold.block_access.values().any(|v|v.decode_successes>0));
        for v in cold.block_access.values(){assert_eq!(v.provider_obtains,v.provider_hits+v.provider_misses);assert_eq!(v.decode_attempts,v.storage_callbacks);assert_eq!(v.decode_successes,v.decode_attempts);}
        // Concurrent misses may duplicate decoding; no race-dependent exact count.
        assert!(cold.block_access.values().map(|v|v.provider_misses).sum::<u64>()>=cold.block_access.len() as u64);
        serial.start_vector_pin_observation();
        let mut actual=serial.rerank_selected_releasing_vectors(&query,&ids).await.unwrap();actual.sort_by_key(|v|v.0);assert_eq!(actual,expected);
        let warm=serial.vector_pin_observation().unwrap();assert_eq!(warm.point_blocks,cold.point_blocks);
        assert_eq!(warm.block_access.values().map(|v|v.provider_hits).sum::<u64>(),16);
        assert_eq!(warm.block_access.values().map(|v|v.decode_attempts).sum::<u64>(),0);
        assert_eq!(warm.current_count,0);
        serial.start_vector_pin_observation();
        assert!(serial.rerank_selected_releasing_vectors(&query,&[9999]).await.is_err());
        let error=serial.vector_pin_observation().unwrap();assert_eq!(error.point_blocks.len(),1);assert_eq!(error.current_count,0);
        assert_eq!(error.block_access.values().map(|v|v.reader_requests).sum::<u64>(),1);
        reference.clear_loaded_blocks();
        // Observation never changes the exact result on the normal path.
        let mut unprofiled=reference.rerank_selected(&query,&ids).await.unwrap();unprofiled.sort_by_key(|v|v.0);assert_eq!(unprofiled,expected);
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn exact_results_release_peaks_and_independent_pins() {
        let (_directory,mut serial,reference)=fixture().await;let ids:Vec<_>=(0..16).collect();let query=vec![0.;32];
        reference.start_vector_pin_observation();let mut expected=reference.rerank_selected(&query,&ids).await.unwrap();expected.sort_by_key(|v|v.0);
        let before=reference.vector_pin_observation().unwrap();assert!(before.peak_count>1);assert_eq!(before.current_count,before.peak_count);
        reference.start_vector_pin_observation();assert_eq!(reference.vector_pin_observation().unwrap().baseline_count,before.current_count);
        serial.scalar_metadata_reader.get(crate::hierarchical_index::persistance::PREFIX_VERSION,0).await.unwrap().unwrap();
        let scalar_before=serial.scalar_metadata_reader.loaded_blocks_stats();assert!(scalar_before.0>0);
        serial.start_vector_pin_observation();let mut actual=serial.rerank_selected_releasing_vectors(&query,&ids).await.unwrap();actual.sort_by_key(|v|v.0);assert_eq!(actual,expected);
        assert_eq!(serial.scalar_metadata_reader.loaded_blocks_stats(),scalar_before);
        let after=serial.vector_pin_observation().unwrap();assert_eq!(after.peak_count,1);assert_eq!(after.current_count,0);assert_eq!(after.insertions,16);
        assert_eq!(reference.vector_pin_observation().unwrap().current_count,before.current_count);
        reference.clear_loaded_blocks();assert_eq!(reference.vector_pin_observation().unwrap().current_count,0);
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn missing_late_nonfinite_and_duplicate_error_then_retry() {
        let (_directory,mut reader,reference)=fixture().await;let query=vec![0.;32];reader.start_vector_pin_observation();
        assert!(reader.rerank_selected_releasing_vectors(&query,&[0,9999]).await.unwrap_err().to_string().contains("missing exact"));
        assert_eq!(reader.vector_pin_observation().unwrap().current_count,0);
        let mut huge=query.clone();huge[0]=1e30;
        assert!(reader.rerank_selected_releasing_vectors(&huge,&[0]).await.unwrap_err().to_string().contains("non-finite"));
        assert_eq!(reader.vector_pin_observation().unwrap().current_count,0);
        assert!(reader.rerank_selected_releasing_vectors(&query,&[0,0]).await.is_err());
        let mut actual=reader.rerank_selected_releasing_vectors(&query,&[1,2]).await.unwrap();actual.sort_by_key(|v|v.0);
        let mut expected=reference.rerank_selected(&query,&[1,2]).await.unwrap();expected.sort_by_key(|v|v.0);assert_eq!(actual,expected);
        assert_eq!(reader.vector_pin_observation().unwrap().current_count,0);
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn shared_map_rejects_before_fetch_and_preserves_borrowed_clone() {
        let (_directory,mut reader,_reference)=fixture().await;let clone=reader.vector_data_reader.clone();
        let borrowed=clone.get(PREFIX_EMBEDDING,0).await.unwrap().unwrap();
        reader.start_vector_pin_observation();let before=reader.vector_pin_observation().unwrap();
        assert!(reader.rerank_selected_releasing_vectors(&vec![0.;32],&[1]).await.unwrap_err().to_string().contains("shared"));
        let after=reader.vector_pin_observation().unwrap();assert_eq!(after.current_count,before.current_count);assert_eq!(after.insertions,0);assert_eq!(borrowed[0],0.);
        drop(clone);
    }
}
/// Full-precision persisted geometry for bounded benchmark diagnostics.
#[derive(Debug, Serialize)]
pub struct FpNodeDescriptor {
    pub node_id: NodeId,
    pub parent_id: Option<NodeId>,
    pub is_leaf: bool,
    pub children: Vec<NodeId>,
    pub centroid_bits: Vec<u32>,
}
#[derive(Debug, Serialize)]
pub struct ChildScore {
    pub parent_id: NodeId,
    pub node_id: NodeId,
    pub is_leaf: bool,
    pub distance: f32,
}
fn integrity(message: impl Into<String>) -> Box<dyn ChromaError> {
    Box::new(ReaderIntegrityError(message.into()))
}
impl HierarchicalSpannReader {
    pub fn describe_root(&self) -> Result<RootDescriptor, Box<dyn ChromaError>> {
        let node = self
            .nodes
            .get(&self.root_id)
            .ok_or_else(|| integrity("missing root"))?;
        let is_leaf = matches!(node.value(), TreeNode::Leaf(_));
        Ok(RootDescriptor {
            id: self.root_id,
            node_id: self.root_id,
            kind: if is_leaf { "leaf" } else { "internal" },
            is_leaf,
            level: 0,
        })
    }
    /// Copy actual stored centroid bits without scoring or normalizing them.
    /// These tiny diagnostic limits bound the returned payload, not provider decoding.
    pub async fn describe_fp_nodes(
        &self,
        ids: &[NodeId],
    ) -> Result<Vec<FpNodeDescriptor>, Box<dyn ChromaError>> {
        if ids.is_empty() || ids.len() > 128 {
            return Err(integrity("diagnostic requires one through 128 nodes"));
        }
        let mut seen = HashSet::new();
        let mut output = Vec::with_capacity(ids.len());
        for &node_id in ids {
            if !seen.insert(node_id) {
                return Err(integrity("duplicate diagnostic node"));
            }
            let node = self.nodes.get(&node_id)
                .ok_or_else(|| integrity(format!("missing diagnostic node {node_id}")))?;
            let (is_leaf, parent_id, children) = match node.value() {
                TreeNode::Leaf(leaf) => (true, leaf.parent_id, Vec::new()),
                TreeNode::Internal(internal) => {
                    if internal.children.len() > 128 {
                        return Err(integrity("diagnostic child count exceeds 128"));
                    }
                    (false, internal.parent_id, internal.children.clone())
                }
            };
            drop(node);
            // Reader topology stores only quantized navigation codes. Fetch the
            // writer's full-precision centroid from its actual persisted key.
            let center = self.vector_data_reader.get(PREFIX_CENTROID, node_id).await?
                .ok_or_else(|| integrity("missing persisted diagnostic centroid"))?;
            if center.is_empty() || center.len() > 16 || !center.iter().all(|v| v.is_finite()) {
                return Err(integrity("diagnostic centroid must have one through sixteen finite dimensions"));
            }
            output.push(FpNodeDescriptor {
                node_id, parent_id, is_leaf, children,
                centroid_bits: center.iter().map(|v| v.to_bits()).collect(),
            });
        }
        Ok(output)
    }
    /// Score every child at one level. The caller applies the shared global beam.
    pub fn expand_nodes(
        &self,
        query: &[f32],
        parents: &[NodeId],
    ) -> Result<Vec<ChildScore>, Box<dyn ChromaError>> {
        let qq = QuantizedQuery::new(query, self.padded_bytes(), 0.0, 0.0, Self::vec_norm(query));
        let mut seen = HashSet::new();
        let mut children = Vec::new();
        for &parent_id in parents {
            if !seen.insert(parent_id) {
                return Err(integrity("duplicate expansion parent"));
            }
            let ids = {
                let parent = self
                    .nodes
                    .get(&parent_id)
                    .ok_or_else(|| integrity(format!("missing parent {parent_id}")))?;
                let TreeNode::Internal(internal) = parent.value() else {
                    return Err(integrity(format!("cannot expand leaf {parent_id}")));
                };
                internal.children.clone()
            };
            for node_id in ids {
                let node = self
                    .nodes
                    .get(&node_id)
                    .ok_or_else(|| integrity(format!("missing child {node_id}")))?;
                let distance = Code::<1, _>::new(node.centroid_code())
                    .distance_quantized_query(&self.distance_fn, &qq);
                if !distance.is_finite() {
                    return Err(integrity("non-finite child navigation distance"));
                }
                children.push(ChildScore {
                    parent_id,
                    node_id,
                    is_leaf: matches!(node.value(), TreeNode::Leaf(_)),
                    distance,
                });
            }
        }
        Ok(children)
    }
    /// Return nearest unique candidates plus every tie at the budget boundary.
    /// The worker resolves those ties using external IDs before returning top-M.
    pub async fn score_selected_leaves(
        &self,
        query: &[f32],
        leaves: &[NodeId],
        budget: usize,
    ) -> Result<(Vec<(u32, f32)>, usize), Box<dyn ChromaError>> {
        if budget == 0 {
            return Err(integrity("candidate budget must be positive"));
        }
        let mut seen = HashSet::new();
        for &id in leaves {
            if !seen.insert(id) {
                return Err(integrity("duplicate selected leaf"));
            }
            if !self
                .nodes
                .get(&id)
                .is_some_and(|node| matches!(node.value(), TreeNode::Leaf(_)))
            {
                return Err(integrity(format!("selected node {id} is not a leaf")));
            }
        }
        let started = Instant::now();
        stream::iter(
            leaves
                .iter()
                .copied()
                .map(|id| self.load_node_posting_list(id)),
        )
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        let selected: Vec<_> = leaves.iter().map(|&id| (id, 0.0)).collect();
        let (mut hits, unique, _, _) = self.score_leaves(
            query,
            usize::MAX,
            1,
            &selected,
            started.elapsed().as_nanos() as u64,
        );
        if hits.iter().any(|(_, distance)| !distance.is_finite()) {
            return Err(integrity("non-finite approximate candidate distance"));
        }
        if hits.len() > budget {
            let boundary = hits[budget - 1].1;
            let keep = hits
                .iter()
                .take_while(|(_, distance)| *distance <= boundary)
                .count();
            hits.truncate(keep);
        }
        Ok((hits, unique))
    }
    /// Bounded compactor proof: compare the persisted raw FP32 vector bits.
    pub async fn verify_vector_bits(
        &self,
        id: u32,
        expected: &[f32],
    ) -> Result<bool, Box<dyn ChromaError>> {
        if expected.len() != self.dim || expected.iter().any(|v| !v.is_finite()) {
            return Err(integrity("invalid expected vector proof"));
        }
        let vector = self
            .vector_data_reader
            .get(PREFIX_EMBEDDING, id)
            .await?
            .ok_or_else(|| integrity("missing proof vector"))?;
        let matches = vector.len() == expected.len()
            && vector
                .iter()
                .zip(expected)
                .all(|(a, b)| a.to_bits() == b.to_bits());
        self.clear_loaded_blocks();
        Ok(matches)
    }

    /// Prove that registration survives persistence in the exact selected leaf.
    pub async fn verify_live_leaf_member(
        &self,
        leaf_id: NodeId,
        id: u32,
    ) -> Result<bool, Box<dyn ChromaError>> {
        use super::super::persistance::PREFIX_VERSION;
        let version = self
            .scalar_metadata_reader
            .get(PREFIX_VERSION, id)
            .await?
            .ok_or_else(|| integrity("missing live version for selected registration"))?;
        let version = u8::try_from(version)
            .map_err(|_| integrity("invalid selected registration version"))?;
        if version & super::super::writer::DELETED_BIT != 0 {
            return Ok(false);
        }
        self.load_node_posting_list(leaf_id).await?;
        let node = self
            .nodes
            .get(&leaf_id)
            .ok_or_else(|| integrity("selected leaf missing after flush"))?;
        let found = match node.value() {
            TreeNode::Leaf(leaf) => leaf
                .ids
                .iter()
                .zip(&leaf.versions)
                .any(|(&local, &v)| local == id && v == version),
            _ => return Err(integrity("selected target is no longer a leaf")),
        };
        drop(node);
        self.clear_loaded_blocks();
        Ok(found)
    }

    /// Score only the coordinator's globally selected candidates using raw vectors.
    pub async fn rerank_selected(
        &self,
        query: &[f32],
        ids: &[u32],
    ) -> Result<Vec<(u32, f32)>, Box<dyn ChromaError>> {
        let unique: HashSet<_> = ids.iter().copied().collect();
        if unique.len() != ids.len() {
            return Err(integrity("duplicate exact-rerank ID"));
        }
        let reader = &self.vector_data_reader;
        stream::iter(ids.iter().copied().map(|id| async move {
            let vector = reader
                .get(PREFIX_EMBEDDING, id)
                .await?
                .ok_or_else(|| integrity(format!("missing exact embedding for {id}")))?;
            let distance = self.dist(query, vector);
            if !distance.is_finite() {
                return Err(integrity("non-finite exact distance"));
            }
            Ok((id, distance))
        }))
        .buffer_unordered(32)
        .try_collect()
        .await
    }

    /// Exact serial scoring with operation-owned results and vector-only pin
    /// release. Exclusive access and unique map ownership are both required;
    /// provider cache references and decode allocations remain independent.
    pub async fn rerank_selected_releasing_vectors(
        &mut self,query:&[f32],ids:&[u32],
    ) -> Result<Vec<(u32,f32)>,Box<dyn ChromaError>> {
        if !self.vector_data_reader.has_exclusive_loaded_blocks() {
            return Err(integrity("exact-rerank vector pin map is shared"));
        }
        if query.len()!=self.dim || query.iter().any(|v|!v.is_finite()) || ids.len()>100000 {
            return Err(integrity("invalid bounded exact-rerank input"));
        }
        if ids.iter().copied().collect::<HashSet<_>>().len()!=ids.len() {
            return Err(integrity("duplicate exact-rerank ID"));
        }
        self.vector_data_reader.clear_loaded_blocks_exclusive().map_err(integrity)?;
        let mut output=Vec::with_capacity(ids.len());
        for id in ids.iter().copied() {
            // The future and its vector borrow both end before any pin clear.
            // An error also leaves no pending borrowed fetch future.
            let result=async {
                let vector=self.vector_data_reader.get(PREFIX_EMBEDDING,id).await?
                    .ok_or_else(||integrity(format!("missing exact embedding for {id}")))?;
                let distance=self.dist(query,vector);
                if !distance.is_finite(){return Err(integrity("non-finite exact distance"));}
                Ok::<_,Box<dyn ChromaError>>((id,distance))
            }.await;
            self.vector_data_reader.clear_loaded_blocks_exclusive().map_err(integrity)?;
            output.push(result?);
        }
        Ok(output)
    }

    pub fn start_vector_pin_observation(&self) { self.vector_data_reader.start_loaded_block_observation(); }
    pub fn vector_pin_observation(&self) -> Option<chroma_blockstore::LoadedBlockObservation> {
        self.vector_data_reader.loaded_block_observation()
    }
}

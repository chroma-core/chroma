//! One-level traversal and candidate operations for a coordinator that owns the beam.
use super::super::{
    common::{NodeId, TreeNode},
    persistance::{PREFIX_CENTROID, PREFIX_EMBEDDING},
};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::{Code, QuantizedQuery, OneBitScorer};
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
    async fn preparation_observer_saved_parity_cached_and_invalid_admission() {
        use chroma_index::quantization::{OneBitScorer,OneBitScorerPolicy};
        let (_directory,mut observed,reference)=fixture().await;
        let mut leaves:Vec<_>=reference.nodes.iter().filter_map(|n|matches!(n.value(),TreeNode::Leaf(_)).then_some(*n.key())).collect();leaves.sort();
        let mut query=vec![0.;32];query[0]=7.5;query[1]=1.;query[2]=-0.;
        let normalize=|rows:Vec<(u32,f32)>|{let mut rows:Vec<_>=rows.into_iter().map(|(id,d)|(id,d.to_bits())).collect();rows.sort();rows};
        let scorer=Some(OneBitScorer::new(OneBitScorerPolicy::Runtime));
        let (expected,u,_)=reference.score_selected_leaves_partitioned_with_scorer(&query,&leaves,1,true,scorer).await.unwrap();
        assert!(reference.scalar_metadata_reader.loaded_block_observation().is_none());
        let (actual,p)=observed.score_selected_leaves_preparation(&query,&leaves,1,true,scorer).await;
        let (actual,v,_)=actual.unwrap();assert_eq!(u,v);assert_eq!(normalize(expected.clone()),normalize(actual));assert_eq!(p["valid"],true,"{p}");
        assert!(p["scalar"]["point_blocks"].as_object().unwrap().is_empty());
        let requests:u64=p["scalar"]["block_access"].as_object().unwrap().values().map(|b|b["reader_requests"].as_u64().unwrap()).sum();assert_eq!(requests,16);
        assert_eq!(p["leaves"].as_object().unwrap().values().map(|v|v["physical_rows"].as_u64().unwrap()).sum::<u64>(),16);
        assert_eq!(p["leaves"].as_object().unwrap().values().map(|v|v["current_rows"].as_u64().unwrap()).sum::<u64>(),16);
        assert!(p["leaves"].as_object().unwrap().values().all(|v|v["completed"]==true && v["lock_released_ns"].as_u64().unwrap()>=v["lock_acquired_ns"].as_u64().unwrap()));
        let (warm,p)=observed.score_selected_leaves_preparation(&query,&leaves,1,true,scorer).await;assert_eq!(normalize(expected),normalize(warm.unwrap().0));assert_eq!(p["valid"],true);assert!(p["scalar"]["block_access"].as_object().unwrap().is_empty());assert!(p["leaves"].as_object().unwrap().values().all(|v|v["cached"]==true));
        let (bad,p)=observed.score_selected_leaves_preparation(&query,&[u32::MAX],1,true,scorer).await;assert!(bad.is_err());assert_eq!(p["valid"],false);
        let clone=observed.scalar_metadata_reader.clone();let (bad,p)=observed.score_selected_leaves_preparation(&query,&leaves,1,true,scorer).await;assert!(bad.is_err());assert_eq!(p["reason"],"admission");drop(clone);
        let (bad,p)=observed.score_selected_leaves_preparation(&query,&vec![0;257],1,true,scorer).await;assert!(bad.is_err());assert_eq!(p["valid"],false);
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn partitioned_selection_saved_postings_matches_reference_and_normal_search() {
        let (_directory,reader,_)=fixture().await;let mut query=vec![0.;32];query[0]=7.5;query[1]=1.;
        let mut leaves:Vec<_>=reader.nodes.iter().filter_map(|row|matches!(row.value(),TreeNode::Leaf(_)).then_some(*row.key())).collect();leaves.sort();
        for budget in [1,2,8,16,32] {
            let (a,unique,old)=reader.score_selected_leaves_partitioned(&query,&leaves,budget,false).await.unwrap();
            let (b,other,new)=reader.score_selected_leaves_partitioned(&query,&leaves,budget,true).await.unwrap();
            let normalize=|values:Vec<(u32,f32)>|{let mut rows:Vec<_>=values.into_iter().map(|(id,d)|(id,d.to_bits())).collect();rows.sort();rows};
            assert_eq!(normalize(a),normalize(b));assert_eq!(unique,other);assert!(new["sorted_rows"].as_u64().unwrap()<=old["sorted_rows"].as_u64().unwrap());
        }
        assert!(reader.score_selected_leaves_partitioned(&query,&leaves,0,true).await.is_err());
        let selected:Vec<_>=leaves.into_iter().map(|id|(id,0.)).collect();
        let (normal,_,_,_)=reader.score_leaves(&query,1,1,&selected,0);
        assert_eq!(normal.len(),1); // Normal search does not acquire all-tie semantics.
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn hardware_scorer_saved_postings_minima_and_all_ties_match_legacy_bits() {
        use chroma_index::quantization::{OneBitScorer,OneBitScorerPolicy};
        let (_directory,reader,_)=fixture().await;let mut query=vec![0.;32];query[0]=7.5;query[1]=1.;query[2]=-0.;
        let mut leaves:Vec<_>=reader.nodes.iter().filter_map(|row|matches!(row.value(),TreeNode::Leaf(_)).then_some(*row.key())).collect();leaves.sort();
        let normalize=|rows:Vec<(u32,f32)>|{let mut rows:Vec<_>=rows.into_iter().map(|(id,d)|(id,d.to_bits())).collect();rows.sort();rows};
        for budget in [1,2,8,16,32] {for partition in [false,true] {
            let (old,unique,_)=reader.score_selected_leaves_partitioned(&query,&leaves,budget,partition).await.unwrap();assert_eq!(unique,16);
            for policy in [OneBitScorerPolicy::Portable,OneBitScorerPolicy::Runtime] {
                let (new,other,_)=reader.score_selected_leaves_partitioned_with_scorer(&query,&leaves,budget,partition,Some(OneBitScorer::new(policy))).await.unwrap();assert_eq!(unique,other);assert_eq!(normalize(old.clone()),normalize(new));
            }
        }}
        // Model a current reassigned copy with the same vector and another
        // leaf's residual code. Both copies must score before strict minima.
        let id={let n=reader.nodes.get(&leaves[0]).unwrap();let TreeNode::Leaf(leaf)=n.value() else{unreachable!()};leaf.ids[0]};
        {let mut n=reader.nodes.get_mut(&leaves[1]).unwrap();let TreeNode::Leaf(leaf)=n.value_mut() else{unreachable!()};let mut vector=vec![0.;32];vector[0]=id as f32;vector[1]=1.;let code=Code::<1>::quantize(&vector,&leaf.centroid);leaf.ids.push(id);leaf.versions.push(1);leaf.codes.extend_from_slice(code.as_ref());leaf.length+=1;}
        for policy in [OneBitScorerPolicy::Portable,OneBitScorerPolicy::Runtime] {let (old,unique,_)=reader.score_selected_leaves_partitioned(&query,&leaves,32,true).await.unwrap();let (new,other,_)=reader.score_selected_leaves_partitioned_with_scorer(&query,&leaves,32,true,Some(OneBitScorer::new(policy))).await.unwrap();assert_eq!((unique,other),(16,16));assert_eq!(normalize(old),normalize(new));}
        // An intentionally tied owned-code fixture tests inclusive retention;
        // this phase does not claim these modified codes are saved vectors.
        let tied=*leaves.iter().find(|id|matches!(reader.nodes.get(id).unwrap().value(),TreeNode::Leaf(leaf) if leaf.ids.len()>=2)).unwrap();
        let unique={let mut n=reader.nodes.get_mut(&tied).unwrap();let TreeNode::Leaf(leaf)=n.value_mut() else{unreachable!()};let size=Code::<1>::size(32);let code=leaf.codes[..size].to_vec();leaf.codes=code.repeat(leaf.ids.len());leaf.ids.iter().copied().collect::<HashSet<_>>().len()};
        for partition in [false,true] {let (old,_,_)=reader.score_selected_leaves_partitioned(&query,&[tied],1,partition).await.unwrap();assert_eq!(old.len(),unique);for policy in [OneBitScorerPolicy::Portable,OneBitScorerPolicy::Runtime] {let (new,other,_)=reader.score_selected_leaves_partitioned_with_scorer(&query,&[tied],1,partition,Some(OneBitScorer::new(policy))).await.unwrap();assert_eq!(other,unique);assert_eq!(normalize(old.clone()),normalize(new));}}
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn refinement_access_cold_contention_warm_reuse_and_default_parity() {
        let (_directory,mut serial,reference)=fixture().await;
        let ids:Vec<_>=(0..16).collect();let query=vec![0.;32];
        let (_other_directory,unprofiled,_other_reader)=fixture().await;
        assert!(unprofiled.vector_pin_observation().is_none());
        let mut unprofiled_hits=unprofiled.rerank_selected(&query,&ids).await.unwrap();unprofiled_hits.sort_by_key(|v|v.0);
        assert!(unprofiled.vector_pin_observation().is_none());
        assert!(reference.vector_pin_observation().is_none());
        reference.start_vector_pin_observation();
        let mut expected=reference.rerank_selected(&query,&ids).await.unwrap();expected.sort_by_key(|v|v.0);
        assert_eq!(expected,unprofiled_hits);
        let cold=reference.vector_pin_observation().unwrap();assert!(!cold.access_overflow);
        assert_eq!(cold.point_blocks.len(),16);assert!(cold.block_access.len()>1);
        let requests:u64=cold.block_access.values().map(|v|v.reader_requests).sum();assert_eq!(requests,16);
        assert_eq!(cold.block_access.values().map(|v|v.pin_hits+v.pin_misses).sum::<u64>(),requests);
        assert!(cold.block_access.values().any(|v|v.decode_successes>0));
        for v in cold.block_access.values(){assert_eq!(v.provider_obtains,v.pin_misses);assert_eq!(v.provider_obtains,v.provider_hits+v.provider_misses);assert_eq!(v.decode_attempts,v.storage_callbacks);assert_eq!(v.decode_successes,v.decode_attempts);}
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
        let mut repeat=reference.rerank_selected(&query,&ids).await.unwrap();repeat.sort_by_key(|v|v.0);assert_eq!(repeat,expected);
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
    pub async fn score_selected_leaves(&self,query:&[f32],leaves:&[NodeId],budget:usize)
        ->Result<(Vec<(u32,f32)>,usize),Box<dyn ChromaError>> {
        let (hits,unique,_)=self.score_selected_leaves_partitioned(query,leaves,budget,false).await?;Ok((hits,unique))
    }

    pub async fn score_selected_leaves_partitioned(
        &self,
        query: &[f32],
        leaves: &[NodeId],
        budget: usize,
        partition: bool,
    ) -> Result<(Vec<(u32, f32)>, usize, serde_json::Value), Box<dyn ChromaError>> {
        self.score_selected_leaves_partitioned_with_scorer(query,leaves,budget,partition,None).await
    }

    pub async fn score_selected_leaves_partitioned_with_scorer(
        &self,query:&[f32],leaves:&[NodeId],budget:usize,partition:bool,scorer:Option<OneBitScorer>,
    )->Result<(Vec<(u32,f32)>,usize,serde_json::Value),Box<dyn ChromaError>> {
        self.score_selected_leaves_preparation_inner(query,leaves,budget,partition,scorer,None).await
    }

    /// Measurement only. Exclusive ownership and a fresh observer isolate this request.
    pub async fn score_selected_leaves_preparation(&mut self,query:&[f32],leaves:&[NodeId],budget:usize,partition:bool,scorer:Option<OneBitScorer>)->(Result<(Vec<(u32,f32)>,usize,serde_json::Value),Box<dyn ChromaError>>,serde_json::Value) {
        if leaves.len()>256 || !self.scalar_metadata_reader.has_exclusive_loaded_blocks() {
            return (Err(integrity("preparation observation requires <=256 leaves and unique scalar pin map")),serde_json::json!({"valid":false,"reason":"admission"}));
        }
        self.scalar_metadata_reader.start_scalar_preparation_observation();
        let observed=super::preparation::Preparation::new();
        let result=self.score_selected_leaves_preparation_inner(query,leaves,budget,partition,scorer,Some(observed.clone())).await;
        let joined_ns=super::preparation::nanos(observed.start);
        let quiescent=self.scalar_metadata_reader.scalar_preparation_quiescent() && std::sync::Arc::strong_count(&observed)==1;
        self.scalar_metadata_reader.stop_scalar_preparation_observation();
        let scalar=self.scalar_metadata_reader.loaded_block_observation();
        let overflow=*observed.overflow.lock().unwrap() || scalar.as_ref().is_none_or(|s|s.access_overflow);
        let profile=serde_json::json!({"policy":"posting-preparation-v1","valid":quiescent && !overflow && result.is_ok(),"quiescent":quiescent,"overflow":overflow,"request_profile_ns":joined_ns,"joined_load_ns":result.as_ref().ok().map(|r|r.2["load_ns"].clone()),"leaves":*observed.leaves.lock().unwrap(),"scalar":scalar,"lookup_batch_limit":64,"lookup_future_limit":16,"leaf_future_limit":32,"cache_label":"provider-obtain-hit; storage-origin Arrow decode only; cache reconstruction unmeasured","root_open":"excluded; reader already opened","timing_label":"overlapping wall intervals; not exclusive CPU or latency fractions"});
        (result,profile)
    }

    async fn score_selected_leaves_preparation_inner(&self,query:&[f32],leaves:&[NodeId],budget:usize,partition:bool,scorer:Option<OneBitScorer>,observation:Option<std::sync::Arc<super::preparation::Preparation>>)->Result<(Vec<(u32,f32)>,usize,serde_json::Value),Box<dyn ChromaError>> {
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
                .map(|id| self.load_node_posting_list_observed(id,observation.clone())),
        )
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await?;
        let selected: Vec<_> = leaves.iter().map(|&id| (id, 0.0)).collect();
        let load_ns=started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let (mut hits,unique,_,timings,nonfinite)=self.score_leaf_minima_with_scorer(query,&selected,load_ns,scorer).map_err(integrity)?;
        if nonfinite{return Err(integrity("non-finite approximate candidate distance"));}
        let work=super::selection::select_candidates(&mut hits,budget,partition).map_err(integrity)?;
        let profile=serde_json::json!({"policy":if partition{"partition-inclusive"}else{"full-sort-inclusive"},"unique_rows":work.input_rows,"sorted_rows":work.sorted_rows,"retained_rows":work.retained_rows,"partition_calls":work.partition_calls,"load_ns":load_ns,"quantize_ns":timings.quantize_nanos,"distance_ns":timings.distance_nanos,"dedup_ns":timings.sort_dedup_nanos,"select_ns":work.select_ns,"sort_ns":work.sort_ns});
        Ok((hits,unique,profile))
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

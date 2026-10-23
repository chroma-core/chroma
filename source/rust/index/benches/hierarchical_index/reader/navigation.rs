//! A bounded FP-centroid control tied to one exact opened saved reader.
use super::{HierarchicalSpannReader,ReaderIntegrityError};
use super::super::{common::{NodeId,TreeNode},persistance::PREFIX_CENTROID};
use chroma_error::ChromaError;
use sha2::{Digest,Sha256};
use std::{collections::{BTreeMap,BTreeSet},sync::Arc,time::Instant};

pub struct FpNavigationCache {
    pub(super) identity:Arc<()>,
    pub(super) centroids:BTreeMap<NodeId,Vec<f32>>,
    pub(super) four_bit_codes:Option<BTreeMap<NodeId,Vec<u8>>>,
    report:serde_json::Value,
}
impl FpNavigationCache {
    pub fn report(&self)->&serde_json::Value {&self.report}
    pub(super) fn distance(&self,reader:&HierarchicalSpannReader,query:&[f32],id:NodeId,q_norm:f32)->Result<f32,Box<dyn ChromaError>> {
        if query.len()!=reader.dim || !query.iter().all(|v|v.is_finite()) || !q_norm.is_finite() {return Err(Box::new(ReaderIntegrityError("invalid centroid query dimension/norm".into())));}
        let distance=if let Some(codes)=&self.four_bit_codes {
            let bytes=codes.get(&id).ok_or_else(||Box::new(ReaderIntegrityError("four-bit cache misses visible child".into())) as Box<dyn ChromaError>)?;
            validate_four_bit(bytes,reader.dim)?;
            chroma_index::quantization::Code::<4,_>::new(bytes).distance_query(&reader.distance_fn,query,0.,0.,q_norm)
        }else{let centroid=self.centroids.get(&id).ok_or_else(||Box::new(ReaderIntegrityError("FP cache misses visible child".into())) as Box<dyn ChromaError>)?;reader.dist(query,centroid)};
        if !distance.is_finite(){return Err(Box::new(ReaderIntegrityError("nonfinite centroid distance".into())));}Ok(distance)
    }
    pub(super) fn check(&self,reader:&HierarchicalSpannReader)->Result<(),Box<dyn ChromaError>> {
        if !Arc::ptr_eq(&self.identity,&reader.navigation_identity) {return Err(Box::new(ReaderIntegrityError("FP centroid cache belongs to a different opened reader/generation".into())));}
        Ok(())
    }
}
fn validate_four_bit(bytes:&[u8],dim:usize)->Result<(),Box<dyn ChromaError>> {
    if dim==0 || dim>768 || bytes.len()!=chroma_index::quantization::Code::<4>::size(dim) {return Err(Box::new(ReaderIntegrityError("invalid four-bit code size/dimension".into())));}
    let h:[f32;3]=std::array::from_fn(|i|f32::from_ne_bytes(bytes[i*4..i*4+4].try_into().unwrap()));
    if !h.iter().all(|v|v.is_finite()) || h[0]<=0. || h[1]<0. || h[2]!=0. {return Err(Box::new(ReaderIntegrityError("invalid four-bit zero-origin header".into())));}Ok(())
}
impl HierarchicalSpannReader {
    /// Uses the existing Code4 wire format and scorer, relative to the zero origin.
    /// The temporary FP cache is discarded only after all codes validate.
    pub async fn prepare_four_bit_navigation(&mut self)->Result<FpNavigationCache,Box<dyn ChromaError>> {
        let started=Instant::now();let mut cache=self.prepare_fp_navigation().await?;let zero=vec![0.;self.dim];let mut codes=BTreeMap::new();let mut sha=Sha256::new();let mut retained=0usize;
        for (&id,values) in &cache.centroids {
            if !Self::vec_norm(values).is_finite(){return Err(Box::new(ReaderIntegrityError("four-bit centroid norm overflow".into())));}
            let code=chroma_index::quantization::Code::<4>::quantize(values,&zero);let bytes=code.as_ref().to_vec();validate_four_bit(&bytes,self.dim)?;
            sha.update(id.to_le_bytes());sha.update(&bytes);retained+=bytes.len();codes.insert(id,bytes);
        }
        let fp_bytes=cache.centroids.len()*self.dim*4;let fp_report=cache.report.clone();cache.centroids.clear();cache.four_bit_codes=Some(codes);
        cache.report=serde_json::json!({"policy":"four-bit","root_id":self.root_id,"nodes":cache.four_bit_codes.as_ref().unwrap().len(),"dimension":self.dim,"metric":format!("{:?}",self.distance_fn),"code_bytes_per_node":chroma_index::quantization::Code::<4>::size(self.dim),"retained_code_payload_bytes":retained,"temporary_owned_fp_payload_bytes":fp_bytes,"accounted_preparation_peak_payload_bytes":fp_bytes+retained+zero.len()*4+chroma_index::quantization::Code::<4>::size(self.dim),"code_bits_sha256":format!("{:x}",sha.finalize()),"per_distance_unpack_logical_payload_bytes":chroma_index::quantization::Code::<4>::packed_len(self.dim)*8+self.dim*4,"prepare_seconds":started.elapsed().as_secs_f64(),"fp_source_load":fp_report,"scope":"ExistingCode4 zero-origin header+256dimension padding; existing distance_query correction and actual FP query. Peak is accounted input+retainedcodes+zero+one duplicate code only, not allocator/RSS bound: quantizer residual/absolute/grid/critical-point/packing scratch excluded; per-distance grid unpack scratch excluded. FP source objects remain on disk, provider buffers/cache unchanged; full FP preload is charged, not eliminated."});Ok(cache)
    }
    /// Exclusive preparation joins all reads and owns copied centroid values.
    /// Caps bound the new FP payload only, not provider decode/cache/RSS.
    pub async fn prepare_fp_navigation(&mut self)->Result<FpNavigationCache,Box<dyn ChromaError>> {
        self.prepare_fp_navigation_with_limits(256,1024*1024).await
    }
    /// Reserves the copied centroid payload before loading it. Provider buffers,
    /// container overhead and the already opened topology remain separately accounted.
    pub async fn prepare_fp_navigation_with_limits(&mut self,cap:usize,payload_cap:usize)->Result<FpNavigationCache,Box<dyn ChromaError>> {
        if cap==0 || cap>4096 || payload_cap==0 || payload_cap>16*1024*1024 {return Err(Box::new(ReaderIntegrityError("invalid FP centroid admission budget".into())));}
        let started=Instant::now();
        if self.dim==0 || self.dim>768 || self.nodes.len()>cap {return Err(Box::new(ReaderIntegrityError("FP centroid node/dimension cap".into())));}
        let mut seen=BTreeSet::new();let mut pending=vec![(self.root_id,None)];
        while let Some((id,parent))=pending.pop() {
            if !seen.insert(id) || seen.len()>cap {return Err(Box::new(ReaderIntegrityError("FP centroid visibility cycle/cap".into())));}
            let node=self.nodes.get(&id).ok_or_else(||Box::new(ReaderIntegrityError("FP centroid missing reachable node".into())) as Box<dyn ChromaError>)?;
            let (actual,children)=match node.value(){TreeNode::Leaf(v)=>(v.parent_id,vec![]),TreeNode::Internal(v)=>{if v.children.len()>cap{return Err(Box::new(ReaderIntegrityError("FP centroid child cap".into())));}(v.parent_id,v.children.clone())}};
            if actual!=parent {return Err(Box::new(ReaderIntegrityError("FP centroid parent visibility mismatch".into())));}
            if pending.len()+children.len()>cap{return Err(Box::new(ReaderIntegrityError("FP centroid pending cap".into())));}
            drop(node);for child in children.into_iter().rev(){pending.push((child,Some(id)));}
        }
        if seen.len()!=self.nodes.len() || seen.len().checked_mul(self.dim).and_then(|v|v.checked_mul(4)).is_none_or(|v|v>payload_cap) {return Err(Box::new(ReaderIntegrityError("FP centroid unreachable nodes/payload cap".into())));}
        self.start_vector_pin_observation();
        let prepared=async {
            let mut centroids=BTreeMap::new();let mut hash=Sha256::new();
            for id in &seen {
                let values={let value=self.vector_data_reader.get(PREFIX_CENTROID,*id).await?;let value=value.ok_or_else(||Box::new(ReaderIntegrityError(format!("missing persisted FP centroid {id}"))) as Box<dyn ChromaError>)?;
                    if value.len()!=self.dim || !value.iter().all(|v|v.is_finite()) {return Err(Box::new(ReaderIntegrityError("invalid persisted FP centroid dimension/finite values".into())) as Box<dyn ChromaError>);}value.to_vec()};
                hash.update(id.to_le_bytes());for v in &values {hash.update(v.to_bits().to_le_bytes());}centroids.insert(*id,values);
            }
            Ok::<_,Box<dyn ChromaError>>((centroids,format!("{:x}",hash.finalize())))
        }.await;
        self.clear_loaded_blocks();let access=serde_json::to_value(self.vector_pin_observation()).expect("bounded observation serialization");
        let (centroids,sha)=prepared?;
        if access.is_null() {return Err(Box::new(ReaderIntegrityError("missing FP centroid load observation".into())));}
        let access_complete=access["access_overflow"]==false;
        let payload=centroids.len()*self.dim*4;
        let report=serde_json::json!({"root_id":self.root_id,"nodes":centroids.len(),"dimension":self.dim,"metric":format!("{:?}",self.distance_fn),"point_gets":centroids.len(),"owned_f32_payload_bytes":payload,"logical_centroid_return_bytes":payload,"node_cap":cap,"owned_payload_cap_bytes":payload_cap,"centroid_bits_sha256":sha,"load_seconds":started.elapsed().as_secs_f64(),"vector_access":access,"vector_access_complete":access_complete,"scope":"Exact opened reader identity; root-reachable finite persisted centroids, no corpus embedding load. Payload excludes map/Vec/container/shared-buffer/decode/provider/RSS. Vector observation covers centroid-key block traffic only; when vector_access_complete=false its bounded access maps are incomplete and must not support total fetch/decode/traffic claims; centroid validity and hash still cover every reachable node; rootopen/providercreation excluded; encoded returns are not physicalIO. Load may warm provider cache; all queries have declared warmup."});
        Ok(FpNavigationCache{identity:self.navigation_identity.clone(),centroids,four_bit_codes:None,report})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchical_index::{config::HierarchicalSpannConfig,writer::HierarchicalSpannWriter,common::{LeafNode,InternalNode,ReadBeamPolicy}};
    use chroma_blockstore::{arrow::provider::{ArrowBlockfileProvider,BlockfileReaderOptions},provider::BlockfileProvider,BlockfileWriterOptions};
    use chroma_cache::new_cache_for_test;use chroma_storage::{Storage,local::LocalStorage};use chroma_distance::DistanceFunction;
    use chroma_index::quantization::{Code,QuantizedQuery};
    fn provider(path:&std::path::Path)->BlockfileProvider {BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(path.to_str().unwrap())),1024,new_cache_for_test(),new_cache_for_test(),2))}
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn fp_navigation_large_valid_cache_marks_access_map_incomplete() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig::default();
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        writer.add(0,&vec![1.;32]);
        let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();
        let mut reader=HierarchicalSpannReader::open(&p,ids,DistanceFunction::Euclidean,config).await.unwrap();
        let root=reader.root_id;let children:Vec<u32>=(1000..1300).collect();let v=vec![1.;32];let code=Code::<1>::quantize(&v,&vec![0.;32]).as_ref().to_vec();
        reader.nodes.clear();
        reader.nodes.insert(root,TreeNode::Internal(InternalNode{centroid:v.clone(),centroid_code:code.clone(),children:children.clone(),parent_id:None}));
        let store=p.write::<u32,Vec<f32>>(BlockfileWriterOptions::new("".into())).await.unwrap();let vector_root=store.id();
        store.set(PREFIX_CENTROID,root,v.clone()).await.unwrap();
        for id in children {
            reader.nodes.insert(id,TreeNode::Leaf(LeafNode{centroid:v.clone(),centroid_code:code.clone(),ids:vec![],versions:vec![],codes:vec![],length:0,parent_id:Some(root)}));
            store.set(PREFIX_CENTROID,id,v.clone()).await.unwrap();
        }
        store.commit::<u32,Vec<f32>>().await.unwrap().flush::<u32,Vec<f32>>().await.unwrap();
        reader.vector_data_reader=p.read::<u32,&[f32]>(BlockfileReaderOptions::new(vector_root,"".into())).await.unwrap();
        assert!(reader.prepare_fp_navigation().await.is_err());
        let cache=reader.prepare_fp_navigation_with_limits(4096,16*1024*1024).await.unwrap();
        assert_eq!(cache.centroids.len(),301);
        assert_eq!(cache.report()["owned_f32_payload_bytes"],301*32*4);
        assert_eq!(cache.report()["vector_access_complete"],false);
        assert_eq!(cache.report()["vector_access"]["access_overflow"],true);
        assert!(cache.centroids.values().all(|values|values==&v));
    }
    // Two internal candidates are offered at level one. Beam one discards one;
    // level two then offers only leaves, so leaf rejection cannot masquerade as pruning.
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn internal_pruning_counters_shared_traversal_and_search_parity() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig::default();
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        let values:Vec<Vec<f32>>=(1..=3).map(|i|vec![i as f32;32]).collect();
        for (id,v) in values.iter().enumerate(){writer.add(id as u32,v);}
        let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();
        let reader=HierarchicalSpannReader::open(&p,ids,DistanceFunction::Euclidean,config).await.unwrap();
        let root=reader.root_id;let zero=vec![0.;32];let code=|v:&[f32]|Code::<1>::quantize(v,&zero).as_ref().to_vec();reader.nodes.clear();
        reader.nodes.insert(root,TreeNode::Internal(InternalNode{centroid:zero.clone(),centroid_code:code(&zero),children:vec![100,101],parent_id:None}));
        let mut centroids=BTreeMap::new();centroids.insert(root,zero.clone());
        for (id,value,children) in [(100,1.,vec![200,201]),(101,3.,vec![202])] {
            let v=vec![value;32];centroids.insert(id,v.clone());reader.nodes.insert(id,TreeNode::Internal(InternalNode{centroid:v.clone(),centroid_code:code(&v),children,parent_id:Some(root)}));
        }
        for (i,v) in values.iter().enumerate() {
            let id=200+i as u32;centroids.insert(id,v.clone());reader.nodes.insert(id,TreeNode::Leaf(LeafNode{centroid:v.clone(),centroid_code:code(v),ids:vec![i as u32],versions:vec![1],codes:code(v),length:1,parent_id:Some(if i<2{100}else{101})}));
        }
        let cache=FpNavigationCache{identity:reader.navigation_identity.clone(),centroids,four_bit_codes:None,report:serde_json::Value::Null};
        let q=vec![0.25;32];let narrow=ReadBeamPolicy::uniform(Some(2.),1,1);
        let (selected,work,trace)=reader.navigate_with_centroids_observed(&q,&narrow,Some(&cache),true).unwrap();
        assert_eq!(selected.iter().map(|v|v.0).collect::<Vec<_>>(),vec![200]);
        let t=trace.unwrap();assert!(t.complete);assert_eq!(t.levels.len(),2);let first=&t.levels[0];let second=&t.levels[1];
        assert_eq!((first.expanded_internal,first.offered_internal,first.retained_internal,first.discarded_internal),(1,2,1,1));
        assert_eq!((second.expanded_internal,second.offered_leaf,second.retained_leaf,second.discarded_leaf),(1,2,1,1));
        assert_eq!(t.levels.iter().map(|v|v.expanded_internal).sum::<usize>(),work.0);
        assert_eq!(t.levels.iter().map(|v|v.offered_internal+v.offered_leaf).sum::<usize>(),work.1);
        for fp in [None,Some(&cache)] {
            let (off,ow,ot)=reader.navigate_with_centroids_observed(&q,&narrow,fp,false).unwrap();let (on,nw,nt)=reader.navigate_with_centroids_observed(&q,&narrow,fp,true).unwrap();assert!(ot.is_none());assert!(nt.unwrap().complete);assert_eq!(ow,nw);assert_eq!(off.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>(),on.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>());
        }
        let off=reader.search_with_policy_lazy_fp(&q,1,2,&narrow,&cache,false).await.unwrap();
        let on=reader.search_with_policy_lazy_fp(&q,1,2,&narrow,&cache,true).await.unwrap();
        assert!(off.3.work.navigation_level_trace.is_none());assert!(on.3.work.navigation_level_trace.as_ref().unwrap().complete);
        let (observed,coverage)=reader.search_with_policy_lazy_fp_gt(&q,1,2,&narrow,&cache,&[0,1,2]).await.unwrap();
        assert_eq!(observed.0.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>(),off.0.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>());
        assert!(observed.3.query_stage_trace.is_none());
        assert!(observed.3.work.navigation_level_trace.as_ref().unwrap().complete);
        assert_eq!(coverage["reached_ids"],serde_json::json!([0]));
        assert_eq!(coverage["retained_ids"],serde_json::json!([0]));
        assert_eq!(coverage["returned_ids"],serde_json::json!([0]));
        assert_eq!(coverage["navigation_missing"],2);
        assert_eq!(coverage["trim_missing"],0);
        assert_eq!(coverage["exact_missing"],0);
        assert!(reader.search_with_policy_lazy_fp_gt(&q,1,2,&narrow,&cache,&[0,0]).await.is_err());

        assert_eq!(off.0.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>(),on.0.iter().map(|(id,d)|(*id,d.to_bits())).collect::<Vec<_>>());assert_eq!((off.1,off.2),(on.1,on.2));
        let (_,_,wide)=reader.navigate_with_centroids_observed(&q,&ReadBeamPolicy::uniform(Some(2.),4,4),Some(&cache),true).unwrap();
        assert!(wide.unwrap().levels.iter().all(|v|v.discarded_internal==0&&v.discarded_leaf==0));
        if let TreeNode::Internal(v)=reader.nodes.get_mut(&root).unwrap().value_mut(){v.children=vec![200,201];}
        let (_,_,leaf_only)=reader.navigate_with_centroids_observed(&q,&narrow,Some(&cache),true).unwrap();let t=leaf_only.unwrap();assert_eq!(t.levels.len(),1);assert_eq!(t.levels[0].offered_internal,0);assert_eq!(t.levels[0].discarded_internal,0);assert_eq!(t.levels[0].discarded_leaf,1);
        for level in &t.levels {assert_eq!(level.offered_internal,level.retained_internal+level.discarded_internal);assert_eq!(level.offered_leaf,level.retained_leaf+level.discarded_leaf);}
        println!("INTERNAL_PRUNING_FIXTURE internal_candidates=2 beam=1 internal_discarded=1 leaf_discarded=1 full_search_bits_equal=true");
    }
    #[test]
    fn internal_pruning_counters_level_cap_is_explicitly_incomplete() {
        use crate::hierarchical_index::instrumentation::{NavigationLevelTrace,NavigationLevelWork};
        let mut trace=NavigationLevelTrace::new();for level in 1..=129 {trace.push(NavigationLevelWork{level,..Default::default()});}
        assert_eq!(trace.levels.len(),128);assert!(!trace.complete);assert_eq!(trace.levels.last().unwrap().level,128);
        let empty=NavigationLevelTrace::new();assert!(empty.complete&&empty.levels.is_empty());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn fp_navigation_saved_identity_versions_and_invalid_centroids() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut config=HierarchicalSpannConfig::default();config.branching_factor=3;config.split_threshold=4;config.merge_threshold=1;
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        for id in 0..32 {let v:Vec<_>=(0..32).map(|d|((id*17+d*13)%37) as f32/7.).collect();writer.add(id,&v);}writer.balance_index_parallel(2);
        let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();let mut reader=HierarchicalSpannReader::open(&p,ids.clone(),DistanceFunction::Euclidean,config.clone()).await.unwrap();let cache=reader.prepare_fp_navigation().await.unwrap();
        assert!(reader.prepare_fp_navigation_with_limits(0,1024).await.is_err());
        assert!(reader.prepare_fp_navigation_with_limits(4097,16*1024*1024).await.is_err());
        assert!(reader.prepare_fp_navigation_with_limits(4096,reader.nodes.len()*32*4-1).await.is_err());
        let budgeted=reader.prepare_fp_navigation_with_limits(4096,16*1024*1024).await.unwrap();
        assert_eq!(budgeted.report()["centroid_bits_sha256"],cache.report()["centroid_bits_sha256"]);
        assert_eq!(cache.report()["nodes"].as_u64().unwrap() as usize,reader.nodes.len());assert_eq!(cache.report()["owned_f32_payload_bytes"],reader.nodes.len()*32*4);
        let q=vec![0.25;32];for centroid in cache.centroids.values(){assert_eq!(reader.dist(&q,centroid).to_bits(),DistanceFunction::Euclidean.distance(&q,centroid).to_bits());}
        let policy=ReadBeamPolicy::uniform(None,4,4);let (old,_,_,t)=reader.search_with_policy_lazy_fp(&q,4,2,&policy,&cache,true).await.unwrap();assert!(t.work.centroid_distance_evaluations>0&&t.work.expanded_internal_nodes>0);
        writer.add(32,&vec![2.;32]);writer.balance_index_parallel(2);let newer=writer.commit(&p,Some(&ids)).await.unwrap().flush().await.unwrap();let next=HierarchicalSpannReader::open(&p,newer,DistanceFunction::Euclidean,config.clone()).await.unwrap();assert!(next.search_with_policy_lazy_fp(&q,4,2,&policy,&cache,true).await.is_err());
        assert_eq!(reader.search_with_policy_lazy_fp(&q,4,2,&policy,&cache,true).await.unwrap().0,old);
        for kind in ["missing","dimension","nonfinite"] {
            let w=p.write::<u32,Vec<f32>>(BlockfileWriterOptions::new("".into())).await.unwrap();let root=w.id();
            for (&id,v) in &cache.centroids {if id==reader.root_id {if kind=="missing"{continue;}w.set(PREFIX_CENTROID,id,if kind=="dimension"{vec![0.;31]}else{vec![f32::NAN;32]}).await.unwrap();}else{w.set(PREFIX_CENTROID,id,v.clone()).await.unwrap();}}
            w.commit::<u32,Vec<f32>>().await.unwrap().flush::<u32,Vec<f32>>().await.unwrap();let mut bad=HierarchicalSpannReader::open(&p,ids.clone(),DistanceFunction::Euclidean,config.clone()).await.unwrap();bad.vector_data_reader=p.read::<u32,&[f32]>(BlockfileReaderOptions::new(root,"".into())).await.unwrap();assert!(bad.prepare_fp_navigation().await.is_err(),"{kind}");
        }
        let mut bad=HierarchicalSpannReader::open(&p,ids,DistanceFunction::Euclidean,config).await.unwrap();bad.dim=769;assert!(bad.prepare_fp_navigation().await.is_err());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn fp_navigation_known_onebit_divergence_and_shared_order() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig::default();let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());writer.add(0,&vec![1.;32]);let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();let mut reader=HierarchicalSpannReader::open(&p,ids,DistanceFunction::Euclidean,config).await.unwrap();
        let q:Vec<f32>=(0..32).map(|d|((d as f32+1.)*0.71).cos()).collect();let zero=vec![0.;32];let qq=QuantizedQuery::new(&q,Code::<1>::packed_len(32),0.,0.,HierarchicalSpannReader::vec_norm(&q));
        let values:Vec<Vec<f32>>=(0..64).map(|i|(0..32).map(|d|((i as f32+1.)*(d as f32+1.)*0.37).sin()*(0.2+(i%5) as f32)).collect()).collect();
        for metric in [DistanceFunction::Euclidean,DistanceFunction::InnerProduct] {
        reader.distance_fn=metric.clone();
        let mut chosen=None;
        'outer:for a in 0..values.len(){for b in a+1..values.len(){let exact_a=reader.dist(&q,&values[a]);let exact_b=reader.dist(&q,&values[b]);let approx_a=Code::<1>::quantize(&values[a],&zero).distance_quantized_query(&metric,&qq);let approx_b=Code::<1>::quantize(&values[b],&zero).distance_quantized_query(&metric,&qq);if (exact_a<exact_b&&approx_a>approx_b)||(exact_a>exact_b&&approx_a<approx_b){chosen=Some((a,b,exact_a,exact_b,approx_a,approx_b));break 'outer;}}}
        let (a,b,ea,eb,aa,ab)=chosen.expect("fixed finite centroid set must exhibit onebit ranking divergence");let root=reader.root_id;reader.nodes.clear();
        reader.nodes.insert(root,TreeNode::Internal(InternalNode{centroid:zero.clone(),centroid_code:Code::<1>::quantize(&zero,&zero).as_ref().to_vec(),children:vec![100,101],parent_id:None}));
        let mut centroids=BTreeMap::new();centroids.insert(root,zero.clone());for (id,v) in [(100,values[a].clone()),(101,values[b].clone())]{reader.nodes.insert(id,TreeNode::Leaf(LeafNode{centroid:v.clone(),centroid_code:Code::<1>::quantize(&v,&zero).as_ref().to_vec(),ids:vec![],versions:vec![],codes:vec![],parent_id:Some(root),length:0}));centroids.insert(id,v);}
        let cache=FpNavigationCache{identity:reader.navigation_identity.clone(),centroids,four_bit_codes:None,report:serde_json::Value::Null};let policy=ReadBeamPolicy::uniform(None,1,1);let (fp,fw)=reader.navigate_with_centroids(&q,&policy,Some(&cache)).unwrap();let (one,ow)=reader.navigate_with_centroids(&q,&policy,None).unwrap();assert_eq!(fp[0].0,if ea<eb{100}else{101});assert_eq!(one[0].0,if aa<ab{100}else{101});assert_ne!(fp[0].0,one[0].0);assert_eq!(fp[0].1.to_bits(),ea.min(eb).to_bits());assert_eq!(fw,ow);assert_eq!(fw.0,1);assert_eq!(fw.1,2);
        println!("KNOWN_ONEBIT_DIVERGENCE {metric:?} {a} {b} {ea} {eb} {aa} {ab}");
        }
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn four_bit_navigation_existing_kernel_headers_and_padding() {
        for dim in [1,31,32,255,256,257,768] {
            let dir=tempfile::tempdir().unwrap();let p=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),16384,new_cache_for_test(),new_cache_for_test(),2));let config=HierarchicalSpannConfig::default();let writer=HierarchicalSpannWriter::new(dim,DistanceFunction::Euclidean,config.clone());
            let vector:Vec<f32>=(0..dim).map(|i|if i%7==0{-0.}else{((i*13%29) as f32-14.)/7.}).collect();writer.add(0,&vector);let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();
            let mut reader=HierarchicalSpannReader::open(&p,ids.clone(),DistanceFunction::Euclidean,config.clone()).await.unwrap();let fp=reader.prepare_fp_navigation().await.unwrap();let mut cache=reader.prepare_four_bit_navigation().await.unwrap();
            assert!(cache.centroids.is_empty());let q:Vec<f32>=(0..dim).map(|i|((i*3%17) as f32-8.)/5.).collect();let norm=HierarchicalSpannReader::vec_norm(&q);let zero=vec![0.;dim];
            for metric in [DistanceFunction::Euclidean,DistanceFunction::InnerProduct,DistanceFunction::Cosine] {
                reader.distance_fn=metric.clone();for (&id,centroid) in &fp.centroids {
                    let expected=Code::<4>::quantize(centroid,&zero);assert_eq!(cache.four_bit_codes.as_ref().unwrap()[&id],expected.as_ref());assert_eq!(expected.as_ref().len(),Code::<4>::size(dim));
                    let actual=cache.distance(&reader,&q,id,norm).unwrap();let independent=expected.distance_query(&metric,&q,0.,0.,norm);assert_eq!(actual.to_bits(),independent.to_bits());
                }
            }
            assert_eq!(cache.report()["retained_code_payload_bytes"],fp.centroids.len()*Code::<4>::size(dim));assert_eq!(cache.report()["temporary_owned_fp_payload_bytes"],fp.centroids.len()*dim*4);
            let other=HierarchicalSpannReader::open(&p,ids,DistanceFunction::Euclidean,config).await.unwrap();assert!(cache.check(&other).is_err());assert!(cache.distance(&reader,&vec![0.;dim+1],reader.root_id,1.).is_err());
            let original=cache.four_bit_codes.as_ref().unwrap()[&reader.root_id].clone();
            for bad in [vec![],vec![0;Code::<4>::size(dim)-1],{let mut b=original.clone();b[..4].copy_from_slice(&f32::NAN.to_ne_bytes());b},{let mut b=original.clone();b[4..8].copy_from_slice(&(-1f32).to_ne_bytes());b},{let mut b=original.clone();b[8..12].copy_from_slice(&1f32.to_ne_bytes());b}] {
                cache.four_bit_codes.as_mut().unwrap().insert(reader.root_id,bad);assert!(cache.distance(&reader,&q,reader.root_id,norm).is_err());
            }
        }
        assert_eq!(Code::<4>::size(768),396);
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn four_bit_navigation_saved_search_and_old_reader() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut config=HierarchicalSpannConfig::default();config.branching_factor=3;config.split_threshold=4;config.merge_threshold=1;
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::InnerProduct,config.clone());for id in 0..32 {writer.add(id,&(0..32).map(|d|((id*17+d*7)%31) as f32/31.).collect::<Vec<_>>());}writer.balance_index_parallel(2);
        let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();let mut reader=HierarchicalSpannReader::open(&p,ids.clone(),DistanceFunction::InnerProduct,config.clone()).await.unwrap();let cache=reader.prepare_four_bit_navigation().await.unwrap();let q=vec![0.2;32];let policy=ReadBeamPolicy::uniform(None,64,64);
        let legacy=reader.search_with_policy_lazy(&q,8,8,&policy).await.unwrap();let compressed=reader.search_with_policy_lazy_fp(&q,8,8,&policy,&cache,true).await.unwrap();assert_eq!(legacy.0,compressed.0);assert_eq!(legacy.1,compressed.1);assert!(compressed.3.work.centroid_distance_evaluations>0);
        writer.add(32,&vec![2.;32]);writer.balance_index_parallel(2);let ids2=writer.commit(&p,Some(&ids)).await.unwrap().flush().await.unwrap();let next=HierarchicalSpannReader::open(&p,ids2,DistanceFunction::InnerProduct,config).await.unwrap();assert!(cache.check(&next).is_err());assert_eq!(reader.search_with_policy_lazy_fp(&q,8,8,&policy,&cache,true).await.unwrap().0,compressed.0);
    }

}

//! A bounded FP-centroid control tied to one exact opened saved reader.
use super::{HierarchicalSpannReader,ReaderIntegrityError};
use super::super::{common::{NodeId,TreeNode},persistance::PREFIX_CENTROID};
use chroma_error::ChromaError;
use sha2::{Digest,Sha256};
use std::{collections::{BTreeMap,BTreeSet},sync::Arc,time::Instant};

pub struct FpNavigationCache {
    pub(super) identity:Arc<()>,
    pub(super) centroids:BTreeMap<NodeId,Vec<f32>>,
    report:serde_json::Value,
}
impl FpNavigationCache {
    pub fn report(&self)->&serde_json::Value {&self.report}
    pub(super) fn check(&self,reader:&HierarchicalSpannReader)->Result<(),Box<dyn ChromaError>> {
        if !Arc::ptr_eq(&self.identity,&reader.navigation_identity) {return Err(Box::new(ReaderIntegrityError("FP centroid cache belongs to a different opened reader/generation".into())));}
        Ok(())
    }
}
impl HierarchicalSpannReader {
    /// Exclusive preparation joins all reads and owns copied centroid values.
    /// Caps bound the new FP payload only, not provider decode/cache/RSS.
    pub async fn prepare_fp_navigation(&mut self)->Result<FpNavigationCache,Box<dyn ChromaError>> {
        let started=Instant::now();let cap=256usize;let payload_cap=1024*1024usize;
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
        if access.is_null() || access["access_overflow"]!=false {return Err(Box::new(ReaderIntegrityError("incomplete FP centroid load observation".into())));}
        let payload=centroids.len()*self.dim*4;
        let report=serde_json::json!({"root_id":self.root_id,"nodes":centroids.len(),"dimension":self.dim,"metric":format!("{:?}",self.distance_fn),"point_gets":centroids.len(),"owned_f32_payload_bytes":payload,"logical_centroid_return_bytes":payload,"node_cap":cap,"owned_payload_cap_bytes":payload_cap,"centroid_bits_sha256":sha,"load_seconds":started.elapsed().as_secs_f64(),"vector_access":access,"scope":"Exact opened reader identity; root-reachable finite persisted centroids, no corpus embedding load. Payload excludes map/Vec/container/shared-buffer/decode/provider/RSS. Vector observation covers centroid-key block traffic only; rootopen/providercreation excluded; encoded returns are not physicalIO. Load may warm provider cache; all queries have declared warmup."});
        Ok(FpNavigationCache{identity:self.navigation_identity.clone(),centroids,report})
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
    async fn fp_navigation_saved_identity_versions_and_invalid_centroids() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut config=HierarchicalSpannConfig::default();config.branching_factor=3;config.split_threshold=4;config.merge_threshold=1;
        let writer=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        for id in 0..32 {let v:Vec<_>=(0..32).map(|d|((id*17+d*13)%37) as f32/7.).collect();writer.add(id,&v);}writer.balance_index_parallel(2);
        let ids=writer.commit(&p,None).await.unwrap().flush().await.unwrap();let mut reader=HierarchicalSpannReader::open(&p,ids.clone(),DistanceFunction::Euclidean,config.clone()).await.unwrap();let cache=reader.prepare_fp_navigation().await.unwrap();
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
        let cache=FpNavigationCache{identity:reader.navigation_identity.clone(),centroids,report:serde_json::Value::Null};let policy=ReadBeamPolicy::uniform(None,1,1);let (fp,fw)=reader.navigate_with_centroids(&q,&policy,Some(&cache)).unwrap();let (one,ow)=reader.navigate_with_centroids(&q,&policy,None).unwrap();assert_eq!(fp[0].0,if ea<eb{100}else{101});assert_eq!(one[0].0,if aa<ab{100}else{101});assert_ne!(fp[0].0,one[0].0);assert_eq!(fp[0].1.to_bits(),ea.min(eb).to_bits());assert_eq!(fw,ow);assert_eq!(fw.0,1);assert_eq!(fw.1,2);
        println!("KNOWN_ONEBIT_DIVERGENCE {metric:?} {a} {b} {ea} {eb} {aa} {ab}");
        }
    }
}

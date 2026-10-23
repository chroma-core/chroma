//! Ordered node fetches feed one global beam. Fetch waves are storage batches,
//! never independent subtree searches. Mutation admission is a separate concern.
use super::common::{NodeId,ReadBeamPolicy,effective_beam};
use anyhow::{ensure,Result};
use chroma_distance::DistanceFunction;
use std::sync::Arc;
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Binding { Saved{generation:u64}, Live{owner:usize,epoch:u64} }
#[derive(Clone,Debug)]
pub enum Kind { Leaf{physical_rows:usize}, Internal{children:Vec<NodeId>} }
#[derive(Clone,Debug)]
pub struct NodeRecord {pub id:NodeId,pub parent:Option<NodeId>,pub centroid:Vec<f32>,pub kind:Kind,pub binding:Binding}
impl NodeRecord {
    pub fn payload_bytes(&self)->usize{self.centroid.len()*4+match &self.kind{Kind::Leaf{..}=>0,Kind::Internal{children}=>children.len()*4}}
}
pub trait NodeAccess:Send+Sync {
    fn wave_rows(&self)->usize;
    /// Results preserve request slots, including missing live nodes. No node,
    /// cache or owner guard escapes this call. Saved implementations reject
    /// missing referenced records instead of treating corruption as a race.
    fn fetch(&self,ids:&[NodeId])->Result<Vec<Option<Arc<NodeRecord>>>>;
}
#[derive(Default,Debug)]
pub struct NavigationWork {
    pub fetch_waves:usize,pub requested_records:usize,pub returned_payload_bytes:usize,pub peak_wave_payload_bytes:usize,pub peak_accounted_record_pin_payload_bytes:usize,
    pub distance_evaluations:usize,pub expanded_internals:usize,pub levels:usize,pub peak_frontier_scores:usize,
}
fn fetch(access:&dyn NodeAccess,ids:&[NodeId],work:&mut NavigationWork)->Result<Vec<Option<Arc<NodeRecord>>>>{
    ensure!(!ids.is_empty() && ids.len()<=access.wave_rows(),"ordered node wave admission");let records=access.fetch(ids)?;ensure!(records.len()==ids.len(),"node wave cardinality");let mut bytes=0;
    for(id,record) in ids.iter().zip(&records){if let Some(record)=record{ensure!(*id==record.id,"node slot identity");bytes+=record.payload_bytes();}}
    work.fetch_waves+=1;work.requested_records+=ids.len();work.returned_payload_bytes+=bytes;work.peak_wave_payload_bytes=work.peak_wave_payload_bytes.max(bytes);work.peak_accounted_record_pin_payload_bytes=work.peak_accounted_record_pin_payload_bytes.max(bytes);Ok(records)
}
/// Reproduces the reference's mixed internal/leaf frontier, distance-only sort,
/// per-level beam and root-leaf distance. Caller owns the frontier score vectors;
/// their memory is separate from the access implementation's clean-record cache.
pub fn navigate(access:&dyn NodeAccess,root:NodeId,metric:&DistanceFunction,query:&[f32],policy:&ReadBeamPolicy)->Result<(Vec<(NodeId,f32)>,NavigationWork)>{
    ensure!(access.wave_rows()>0 && access.wave_rows()<=64 && !query.is_empty() && query.iter().all(|v|v.is_finite()),"navigation admission");let mut work=NavigationWork::default();let mut initial=fetch(access,&[root],&mut work)?;let Some(node)=initial.pop().unwrap()else{return Ok((vec![],work));};
    if matches!(node.kind,Kind::Leaf{..}){ensure!(node.centroid.len()==query.len(),"root centroid dimension");let score=metric.distance(query,&node.centroid);ensure!(score.is_finite(),"root distance finite");work.distance_evaluations+=1;return Ok((vec![(root,score)],work));}drop(node);
    let mut beam=vec![root];let mut leaves=Vec::new();
    loop{
        ensure!(work.levels<64,"navigation depth/cycle admission");let mut scores=Vec::new();
        for roots in beam.chunks(access.wave_rows()){
            let records=fetch(access,roots,&mut work)?;let parent_wave_bytes:usize=records.iter().flatten().map(|v|v.payload_bytes()).sum();
            for record in records.into_iter().flatten(){if let Kind::Internal{children}=&record.kind{work.expanded_internals+=1;
                for chunk in children.chunks(access.wave_rows()) {let candidates=fetch(access,chunk,&mut work)?;let child_wave_bytes:usize=candidates.iter().flatten().map(|v|v.payload_bytes()).sum();work.peak_accounted_record_pin_payload_bytes=work.peak_accounted_record_pin_payload_bytes.max(parent_wave_bytes+child_wave_bytes);for child in candidates.into_iter().flatten(){ensure!(child.centroid.len()==query.len(),"child centroid dimension");let score=metric.distance(query,&child.centroid);ensure!(score.is_finite(),"child distance finite");scores.push((child.id,score));work.distance_evaluations+=1;}}
            }}
        }
        if scores.is_empty(){break;}work.levels+=1;work.peak_frontier_scores=work.peak_frontier_scores.max(scores.len());let params=policy.level_params(work.levels);scores.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());let count=effective_beam(&scores,params.tau,params.beam_min,params.beam_max);scores.truncate(count);
        let mut next=Vec::new();for chunk in scores.chunks(access.wave_rows()){let ids:Vec<_>=chunk.iter().map(|v|v.0).collect();let records=fetch(access,&ids,&mut work)?;for ((id,score),record) in chunk.iter().zip(records){if let Some(record)=record{match record.kind{Kind::Leaf{..}=>leaves.push((*id,*score)),Kind::Internal{..}=>next.push(*id)}}}}
        if next.is_empty(){break;}beam=next;
    }
    leaves.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());Ok((leaves,work))
}

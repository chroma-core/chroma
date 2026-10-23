//! Joins durable node payloads to the reference mutation metadata. The latter
//! remains resident and is explicitly not a partitioned topology solution.
use super::*;
use super::super::super::{live_topology::{LiveTopology,WriteScope,Working},topology_access::{NodeAccess,NodeRecord,Kind,Binding}};
pub(crate) struct LiveAccess<'a,I:VectorKey>{pub kernel:&'a HierarchicalSpannWriter<I>}
impl<I:VectorKey> NodeAccess for LiveAccess<'_,I>{
 fn wave_rows(&self)->usize{self.kernel.live_topology.as_ref().unwrap().limits.wave_rows}
 fn fetch(&self,ids:&[u32])->Result<Vec<Option<Arc<NodeRecord>>>>{let store=self.kernel.live_topology.as_ref().unwrap();ensure!(ids.len()<=store.limits.wave_rows,"live wave admission");ids.iter().map(|id|store.get(self.kernel.node_owner(*id,self.kernel.owner_backend.as_ref().unwrap().leaf_owner.contains_key(id)),*id)).collect()}
}
pub(crate) struct Mutation<'a,I:VectorKey>{kernel:&'a HierarchicalSpannWriter<I>,_scope:WriteScope<'a>,outer:bool}
impl<I:VectorKey> Drop for Mutation<'_,I>{fn drop(&mut self){if self.outer{let store=self.kernel.live_topology.as_ref().unwrap();if thread::panicking(){store.poison();return;}if let Err(error)=self.kernel.flush_topology_metadata().and_then(|_|store.flush()){store.poison();panic!("live structural publication failed: {error}");}}}}
impl<I:VectorKey> HierarchicalSpannWriter<I>{
 pub(crate) fn topology_mutation(&self)->Option<Mutation<'_,I>>{self.live_topology.as_ref().map(|store|{let outer=!store.owns_write();let scope=store.write_scope();Mutation{kernel:self,_scope:scope,outer}})}
 pub(crate) fn topology_working(&self,records:usize)->Option<Working<'_>>{self.live_topology.as_ref().map(|store|store.reserve_mutation(records).expect("live topology pre-detach working admission"))}
 pub(crate) fn node_owner(&self,id:u32,leaf:bool)->usize{if leaf{let b=self.owner_backend.as_ref().unwrap();b.leaf_owner.get(&id).map(|v|*v).unwrap_or_else(||b.hint())}else{0}}
 pub(crate) fn node_centroid(&self,id:u32,fallback:&[f32])->Vec<f32>{if let Some(store)=&self.live_topology{store.get(self.node_owner(id,self.owner_backend.as_ref().unwrap().leaf_owner.contains_key(&id)),id).expect("live centroid fetch").expect("live centroid record missing").centroid.clone()}else{fallback.to_vec()}}
 pub(crate) fn live_record(&self,id:u32,node:&TreeNode<I>,centroid:Vec<f32>,owner:usize)->NodeRecord{match node{TreeNode::Leaf(v)=>NodeRecord{id,parent:v.parent_id,centroid,kind:Kind::Leaf{physical_rows:v.length},binding:Binding::Live{owner,epoch:0}},TreeNode::Internal(v)=>NodeRecord{id,parent:v.parent_id,centroid,kind:Kind::Internal{children:v.children.clone()},binding:Binding::Live{owner,epoch:0}}}}
 pub(crate) fn put_live_centroid(&self,id:u32,centroid:&[f32])->Result<()>{let store=self.live_topology.as_ref().unwrap();let owner=self.node_owner(id,self.owner_backend.as_ref().unwrap().leaf_owner.contains_key(&id));let old=store.get(owner,id)?.ok_or_else(||anyhow::anyhow!("centroid update node missing"))?;let mut record=(*old).clone();record.centroid=centroid.to_vec();store.put(owner,record)}
 pub(crate) fn flush_topology_metadata(&self)->Result<()>{let Some(store)=&self.live_topology else{return Ok(());};ensure!(store.owns_write(),"metadata flush requires structural scope");let ids:Vec<_>=self.topology_dirty.iter().map(|v|*v).collect();for id in ids{let action=self.nodes.get(&id).map(|node|{let owner=self.node_owner(id,matches!(node.value(),TreeNode::Leaf(_)));let centroid=self.node_centroid(id,node.centroid());(owner,self.live_record(id,node.value(),centroid,owner))});if let Some((owner,record))=action{store.put(owner,record)?;}else{let owner=self.node_owner(id,self.owner_backend.as_ref().unwrap().leaf_owner.contains_key(&id));store.delete(owner,id)?;}self.topology_dirty.remove(&id);}Ok(())}
}
impl<I:VectorKey> Backend<I>{
 pub(super) fn restore_leaf_centroid(&self,id:u32,leaf:&mut LeafNode<I>)->Result<()>{if let Some(store)=self.topology.lock().clone(){let owner=self.leaf_owner.get(&id).map(|v|*v).ok_or_else(||anyhow::anyhow!("leaf owner missing"))?;let record=store.get(owner,id)?.ok_or_else(||anyhow::anyhow!("live leaf node missing"))?;leaf.centroid=record.centroid.clone();leaf.centroid_code=Code::<1>::quantize(&leaf.centroid,&vec![0.;leaf.centroid.len()]).as_ref().to_vec();}Ok(())}
}

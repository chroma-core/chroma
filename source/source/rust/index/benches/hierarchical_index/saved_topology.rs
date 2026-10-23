//! Point-read, immutable generation node access. Opening a query reader loads
//! only scalar admission and the root; complete-tree census remains explicit.
use super::{topology_access::*,persistance::{PREFIX_CENTROID,NO_PARENT},writer::persistence::block_on_for_sync_writer};
use anyhow::{ensure,Result};
use chroma_blockstore::{provider::BlockfileProvider,arrow::provider::BlockfileReaderOptions,BlockfileReader};
use chroma_types::hierarchical_spann::{HierarchicalLeafNode,HierarchicalInternalNode};
use parking_lot::Mutex;
use std::{collections::{BTreeMap,VecDeque},sync::Arc};
#[derive(Clone,Copy)]
pub struct Limits{pub entries:usize,pub cache_payload_bytes:usize,pub wave_rows:usize,pub record_payload_bytes:usize,pub children:usize}
impl Default for Limits{fn default()->Self{Self{entries:16,cache_payload_bytes:64*1024,wave_rows:16,record_payload_bytes:64*1024,children:4096}}}
#[derive(Default)]struct Cache{records:BTreeMap<u32,Arc<NodeRecord>>,order:VecDeque<u32>,bytes:usize,peak_bytes:usize,peak_entries:usize,evictions:usize,hits:usize,misses:usize,point_reads:usize,centroid_return_bytes:usize,peak_decoded_record_payload_bytes:usize,errors:usize}
pub struct SavedTopology{
    leaf:BlockfileReader<'static,u32,HierarchicalLeafNode<'static>>,
    internal:BlockfileReader<'static,u32,HierarchicalInternalNode<'static>>,
    vector:BlockfileReader<'static,u32,&'static [f32]>,
    generation:u64,dim:usize,limits:Limits,cache:Mutex<Cache>,reads:Mutex<()>,
}
impl SavedTopology{
    pub async fn open(p:&BlockfileProvider,roots:&super::writer::HierarchicalSpannIds,generation:u64,dim:usize,limits:Limits)->Result<Self>{
        ensure!(dim>0 && dim<=1024 && limits.entries>0 && limits.entries<=256 && limits.cache_payload_bytes>0 && limits.cache_payload_bytes<=16*1024*1024 && limits.record_payload_bytes>0 && limits.record_payload_bytes<=limits.cache_payload_bytes && limits.children>0 && limits.children<=4096 && limits.wave_rows>0 && limits.wave_rows<=64,"saved topology admission");
        macro_rules! io{($v:expr)=>{$v.map_err(|e|anyhow::anyhow!("{e}"))?};}
        Ok(Self{leaf:io!(p.read::<u32,HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(roots.leaf_node_id,String::new())).await),internal:io!(p.read::<u32,HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(roots.internal_node_id,String::new())).await),vector:io!(p.read::<u32,&[f32]>(BlockfileReaderOptions::new(roots.vector_data_id,String::new())).await),generation,dim,limits,cache:Mutex::default(),reads:Mutex::default()})
    }
    fn record(&self,id:u32)->Result<Arc<NodeRecord>>{
        {let mut c=self.cache.lock();if let Some(v)=c.records.get(&id).cloned(){c.hits+=1;c.order.retain(|key|*key!=id);c.order.push_back(id);return Ok(v);}c.misses+=1;}
        // No cache guard spans storage. This reader guard ensures no borrowed
        // block value survives a clear, even with concurrent fetch callers.
        let _read=self.reads.lock();
        let loaded=block_on_for_sync_writer(async{
            macro_rules! io{($v:expr)=>{$v.map_err(|e|anyhow::anyhow!("{e}"))?};}
            self.cache.lock().point_reads+=1;let leaf=io!(self.leaf.get("",id).await);self.cache.lock().point_reads+=1;let internal=io!(self.internal.get("",id).await);ensure!(leaf.is_some()!=internal.is_some(),"missing or duplicate typed saved node {id}");
            let (parent,kind)=if let Some(v)=leaf{(v.parent,Kind::Leaf{physical_rows:v.length as usize})}else{let v=internal.unwrap();ensure!(v.children.len()<=self.limits.children,"saved child-count admission");(v.parent,Kind::Internal{children:v.children.to_vec()})};
            self.cache.lock().point_reads+=1;let centroid=io!(self.vector.get(PREFIX_CENTROID,id).await).ok_or_else(||anyhow::anyhow!("missing saved centroid {id}"))?;self.cache.lock().centroid_return_bytes+=centroid.len()*4;ensure!(centroid.len()==self.dim && centroid.iter().all(|v|v.is_finite()),"saved centroid shape");
            Ok::<_,anyhow::Error>(NodeRecord{id,parent:(parent!=NO_PARENT).then_some(parent),centroid:centroid.to_vec(),kind,binding:Binding::Saved{generation:self.generation}})
        });
        self.leaf.clear_loaded_blocks();self.internal.clear_loaded_blocks();self.vector.clear_loaded_blocks();let record=Arc::new(match loaded{Ok(v)=>v,Err(e)=>{self.cache.lock().errors+=1;return Err(e);}});let bytes=record.payload_bytes();let mut c=self.cache.lock();c.peak_decoded_record_payload_bytes=c.peak_decoded_record_payload_bytes.max(bytes);if bytes>self.limits.record_payload_bytes{c.errors+=1;anyhow::bail!("decoded node payload admission exceeded");}
        if c.records.contains_key(&id){return Ok(c.records[&id].clone());}
        while c.records.len()>=self.limits.entries || c.bytes.checked_add(bytes).is_none_or(|v|v>self.limits.cache_payload_bytes){let victim=c.order.pop_front().expect("record fits empty cache");c.bytes-=c.records.remove(&victim).unwrap().payload_bytes();c.evictions+=1;}
        c.bytes+=bytes;c.records.insert(id,record.clone());c.order.push_back(id);c.peak_bytes=c.peak_bytes.max(c.bytes);c.peak_entries=c.peak_entries.max(c.records.len());Ok(record)
    }
    pub fn report(&self)->serde_json::Value{let c=self.cache.lock();serde_json::json!({"generation":self.generation,"fetch_errors":c.errors,"traffic_complete":c.errors==0,"cache_entries":c.records.len(),"cache_payload_bytes":c.bytes,"peak_cache_entries":c.peak_entries,"peak_cache_payload_bytes":c.peak_bytes,"cache_entry_cap":self.limits.entries,"cache_payload_cap_bytes":self.limits.cache_payload_bytes,"evictions":c.evictions,"hits":c.hits,"misses":c.misses,"typed_metadata_and_centroid_point_reads":c.point_reads,"logical_centroid_return_bytes":c.centroid_return_bytes,"peak_decoded_record_payload_bytes":c.peak_decoded_record_payload_bytes,"scope":"Point-read pinned tuple only; no whole-tree range read. Accounted clean-record payload excludes container/Arc/provider/root-index/encoded/decode allocations. Evicted records may remain in the caller's bounded fetch wave. Post-decode admission is not a maximum allocation or RSS bound. Physical IO and network bytes are unavailable."})}
}
impl NodeAccess for SavedTopology{
    fn wave_rows(&self)->usize{self.limits.wave_rows}
    fn fetch(&self,ids:&[u32])->Result<Vec<Option<Arc<NodeRecord>>>>{ensure!(!ids.is_empty() && ids.len()<=self.limits.wave_rows,"saved fetch wave");ids.iter().map(|id|self.record(*id).map(Some)).collect()}
}

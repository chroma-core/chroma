//! Append-only immutable vector objects for the owner adapter. Membership stays
//! in owner actors; this module stores content, not current versions or routing.
use anyhow::{ensure, Result};
use chroma_blockstore::{arrow::provider::BlockfileReaderOptions,provider::BlockfileProvider,BlockfileWriterOptions};
use parking_lot::{Condvar,Mutex};
use sha2::{Digest,Sha256};
use std::{collections::{BTreeMap,VecDeque},sync::{Arc,atomic::{AtomicU64,Ordering}}};
use uuid::Uuid;
use super::super::persistance::PREFIX_EMBEDDING;

#[derive(Clone,Debug,PartialEq,Eq)]
pub(super) struct VectorHandle { pub root:Uuid,pub key:u32,dim:usize,sha:[u8;32] }
#[derive(Clone,Copy)]
pub struct VectorLimits { pub append_rows:usize,pub fetch_rows:usize,pub cache_rows:usize,pub readers_per_owner:usize,pub workers:usize,pub split_rows:usize,pub reader_pin_bytes:usize }
impl Default for VectorLimits {fn default()->Self{Self{append_rows:64,fetch_rows:16,cache_rows:32,readers_per_owner:2,workers:4,split_rows:256,reader_pin_bytes:256*1024}}}
#[derive(Default)]
struct Cache { values:BTreeMap<u32,(VectorHandle,Arc<[f32]>)>,order:VecDeque<u32> }
#[derive(Default)]
struct Activity { owners:Vec<usize>,total:usize,peak:usize,owner_peak:usize }
#[derive(Default)]
struct Counts { gets:AtomicU64,hits:AtomicU64,opens:AtomicU64,staged:AtomicU64,fetch_bytes:AtomicU64,peak_pins:AtomicU64,errors:AtomicU64,stage_ns:AtomicU64,publication_ns:AtomicU64,publication_calls:AtomicU64,pin_rejections:AtomicU64 }
pub(super) struct OwnerVectorStore {
    provider:BlockfileProvider,dim:usize,pub limits:VectorLimits,
    latest:Mutex<Option<Uuid>>,pending:Mutex<BTreeMap<u32,VectorHandle>>,
    caches:Vec<Mutex<Cache>>,activity:Mutex<Activity>,available:Condvar,
    counts:Counts,traffic:Mutex<BTreeMap<String,u64>>,
    #[cfg(test)]
    fetch_pause:Mutex<Option<(std::sync::mpsc::Sender<()>,std::sync::mpsc::Receiver<()>)>>,
}
fn digest(values:&[f32])->[u8;32]{let mut h=Sha256::new();for v in values{h.update(v.to_bits().to_le_bytes());}h.finalize().into()}
struct FetchSlot<'a>(&'a OwnerVectorStore,usize);
impl Drop for FetchSlot<'_>{fn drop(&mut self){let mut a=self.0.activity.lock();a.total-=1;a.owners[self.1]-=1;self.0.available.notify_all();}}
impl OwnerVectorStore {
    pub fn new(provider:BlockfileProvider,dim:usize,owners:usize,limits:VectorLimits)->Result<Self>{
        ensure!(owners>0 && dim>0 && dim<=768 && limits.reader_pin_bytes>0 && limits.reader_pin_bytes<=256*1024 && limits.append_rows>0 && limits.append_rows<=64 && limits.fetch_rows>0 && limits.fetch_rows<=16 && limits.cache_rows>0 && limits.cache_rows<=32 && limits.workers>0 && limits.workers<=4 && limits.readers_per_owner>0 && limits.readers_per_owner<=2 && limits.split_rows>0 && limits.split_rows<=256,"bounded vector limits");
        Ok(Self{provider,dim,limits,latest:Mutex::new(None),pending:Mutex::new(BTreeMap::new()),caches:(0..owners).map(|_|Mutex::default()).collect(),activity:Mutex::new(Activity{owners:vec![0;owners],..Default::default()}),available:Condvar::new(),counts:Counts::default(),traffic:Mutex::default(),#[cfg(test)] fetch_pause:Mutex::new(None)})
    }
    /// A joined append phase writes at most one bounded batch. No current owner
    /// is installed here. Flush completes before any handle can be registered.
    pub fn stage(&self,rows:&[(u32,Vec<f32>)])->Result<()> {
        let started=std::time::Instant::now();
        ensure!(!rows.is_empty() && rows.len()<=self.limits.append_rows && self.pending.lock().is_empty(),"append staging capacity/state");
        ensure!(rows.windows(2).all(|v|v[0].0<v[1].0) && rows.iter().all(|(_,v)|v.len()==self.dim && v.iter().all(|x|x.is_finite())),"append vector order/shape");
        let previous=*self.latest.lock();
        let root=super::persistence::block_on_for_sync_writer(async {
            let mut options=BlockfileWriterOptions::new(String::new()).ordered_mutations().max_block_size_bytes(16*1024);if let Some(root)=previous{options=options.fork(root);}
            let writer=self.provider.write::<u32,Vec<f32>>(options).await.map_err(|e|anyhow::anyhow!("vector stage open: {e}"))?;
            for (id,v) in rows{writer.set(PREFIX_EMBEDDING,*id,v.clone()).await.map_err(|e|anyhow::anyhow!("vector stage set: {e}"))?;}
            let flush=writer.commit::<u32,Vec<f32>>().await.map_err(|e|anyhow::anyhow!("vector stage commit: {e}"))?;let root=flush.id();flush.flush::<u32,Vec<f32>>().await.map_err(|e|anyhow::anyhow!("vector stage flush: {e}"))?;Ok::<_,anyhow::Error>(root)
        })?;
        let handles=rows.iter().map(|(key,v)|(*key,VectorHandle{root,key:*key,dim:self.dim,sha:digest(v)})).collect();
        *self.latest.lock()=Some(root);*self.pending.lock()=handles;self.counts.staged.fetch_add((rows.len()*self.dim*4) as u64,Ordering::Relaxed);self.counts.stage_ns.fetch_add(started.elapsed().as_nanos() as u64,Ordering::Relaxed);Ok(())
    }
    pub fn publication_completed(&self,nanos:u64){self.counts.publication_calls.fetch_add(1,Ordering::Relaxed);self.counts.publication_ns.fetch_add(nanos,Ordering::Relaxed);}
    #[cfg(test)]
    pub fn pause_next_fetch(&self,entered:std::sync::mpsc::Sender<()>,resume:std::sync::mpsc::Receiver<()>){*self.fetch_pause.lock()=Some((entered,resume));}
    pub fn staged(&self,id:u32)->Result<VectorHandle>{self.pending.lock().get(&id).cloned().ok_or_else(||anyhow::anyhow!("missing staged immutable vector {id}"))}
    pub fn finish_stage(&self){self.pending.lock().clear();}
    pub fn latest(&self)->Result<Uuid>{self.latest.lock().ok_or_else(||anyhow::anyhow!("no immutable vector root"))}
    fn slot(&self,owner:usize)->Result<FetchSlot<'_>>{
        ensure!(owner<self.caches.len(),"wrong vector owner");let mut a=self.activity.lock();
        while a.total>=self.limits.workers || a.owners[owner]>=self.limits.readers_per_owner{self.available.wait(&mut a);}
        a.total+=1;a.owners[owner]+=1;a.peak=a.peak.max(a.total);a.owner_peak=a.owner_peak.max(a.owners[owner]);Ok(FetchSlot(self,owner))
    }
    /// Call only after authority and ID guards drop. Cache guards never cover
    /// storage I/O. Each active fetch owns its reader and copies bytes before
    /// dropping reader pins; no borrowed vector escapes this method.
    pub fn fetch(&self,owner:usize,handles:&[VectorHandle])->Result<Vec<Arc<[f32]>>>{self.fetch_impl(owner,handles,true)}
    #[cfg(test)]
    pub fn fetch_point_reference(&self,owner:usize,handles:&[VectorHandle])->Result<Vec<Arc<[f32]>>>{self.fetch_impl(owner,handles,false)}
    fn fetch_impl(&self,owner:usize,handles:&[VectorHandle],reuse:bool)->Result<Vec<Arc<[f32]>>>{
        ensure!(handles.len()<=self.limits.fetch_rows,"vector fetch batch cap");let _slot=self.slot(owner)?;let mut result=Vec::with_capacity(handles.len());
        let outcome=super::persistence::block_on_for_sync_writer(async {
            let mut start=0;
            while start<handles.len(){
                // Reuse never reorders requests or crosses an immutable root.
                let root=handles[start].root;let mut end=start+1;
                if reuse{while end<handles.len() && handles[end].root==root{end+=1;}}
                let cell=tokio::sync::OnceCell::new();
                for handle in &handles[start..end]{
                    ensure!(handle.dim==self.dim,"vector handle dimension");self.counts.gets.fetch_add(1,Ordering::Relaxed);
                    let hit={let cache=self.caches[owner].lock();cache.values.get(&handle.key).filter(|(h,_)|h==handle).map(|(_,v)|v.clone())};
                    if let Some(v)=hit{self.counts.hits.fetch_add(1,Ordering::Relaxed);result.push(v);continue;}
                    #[cfg(test)]
                    {let pause={self.fetch_pause.lock().take()};if let Some((entered,resume))=pause{entered.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(10)).expect("test fetch resume");}}
                    let reader=cell.get_or_try_init(||async {
                        let reader=self.provider.read::<u32,&[f32]>(BlockfileReaderOptions::new(root,String::new())).await.map_err(|e|anyhow::anyhow!("vector root open: {e}"))?;
                        self.counts.opens.fetch_add(1,Ordering::Relaxed);reader.start_loaded_block_observation();Ok::<_,anyhow::Error>(reader)
                    }).await?;
                    let v=reader.get(PREFIX_EMBEDDING,handle.key).await.map_err(|e|anyhow::anyhow!("vector fetch: {e}"))?.ok_or_else(||anyhow::anyhow!("registered immutable vector missing: {}",handle.key))?;
                    let pins=reader.loaded_blocks_stats().1;self.counts.peak_pins.fetch_max(pins,Ordering::Relaxed);
                    if pins>self.limits.reader_pin_bytes as u64{self.counts.pin_rejections.fetch_add(1,Ordering::Relaxed);anyhow::bail!("decoded reader pin admission exceeded: {pins}");}
                    ensure!(v.len()==self.dim && digest(v)==handle.sha,"immutable vector content mismatch");let v:Arc<[f32]>=Arc::from(v);
                    self.counts.fetch_bytes.fetch_add((v.len()*4) as u64,Ordering::Relaxed);
                    {let mut cache=self.caches[owner].lock();if !cache.values.contains_key(&handle.key){if cache.values.len()==self.limits.cache_rows{let id=cache.order.pop_front().unwrap();cache.values.remove(&id);}cache.order.push_back(handle.key);}cache.values.insert(handle.key,(handle.clone(),v.clone()));}
                    result.push(v);
                }
                if let Some(reader)=cell.get(){
                    let observation=serde_json::to_value(reader.loaded_block_observation().expect("fresh vector observer"))?;
                    ensure!(observation["access_overflow"]==false,"vector observation overflow");
                    let mut traffic=self.traffic.lock();for block in observation["block_access"].as_object().unwrap().values(){for key in ["reader_requests","pin_hits","pin_misses","provider_obtains","provider_hits","provider_misses","storage_callbacks","encoded_bytes","decode_attempts","decode_successes"]{*traffic.entry(key.into()).or_default()+=block[key].as_u64().unwrap();}}
                }
                // Every pin and root reader drops at this run boundary.
                start=end;
            }Ok::<_,anyhow::Error>(result)
        });if outcome.is_err(){self.counts.errors.fetch_add(1,Ordering::Relaxed);}outcome
    }
    pub fn report(&self)->serde_json::Value{
        let retained:usize=self.caches.iter().map(|v|v.lock().values.len()).sum();let a=self.activity.lock();
        serde_json::json!({"policy":"bounded-append-only-owner-vectors-v1","fetch_policy":"consecutive-immutable-root-batch-reuse-v1","reader_pin_admission_bytes":self.limits.reader_pin_bytes,"all_slots_admitted_pin_bytes":self.limits.workers*self.limits.reader_pin_bytes,"authored_block_target_bytes":16384,"reader_pin_admission_rejections":self.counts.pin_rejections.load(Ordering::Relaxed),"provider_traffic_complete":self.counts.errors.load(Ordering::Relaxed)==0,"root_index_bytes":serde_json::Value::Null,"root_index_scope":"No public byte-size API; root-index/encoded/provider allocation is excluded from accounted block-pin admission.","logical_gets":self.counts.gets.load(Ordering::Relaxed),"cache_hits":self.counts.hits.load(Ordering::Relaxed),"reader_opens":self.counts.opens.load(Ordering::Relaxed),"reader_drops_at_quiescence":if a.total==0{Some(self.counts.opens.load(Ordering::Relaxed))}else{None},"explicit_reader_clears":0,"quiescent":a.total==0,"staged_logical_bytes":self.counts.staged.load(Ordering::Relaxed),"copied_return_bytes":self.counts.fetch_bytes.load(Ordering::Relaxed),"retained_cache_vectors":retained,"retained_cache_owned_payload_bytes":retained*self.dim*4,"cache_payload_cap_bytes":self.caches.len()*self.limits.cache_rows*self.dim*4,"append_input_payload_cap_bytes":self.limits.append_rows*self.dim*4,"provider_writer_embedding_set_payload_cap_bytes":self.limits.append_rows*self.dim*4,"single_split_tuple_payload_cap_bytes":self.limits.split_rows*self.dim*4,"single_npa_window_payload_cap_bytes":64*self.dim*4,"peak_single_reader_pinned_bytes":self.counts.peak_pins.load(Ordering::Relaxed),"active_fetch_workers":a.total,"peak_fetch_workers":a.peak,"peak_fetch_slots_per_owner":a.owner_peak,"max_readers_per_fetch_slot":1,"staging_elapsed_ns":self.counts.stage_ns.load(Ordering::Relaxed),"joined_publication_elapsed_ns":self.counts.publication_ns.load(Ordering::Relaxed),"publication_forks":self.counts.publication_calls.load(Ordering::Relaxed),"publication_logical_embedding_set_bytes":0,"fetch_errors":self.counts.errors.load(Ordering::Relaxed),"provider_traffic":*self.traffic.lock(),"scope":"Cache-owned payload only. Caller split/NPA tuples, staged input/builders and provider root/block caches are separate. Encoded provider returns are not physical IO. Save forks the latest durable embedding root; joined postings and versions remain full publication work."})
    }
}

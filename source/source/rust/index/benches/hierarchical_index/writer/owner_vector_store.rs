//! Append-only immutable vector objects for the owner adapter. Membership stays
//! in owner actors; this module stores content, not current versions or routing.
use anyhow::{ensure, Result};
use chroma_blockstore::{arrow::provider::BlockfileReaderOptions,provider::BlockfileProvider,BlockfileWriterOptions};
use parking_lot::{Condvar,Mutex};
use sha2::{Digest,Sha256};
use std::{collections::{BTreeMap,VecDeque},sync::{Arc,atomic::{AtomicU64,Ordering}}};
use uuid::Uuid;
use super::super::persistance::PREFIX_EMBEDDING;
use super::super::common::VectorKey;
fn embedding_prefix(home:u32)->String{if home==0{PREFIX_EMBEDDING.into()}else{format!("embedding/home/{home:08x}")}}

#[derive(Clone,Debug,PartialEq,Eq)]
pub(super) struct VectorHandle { pub root:Uuid,pub key:u32,pub home:u32,pub(super) dim:usize,pub(super) sha:[u8;32] }
/// An explicit decoded-payload allowance replaces the legacy row ceiling.
/// This excludes caller clones, provider/pins and allocator/container overhead.
#[derive(Clone,Copy,Debug,serde::Deserialize,serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecodedVectorCachePolicy {pub payload_bytes_per_owner:usize,pub aggregate_payload_bytes:usize}
#[derive(Clone,Copy,Debug,serde::Serialize)]
pub struct CacheAdmission {pub rows_per_owner:usize,pub payload_bytes_per_vector:usize,pub payload_cap_per_owner:usize,pub aggregate_payload_cap:usize,pub selected_metadata_bytes_per_entry:usize,pub aggregate_selected_metadata_cap:usize}
impl DecodedVectorCachePolicy {
 pub fn admit(self,dim:usize,owners:usize)->Result<CacheAdmission>{
  ensure!(dim>0&&dim<=768&&owners>0,"decoded cache dimension/owners");let row=dim.checked_mul(4).ok_or_else(||anyhow::anyhow!("cache row overflow"))?;let rows=self.payload_bytes_per_owner/row;ensure!(rows>0,"decoded cache must fit one vector");
  let requested=self.payload_bytes_per_owner.checked_mul(owners).ok_or_else(||anyhow::anyhow!("aggregate decoded cache overflow"))?;ensure!(requested<=self.aggregate_payload_bytes,"aggregate decoded cache payload admission");
  let per=rows.checked_mul(row).ok_or_else(||anyhow::anyhow!("decoded cache extent"))?;let aggregate=per.checked_mul(owners).ok_or_else(||anyhow::anyhow!("decoded cache aggregate extent"))?;
  let fields=std::mem::size_of::<VectorHandle>()+std::mem::size_of::<Arc<[f32]>>()+2*std::mem::size_of::<u64>();let metadata=rows.checked_mul(fields).and_then(|v|v.checked_mul(owners)).ok_or_else(||anyhow::anyhow!("cache selected metadata overflow"))?;
  Ok(CacheAdmission{rows_per_owner:rows,payload_bytes_per_vector:row,payload_cap_per_owner:per,aggregate_payload_cap:aggregate,selected_metadata_bytes_per_entry:fields,aggregate_selected_metadata_cap:metadata})
 }
}
#[derive(Clone,Copy)]
pub struct VectorLimits { pub decoded_vector_cache:Option<DecodedVectorCachePolicy>,pub append_rows:usize,pub fetch_rows:usize,pub cache_rows:usize,pub readers_per_owner:usize,pub workers:usize,pub split_rows:usize,pub reader_pin_bytes:usize,pub split_working_bytes:usize,pub staging_working_bytes:usize }
impl Default for VectorLimits {fn default()->Self{Self{decoded_vector_cache:None,append_rows:64,fetch_rows:16,cache_rows:32,readers_per_owner:2,workers:4,split_rows:256,reader_pin_bytes:256*1024,split_working_bytes:16*1024*1024,staging_working_bytes:1024*1024}}}
#[derive(Default)]
struct Cache { values:BTreeMap<u64,(VectorHandle,Arc<[f32]>)>,order:VecDeque<u64> }
#[derive(Default)]
struct Activity { owners:Vec<usize>,total:usize,peak:usize,owner_peak:usize }
#[derive(Default)]
struct Counts { gets:AtomicU64,hits:AtomicU64,opens:AtomicU64,staged:AtomicU64,stage_chunks:AtomicU64,peak_pending:AtomicU64,fetch_bytes:AtomicU64,peak_pins:AtomicU64,errors:AtomicU64,stage_ns:AtomicU64,publication_ns:AtomicU64,publication_calls:AtomicU64,pin_rejections:AtomicU64,split_active:AtomicU64,split_peak:AtomicU64,split_rejections:AtomicU64 }
pub(super) struct OwnerVectorStore {
    provider:BlockfileProvider,dim:usize,pub limits:VectorLimits,cache_admission:CacheAdmission,
    latest:Mutex<Option<Uuid>>,pending:Mutex<BTreeMap<u64,VectorHandle>>,
    caches:Vec<Mutex<Cache>>,activity:Mutex<Activity>,available:Condvar,
    counts:Counts,traffic:Mutex<BTreeMap<String,u64>>,
    #[cfg(test)]
    fetch_pause:Mutex<Option<(std::sync::mpsc::Sender<()>,std::sync::mpsc::Receiver<()>)>>,
}
fn digest(values:&[f32])->[u8;32]{let mut h=Sha256::new();for v in values{h.update(v.to_bits().to_le_bytes());}h.finalize().into()}
pub(super) struct SplitReservation<'a>(&'a OwnerVectorStore,u64);
impl Drop for SplitReservation<'_>{fn drop(&mut self){self.0.counts.split_active.fetch_sub(self.1,Ordering::Relaxed);}}
pub fn split_reservation_bytes(rows:usize,dim:usize,wide:bool)->Result<usize>{
    let extra=if wide{rows.checked_mul(8+std::mem::size_of::<chroma_index::spann::utils::EmbeddingPoint>()).ok_or_else(||anyhow::anyhow!("wide split scratch overflow"))?}else{0};
    split_bytes(rows,dim)?.checked_add(extra).ok_or_else(||anyhow::anyhow!("wide split reservation overflow"))
}
/// Conservative parallel owned-window admission; stacks/provider/routing are separate.
pub fn parallel_working_bytes(workers:usize,dim:usize,stable_worker_bytes:usize)->Result<usize>{
    ensure!(workers>0&&workers<=32&&dim>0&&dim<=768,"parallel worker/dimension bounds");
    let per_worker=dim.checked_mul(64*4).and_then(|n|n.checked_add(256*1024)).and_then(|n|n.checked_add(stable_worker_bytes)).ok_or_else(||anyhow::anyhow!("parallel per-worker extent"))?;
    workers.checked_mul(per_worker).ok_or_else(||anyhow::anyhow!("parallel working extent"))
}
fn split_bytes(rows:usize,dim:usize)->Result<usize>{
    // Payload plus tuple/group capacity, old/output codes, distance/label/index
    // scratch and centers. This modeled admission excludes allocator/provider state.
    rows.checked_mul(dim.checked_mul(4).and_then(|n|n.checked_add(256+2*chroma_index::quantization::Code::<1>::size(dim))).ok_or_else(||anyhow::anyhow!("split row extent"))?)
        .and_then(|n|n.checked_add(dim.checked_mul(64)?)).and_then(|n|n.checked_add(65536)).ok_or_else(||anyhow::anyhow!("split working extent"))
}
/// Modeled staging admission, not an allocator or provider-cache ceiling.
pub fn staging_working_bytes(rows:usize,dim:usize)->Result<usize>{
    ensure!(rows>0 && rows<u32::MAX as usize && dim>0 && dim<=768,"staging row/dimension bounds");
    rows.checked_mul(dim.checked_mul(16).and_then(|v|v.checked_add(256)).ok_or_else(||anyhow::anyhow!("staging row extent"))?).and_then(|v|v.checked_add(65536)).ok_or_else(||anyhow::anyhow!("staging working extent"))
}
struct FetchSlot<'a>(&'a OwnerVectorStore,usize);
impl Drop for FetchSlot<'_>{fn drop(&mut self){let mut a=self.0.activity.lock();a.total-=1;a.owners[self.1]-=1;self.0.available.notify_all();}}
impl OwnerVectorStore {
    pub fn stage(&self,rows:&[(u32,Vec<f32>)])->Result<()>{self.stage_identity(rows)}
    pub fn staged(&self,id:u32)->Result<VectorHandle>{self.staged_identity(id)}
    pub fn new(provider:BlockfileProvider,dim:usize,owners:usize,limits:VectorLimits)->Result<Self>{
        ensure!(owners>0 && dim>0 && dim<=768 && limits.reader_pin_bytes>0 && limits.reader_pin_bytes<=256*1024 && limits.append_rows>0 && limits.append_rows<u32::MAX as usize && limits.fetch_rows>0 && limits.fetch_rows<=16 && limits.cache_rows>0 && (limits.decoded_vector_cache.is_some()||limits.cache_rows<=32) && limits.workers>0 && limits.workers<=32 && limits.readers_per_owner>0 && limits.readers_per_owner<=32 && limits.split_rows>0 && limits.split_rows<u32::MAX as usize && limits.split_working_bytes>0,"bounded vector limits");
        let cache_admission=if let Some(policy)=limits.decoded_vector_cache{policy.admit(dim,owners)?}else{let per=limits.cache_rows.checked_mul(dim).and_then(|v|v.checked_mul(4)).ok_or_else(||anyhow::anyhow!("legacy cache payload overflow"))?;DecodedVectorCachePolicy{payload_bytes_per_owner:per,aggregate_payload_bytes:per.checked_mul(owners).ok_or_else(||anyhow::anyhow!("legacy aggregate cache overflow"))?}.admit(dim,owners)?};
        ensure!(staging_working_bytes(limits.append_rows,dim)?<=limits.staging_working_bytes,"staging working budget below configured row capacity");
        ensure!(split_bytes(limits.split_rows,dim)?<=limits.split_working_bytes,"split working budget below configured row capacity");
        Ok(Self{provider,dim,limits,cache_admission,latest:Mutex::new(None),pending:Mutex::new(BTreeMap::new()),caches:(0..owners).map(|_|Mutex::default()).collect(),activity:Mutex::new(Activity{owners:vec![0;owners],..Default::default()}),available:Condvar::new(),counts:Counts::default(),traffic:Mutex::default(),#[cfg(test)] fetch_pause:Mutex::new(None)})
    }
    /// Each storage chunk writes at most one bounded batch within a logical phase. No current owner
    /// is installed here. Flush completes before any handle can be registered.
    pub fn stage_identity<I:VectorKey>(&self,rows:&[(I,Vec<f32>)])->Result<()> {
        let started=std::time::Instant::now();
        ensure!(!rows.is_empty() && rows.len()<=self.limits.append_rows && self.pending.lock().is_empty(),"append staging capacity/state");
        ensure!(staging_working_bytes(rows.len(),self.dim)?<=self.limits.staging_working_bytes,"staging working admission before publication");
        ensure!(rows.windows(2).all(|v|v[0].0<v[1].0) && rows.iter().all(|(_,v)|v.len()==self.dim && v.iter().all(|x|x.is_finite())),"append vector order/shape");
        let previous=*self.latest.lock();
        let root=super::persistence::block_on_for_sync_writer(async {
            let mut options=BlockfileWriterOptions::new(String::new()).ordered_mutations().max_block_size_bytes(16*1024);if let Some(root)=previous{options=options.fork(root);}
            let writer=self.provider.write::<u32,Vec<f32>>(options).await.map_err(|e|anyhow::anyhow!("vector stage open: {e}"))?;
            for (id,v) in rows{writer.set(&embedding_prefix((id.wide()>>32) as u32),id.wide() as u32,v.clone()).await.map_err(|e|anyhow::anyhow!("vector stage set: {e}"))?;}
            let flush=writer.commit::<u32,Vec<f32>>().await.map_err(|e|anyhow::anyhow!("vector stage commit: {e}"))?;let root=flush.id();flush.flush::<u32,Vec<f32>>().await.map_err(|e|anyhow::anyhow!("vector stage flush: {e}"))?;Ok::<_,anyhow::Error>(root)
        })?;
        let handles=rows.iter().map(|(key,v)|(key.wide(),VectorHandle{root,key:key.wide() as u32,home:(key.wide()>>32) as u32,dim:self.dim,sha:digest(v)})).collect();
        *self.latest.lock()=Some(root);*self.pending.lock()=handles;self.counts.stage_chunks.fetch_add(1,Ordering::Relaxed);self.counts.peak_pending.fetch_max(rows.len() as u64,Ordering::Relaxed);self.counts.staged.fetch_add((rows.len()*self.dim*4) as u64,Ordering::Relaxed);self.counts.stage_ns.fetch_add(started.elapsed().as_nanos() as u64,Ordering::Relaxed);Ok(())
    }
    pub fn reserve_split(&self,rows:usize)->Result<SplitReservation<'_>>{self.reserve_split_identity(rows,false)}
    pub fn reserve_split_identity(&self,rows:usize,wide:bool)->Result<SplitReservation<'_>>{
        ensure!(rows<=self.limits.split_rows,"bounded split tuple capacity before detach");let bytes=split_reservation_bytes(self.limits.split_rows,self.dim,wide)? as u64;
        let mut active=self.counts.split_active.load(Ordering::Relaxed);
        loop{let Some(next)=active.checked_add(bytes).filter(|v|*v<=self.limits.split_working_bytes as u64)else{self.counts.split_rejections.fetch_add(1,Ordering::Relaxed);anyhow::bail!("split working budget exhausted before detach");};
            match self.counts.split_active.compare_exchange_weak(active,next,Ordering::Relaxed,Ordering::Relaxed){Ok(_)=>{self.counts.split_peak.fetch_max(next,Ordering::Relaxed);return Ok(SplitReservation(self,bytes));},Err(v)=>active=v}
        }
    }
    pub fn acknowledge_published_root(&self,root:Uuid){*self.latest.lock()=Some(root);}
    pub fn publication_completed(&self,nanos:u64){self.counts.publication_calls.fetch_add(1,Ordering::Relaxed);self.counts.publication_ns.fetch_add(nanos,Ordering::Relaxed);}
    #[cfg(test)]
    pub fn pause_next_fetch(&self,entered:std::sync::mpsc::Sender<()>,resume:std::sync::mpsc::Receiver<()>){*self.fetch_pause.lock()=Some((entered,resume));}
    pub fn staged_identity<I:VectorKey>(&self,id:I)->Result<VectorHandle>{self.pending.lock().get(&id.wide()).cloned().ok_or_else(||anyhow::anyhow!("missing staged immutable vector {id}"))}
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
                    let hit={let cache=self.caches[owner].lock();cache.values.get(&((u64::from(handle.home)<<32)|u64::from(handle.key))).filter(|(h,_)|h==handle).map(|(_,v)|v.clone())};
                    if let Some(v)=hit{self.counts.hits.fetch_add(1,Ordering::Relaxed);result.push(v);continue;}
                    #[cfg(test)]
                    {let pause={self.fetch_pause.lock().take()};if let Some((entered,resume))=pause{entered.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(10)).expect("test fetch resume");}}
                    let reader=cell.get_or_try_init(||async {
                        let reader=self.provider.read::<u32,&[f32]>(BlockfileReaderOptions::new(root,String::new())).await.map_err(|e|anyhow::anyhow!("vector root open: {e}"))?;
                        self.counts.opens.fetch_add(1,Ordering::Relaxed);reader.start_loaded_block_observation();Ok::<_,anyhow::Error>(reader)
                    }).await?;
                    let v=reader.get(&embedding_prefix(handle.home),handle.key).await.map_err(|e|anyhow::anyhow!("vector fetch: {e}"))?.ok_or_else(||anyhow::anyhow!("registered immutable vector missing: {}",handle.key))?;
                    let pins=reader.loaded_blocks_stats().1;self.counts.peak_pins.fetch_max(pins,Ordering::Relaxed);
                    if pins>self.limits.reader_pin_bytes as u64{self.counts.pin_rejections.fetch_add(1,Ordering::Relaxed);anyhow::bail!("decoded reader pin admission exceeded: {pins}");}
                    ensure!(v.len()==self.dim && digest(v)==handle.sha,"immutable vector content mismatch");let v:Arc<[f32]>=Arc::from(v);
                    self.counts.fetch_bytes.fetch_add((v.len()*4) as u64,Ordering::Relaxed);
                    {let mut cache=self.caches[owner].lock();let identity=(u64::from(handle.home)<<32)|u64::from(handle.key);if !cache.values.contains_key(&identity){if cache.values.len()==self.cache_admission.rows_per_owner{let id=cache.order.pop_front().unwrap();cache.values.remove(&id);}cache.order.push_back(identity);}cache.values.insert(identity,(handle.clone(),v.clone()));}
                    result.push(v);
                }
                if let Some(reader)=cell.get(){
                    let observation=serde_json::to_value(reader.loaded_block_observation().expect("fresh vector observer"))?;
                    ensure!(observation["access_overflow"]==false,"vector observation overflow");
                    let mut traffic=self.traffic.lock();for block in observation["block_access"].as_object().unwrap().values(){for key in ["reader_requests","pin_hits","pin_misses","provider_obtains","provider_hits","provider_misses","storage_callbacks","encoded_bytes","decode_attempts","decode_successes","decode_ns"]{*traffic.entry(key.into()).or_default()+=block[key].as_u64().unwrap();}}
                }
                // Every pin and root reader drops at this run boundary.
                start=end;
            }Ok::<_,anyhow::Error>(result)
        });if outcome.is_err(){self.counts.errors.fetch_add(1,Ordering::Relaxed);}outcome
    }
    /// Fixed-key cumulative observations only; this never walks vector caches or block identities.
    pub fn progress_counts(&self)->serde_json::Value{
        let traffic=self.traffic.lock();
        serde_json::json!({"logical_vector_gets":self.counts.gets.load(Ordering::Relaxed),"fetch_batch_calls":serde_json::Value::Null,"reader_opens":self.counts.opens.load(Ordering::Relaxed),"copied_return_bytes":self.counts.fetch_bytes.load(Ordering::Relaxed),"provider_decode_attempts":traffic.get("decode_attempts").copied().unwrap_or(0),"provider_decode_elapsed_ns":traffic.get("decode_ns").copied().unwrap_or(0),"provider_encoded_bytes":traffic.get("encoded_bytes").copied().unwrap_or(0),"provider_traffic_complete":self.counts.errors.load(Ordering::Relaxed)==0})
    }
    pub fn report(&self)->serde_json::Value{
        let retained:usize=self.caches.iter().map(|v|v.lock().values.len()).sum();let a=self.activity.lock();
        let mut report=serde_json::json!({"decoded_vector_cache_policy":self.limits.decoded_vector_cache,"decoded_vector_cache_admission":self.cache_admission,"retained_cache_selected_metadata_field_bytes":retained*self.cache_admission.selected_metadata_bytes_per_entry,"cache_metadata_scope":"Selected handle/Arc/two identity fields only; BTreeMap node headers, VecDeque spare capacity, Arc headers, allocator overhead and caller clones are excluded. Payload cap is not total RSS. Replacement remains insertion-order FIFO; hits do not promote.","policy":"bounded-append-only-owner-vectors-v1","fetch_policy":"consecutive-immutable-root-batch-reuse-v1","reader_pin_admission_bytes":self.limits.reader_pin_bytes,"configured_fetch_workers":self.limits.workers,"configured_readers_per_owner":self.limits.readers_per_owner,"all_slots_admitted_pin_bytes":self.limits.workers*self.limits.reader_pin_bytes,"authored_block_target_bytes":16384,"reader_pin_admission_rejections":self.counts.pin_rejections.load(Ordering::Relaxed),"provider_traffic_complete":self.counts.errors.load(Ordering::Relaxed)==0,"root_index_bytes":serde_json::Value::Null,"root_index_scope":"No public byte-size API; root-index/encoded/provider allocation is excluded from accounted block-pin admission.","logical_gets":self.counts.gets.load(Ordering::Relaxed),"cache_hits":self.counts.hits.load(Ordering::Relaxed),"reader_opens":self.counts.opens.load(Ordering::Relaxed),"reader_drops_at_quiescence":if a.total==0{Some(self.counts.opens.load(Ordering::Relaxed))}else{None},"explicit_reader_clears":0,"quiescent":a.total==0,"staged_logical_bytes":self.counts.staged.load(Ordering::Relaxed),"copied_return_bytes":self.counts.fetch_bytes.load(Ordering::Relaxed),"retained_cache_vectors":retained,"retained_cache_owned_payload_bytes":retained*self.dim*4,"cache_payload_cap_bytes":self.cache_admission.aggregate_payload_cap,"append_input_payload_cap_bytes":self.limits.append_rows*self.dim*4,"provider_writer_embedding_set_payload_cap_bytes":self.limits.append_rows*self.dim*4,"single_split_tuple_payload_cap_bytes":self.limits.split_rows*self.dim*4,"single_npa_window_payload_cap_bytes":64*self.dim*4,"peak_single_reader_pinned_bytes":self.counts.peak_pins.load(Ordering::Relaxed),"active_fetch_workers":a.total,"peak_fetch_workers":a.peak,"peak_fetch_slots_per_owner":a.owner_peak,"max_readers_per_fetch_slot":1,"staging_elapsed_ns":self.counts.stage_ns.load(Ordering::Relaxed),"joined_publication_elapsed_ns":self.counts.publication_ns.load(Ordering::Relaxed),"publication_forks":self.counts.publication_calls.load(Ordering::Relaxed),"publication_logical_embedding_set_bytes":0,"fetch_errors":self.counts.errors.load(Ordering::Relaxed),"provider_traffic":*self.traffic.lock(),"scope":"Cache-owned payload only. Caller split/NPA tuples, staged input/builders and provider root/block caches are separate. Encoded provider returns are not physical IO. Save forks the latest durable embedding root. Owner publication streams changed postings and versions; changed-key metadata, provider/writer buffers and routing state remain separate."});
        report.as_object_mut().unwrap().extend(serde_json::json!({"staging_row_limit":self.limits.append_rows,"staging_working_budget_bytes":self.limits.staging_working_bytes,"staging_modeled_max_bytes":staging_working_bytes(self.limits.append_rows,self.dim).unwrap(),"staging_working_model":"rows*(fourFP32payloads+256metadata)+65536; excludes root indexes/provider caches/allocator; not RSS","split_working_budget_bytes":self.limits.split_working_bytes,"split_working_active_bytes":self.counts.split_active.load(Ordering::Relaxed),"split_working_peak_reserved_bytes":self.counts.split_peak.load(Ordering::Relaxed),"split_working_rejections":self.counts.split_rejections.load(Ordering::Relaxed),"split_working_model":"rows*(FP32 payload+256metadata/scratch+twoCode1codes)+64*dimension+65536; wide identity additionally reserves rows*(8-byte ID table+size_of EmbeddingPoint ordinal tuples); each split reserves configured maximum rows, not observed rows; modeled owned preparation, not allocator/provider/RSS ceiling","staged_storage_chunks":self.counts.stage_chunks.load(Ordering::Relaxed),"peak_pending_handles":self.counts.peak_pending.load(Ordering::Relaxed)}).as_object().unwrap().clone());report
    }
}

#[cfg(test)]
mod worker_admission_tests {
 use super::*;
 #[test]
 fn thirty_two_worker_admission_slots_bits_and_split_headroom(){
  use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
  use chroma_cache::new_cache_for_test;
  use chroma_storage::{local::LocalStorage,Storage};
  let dir=tempfile::tempdir().unwrap();let provider=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),16384,new_cache_for_test(),new_cache_for_test(),32));
  let limits=VectorLimits{workers:32,readers_per_owner:32,split_rows:8192,split_working_bytes:8*1024*1024*1024,..Default::default()};
  let store=Arc::new(OwnerVectorStore::new(provider.clone(),768,1,limits).unwrap());
  assert!(OwnerVectorStore::new(provider.clone(),768,1,VectorLimits{workers:33,..limits}).is_err());
  assert!(OwnerVectorStore::new(provider,769,1,limits).is_err());
  assert!(parallel_working_bytes(32,768,usize::MAX).is_err());
  let one=split_reservation_bytes(8192,768,true).unwrap();let mut reservations=Vec::new();for _ in 0..160{reservations.push(store.reserve_split_identity(8192,true).unwrap());}
  assert_eq!(store.counts.split_active.load(Ordering::Relaxed),160*one as u64);drop(reservations);assert_eq!(store.counts.split_active.load(Ordering::Relaxed),0);
  let rows:Vec<_>=(0..32u64).map(|id|(id,(0..768).map(|d|if d==2{-0.0}else{id as f32+d as f32/1024.}).collect::<Vec<_>>())).collect();store.stage_identity(&rows).unwrap();
  let barrier=Arc::new(std::sync::Barrier::new(33));std::thread::scope(|scope|{
   for _ in 0..32{let store=store.clone();let barrier=barrier.clone();scope.spawn(move||{let _slot=store.slot(0).unwrap();barrier.wait();barrier.wait();});}
   barrier.wait();assert_eq!(store.activity.lock().total,32);barrier.wait();
  });
  std::thread::scope(|scope|{for(id,values)in &rows{let store=store.clone();let handle=store.staged_identity(*id).unwrap();scope.spawn(move||{let actual=store.fetch(0,&[handle]).unwrap();assert!(actual[0].iter().map(|v|v.to_bits()).eq(values.iter().map(|v|v.to_bits())));});}});
  assert_eq!(store.activity.lock().total,0);assert_eq!(store.activity.lock().peak,32);assert_eq!(store.counts.errors.load(Ordering::Relaxed),0);
  println!("WORKER32 {}",serde_json::json!({"single_wide_split_bytes":one,"modeled_32_by_5_bytes":160*one,"split_budget_bytes":limits.split_working_bytes,"parallel_worker_model_bytes":parallel_working_bytes(32,768,1024*1024).unwrap(),"peak_admitted_slots":32,"exact_vectors":32,"report":store.report()}));
 }
}

#[cfg(test)]
mod decoded_cache_tests {
 use super::*;
 fn provider(path:&std::path::Path)->BlockfileProvider {
  use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
  use chroma_storage::{local::LocalStorage,Storage};
  BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(path.to_str().unwrap())),16384,super::super::persistence::block_on_for_sync_writer(chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)).unwrap(),super::super::persistence::block_on_for_sync_writer(chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)).unwrap(),2))
 }
 fn bits(v:&[f32])->Vec<u32>{v.iter().map(|v|v.to_bits()).collect()}
 #[test]
 fn decoded_cache_checked_admission_and_legacy_default(){
  let trial=DecodedVectorCachePolicy{payload_bytes_per_owner:384*1024*1024,aggregate_payload_bytes:384*1024*1024};let a=trial.admit(768,1).unwrap();assert_eq!(a.rows_per_owner,131072);assert_eq!(a.aggregate_payload_cap,384*1024*1024);assert!(trial.admit(768,2).is_err());assert!(DecodedVectorCachePolicy{aggregate_payload_bytes:2*trial.aggregate_payload_bytes,..trial}.admit(768,2).is_ok());
  for policy in [DecodedVectorCachePolicy{payload_bytes_per_owner:0,aggregate_payload_bytes:0},DecodedVectorCachePolicy{payload_bytes_per_owner:3071,aggregate_payload_bytes:3071},DecodedVectorCachePolicy{payload_bytes_per_owner:4096,aggregate_payload_bytes:4095}]{assert!(policy.admit(768,1).is_err());}
  let overflow=DecodedVectorCachePolicy{payload_bytes_per_owner:usize::MAX,aggregate_payload_bytes:usize::MAX};assert!(overflow.admit(768,2).is_err());assert!(overflow.admit(1,1).is_err());assert!(trial.admit(0,1).is_err());
  let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let default=OwnerVectorStore::new(p.clone(),768,1,VectorLimits::default()).unwrap();assert_eq!(default.report()["decoded_vector_cache_admission"]["rows_per_owner"],32);assert_eq!(default.report()["cache_payload_cap_bytes"],98304);assert!(OwnerVectorStore::new(p.clone(),768,1,VectorLimits{cache_rows:33,..Default::default()}).is_err());
  let large=OwnerVectorStore::new(p,768,1,VectorLimits{decoded_vector_cache:Some(trial),..Default::default()}).unwrap();assert_eq!(large.report()["retained_cache_vectors"],0,"admission does not allocate a working set");assert_eq!(large.report()["decoded_vector_cache_admission"]["rows_per_owner"],131072);println!("DECODED_CACHE_ADMISSION {}",large.report());
 }
 #[test]
 fn decoded_cache_fixed_requests_fit_and_eviction_bits(){
  let dim=32;let rows:Vec<_>=(0..64u64).map(|id|(id,(0..dim).map(|d|if d==2{-0.}else{id as f32+d as f32/1024.}).collect::<Vec<_>>())).collect();let mut evidence=Vec::new();
  for(name,capacity)in [("legacy",None),("fit",Some(64)),("eviction",Some(8))]{let dir=tempfile::tempdir().unwrap();let policy=capacity.map(|n|DecodedVectorCachePolicy{payload_bytes_per_owner:n*dim*4,aggregate_payload_bytes:n*dim*4});let store=OwnerVectorStore::new(provider(dir.path()),dim,1,VectorLimits{decoded_vector_cache:policy,..Default::default()}).unwrap();store.stage_identity(&rows).unwrap();let handles:Vec<_>=rows.iter().map(|(id,_)|store.staged_identity(*id).unwrap()).collect();store.finish_stage();let mut held=None;
   for pass in 0..2{for(start,chunk)in handles.chunks(16).enumerate(){let actual=store.fetch(0,chunk).unwrap();for(offset,v)in actual.iter().enumerate(){assert_eq!(bits(v),bits(&rows[start*16+offset].1));}if pass==0&&start==0{held=Some(actual[0].clone());}}}
   assert_eq!(bits(&held.unwrap()),bits(&rows[0].1),"caller Arc remains valid after cache eviction");let report=store.report();assert_eq!(report["logical_gets"],128);assert_eq!(report["cache_hits"],if name=="fit"{64}else{0});assert_eq!(report["reader_opens"],if name=="fit"{4}else{8});assert!(report["retained_cache_owned_payload_bytes"].as_u64().unwrap()<=report["cache_payload_cap_bytes"].as_u64().unwrap());assert_eq!(report["fetch_errors"],0);evidence.push(serde_json::json!({"case":name,"report":report}));
  }println!("DECODED_CACHE_FIXED {}",serde_json::json!(evidence));
 }
 #[test]
 fn decoded_cache_full_handle_replacement_and_late_old_fetch(){
  let dir=tempfile::tempdir().unwrap();let store=Arc::new(OwnerVectorStore::new(provider(dir.path()),32,1,VectorLimits{decoded_vector_cache:Some(DecodedVectorCachePolicy{payload_bytes_per_owner:256,aggregate_payload_bytes:256}),..Default::default()}).unwrap());let old=vec![1.;32];let new=vec![2.;32];store.stage_identity(&[(0u64,old.clone())]).unwrap();let old_handle=store.staged_identity(0u64).unwrap();store.finish_stage();
  // This store-boundary test writes two immutable objects with the same key.
  // It does not enable replacement in the append-only construction writer.
  store.stage_identity(&[(0u64,new.clone())]).unwrap();let new_handle=store.staged_identity(0u64).unwrap();store.finish_stage();assert_ne!(old_handle.root,new_handle.root);
  let(entered_tx,entered_rx)=std::sync::mpsc::channel();let(resume_tx,resume_rx)=std::sync::mpsc::channel();store.pause_next_fetch(entered_tx,resume_rx);
  std::thread::scope(|scope|{let s=store.clone();let h=old_handle.clone();let older=scope.spawn(move||s.fetch(0,&[h]).unwrap().remove(0));entered_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();let current=store.fetch(0,&[new_handle.clone()]).unwrap();assert_eq!(bits(&current[0]),bits(&new));resume_tx.send(()).unwrap();assert_eq!(bits(&older.join().unwrap()),bits(&old));});
  assert_eq!(store.caches[0].lock().values[&0].0,old_handle,"late completion may replace cache entry but never changes its binding");let opens=store.counts.opens.load(Ordering::Relaxed);assert_eq!(bits(&store.fetch(0,&[new_handle.clone()]).unwrap()[0]),bits(&new));assert_eq!(store.counts.opens.load(Ordering::Relaxed),opens+1,"wrong cached root is not a hit");
  store.stage_identity(&[(0u64,new.clone())]).unwrap();let same_bits_new_root=store.staged_identity(0u64).unwrap();store.finish_stage();assert_eq!(same_bits_new_root.sha,new_handle.sha);assert_ne!(same_bits_new_root.root,new_handle.root);let opens=store.counts.opens.load(Ordering::Relaxed);assert_eq!(bits(&store.fetch(0,&[same_bits_new_root.clone()]).unwrap()[0]),bits(&new));assert_eq!(store.counts.opens.load(Ordering::Relaxed),opens+1,"same SHA does not substitute another root");
  let mut wrong_sha=same_bits_new_root.clone();wrong_sha.sha[0]^=1;let mut wrong_dim=same_bits_new_root.clone();wrong_dim.dim=31;let mut wrong_home=same_bits_new_root.clone();wrong_home.home=1;let mut missing=same_bits_new_root.clone();missing.key=999;let mut wrong_binding=new_handle.clone();wrong_binding.root=old_handle.root;
  for h in [wrong_sha,wrong_dim,wrong_home,missing,wrong_binding]{assert!(store.fetch(0,&[h]).is_err());}assert_eq!(bits(&store.fetch(0,&[same_bits_new_root]).unwrap()[0]),bits(&new));assert!(store.report()["retained_cache_owned_payload_bytes"].as_u64().unwrap()<=256);println!("DECODED_CACHE_BINDING {}",store.report());
 }
}

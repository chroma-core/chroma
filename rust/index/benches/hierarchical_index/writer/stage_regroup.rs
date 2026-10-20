//! Optional joined-stage insertion. No balancing, placement or publication can
//! interleave through the public writer while worker deltas are outstanding.
use super::*;
use std::sync::atomic::AtomicBool;
use crate::hierarchical_index::common::{NodeId,TreeNode};
use crate::hierarchical_index::writer::owner_node_access::{MembershipPublication,Store as NodeStore};
use crate::hierarchical_index::writer::partition_authority::VectorId64;

#[derive(Clone,Copy,Debug,serde::Serialize,serde::Deserialize)]
pub struct StageRegroupPolicy { pub max_rows:usize, pub delta_working_bytes:usize }
impl StageRegroupPolicy {
    pub fn modeled_bytes(rows:usize,dimension:usize,code:usize)->Result<usize>{
        // One retained centroid per worst-case destination/row; four code
        // capacities plus explicit metadata/IDs/handles/reservation/regroup headroom.
        // The 8192-byte row allowance covers overlapping tree nodes and Vec
        // capacities; see revision-v2 allocation accounting for the breakdown.
        rows.checked_mul(dimension.checked_mul(4).and_then(|v|code.checked_mul(4).and_then(|c|v.checked_add(c))).and_then(|v|v.checked_add(8192)).ok_or_else(||anyhow::anyhow!("stage delta row overflow"))?).and_then(|v|v.checked_add(65536)).ok_or_else(||anyhow::anyhow!("stage delta extent overflow"))
    }
    pub(super) fn validate(&self,dimension:usize,code:usize,staging_rows:usize)->Result<()>{ensure!(self.max_rows>0&&self.max_rows<=8192&&staging_rows<=self.max_rows&&Self::modeled_bytes(self.max_rows,dimension,code)?<=self.delta_working_bytes,"explicit stage delta admission");Ok(())}
}
#[derive(Default)]
pub(super) struct StageRegroupStats {
    stages:AtomicU64,windows:AtomicU64,rows:AtomicU64,groups:AtomicU64,node_leases:AtomicU64,
    owner_appends:AtomicU64,home_subrequests:AtomicU64,appended_rows:AtomicU64,max_home_rows:AtomicU64,attempts:AtomicU64,
    modeled_peak:AtomicU64,collect_ns:AtomicU64,flush_ns:AtomicU64,workers:Arc<Work>,
    #[cfg(test)]fail_before_append_at:AtomicU64,
    #[cfg(test)]pause_after_append:Mutex<Option<(std::sync::mpsc::Sender<()>,std::sync::mpsc::Receiver<()>)>>,
    #[cfg(test)]fail_before_append:AtomicBool,
    #[cfg(test)]fail_after_append:AtomicBool,
}
struct Delta { owner:usize, center:Arc<[f32]>, rows:Vec<(u64,Vec<u8>)> }
impl WideOwnerWriter {
    pub fn set_stage_regroup(&mut self,policy:Option<StageRegroupPolicy>)->Result<()>{
        ensure!(!self.failed,"stage policy on failed writer");if let Some(policy)=policy{ensure!(self.backend().prepared_split_supported()&&self.kernel.live_owner_hook.lock().is_none()&&self.kernel.node_store.is_some()&&self.stable_insert.is_some()&&!self.kernel.prepared_split.enabled.load(Ordering::Relaxed),"stage regroup requires stable bounded full-record nonpaged single owner with prepared split/local reassignment OFF");policy.validate(self.kernel.dim,self.kernel.code_size(),self.backend().vector_store().unwrap().limits.append_rows)?;}self.stage_regroup=policy;Ok(())
    }
    pub fn stage_regroup_observation(&self)->serde_json::Value{let s=&self.stage_regroup_stats;serde_json::json!({"policy":self.stage_regroup,"stages":s.stages.load(Ordering::Relaxed),"windows":s.windows.load(Ordering::Relaxed),"collected_rows":s.rows.load(Ordering::Relaxed),"destination_groups":s.groups.load(Ordering::Relaxed),"node_leases":s.node_leases.load(Ordering::Relaxed),"owner_appends":s.owner_appends.load(Ordering::Relaxed),"home_subrequests":s.home_subrequests.load(Ordering::Relaxed),"max_home_subrequest_rows":s.max_home_rows.load(Ordering::Relaxed),"destination_attempts":s.attempts.load(Ordering::Relaxed),"appended_rows":s.appended_rows.load(Ordering::Relaxed),"modeled_peak_stage_delta_bytes":s.modeled_peak.load(Ordering::Relaxed),"peak_collect_workers":s.workers.peak.load(Ordering::Relaxed),"collect_elapsed_ns":s.collect_ns.load(Ordering::Relaxed),"flush_elapsed_ns":s.flush_ns.load(Ordering::Relaxed),"scope":"One real owner append and held-credit node lease per destination per admitted durable stage. Home reservation/activation remain <=64-ID subrequests. Stage modeled delta budget excludes caller/staging/provider/owner resident growth; worker windows have their separate existing admission. Parallel collection elapsed sums overlap and are not wall time or CPU."})}
    pub(super) fn preflight_regroup_capacity(&self,rows:usize)->Result<()>{ensure!(self.backend().prepared_split_supported()&&self.kernel.live_owner_hook.lock().is_none(),"stage regroup policy changed or live owner hook installed");let capacity=self.backend().physical_capacity.load(Ordering::Relaxed);self.backend().actor(0).call(move|s|{ensure!(s.physical.checked_add(rows).is_some_and(|v|v<=capacity),"stage physical capacity before durable staging");Ok(())})}
    fn collect_regroup_window(&self,rows:&[(u64,Vec<f32>)],worker_bytes:usize)->Result<BTreeMap<NodeId,Delta>>{
        ensure!(rows.len()<=64&&StableInsertPolicy::model_bytes(rows.len(),self.kernel.dim,self.kernel.code_size())?<=worker_bytes,"regroup worker admission");let _worker=WorkSpan::new(&self.stage_regroup_stats.workers);self.stage_regroup_stats.windows.fetch_add(1,Ordering::Relaxed);self.stage_regroup_stats.rows.fetch_add(rows.len()as u64,Ordering::Relaxed);let start=Instant::now();let mut deltas=BTreeMap::<NodeId,Delta>::new();
        for(id,vector)in rows{let selected=self.kernel.route_stable_destination(vector);ensure!(selected.len()==1,"stage one-copy destination");let leaf=selected[0];if !deltas.contains_key(&leaf){let node=self.kernel.node_get(&leaf).ok_or_else(||anyhow::anyhow!("regroup destination absent"))?;let TreeNode::Leaf(source)=node.value()else{anyhow::bail!("regroup destination kind");};let owner=self.backend().located_owner(leaf).ok_or_else(||anyhow::anyhow!("regroup locator absent"))?;deltas.insert(leaf,Delta{owner,center:Arc::from(source.centroid.clone()),rows:Vec::new()});}let delta=deltas.get_mut(&leaf).unwrap();delta.rows.push((*id,Code::<1>::quantize(vector,&delta.center).as_ref().to_vec()));}
        self.stage_regroup_stats.collect_ns.fetch_add(start.elapsed().as_nanos()as u64,Ordering::Relaxed);Ok(deltas)
    }
    pub(super) fn add_regrouped_stage(&self,rows:&[(u64,Vec<f32>)],threads:usize,policy:StageRegroupPolicy,stage_started:Arc<AtomicBool>)->Result<()>{
        ensure!(rows.len()<=policy.max_rows,"stage row cap");let model=StageRegroupPolicy::modeled_bytes(rows.len(),self.kernel.dim,self.kernel.code_size())?;ensure!(model<=policy.delta_working_bytes,"stage delta bytes before collection");self.stage_regroup_stats.modeled_peak.fetch_max(model as u64,Ordering::Relaxed);self.stage_regroup_stats.stages.fetch_add(1,Ordering::Relaxed);let stable=self.stable_insert.unwrap();
        // Join order is input-slice order, not completion order. Each worker's
        // successive windows retain row order; regroup never sorts identities.
        let workers=thread::scope(|scope|->Result<Vec<Vec<BTreeMap<NodeId,Delta>>>>{let handles:Vec<_>=rows.chunks(rows.len().div_ceil(threads)).map(|group|scope.spawn(move||->Result<Vec<_>>{group.chunks(stable.rows).map(|window|self.collect_regroup_window(window,stable.worker_bytes)).collect()})).collect();handles.into_iter().map(|handle|handle.join().map_err(|_|anyhow::anyhow!("regroup worker panic"))?).collect()})?;
        let mut grouped=BTreeMap::<NodeId,Delta>::new();for worker in workers{for window in worker{for(id,mut delta)in window{if let Some(target)=grouped.get_mut(&id){ensure!(target.owner==delta.owner&&target.center.iter().map(|v|v.to_bits()).eq(delta.center.iter().map(|v|v.to_bits())),"stable regroup destination changed");target.rows.append(&mut delta.rows);}else{grouped.insert(id,delta);}}}}
        let start=Instant::now();let store=self.kernel.node_store.as_ref().unwrap();
        // Validate every destination before any posting append. The exclusive
        // joined public phase prevents changes between this pass and each lease.
        for(id,delta)in &grouped{let node=self.kernel.node_get(id).ok_or_else(||anyhow::anyhow!("stage preflight destination absent"))?;ensure!(matches!(node.value(),TreeNode::Leaf(_))&&node.centroid().iter().map(|v|v.to_bits()).eq(delta.center.iter().map(|v|v.to_bits()))&&self.backend().located_owner(*id)==Some(delta.owner),"stage preflight centroid/owner mismatch");}
        let stage_attempt=AppendAttempt{publication:Some(store.membership_publication()),started:stage_started.clone()};
        for(leaf,delta)in grouped{let count=delta.rows.len();let mut lease=store.write_transaction(leaf)?.ok_or_else(||anyhow::anyhow!("stage destination lease absent"))?;ensure!(lease.draft.posting_owner==delta.owner&&lease.draft.node.centroid.iter().map(|v|v.to_bits()).eq(delta.center.iter().map(|v|v.to_bits())),"stage lease binding");self.stage_regroup_stats.node_leases.fetch_add(1,Ordering::Relaxed);let ids:Vec<_>=delta.rows.iter().map(|v|v.0).collect();let(length,publication)=self.backend().register_stage_group(leaf,delta.owner,delta.rows,self.kernel.code_size(),policy.max_rows,store,&self.stage_regroup_stats,stage_started.clone())?;
            #[cfg(test)]{let pause=self.stage_regroup_stats.pause_after_append.lock().take();if let Some((entered,resume))=pause{entered.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(10)).expect("stage publication resume");}}
            #[cfg(test)]if self.stage_regroup_stats.fail_after_append.swap(false,Ordering::Relaxed){anyhow::bail!("injected stage failure after append before node publication");}
            lease.draft.node.kind=crate::hierarchical_index::topology_access::Kind::Leaf{physical_rows:length};lease.commit()?;self.kernel.mark_node_dirty(leaf);for id in ids{self.kernel.mark_version_dirty(id);}publication.complete();self.stage_regroup_stats.groups.fetch_add(1,Ordering::Relaxed);self.kernel.stats.registers.fetch_add(count as u64,Ordering::Relaxed);self.kernel.stats.adds.fetch_add(count as u64,Ordering::Relaxed);self.kernel.stats.embeddings_added.fetch_add(count as u64,Ordering::Relaxed);
        }stage_attempt.appended().complete();self.stage_regroup_stats.flush_ns.fetch_add(start.elapsed().as_nanos()as u64,Ordering::Relaxed);Ok(())
    }
}
/// Before mutation an error cancels reservations without poisoning. Once the
/// callback marks append started, any unwind also poisons the node authority.
pub(super) struct PhaseAttempt{pub(super) store:Arc<NodeStore>,pub(super) started:Arc<AtomicBool>,pub(super) complete:bool}
impl Drop for PhaseAttempt{fn drop(&mut self){if !self.complete&&self.started.load(Ordering::Acquire){self.store.visibility.poison();}}}
struct AppendAttempt<'a>{publication:Option<MembershipPublication<'a>>,started:Arc<AtomicBool>}
impl<'a> AppendAttempt<'a>{fn appended(mut self)->MembershipPublication<'a>{self.publication.take().unwrap()}}
impl Drop for AppendAttempt<'_>{fn drop(&mut self){if !self.started.load(Ordering::Acquire){if let Some(publication)=self.publication.take(){publication.complete();}}}}
impl Backend<u64>{
    fn register_stage_group<'a>(&self,leaf:NodeId,owner:usize,rows:Vec<(u64,Vec<u8>)>,code_size:usize,max_rows:usize,store:&'a NodeStore,stats:&StageRegroupStats,stage_started:Arc<AtomicBool>)->Result<(usize,MembershipPublication<'a>)>{
        ensure!(self.prepared_split_supported()&&owner==0&&!rows.is_empty()&&rows.len()<=max_rows&&rows.iter().all(|v|v.1.len()==code_size),"stage owner transaction admission");let ids:Vec<_>=rows.iter().map(|v|VectorId64(v.0)).collect();let mut unique=ids.clone();unique.sort_unstable();unique.dedup();ensure!(unique.len()==ids.len(),"stage duplicate identities");let locks=self.authority.locks(ids.iter().copied());let _guards:Vec<_>=locks.iter().map(|lock|lock.lock()).collect();
        // All reservation tokens are owned by this frame. Any early return
        // drops them before identity guards, cancelling every pending home row.
        let mut pending=Vec::with_capacity(ids.len().div_ceil(64));for batch in ids.chunks(64){pending.push(self.authority.reserve_new_batch(batch)?);stats.home_subrequests.fetch_add(1,Ordering::Relaxed);stats.max_home_rows.fetch_max(batch.len()as u64,Ordering::Relaxed);}
        let prepared:Vec<_>=rows.into_iter().map(|(id,code)|Ok((id,code,self.staged_vector(id)?))).collect::<Result<_>>()?;
        let attempt_number=stats.attempts.fetch_add(1,Ordering::Relaxed)+1;
        #[cfg(not(test))]let _=attempt_number;
        #[cfg(test)]if stats.fail_before_append.swap(false,Ordering::Relaxed)||stats.fail_before_append_at.load(Ordering::Relaxed)==attempt_number{anyhow::bail!("injected stage failure before append");}
        let count=prepared.len();let capacity=self.physical_capacity.load(Ordering::Relaxed);stats.owner_appends.fetch_add(1,Ordering::Relaxed);
        let callback_started=stage_started.clone();let attempt=AppendAttempt{publication:Some(store.membership_publication()),started:stage_started};
        let length=self.actor(owner).call_op(Operation::Registration,move|s|->Result<usize>{ensure!(s.physical.checked_add(count).is_some_and(|v|v<=capacity)&&s.leaves.contains_key(&leaf),"stage owner physical capacity/destination");{let current=s.current.read();ensure!(prepared.iter().all(|(id,_,_)|!current.contains_key(id)&&!s.vectors.contains_key(id)),"stage owner identity exists");}let leaf=s.leaves.get_mut(&leaf).unwrap();let mut current=s.current.write();callback_started.store(true,Ordering::Release);for(id,code,handle)in prepared{leaf.ids.push(id);leaf.versions.push(1);leaf.codes.extend_from_slice(&code);current.insert(id,1);s.vectors.insert(id,handle);s.dirty_current.insert(id);}leaf.length=leaf.ids.len();s.physical+=count;Ok(leaf.length)})?;
        // From this nonfallible handoff onward any error/unwind must poison.
        let publication=attempt.appended();for reservation in pending{reservation.activate(owner);}stats.appended_rows.fetch_add(count as u64,Ordering::Relaxed);self.registrations.fetch_add(count as u64,Ordering::Relaxed);Ok((length,publication))
    }
}

#[cfg(test)]
mod tests{
 use super::*;
 use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider,provider::BlockfileProvider};
 use chroma_cache::new_cache_for_test;
 use chroma_storage::{Storage,local::LocalStorage};
 use crate::hierarchical_index::{common::{InternalNode,LeafNode},config::HierarchicalSpannConfig,writer::{owner_vector_store::VectorLimits,owner_node_access::Limits as NodeLimits,owner_work_queue::Limits as QueueLimits}};
 use sha2::{Digest,Sha256};
 fn provider(path:&std::path::Path)->BlockfileProvider{BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(path.to_str().unwrap())),16*1024,new_cache_for_test(),new_cache_for_test(),2))}
 fn center(destination:usize)->Vec<f32>{(0..32).map(|d|if d==2{-0.0}else{destination as f32*10.+d as f32/100.}).collect()}
 fn policy()->StageRegroupPolicy{StageRegroupPolicy{max_rows:256,delta_working_bytes:4*1024*1024}}
 fn fixture(destinations:usize)->(tempfile::TempDir,BlockfileProvider,WideOwnerWriter,Vec<(u64,Vec<f32>)>){
  let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig{branching_factor:256,split_threshold:16,merge_threshold:0,max_replicas:1,write_beam_min:1,write_beam_max:1,write_navigation:NavigationMode::Fp,fp_npa:true,..Default::default()};let mut writer=WideOwnerWriter::new(32,DistanceFunction::Euclidean,config,1,&[0],20000,p.clone(),VectorLimits{append_rows:256,staging_working_bytes:1024*1024,split_rows:256,split_working_bytes:16*1024*1024,..Default::default()}).unwrap();writer.enable_dirty_scheduler(p.clone(),QueueLimits::default()).unwrap();writer.enable_full_records(p.clone(),NodeLimits{cache_entries:2,record_bytes:4096,pin_bytes:4*1024*1024,dirty_records:512,dirty_bytes:4*1024*1024,..Default::default()}).unwrap();writer.set_stable_insertion(Some(StableInsertPolicy{rows:64,worker_bytes:1024*1024})).unwrap();writer.set_immutable_handle_batching(false).unwrap();writer.set_single_owner_local_scrub(true).unwrap();writer.set_diagnostic_capacity(40000,true).unwrap();
  let seeds:Vec<_>=(0..destinations).map(|i|(10000+i as u64,center(i))).collect();writer.add_phase(&seeds,1).unwrap();
  {let _tree=writer.kernel.tree_lock.lock();let _working=writer.kernel.topology_working(destinations+2);let _view=writer.kernel.topology_mutation();writer.kernel.take_reference_node(0).unwrap();writer.kernel.tombstones.insert(0);writer.kernel.dirty_nodes.remove(&0);let parent=(destinations>1).then_some(destinations as u32+1);
   for(i,(vector_id,v))in seeds.iter().enumerate(){let id=i as u32+1;writer.kernel.install_reference_node(id,TreeNode::Leaf(LeafNode{centroid:v.clone(),centroid_code:writer.kernel.quantize_origin(v),ids:vec![*vector_id],versions:vec![1],codes:Code::<1>::quantize(v,v).as_ref().to_vec(),length:1,parent_id:parent}));writer.kernel.mark_node_dirty(id);}
   if let Some(root)=parent{let c=center(destinations/2);writer.kernel.install_reference_node(root,TreeNode::Internal(InternalNode{centroid:c.clone(),centroid_code:writer.kernel.quantize_origin(&c),children:(1..=destinations as u32).collect(),parent_id:None}));writer.kernel.mark_node_dirty(root);writer.kernel.root_id.store(root,Ordering::Relaxed);}else{writer.kernel.root_id.store(1,Ordering::Relaxed);}writer.kernel.next_node_id.store(destinations as u32+2,Ordering::Relaxed);
  }
  let input=(0..129u64).map(|id|{let mut v=center(id as usize%destinations);v[0]+=(id%7)as f32/1000.;(id,v)}).collect();(dir,p,writer,input)
 }
 fn rows(writer:&WideOwnerWriter,destinations:usize)->Vec<(Vec<u64>,Vec<u8>,Vec<u8>)>{(1..=destinations as u32).map(|id|{let leaf=writer.backend().snapshot(id).unwrap();(leaf.ids,leaf.versions,leaf.codes)}).collect()}
 fn number(v:&serde_json::Value,k:&str)->u64{v[k].as_u64().unwrap()}
 #[tokio::test(flavor="multi_thread",worker_threads=2)]
 async fn stage_regroup_fixed_129_order_codes_handles_and_actual_dispatch(){
  for destinations in [1,3,129]{let(_bd,bp,mut baseline,input)=fixture(destinations);let(_od,op,mut owner,_)=fixture(destinations);assert!(owner.stage_regroup.is_none());let b_before=baseline.backend().stable_observation();let b_dispatch_before=baseline.backend().message_timing.snapshot()["operations"]["registration"]["calls"].as_u64().unwrap();baseline.add_phase(&input,1).unwrap();let b_after=baseline.backend().stable_observation();let b_calls=number(&b_after,"owner_registration_calls")-number(&b_before,"owner_registration_calls");let b_leases=number(&b_after,"stable_node_leases")-number(&b_before,"stable_node_leases");assert_eq!(baseline.backend().message_timing.snapshot()["operations"]["registration"]["calls"].as_u64().unwrap()-b_dispatch_before,b_calls);let expected=rows(&baseline,destinations);
   let previous=owner.save(&op).await.unwrap();let mut old=Reader64::open(&op,previous,1024).await.unwrap();let old_census=old.census().await.unwrap();let home_before=owner.backend().authority.registration_observation();let dispatch_before=owner.backend().message_timing.snapshot()["operations"]["registration"]["calls"].as_u64().unwrap();owner.set_stage_regroup(Some(policy())).unwrap();owner.add_phase(&input,4).unwrap();let actual=rows(&owner,destinations);assert_eq!(actual,expected,"same frozen tree and input ordering, serial64-window oracle vs parallel joined regroup");let report=owner.stage_regroup_observation();assert_eq!(number(&report,"owner_appends"),destinations as u64);assert_eq!(number(&report,"node_leases"),destinations as u64);assert_eq!(number(&report,"appended_rows"),129);assert_eq!(owner.backend().message_timing.snapshot()["operations"]["registration"]["calls"].as_u64().unwrap()-dispatch_before,destinations as u64,"actual actor dispatches, not wrapper-inferred groups");assert!(number(&report,"max_home_subrequest_rows")<=64);if destinations<129{assert!(number(&report,"owner_appends")<b_calls&&number(&report,"node_leases")<b_leases);}else{assert_eq!(number(&report,"owner_appends"),b_calls,"one row per destination gives no coalescing gain");}
   let home_after=owner.backend().authority.registration_observation();let home_messages=number(&home_after,"reserve_home_messages")-number(&home_before,"reserve_home_messages");assert_eq!(home_messages,number(&report,"home_subrequests"));assert_eq!(home_messages,if destinations==1{3}else{destinations as u64});
   let store=owner.backend().vector_store().unwrap();let root=store.latest().unwrap();for(id,vector)in &input{assert_eq!(owner.backend().current(*id),Some(1));let(location,handle)=owner.backend().resolve_vector(*id).unwrap();assert_eq!(location,0);assert_eq!(handle.root,root);assert_eq!(handle.key,*id as u32);assert_eq!(handle.home,0);let mut digest=Sha256::new();for v in vector{digest.update(v.to_bits().to_le_bytes());}let expected_sha:[u8;32]=digest.finalize().into();assert_eq!(handle.sha,expected_sha);}
   owner.balance_phase(4).unwrap();assert_eq!(owner.kernel.pending_balance_leaves(),0);let saved=owner.save(&op).await.unwrap();let mut current=Reader64::open(&op,saved,2048).await.unwrap();let census=current.census().await.unwrap();assert_eq!(census.len(),129+destinations);for(id,v)in &input{assert_eq!(census[id].2,v.iter().map(|x|x.to_bits()).collect::<Vec<_>>());}assert_eq!(old.census().await.unwrap(),old_census);assert!(owner.kernel.nodes.is_empty()&&owner.backend().leaf_owner.is_empty());println!("STAGE_REGROUP_FIXED {}",serde_json::json!({"destinations":destinations,"baseline_owner_calls":b_calls,"baseline_node_leases":b_leases,"regroup":report,"home_reserve_messages":home_messages,"current_rows":census.len(),"old_rows":old_census.len(),"balanced":true}));drop(bp);
  }
 }
 #[tokio::test(flavor="multi_thread",worker_threads=2)]
 async fn stage_regroup_preappend_rollback_and_postappend_poison_preserve_old(){
  for after in [false,true]{let(_dir,p,mut writer,input)=fixture(1);let old_snapshot=writer.save(&p).await.unwrap();let mut old=Reader64::open(&p,old_snapshot,256).await.unwrap();let before=old.census().await.unwrap();let home_before=writer.backend().authority.registration_observation();writer.set_stage_regroup(Some(policy())).unwrap();if after{writer.stage_regroup_stats.fail_after_append.store(true,Ordering::Relaxed);}else{writer.stage_regroup_stats.fail_before_append.store(true,Ordering::Relaxed);}assert!(writer.add_phase(&input,4).is_err());assert!(writer.failed);assert!(writer.save(&p).await.is_err());assert_eq!(old.census().await.unwrap(),before);let home_after=writer.backend().authority.registration_observation();
   if !after{assert!(!writer.kernel.topology_visibility.as_ref().unwrap().failed());for(id,_)in &input{assert_eq!(writer.backend().current(*id),None);assert!(writer.backend().authority.location(VectorId64(*id)).is_none());}assert_eq!(number(&home_after,"cancel_home_messages")-number(&home_before,"cancel_home_messages"),3);assert_eq!(writer.backend().snapshot(1).unwrap().ids,vec![10000]);assert_eq!(writer.kernel.node_store.as_ref().unwrap().report()["reserved_dirty_records"],0);}else{assert!(writer.kernel.topology_visibility.as_ref().unwrap().failed());assert_eq!(writer.backend().actor(0).call(|s|s.current.read().get(&0).copied()),Some(1));assert_eq!(writer.backend().actor(0).call(|s|s.leaves[&1].length),130);}
   println!("STAGE_REGROUP_FAILURE after_append={} home_before={} home_after={} report={}",after,home_before,home_after,writer.stage_regroup_observation());
  }
 }
 #[test]
 fn stage_regroup_duplicate_capacity_budget_and_policy_reject_before_mutation(){
  let(_dir,_p,mut writer,input)=fixture(1);assert!(writer.set_stage_regroup(Some(StageRegroupPolicy{max_rows:256,delta_working_bytes:1})).is_err());assert!(StageRegroupPolicy::modeled_bytes(usize::MAX,768,112).is_err());assert!(writer.stage_regroup.is_none());writer.set_stage_regroup(Some(policy())).unwrap();assert!(writer.set_stable_insertion(None).is_err());assert!(writer.set_prepared_leaf_split(true).is_err());assert!(writer.set_single_owner_local_reassignment(true).is_err());let store=writer.backend().vector_store().unwrap();let root=store.latest().unwrap();
  assert!(writer.add_phase(&[input[0].clone(),input[0].clone()],2).is_err());assert!(writer.add_phase(&[(10000,input[0].1.clone())],2).is_err());assert!(!writer.failed);assert_eq!(store.latest().unwrap(),root);writer.backend().physical_capacity.store(129,Ordering::Relaxed);assert!(writer.add_phase(&input,4).is_err());assert!(!writer.failed);assert_eq!(store.latest().unwrap(),root);assert_eq!(writer.backend().current(0),None);assert_eq!(writer.stage_regroup_observation()["owner_appends"],0);
  let config=writer.kernel.config.clone();let p=provider(_dir.path());let mut unsupported=WideOwnerWriter::new(32,DistanceFunction::Euclidean,config,2,&[0],1024,p,VectorLimits::default()).unwrap();assert!(unsupported.set_stage_regroup(Some(policy())).is_err());println!("STAGE_REGROUP_ADMISSION duplicates/capacity/budget/overflow/unsupported policies reject before new authority or staging mutation");
 }
 #[test]
 fn stage_regroup_allocation_layout_assumptions(){
  use std::mem::size_of;assert!(size_of::<usize>()<=8);assert!(size_of::<Delta>()<=48);assert!(size_of::<super::super::super::owner_vector_store::VectorHandle>()<=64);assert!(size_of::<(u64,Vec<u8>)>()<=32);assert!(size_of::<(u64,Vec<u8>,super::super::super::owner_vector_store::VectorHandle)>()<=96);assert!(size_of::<super::super::super::partition_authority::NewBatchReservation<'_>>()<=32);assert!(size_of::<BTreeMap<NodeId,Delta>>()<=24);assert_eq!(StageRegroupPolicy::modeled_bytes(8192,768,112).unwrap(),96010240);
 }
 #[test]
 fn stage_regroup_rejects_hook_at_enable_and_phase_preflight(){
  use crate::hierarchical_index::writer::live_owner::{LiveOwnerHook,Mode};
  let(_dir,_p,mut writer,input)=fixture(1);let hook=Arc::new(LiveOwnerHook::new(Mode::ReferenceTrace,BTreeMap::new(),1,32,DistanceFunction::Euclidean).unwrap());*writer.kernel.live_owner_hook.lock()=Some(hook.clone());assert!(writer.set_stage_regroup(Some(policy())).is_err());*writer.kernel.live_owner_hook.lock()=None;writer.set_stage_regroup(Some(policy())).unwrap();let root=writer.backend().vector_store().unwrap().latest().unwrap();*writer.kernel.live_owner_hook.lock()=Some(hook);assert!(writer.add_phase(&input,4).is_err());assert!(!writer.failed);assert_eq!(writer.backend().vector_store().unwrap().latest().unwrap(),root);assert_eq!(writer.stage_regroup_observation()["owner_appends"],0);assert_eq!(writer.backend().current(0),None);
 }
 #[test]
 fn stage_regroup_dirty_credit_exhaustion_precedes_append(){
  let(_dir,_p,mut writer,input)=fixture(1);writer.set_stage_regroup(Some(policy())).unwrap();let store=writer.kernel.node_store.as_ref().unwrap().clone();store.flush().unwrap();let held=store.reserve(store.limits.dirty_records).unwrap();assert!(writer.add_phase(&input,4).is_err());assert_eq!(writer.stage_regroup_observation()["owner_appends"],0);assert_eq!(writer.backend().current(0),None);assert!(!writer.kernel.topology_visibility.as_ref().unwrap().failed());drop(held);assert_eq!(store.report()["reserved_dirty_records"],0);println!("STAGE_REGROUP_CREDIT exhausted held credit rejects before owner append");
 }
 #[tokio::test(flavor="multi_thread",worker_threads=2)]
 async fn stage_regroup_later_destination_failure_poisons_complete_stage(){
  let(_dir,p,mut writer,input)=fixture(3);let snapshot=writer.save(&p).await.unwrap();let mut old=Reader64::open(&p,snapshot,256).await.unwrap();let before=old.census().await.unwrap();writer.set_stage_regroup(Some(policy())).unwrap();writer.stage_regroup_stats.fail_before_append_at.store(2,Ordering::Relaxed);assert!(writer.add_phase(&input,4).is_err());assert!(writer.failed&&writer.kernel.topology_visibility.as_ref().unwrap().failed());assert_eq!(writer.stage_regroup_observation()["destination_groups"],1);assert_eq!(writer.stage_regroup_observation()["owner_appends"],1);assert!(writer.save(&p).await.is_err());assert!(writer.add_phase(&[(999,center(0))],1).is_err());assert_eq!(old.census().await.unwrap(),before);assert_eq!(writer.kernel.node_store.as_ref().unwrap().report()["reserved_dirty_records"],0);assert_eq!(writer.backend().authority.remaining(0),20000-3-43,"only successful first destination remains active; second pending reservation cancelled");println!("STAGE_REGROUP_LATE_FAILURE first43rows committed; next pending rows cancelled; whole writer poisoned; old3rowgeneration unchanged");
 }
 #[tokio::test(flavor="multi_thread")]
 async fn stage_regroup_later_durable_chunk_failure_poisons_phase(){
  let(_dir,p,mut writer,_)=fixture(1);let snapshot=writer.save(&p).await.unwrap();let mut old=Reader64::open(&p,snapshot,256).await.unwrap();let before=old.census().await.unwrap();writer.set_stage_regroup(Some(policy())).unwrap();writer.stage_regroup_stats.fail_before_append_at.store(2,Ordering::Relaxed);let input:Vec<_>=(0..257).map(|id|(id,center(0))).collect();assert!(writer.add_phase(&input,4).is_err());assert!(writer.failed&&writer.kernel.topology_visibility.as_ref().unwrap().failed());assert_eq!(writer.stage_regroup_observation()["appended_rows"],256);assert_eq!(writer.backend().authority.remaining(0),20000-1-256);assert_eq!(writer.kernel.node_store.as_ref().unwrap().report()["reserved_dirty_records"],0);assert!(writer.save(&p).await.is_err());assert!(writer.add_phase(&[(999,center(0))],1).is_err());assert_eq!(old.census().await.unwrap(),before);println!("STAGE_REGROUP_CHUNK_FAILURE first256 committed, final partial chunk cancelled, phase poisoned, old generation retained");
 }
 #[tokio::test(flavor="multi_thread",worker_threads=2)]
 async fn stage_regroup_held_credit_survives_concurrent_flush(){
  let(_dir,p,mut writer,input)=fixture(1);let snapshot=writer.save(&p).await.unwrap();let mut old=Reader64::open(&p,snapshot,256).await.unwrap();let before=old.census().await.unwrap();writer.set_stage_regroup(Some(policy())).unwrap();let store=writer.kernel.node_store.as_ref().unwrap().clone();let(entered,seen)=std::sync::mpsc::channel();let(resume,resuming)=std::sync::mpsc::channel();*writer.stage_regroup_stats.pause_after_append.lock()=Some((entered,resuming));
  std::thread::scope(|scope|{let add=scope.spawn(||writer.add_phase(&input,4));seen.recv_timeout(std::time::Duration::from_secs(5)).unwrap();assert_eq!(store.report()["reserved_dirty_records"],1);store.flush().unwrap();assert_eq!(store.report()["reserved_dirty_records"],1,"flush cannot consume the held write credit");resume.send(()).unwrap();add.join().unwrap().unwrap();});assert_eq!(store.report()["reserved_dirty_records"],0);assert_eq!(writer.backend().actor(0).call(|s|s.leaves[&1].length),130);let saved=writer.save(&p).await.unwrap();let mut current=Reader64::open(&p,saved,256).await.unwrap();assert_eq!(current.census().await.unwrap().len(),130);assert_eq!(old.census().await.unwrap(),before);println!("STAGE_REGROUP_FLUSH held credit survives old-revision flush; current130rows and old1row remain readable");
 }

}

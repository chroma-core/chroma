//! Owner-authoritative data access for the existing concurrent reference kernel.
//! Routing/structure keep the reference DashMaps and conflict handling. Only
//! joined phase boundaries may change placement or publish an immutable reader.
use super::*;
use super::super::common::LeafNode;
use chroma_index::quantization::Code;
use std::{cell::RefCell,collections::{BTreeMap,BTreeSet},sync::{mpsc,atomic::{AtomicU64,AtomicUsize,Ordering}},thread};
use anyhow::{ensure,Result};
use std::time::Instant;

type Job=Box<dyn FnOnce(&mut OwnerState)+Send>;
#[derive(Default)]
struct OwnerState { leaves:BTreeMap<u32,LeafNode>, current:BTreeMap<u32,u8>, physical:usize }
struct Actor { sender:Option<mpsc::Sender<Job>>, thread:Option<thread::JoinHandle<()>>, observation:Arc<Work> }
impl Actor {
    fn new(observation:Arc<Work>)->Self {
        let (sender,receiver)=mpsc::channel::<Job>();let thread=thread::spawn(move||{let mut state=OwnerState::default();while let Ok(job)=receiver.recv(){job(&mut state);}});
        Self{sender:Some(sender),thread:Some(thread),observation}
    }
    fn call<T:Send+'static>(&self,f:impl FnOnce(&mut OwnerState)->T+Send+'static)->T {
        let (tx,rx)=mpsc::sync_channel(1);let observation=self.observation.clone();self.sender.as_ref().expect("owner closed").send(Box::new(move|s|{let active=WorkSpan::new(&observation);let value=f(s);drop(active);let _=tx.send(value);})).expect("owner missing");rx.recv().expect("owner reply missing")
    }
}
impl Drop for Actor {fn drop(&mut self){self.sender.take();if let Some(t)=self.thread.take(){t.join().expect("owner actor panic");}}}
fn clone_leaf(v:&LeafNode)->LeafNode {LeafNode{centroid:v.centroid.clone(),centroid_code:v.centroid_code.clone(),ids:v.ids.clone(),versions:v.versions.clone(),codes:v.codes.clone(),length:v.length,parent_id:v.parent_id}}
fn shell(v:&LeafNode)->LeafNode {LeafNode{centroid:v.centroid.clone(),centroid_code:v.centroid_code.clone(),ids:vec![],versions:vec![],codes:vec![],length:v.length,parent_id:v.parent_id}}
thread_local! {static OWNER_SCOPE:RefCell<Vec<(usize,usize)>>=const{RefCell::new(Vec::new())};}
pub(super) struct OwnerScope { key:usize }
impl Drop for OwnerScope {fn drop(&mut self){OWNER_SCOPE.with(|v|{let item=v.borrow_mut().pop().expect("owner scope missing");assert_eq!(item.0,self.key);});}}
thread_local! {static CRITICAL_DEPTH:RefCell<BTreeMap<usize,usize>>=const{RefCell::new(BTreeMap::new())};}
pub(super) struct CriticalSpan {key:usize,span:Option<WorkSpan>}
impl Drop for CriticalSpan {fn drop(&mut self){CRITICAL_DEPTH.with(|v|{let mut v=v.borrow_mut();let d=v.get_mut(&self.key).expect("critical depth missing");*d-=1;if *d==0 {v.remove(&self.key);}});self.span.take();}}
static NEXT_BACKEND:AtomicUsize=AtomicUsize::new(1);
#[derive(Default)]
struct Work {active:AtomicUsize,peak:AtomicUsize,calls:AtomicU64,nanos:AtomicU64}
pub(super) struct WorkSpan(Arc<Work>,Instant);
impl WorkSpan {fn new(v:&Arc<Work>)->Self{v.calls.fetch_add(1,Ordering::SeqCst);let n=v.active.fetch_add(1,Ordering::SeqCst)+1;v.peak.fetch_max(n,Ordering::SeqCst);Self(v.clone(),Instant::now())}}
impl Drop for WorkSpan {fn drop(&mut self){self.0.nanos.fetch_add(self.1.elapsed().as_nanos() as u64,Ordering::Relaxed);self.0.active.fetch_sub(1,Ordering::SeqCst);}}
/// The directories store only owner identities and per-ID conflict locks. Mutable
/// posting payload and current versions exist exclusively on the owner actors.
pub(super) struct Backend {
    key:usize,actors:Vec<Actor>,leaf_owner:DashMap<u32,usize>,id_owner:DashMap<u32,usize>,id_locks:DashMap<u32,Arc<Mutex<()>>>,
    add_navigation:Arc<Work>,balance_leaf:Arc<Work>,registrations:AtomicU64,predicate_calls:AtomicU64,moves:AtomicU64,actor_work:Arc<Work>,critical:Arc<Work>,noncritical:Arc<Work>,critical_wait_ns:AtomicU64,
}
impl Backend {
    fn new(owners:usize)->Self {let actor_work=Arc::new(Work::default());Self{key:NEXT_BACKEND.fetch_add(1,Ordering::Relaxed),actors:(0..owners).map(|_|Actor::new(actor_work.clone())).collect(),leaf_owner:DashMap::new(),id_owner:DashMap::new(),id_locks:DashMap::new(),add_navigation:Arc::default(),balance_leaf:Arc::default(),registrations:AtomicU64::new(0),predicate_calls:AtomicU64::new(0),moves:AtomicU64::new(0),actor_work,critical:Arc::default(),noncritical:Arc::default(),critical_wait_ns:AtomicU64::new(0)}}
    fn lock_for(&self,id:u32)->Arc<Mutex<()>> {self.id_locks.entry(id).or_insert_with(||Arc::new(Mutex::new(()))).clone()}
    fn actor(&self,owner:usize)->&Actor {self.actors.get(owner).expect("wrong/missing owner")}
    pub(super) fn scope(&self,leaf:u32)->OwnerScope {let owner=self.leaf_owner.get(&leaf).map(|v|*v).unwrap_or(0);OWNER_SCOPE.with(|v|v.borrow_mut().push((self.key,owner)));OwnerScope{key:self.key}}
    fn hint(&self)->usize {OWNER_SCOPE.with(|v|v.borrow().iter().rev().find(|v|v.0==self.key).map(|v|v.1).unwrap_or(0))}
    pub(super) fn current(&self,id:u32)->Option<u8> {let lock=self.lock_for(id);let _guard=lock.lock();let owner=self.id_owner.get(&id).map(|v|*v)?;self.actor(owner).call(move|s|s.current.get(&id).copied())}
    pub(super) fn bump(&self,id:u32,expected:Option<u8>)->Option<u8> {
        let lock=self.lock_for(id);let _guard=lock.lock();let owner=*self.id_owner.entry(id).or_insert(0);
        self.actor(owner).call(move|s|{let value=s.current.entry(id).or_insert(expected.unwrap_or(0));if *value&DELETED_BIT!=0||expected.is_some_and(|v|v!=*value){return None;}*value=value.wrapping_add(1)&0x7f;Some(*value)})
    }
    pub(super) fn install(&self,id:u32,leaf:LeafNode)->LeafNode {
        let metadata=shell(&leaf);let owner=self.leaf_owner.get(&id).map(|v|*v).unwrap_or_else(||self.hint());self.leaf_owner.insert(id,owner);
        self.actor(owner).call(move|s|{let old=s.leaves.get(&id).map(|l|l.ids.len()).unwrap_or(0);let count=s.physical-old+leaf.ids.len();assert!(count<=1048576,"owner physical capacity");s.leaves.insert(id,leaf);s.physical=count;});metadata
    }
    pub(super) fn take(&self,id:u32,metadata:LeafNode)->LeafNode {
        let owner=*self.leaf_owner.get(&id).expect("leaf owner missing");let mut leaf=self.actor(owner).call(move|s|{let leaf=s.leaves.remove(&id).expect("owner leaf missing");s.physical-=leaf.ids.len();leaf});leaf.parent_id=metadata.parent_id;leaf.centroid_code=metadata.centroid_code;leaf
    }
    pub(super) fn snapshot(&self,id:u32)->Option<LeafNode> {let owner=self.leaf_owner.get(&id).map(|v|*v)?;self.actor(owner).call(move|s|s.leaves.get(&id).map(clone_leaf))}
    pub(super) fn register(&self,leaf:u32,id:u32,version:u8,embedding:&[f32])->Option<usize> {
        let owner=self.leaf_owner.get(&leaf).map(|v|*v)?;let values:Arc<[f32]>=Arc::from(embedding);let lock=self.lock_for(id);let _guard=lock.lock();
        let length=self.actor(owner).call(move|s|{assert!(s.physical<1048576,"owner physical capacity");let leaf=s.leaves.get_mut(&leaf)?;let code=Code::<1>::quantize(&values,&leaf.centroid);leaf.ids.push(id);leaf.versions.push(version);leaf.codes.extend_from_slice(code.as_ref());leaf.length+=1;s.physical+=1;Some(leaf.length)})?;
        let from=*self.id_owner.get(&id).expect("current owner missing");
        if from!=owner {let current=self.actor(from).call(move|s|s.current.get(&id).copied());if current==Some(version){self.actor(from).call(move|s|{s.current.remove(&id);});self.actor(owner).call(move|s|{s.current.insert(id,version);});self.id_owner.insert(id,owner);}}
        self.registrations.fetch_add(1,Ordering::Relaxed);Some(length)
    }
    pub(super) fn scrub(&self,leaf:u32,code_size:usize)->Option<usize> {
        let owner=self.leaf_owner.get(&leaf).map(|v|*v)?;let rows=self.actor(owner).call(move|s|s.leaves.get(&leaf).map(|v|v.ids.iter().copied().zip(v.versions.iter().copied()).collect::<Vec<_>>()))?;
        let valid:Vec<_>=rows.iter().map(|(id,v)|self.current(*id)==Some(*v)).collect();
        self.actor(owner).call(move|s|{let leaf=s.leaves.get_mut(&leaf)?;let old=leaf.ids.len();let mut i=0;let mut flags=valid;while i<leaf.ids.len(){if flags[i]{i+=1;}else{leaf.ids.swap_remove(i);leaf.versions.swap_remove(i);flags.swap_remove(i);let last=leaf.codes.len()/code_size-1;if i!=last {leaf.codes.copy_within(last*code_size..(last+1)*code_size,i*code_size);}leaf.codes.truncate(last*code_size);}}leaf.length=leaf.ids.len();s.physical-=old-leaf.length;Some(leaf.length)})
    }
    pub(super) fn fp_predicate(&self,leaf:u32,values:Arc<[f32]>,metric:DistanceFunction,centroid:Vec<f32>,old:Vec<f32>,left:Vec<f32>,right:Vec<f32>)->bool {
        let owner=*self.leaf_owner.get(&leaf).expect("predicate owner missing");self.predicate_calls.fetch_add(1,Ordering::Relaxed);
        self.actor(owner).call(move|_s|super::eligibility::fp_eligible(&metric,&values,&centroid,&old,&left,&right))
    }
    pub(super) fn critical_span(&self,start:Instant)->CriticalSpan {
        self.critical_wait_ns.fetch_add(start.elapsed().as_nanos() as u64,Ordering::Relaxed);
        let outer=CRITICAL_DEPTH.with(|v|{let mut v=v.borrow_mut();let d=v.entry(self.key).or_default();let outer=*d==0;*d+=1;outer});
        CriticalSpan{key:self.key,span:outer.then(||WorkSpan::new(&self.critical))}
    }
    pub(super) fn noncritical_span(&self)->WorkSpan {WorkSpan::new(&self.noncritical)}
    pub(super) fn navigation_span(&self)->WorkSpan {WorkSpan::new(&self.add_navigation)}
    pub(super) fn balance_span(&self)->WorkSpan {WorkSpan::new(&self.balance_leaf)}
}
impl HierarchicalSpannWriter {
    pub(super) fn install_reference_node(&self,id:u32,node:TreeNode)->Option<TreeNode> {let node=match (&self.owner_backend,node){(Some(b),TreeNode::Leaf(leaf))=>TreeNode::Leaf(b.install(id,leaf)),(_,n)=>n};self.nodes.insert(id,node)}
    pub(super) fn take_reference_node(&self,id:u32)->Option<(u32,TreeNode)> {self.nodes.remove(&id).map(|(id,node)|{let node=match (&self.owner_backend,node){(Some(b),TreeNode::Leaf(leaf))=>TreeNode::Leaf(b.take(id,leaf)),(_,n)=>n};(id,node)})}
}

/// This wrapper admits new IDs only and exposes joined construction phases.
/// The underlying reference kernel is private so callers cannot bypass ownership.
pub struct ConcurrentOwnerWriter { kernel:HierarchicalSpannWriter, max_rows:usize, next:u32, ready:bool }
impl ConcurrentOwnerWriter {
    pub fn new(dim:usize,metric:DistanceFunction,config:HierarchicalSpannConfig,owners:usize,max_rows:usize)->Result<Self> {
        ensure!(owners>0&&owners<=8&&max_rows>0&&max_rows<=8192&&dim>0&&dim<=768,"owner adapter capacity");
        ensure!(config.write_navigation==super::super::config::NavigationMode::Fp&&config.fp_npa&&config.max_replicas==1,"owner adapter requires explicit FP and one current copy");
        let mut kernel=HierarchicalSpannWriter::new(dim,metric,config);let backend=Arc::new(Backend::new(owners));
        let (_,node)=kernel.nodes.remove(&0).unwrap();let TreeNode::Leaf(leaf)=node else{unreachable!()};kernel.nodes.insert(0,TreeNode::Leaf(backend.install(0,leaf)));kernel.owner_backend=Some(backend);
        Ok(Self{kernel,max_rows,next:0,ready:true})
    }
    pub fn add_phase(&mut self,rows:&[(u32,Vec<f32>)],workers:usize)->Result<()> {
        ensure!(self.ready&&workers>0&&workers<=8&&!rows.is_empty()&&self.next as usize+rows.len()<=self.max_rows,"add phase capacity/state");
        for (offset,(id,vector)) in rows.iter().enumerate() {ensure!(*id==self.next+offset as u32&&vector.len()==self.kernel.dim&&vector.iter().all(|v|v.is_finite()),"new dense IDs/vector shape");}
        let backend=self.kernel.owner_backend.as_ref().unwrap();
        for actor in &backend.actors {let physical=actor.call(|s|s.physical);ensure!(physical.checked_add(rows.len()*2).is_some_and(|v|v<=1048576),"owner physical reservation exhausted");}
        self.ready=false;let size=rows.len().div_ceil(workers);
        std::thread::scope(|scope|{for rows in rows.chunks(size){let kernel=&self.kernel;scope.spawn(move||for (id,v) in rows{kernel.add(*id,v);});}});
        self.next+=rows.len() as u32;Ok(())
    }
    pub fn balance_phase(&mut self,workers:usize)->Result<()> {
        ensure!(!self.ready&&workers>0&&workers<=8,"balance phase state");self.kernel.balance_index_parallel(workers);self.ready=true;Ok(())
    }
    /// Moves only existing leaf payload and its current-version authority. No
    /// centroid, ID, posting row or full-vector bytes are changed by placement.
    pub fn reshard(&mut self,move_above:usize)->Result<usize> {
        ensure!(self.ready&&move_above>0,"reshard requires joined phase");let b=self.kernel.owner_backend.as_ref().unwrap();let mut loads=vec![0usize;b.actors.len()];
        let leaves:Vec<_>=self.kernel.nodes.iter().filter_map(|v|if matches!(v.value(),TreeNode::Leaf(_)){Some(*v.key())}else{None}).collect();
        for id in &leaves {let owner=*b.leaf_owner.get(id).unwrap();let l=b.snapshot(*id).unwrap();loads[owner]+=l.ids.iter().zip(&l.versions).filter(|(id,v)|b.current(**id)==Some(**v)).count();}
        let mut moved=0;
        for leaf in leaves {let source=*b.leaf_owner.get(&leaf).unwrap();if loads[source]<=move_above {continue;}let dest=(0..loads.len()).min_by_key(|i|loads[*i]).unwrap();if dest==source {continue;}
            let payload=b.actor(source).call(move|s|{let v=s.leaves.remove(&leaf).expect("move leaf missing");s.physical-=v.ids.len();v});let mut current=vec![];
            for (&id,&version) in payload.ids.iter().zip(&payload.versions) {let owner=b.id_owner.get(&id).map(|v|*v);if owner==Some(source)&&b.current(id)==Some(version) {let value=b.actor(source).call(move|s|s.current.remove(&id).unwrap());current.push((id,value));}}
            let count=current.len();for (id,_) in &current {b.id_owner.insert(*id,dest);}b.actor(dest).call(move|s|{s.physical+=payload.ids.len();assert!(s.physical<=1048576,"destination physical capacity");assert!(s.leaves.insert(leaf,payload).is_none());for (id,version) in current {assert!(s.current.insert(id,version).is_none());}});b.leaf_owner.insert(leaf,dest);loads[source]-=count;loads[dest]+=count;moved+=1;b.moves.fetch_add(1,Ordering::Relaxed);
        }Ok(moved)
    }
    /// A joined snapshot is the only place that materializes all owner postings
    /// together. It is publication cost, never a mutable construction fallback.
    pub async fn save(&self,provider:&chroma_blockstore::provider::BlockfileProvider)->Result<super::HierarchicalSpannIds> {
        ensure!(self.ready,"save requires joined balance");let b=self.kernel.owner_backend.as_ref().unwrap();let snapshot=HierarchicalSpannWriter::new(self.kernel.dim,self.kernel.distance_fn.clone(),self.kernel.config.clone());snapshot.nodes.clear();
        for entry in self.kernel.nodes.iter() {let id=*entry.key();let node=match entry.value(){TreeNode::Leaf(meta)=>{let mut leaf=b.snapshot(id).expect("saved leaf missing");leaf.centroid=meta.centroid.clone();leaf.centroid_code=meta.centroid_code.clone();leaf.parent_id=meta.parent_id;TreeNode::Leaf(leaf)},TreeNode::Internal(node)=>TreeNode::Internal(super::super::common::InternalNode{centroid:node.centroid.clone(),centroid_code:node.centroid_code.clone(),children:node.children.clone(),parent_id:node.parent_id})};snapshot.nodes.insert(id,node);snapshot.mark_node_dirty(id);}
        snapshot.root_id.store(self.kernel.root_id(),Ordering::Relaxed);snapshot.next_node_id.store(self.kernel.next_node_id.load(Ordering::Relaxed),Ordering::Relaxed);
        for actor in &b.actors {let versions=actor.call(|s|s.current.clone());for (id,v) in versions {ensure!(snapshot.versions.insert(id,v).is_none(),"duplicate owner current authority");snapshot.mark_version_dirty(id);}}
        for value in self.kernel.embeddings.iter(){snapshot.embeddings.insert(*value.key(),value.value().clone());snapshot.mark_embedding_dirty(*value.key());}
        let flushed=snapshot.commit(provider,None).await.map_err(|e|anyhow::anyhow!("snapshot commit: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("snapshot flush: {e}"))?;Ok(flushed)
    }
    pub fn report(&self)->serde_json::Value {
        let b=self.kernel.owner_backend.as_ref().unwrap();let mut owners=vec![];for (id,actor) in b.actors.iter().enumerate(){let counts=actor.call(|s|{assert_eq!(s.physical,s.leaves.values().map(|v|v.ids.len()).sum::<usize>());(s.leaves.len(),s.current.len(),s.physical)});owners.push(serde_json::json!({"owner":id,"leaves":counts.0,"current":counts.1,"physical":counts.2}));}
        serde_json::json!({"rows":self.next,"owners":owners,"nodes":self.kernel.nodes.len(),"pending_balance":self.kernel.pending_balance_leaves(),"central_mutable_posting_rows":self.kernel.nodes.iter().map(|v|match v.value(){TreeNode::Leaf(v)=>v.ids.len(),_=>0}).sum::<usize>(),"central_current_versions":self.kernel.versions.len(),"add_navigation_calls":b.add_navigation.calls.load(Ordering::Relaxed),"peak_add_navigation_spans":b.add_navigation.peak.load(Ordering::Relaxed),"balance_leaf_calls":b.balance_leaf.calls.load(Ordering::Relaxed),"peak_balance_leaf_spans":b.balance_leaf.peak.load(Ordering::Relaxed),"registrations":b.registrations.load(Ordering::Relaxed),"peak_actual_owner_callbacks":b.actor_work.peak.load(Ordering::Relaxed),"critical_outer_entries":b.critical.calls.load(Ordering::Relaxed),"critical_outer_hold_ns":b.critical.nanos.load(Ordering::Relaxed),"critical_wait_ns":b.critical_wait_ns.load(Ordering::Relaxed),"peak_distinct_critical_threads":b.critical.peak.load(Ordering::Relaxed),"noncritical_npa_entries":b.noncritical.calls.load(Ordering::Relaxed),"noncritical_npa_inclusive_ns":b.noncritical.nanos.load(Ordering::Relaxed),"peak_npa_contexts":b.noncritical.peak.load(Ordering::Relaxed),"owner_predicates":b.predicate_calls.load(Ordering::Relaxed),"placement_moves":b.moves.load(Ordering::Relaxed),"scope":"Actual local owner actors; reference kernel routing/scheduler and existing structural lock. Span overlap includes waits, not CPU. Shared immutable embedding cache remains central/local. Snapshot publication materializes all joined owner postings. No cross-host claim."})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::reader::{HierarchicalSpannReader,AuditLimits};
    fn provider(path:&std::path::Path)->chroma_blockstore::provider::BlockfileProvider {
        use chroma_blockstore::arrow::{config::BlockManagerConfig,provider::ArrowBlockfileProvider};use chroma_cache::new_cache_for_test;use chroma_storage::{local::LocalStorage,Storage};
        chroma_blockstore::provider::BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(path.to_str().unwrap())),BlockManagerConfig::default().max_block_size_bytes,new_cache_for_test(),new_cache_for_test(),2))
    }
    async fn reader(p:&chroma_blockstore::provider::BlockfileProvider,ids:super::super::HierarchicalSpannIds,c:HierarchicalSpannConfig,n:usize)->HierarchicalSpannReader {
        let reader=HierarchicalSpannReader::open(p,ids,DistanceFunction::Euclidean,c).await.unwrap();let census=reader.audit_reachable_ids(n as u64,AuditLimits{max_bitmap_bytes:(n as u64).div_ceil(8),max_nodes:10000,max_leaf_entries:1048576}).await.unwrap();census.require_single_current_copy().unwrap();assert_eq!(census.unique_live_rows,n as u64);assert_eq!(census.unreachable_nodes,0);reader
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn reference_owner_parallel_regions_moves_conflicts_and_saved_readers() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());
        let config=HierarchicalSpannConfig{branching_factor:4,split_threshold:16,merge_threshold:4,write_beam_min:8,write_beam_max:16,reassign_neighbor_count:16,max_replicas:1,write_navigation:super::super::super::config::NavigationMode::Fp,fp_npa:true,..Default::default()};
        let rows:Vec<_>=(0..1024u32).map(|id|{let mut v=vec![0.;32];for (d,x) in v.iter_mut().enumerate(){let q=id.wrapping_mul(1664525).wrapping_add((d as u32+1)*1013904223u32.wrapping_div(32));*x=((q>>8)%1024) as f32/128.;}v[0]+=(id%16) as f32*16.;v[2]=-0.;v}).collect();
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config.clone(),4,1024).unwrap();let reference=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config.clone());
        let first:Vec<_>=(0..512u32).map(|id|(id,rows[id as usize].clone())).collect();owner.add_phase(&first,4).unwrap();owner.balance_phase(4).unwrap();
        std::thread::scope(|scope|{for group in first.chunks(128){let w=&reference;scope.spawn(move||for (id,v) in group {w.add(*id,v);});}});reference.balance_index_parallel(4);
        let old_ids=owner.save(&p).await.unwrap();let old=reader(&p,old_ids,config.clone(),512).await;let old_query=rows[17].clone();let old_results=old.rerank_selected(&old_query,&(0..512u32).collect::<Vec<_>>()).await.unwrap();
        let before=owner.report();let moves=owner.reshard(128).unwrap();assert!(moves>0);let moved=owner.report();assert_eq!(moved["rows"],before["rows"]);assert_eq!(moved["central_mutable_posting_rows"],0);assert_eq!(moved["central_current_versions"],0);assert!(moved["owners"].as_array().unwrap().iter().filter(|v|v["current"].as_u64().unwrap()>0).count()>1);
        let second:Vec<_>=(512..1024u32).map(|id|(id,rows[id as usize].clone())).collect();assert!(owner.add_phase(&[(999,rows[0].clone())],4).is_err());owner.add_phase(&second,4).unwrap();assert!(owner.reshard(128).is_err());owner.balance_phase(4).unwrap();
        std::thread::scope(|scope|{for group in second.chunks(128){let w=&reference;scope.spawn(move||for (id,v) in group {w.add(*id,v);});}});reference.balance_index_parallel(4);
        let saved=owner.save(&p).await.unwrap();let owned=reader(&p,saved,config.clone(),1024).await;let expected=reference.commit(&p,None).await.unwrap().flush().await.unwrap();let baseline=reader(&p,expected,config.clone(),1024).await;
        let ids:Vec<_>=(0..1024).collect();assert_eq!(owned.rerank_selected(&old_query,&ids).await.unwrap(),baseline.rerank_selected(&old_query,&ids).await.unwrap());assert_eq!(old.rerank_selected(&old_query,&(0..512).collect::<Vec<_>>()).await.unwrap(),old_results);
        let report=owner.report();assert_eq!(report["central_mutable_posting_rows"],0);assert_eq!(report["central_current_versions"],0);assert!(report["peak_add_navigation_spans"].as_u64().unwrap()>1);assert!(report["peak_balance_leaf_spans"].as_u64().unwrap()>1);assert!(report["peak_npa_contexts"].as_u64().unwrap()>1);assert_eq!(report["peak_distinct_critical_threads"],1);assert!(report["owner_predicates"].as_u64().unwrap()>0);println!("REFERENCE_OWNER_REPORT {report}");
    }
    #[test]
    fn reference_owner_missing_wrong_owner_and_configuration_reject() {
        let mut c=HierarchicalSpannConfig::default();c.fp_npa=false;assert!(ConcurrentOwnerWriter::new(4,DistanceFunction::Euclidean,c,2,64).is_err());
        let b=Backend::new(2);let leaf=LeafNode{centroid:vec![0.;4],centroid_code:vec![],ids:vec![],versions:vec![],codes:vec![],parent_id:None,length:0};b.install(0,leaf);assert_eq!(b.bump(0,None),Some(1));b.leaf_owner.insert(0,1);assert!(b.register(0,0,1,&[1.;4]).is_none());assert!(b.snapshot(0).is_none());assert_eq!(b.current(0),Some(1));b.leaf_owner.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.snapshot(0))).is_err());b.leaf_owner.insert(0,0);assert_eq!(b.snapshot(0).unwrap().length,0);
    }
}

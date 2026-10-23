//! Owner-authoritative data access for the existing concurrent reference kernel.
//! Routing/structure keep the reference DashMaps and conflict handling. Only
//! joined phase boundaries may change placement or publish an immutable reader.
use super::super::common::LeafNode;
use super::*;
use anyhow::{ensure, Result};
use chroma_index::quantization::Code;
use std::time::Instant;
use std::{
    cell::RefCell,
    collections::{BTreeMap,BTreeSet},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

const OWNER_PUBLICATION_LEAF_BYTES:usize=4*1024*1024;

type Job = Box<dyn FnOnce(&mut OwnerState) + Send>;
#[derive(Default)]
struct OwnerState {
    leaves: BTreeMap<u32, LeafNode>,
    dirty_current:BTreeSet<u32>,
    current: Arc<parking_lot::RwLock<BTreeMap<u32, u8>>>,
    physical: usize,
    vectors:BTreeMap<u32,super::owner_vector_store::VectorHandle>,
}
#[derive(Clone, Copy)]
#[repr(usize)]
enum Operation { Other, Current, VersionMutation, Posting, Predicate, Registration }
const OP_NAMES: [&str;6] = ["other","current","version_mutation","posting","predicate","registration"];
#[derive(Default)]
struct TimingBucket { calls:AtomicU64, queue:AtomicU64, service:AtomicU64, caller:AtomicU64, max_queue:AtomicU64, max_service:AtomicU64, max_caller:AtomicU64 }
#[derive(Default)]
struct MessageTiming { enabled:std::sync::atomic::AtomicBool, buckets:[TimingBucket;6], queued:AtomicUsize, peak_queued:AtomicUsize }
impl MessageTiming {
 fn snapshot(&self)->serde_json::Value {
  let mut out=serde_json::Map::new();
  for (i,n) in OP_NAMES.iter().enumerate(){let b=&self.buckets[i];out.insert((*n).into(),serde_json::json!({"calls":b.calls.load(Ordering::Relaxed),"queue_ns":b.queue.load(Ordering::Relaxed),"service_ns":b.service.load(Ordering::Relaxed),"caller_ns":b.caller.load(Ordering::Relaxed),"max_queue_ns":b.max_queue.load(Ordering::Relaxed),"max_service_ns":b.max_service.load(Ordering::Relaxed),"max_caller_ns":b.max_caller.load(Ordering::Relaxed)}));}
  serde_json::json!({"enabled":self.enabled.load(Ordering::Relaxed),"queued":self.queued.load(Ordering::Relaxed),"peak_queued":self.peak_queued.load(Ordering::Relaxed),"operations":out})
 }
}
struct Actor {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
    observation: Arc<Work>,
    timing: Arc<MessageTiming>,
    versions: Arc<parking_lot::RwLock<BTreeMap<u32,u8>>>,
}
impl Actor {
    fn new(observation: Arc<Work>, timing: Arc<MessageTiming>) -> Self {
        let (sender, receiver) = mpsc::channel::<Job>();
        let versions=Arc::new(parking_lot::RwLock::new(BTreeMap::new()));
        let owner_versions=versions.clone();
        let thread = thread::spawn(move || {
            let mut state = OwnerState {current:owner_versions,..OwnerState::default()};
            while let Ok(job) = receiver.recv() {
                job(&mut state);
            }
        });
        Self {
            sender: Some(sender),
            thread: Some(thread),
            observation,
            timing,
            versions,
        }
    }
    fn call<T: Send + 'static>(&self, f: impl FnOnce(&mut OwnerState) -> T + Send + 'static) -> T {
        self.call_op(Operation::Other,f)
    }
    fn call_op<T: Send + 'static>(&self, op:Operation, f: impl FnOnce(&mut OwnerState)->T+Send+'static)->T {
        let timing = self.timing.enabled.load(Ordering::Relaxed).then(||self.timing.clone());
        let started=timing.as_ref().map(|_|Instant::now());
        if let Some(t)=&timing {let q=t.queued.fetch_add(1,Ordering::Relaxed)+1;t.peak_queued.fetch_max(q,Ordering::Relaxed);}
        let service_timing=timing.clone();
        let (tx, rx) = mpsc::sync_channel(1);
        let observation = self.observation.clone();
        self.sender
            .as_ref()
            .expect("owner closed")
            .send(Box::new(move |s| {
                let service=service_timing.as_ref().map(|t| {
                    let b=&t.buckets[op as usize];let q=started.unwrap().elapsed().as_nanos() as u64;
                    b.calls.fetch_add(1,Ordering::Relaxed);b.queue.fetch_add(q,Ordering::Relaxed);b.max_queue.fetch_max(q,Ordering::Relaxed);t.queued.fetch_sub(1,Ordering::Relaxed);Instant::now()
                });
                let active = WorkSpan::new(&observation);
                let value = f(s);
                drop(active);
                if let Some(t)=&service_timing {let ns=service.unwrap().elapsed().as_nanos() as u64;let b=&t.buckets[op as usize];b.service.fetch_add(ns,Ordering::Relaxed);b.max_service.fetch_max(ns,Ordering::Relaxed);}
                let _ = tx.send(value);
            }))
            .expect("owner missing");
        let value=rx.recv().expect("owner reply missing");
        if let Some(t)=&timing {let ns=started.unwrap().elapsed().as_nanos() as u64;let b=&t.buckets[op as usize];b.caller.fetch_add(ns,Ordering::Relaxed);b.max_caller.fetch_max(ns,Ordering::Relaxed);}
        value
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(t) = self.thread.take() {
            t.join().expect("owner actor panic");
        }
    }
}
fn clone_leaf(v: &LeafNode) -> LeafNode {
    LeafNode {
        centroid: v.centroid.clone(),
        centroid_code: v.centroid_code.clone(),
        ids: v.ids.clone(),
        versions: v.versions.clone(),
        codes: v.codes.clone(),
        length: v.length,
        parent_id: v.parent_id,
    }
}
fn shell(v: &LeafNode) -> LeafNode {
    LeafNode {
        centroid: v.centroid.clone(),
        centroid_code: v.centroid_code.clone(),
        ids: vec![],
        versions: vec![],
        codes: vec![],
        length: v.length,
        parent_id: v.parent_id,
    }
}
thread_local! {static OWNER_SCOPE:RefCell<Vec<(usize,usize)>>=const{RefCell::new(Vec::new())};}
pub(super) struct OwnerScope {
    key: usize,
}
impl Drop for OwnerScope {
    fn drop(&mut self) {
        OWNER_SCOPE.with(|v| {
            let item = v.borrow_mut().pop().expect("owner scope missing");
            assert_eq!(item.0, self.key);
        });
    }
}
thread_local! {static CRITICAL_DEPTH:RefCell<BTreeMap<usize,usize>>=const{RefCell::new(BTreeMap::new())};}
pub(super) struct CriticalSpan {
    key: usize,
    span: Option<WorkSpan>,
}
impl Drop for CriticalSpan {
    fn drop(&mut self) {
        CRITICAL_DEPTH.with(|v| {
            let mut v = v.borrow_mut();
            let d = v.get_mut(&self.key).expect("critical depth missing");
            *d -= 1;
            if *d == 0 {
                v.remove(&self.key);
            }
        });
        self.span.take();
    }
}
static NEXT_BACKEND: AtomicUsize = AtomicUsize::new(1);
#[derive(Default)]
struct Work {
    active: AtomicUsize,
    peak: AtomicUsize,
    calls: AtomicU64,
    nanos: AtomicU64,
}
thread_local! {static WORK_DEPTH:RefCell<BTreeMap<usize,usize>>=const{RefCell::new(BTreeMap::new())};}
pub(super) struct WorkSpan(Arc<Work>, Instant, bool);
impl WorkSpan {
    fn new(v: &Arc<Work>) -> Self {
        v.calls.fetch_add(1, Ordering::SeqCst);
        let key = Arc::as_ptr(v) as usize;
        let outer = WORK_DEPTH.with(|depth| {
            let mut depth = depth.borrow_mut();
            let d = depth.entry(key).or_default();
            let outer = *d == 0;
            *d += 1;
            outer
        });
        if outer {
            let n = v.active.fetch_add(1, Ordering::SeqCst) + 1;
            v.peak.fetch_max(n, Ordering::SeqCst);
        }
        Self(v.clone(), Instant::now(), outer)
    }
}
impl Drop for WorkSpan {
    fn drop(&mut self) {
        WORK_DEPTH.with(|depth| {
            let mut depth = depth.borrow_mut();
            let key = Arc::as_ptr(&self.0) as usize;
            let d = depth.get_mut(&key).unwrap();
            *d -= 1;
            if *d == 0 {
                depth.remove(&key);
            }
        });
        if self.2 {
            self.0
                .nanos
                .fetch_add(self.1.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
/// The directories store only owner identities and per-ID conflict locks. Mutable
/// posting payload and current versions exist exclusively on the owner actors.
pub(super) struct Predicate {
    metric: DistanceFunction,
    centroid: Vec<f32>,
    old: Vec<f32>,
    left: Vec<f32>,
    right: Vec<f32>,
}
pub(super) struct LivePrefix { pub consumed:usize, pub evaluated:usize, pub action:Option<usize> }
pub(crate) struct Backend {
    vector_store:Mutex<Option<Arc<super::owner_vector_store::OwnerVectorStore>>>,
    #[cfg(test)]
    first_registration_pause:Mutex<Option<(mpsc::Sender<()>,mpsc::Receiver<()>)>>,
    key: usize,
    actors: Vec<Actor>,
    leaf_owner: DashMap<u32, usize>,
    authority: super::partition_authority::PartitionAuthority,
    add_navigation: Arc<Work>,
    balance_leaf: Arc<Work>,
    registrations: AtomicU64,
    predicate_calls: AtomicU64,
    current_requests: AtomicU64,
    direct_current_reads: AtomicU64,
    direct_current_policy: AtomicUsize,
    split_input_policy: AtomicUsize,
    split_input_rows: AtomicU64,
    split_input_retained: AtomicU64,
    split_input_scalar_calls: AtomicU64,
    split_input_batches: AtomicU64,
    split_input_owner_calls: AtomicU64,
    split_input_owner_rows: AtomicU64,
    split_input_lengths: Mutex<[u64;65]>,
    live_prefix_policy: AtomicUsize,
    live_prefix_batches: AtomicU64,
    live_prefix_rows: AtomicU64,
    live_prefix_consumed: AtomicU64,
    live_prefix_actions: AtomicU64,
    live_prefix_owner_calls: AtomicU64,
    live_prefix_request_rows: AtomicU64,
    live_prefix_lengths: Mutex<([u64;65],[u64;65])>,
    scrub_batch_policy: AtomicUsize,
    scrub_validation_batches: AtomicU64,
    scrub_validation_rows: AtomicU64,
    scrub_validation_request_rows: AtomicU64,
    scrub_validation_owner_calls: AtomicU64,
    #[cfg(test)]
    scrub_pause: Mutex<Option<(mpsc::Sender<()>,mpsc::Receiver<()>)>>,
    #[cfg(test)]
    transfer_pause: Mutex<Option<(mpsc::Sender<()>,mpsc::Receiver<()>)>>,
    predicate_batching: AtomicUsize,
    predicate_batch_callbacks: AtomicU64,
    predicate_batch_rows: AtomicU64,
    physical_capacity: AtomicUsize,
    moves: AtomicU64,
    orphan_adoptions: AtomicU64,
    parent_edits: AtomicU64,
    actor_work: Arc<Work>,
    message_timing: Arc<MessageTiming>,
    critical: Arc<Work>,
    noncritical: Arc<Work>,
    subtrees: Arc<Work>,
    schedule_rounds: AtomicU64,
    last_schedule: Mutex<Vec<Vec<u32>>>,
    schedule_sample_overflow: std::sync::atomic::AtomicBool,
    critical_wait_ns: AtomicU64,
    predicate_center_copy_bytes: AtomicU64,
    registration_vector_copy_bytes: AtomicU64,
    snapshot_copy_bytes: AtomicU64,
    moved_posting_payload_bytes: AtomicU64,
}
impl Backend {
    fn new(owners: usize) -> Self { Self::with_authority_capacity(owners, 16384) }
    fn with_authority_capacity(owners: usize, capacity: usize) -> Self {
        let actor_work = Arc::new(Work::default());
        let message_timing=Arc::new(MessageTiming::default());
        Self {
            key: NEXT_BACKEND.fetch_add(1, Ordering::Relaxed),
            vector_store:Mutex::new(None),
            #[cfg(test)] first_registration_pause:Mutex::new(None),
            actors: (0..owners)
                .map(|_| Actor::new(actor_work.clone(),message_timing.clone()))
                .collect(),
            leaf_owner: DashMap::new(),
            authority: super::partition_authority::PartitionAuthority::new(&[0], capacity).expect("authority capacity"),
            add_navigation: Arc::default(),
            balance_leaf: Arc::default(),
            registrations: AtomicU64::new(0),
            predicate_calls: AtomicU64::new(0),
            current_requests: AtomicU64::new(0),
            direct_current_reads: AtomicU64::new(0),
            direct_current_policy: AtomicUsize::new(0),
            split_input_policy: AtomicUsize::new(0),
            split_input_rows: AtomicU64::new(0),split_input_retained: AtomicU64::new(0),
            split_input_scalar_calls: AtomicU64::new(0),split_input_batches: AtomicU64::new(0),
            split_input_owner_calls: AtomicU64::new(0),split_input_owner_rows: AtomicU64::new(0),
            split_input_lengths: Mutex::new([0;65]),
            live_prefix_policy: AtomicUsize::new(0),
            live_prefix_batches: AtomicU64::new(0),live_prefix_rows:AtomicU64::new(0),
            live_prefix_consumed:AtomicU64::new(0),live_prefix_actions:AtomicU64::new(0),
            live_prefix_owner_calls:AtomicU64::new(0),live_prefix_request_rows:AtomicU64::new(0),
            live_prefix_lengths:Mutex::new(([0;65],[0;65])),
            scrub_batch_policy: AtomicUsize::new(0),
            scrub_validation_batches: AtomicU64::new(0),
            scrub_validation_rows: AtomicU64::new(0),
            scrub_validation_request_rows: AtomicU64::new(0),
            scrub_validation_owner_calls: AtomicU64::new(0),
            #[cfg(test)]
            scrub_pause: Mutex::new(None),
            #[cfg(test)]
            transfer_pause: Mutex::new(None),
            predicate_batching: AtomicUsize::new(0),
            predicate_batch_callbacks: AtomicU64::new(0),
            predicate_batch_rows: AtomicU64::new(0),
            physical_capacity: AtomicUsize::new(1048576),
            moves: AtomicU64::new(0),
            orphan_adoptions: AtomicU64::new(0),
            parent_edits: AtomicU64::new(0),
            actor_work,
            message_timing,
            critical: Arc::default(),
            noncritical: Arc::default(),
            subtrees: Arc::default(),
            schedule_rounds: AtomicU64::new(0),
            last_schedule: Mutex::new(vec![]),
            schedule_sample_overflow: std::sync::atomic::AtomicBool::new(false),
            critical_wait_ns: AtomicU64::new(0),
            predicate_center_copy_bytes: AtomicU64::new(0),
            registration_vector_copy_bytes: AtomicU64::new(0),
            snapshot_copy_bytes: AtomicU64::new(0),
            moved_posting_payload_bytes: AtomicU64::new(0),
        }
    }
    fn publication_has_dirty_versions(&self)->bool{self.actors.iter().any(|actor|actor.call(|s|!s.dirty_current.is_empty()))}
    pub(super) fn publication_dirty_versions(&self)->BTreeSet<u32>{
        let mut keys=BTreeSet::new();for actor in &self.actors{keys.extend(actor.call(|s|s.dirty_current.iter().copied().collect::<Vec<_>>()));}keys
    }
    pub(super) fn publication_versions(&self,ids:&[u32])->Result<Vec<(u32,u8)>>{
        ensure!(ids.len()<=64,"publication version batch");let mut groups:BTreeMap<usize,Vec<u32>>=BTreeMap::new();
        for id in ids{let owner=self.authority.get(id).map(|v|*v).ok_or_else(||anyhow::anyhow!("dirty ID has no authority"))?;groups.entry(owner).or_default().push(*id);}
        let mut values=Vec::with_capacity(ids.len());for(owner,keys) in groups{values.extend(self.actor(owner).call(move|s|{let current=s.current.read();keys.into_iter().map(|id|current.get(&id).map(|v|(id,*v))).collect::<Option<Vec<_>>>()}).ok_or_else(||anyhow::anyhow!("dirty current missing at owner"))?);}values.sort_unstable_by_key(|v|v.0);Ok(values)
    }
    pub(super) fn publication_leaf(&self,id:u32,limit:usize)->Result<LeafNode>{
        let owner=self.leaf_owner.get(&id).map(|v|*v).ok_or_else(||anyhow::anyhow!("publication leaf owner missing"))?;
        self.actor(owner).call(move|s|{let leaf=s.leaves.get(&id).ok_or_else(||anyhow::anyhow!("publication leaf missing"))?;
            let bytes=leaf.ids.len().checked_mul(5).and_then(|v|v.checked_add(leaf.codes.len())).and_then(|v|v.checked_add(leaf.centroid.len()*4+leaf.centroid_code.len())).ok_or_else(||anyhow::anyhow!("leaf extent"))?;
            ensure!(bytes<=limit,"publication leaf owned payload admission");Ok(clone_leaf(leaf))})
    }
    fn acknowledge_publication(&self){for actor in &self.actors{actor.call(|s|s.dirty_current.clear());}}
    pub(super) fn vector_store(&self)->Option<Arc<super::owner_vector_store::OwnerVectorStore>>{self.vector_store.lock().clone()}
    pub(super) fn staged_vector(&self,id:u32)->Result<super::owner_vector_store::VectorHandle>{self.vector_store().ok_or_else(||anyhow::anyhow!("bounded vector policy absent"))?.staged(id)}
    pub(super) fn resolve_vector(&self,id:u32)->Result<(usize,super::owner_vector_store::VectorHandle)>{
        let lock=self.lock_for(id);let _guard=lock.lock();let owner=self.authority.get(&id).map(|v|*v).ok_or_else(||anyhow::anyhow!("vector owner missing for {id}"))?;
        let handle=self.actor(owner).call(move|s|{if !s.current.read().contains_key(&id){return None;}s.vectors.get(&id).cloned()}).ok_or_else(||anyhow::anyhow!("authoritative immutable handle missing for {id}"))?;
        Ok((owner,handle))
    }
    pub(super) fn fetch_vectors(&self,ids:&[u32])->Result<Vec<Arc<[f32]>>>{
        let store=self.vector_store().ok_or_else(||anyhow::anyhow!("bounded vector policy absent"))?;ensure!(ids.len()<=store.limits.split_rows,"owned vector result cap");let mut result=Vec::with_capacity(ids.len());
        for ids in ids.chunks(store.limits.fetch_rows){
            let handles=ids.iter().map(|id|self.resolve_vector(*id)).collect::<Result<Vec<_>>>()?;
            // Every transfer/actor guard is gone before waiting for vector IO.
            let mut slots:Vec<Option<Arc<[f32]>>>=vec![None;handles.len()];
            for owner in 0..self.actors.len(){let selected:Vec<_>=handles.iter().enumerate().filter(|(_,v)|v.0==owner).map(|(i,v)|(i,v.1.clone())).collect();if selected.is_empty(){continue;}let keys:Vec<_>=selected.iter().map(|v|v.1.clone()).collect();let values=store.fetch(owner,&keys)?;for ((i,_),value) in selected.into_iter().zip(values){slots[i]=Some(value);}}
            result.extend(slots.into_iter().map(|v|v.expect("resolved owner slot")));
        }Ok(result)
    }
    fn lock_for(&self, id: u32) -> Arc<Mutex<()>> {
        self.authority.locks([id.into()]).pop().unwrap()
    }
    fn locks_for(&self, ids: &[u32]) -> Vec<Arc<Mutex<()>>> {
        self.authority.locks(ids.iter().copied().map(Into::into))
    }
    fn actor(&self, owner: usize) -> &Actor {
        self.actors.get(owner).expect("wrong/missing owner")
    }
    pub(super) fn scope(&self, leaf: u32) -> OwnerScope {
        let owner = self.leaf_owner.get(&leaf).map(|v| *v).unwrap_or(0);
        OWNER_SCOPE.with(|v| v.borrow_mut().push((self.key, owner)));
        OwnerScope { key: self.key }
    }
    fn hint(&self) -> usize {
        OWNER_SCOPE.with(|v| {
            v.borrow()
                .iter()
                .rev()
                .find(|v| v.0 == self.key)
                .map(|v| v.1)
                .unwrap_or(0)
        })
    }
    pub(super) fn current(&self, id: u32) -> Option<u8> {
        self.current_requests.fetch_add(1, Ordering::Relaxed);
        let lock = self.lock_for(id);
        let _guard = lock.lock();
        let owner = self.authority.get(&id).map(|v| *v)?;
        if self.direct_current_policy.load(Ordering::Relaxed)!=0 {
            self.direct_current_reads.fetch_add(1,Ordering::Relaxed);
            // This is the owner's original allocation, not a version cache. The
            // ID lock stays held through this read and through authority moves.
            return self.actor(owner).versions.read().get(&id).copied();
        }
        self.actor(owner).call_op(Operation::Current,move |s| s.current.read().get(&id).copied())
    }
    pub(super) fn bump(&self, id: u32, expected: Option<u8>) -> Option<u8> {
        let lock = self.lock_for(id);
        let _guard = lock.lock();
        let owner = if let Some(owner)=self.authority.get(&id){*owner}else{self.authority.reserve(id.into()).expect("authority admission").activate(0);0};
        self.actor(owner).call_op(Operation::VersionMutation,move |s| {
            let mut versions=s.current.write();
            let value = versions.entry(id).or_insert(expected.unwrap_or(0));
            if *value & DELETED_BIT != 0 || expected.is_some_and(|v| v != *value) {
                return None;
            }
            *value = value.wrapping_add(1) & 0x7f;
            let result=*value;drop(versions);s.dirty_current.insert(id);Some(result)
        })
    }
    pub(super) fn install(&self, id: u32, leaf: LeafNode) -> LeafNode {
        let metadata = shell(&leaf);
        let owner = self
            .leaf_owner
            .get(&id)
            .map(|v| *v)
            .unwrap_or_else(|| self.hint());
        self.leaf_owner.insert(id, owner);
        let capacity=self.physical_capacity.load(Ordering::Relaxed);
        self.actor(owner).call_op(Operation::Posting,move |s| {
            let old = s.leaves.get(&id).map(|l| l.ids.len()).unwrap_or(0);
            let count = s.physical - old + leaf.ids.len();
            assert!(count <= capacity, "owner physical capacity");
            s.leaves.insert(id, leaf);
            s.physical = count;
        });
        metadata
    }
    pub(super) fn take(&self, id: u32, metadata: LeafNode) -> LeafNode {
        let owner = *self.leaf_owner.get(&id).expect("leaf owner missing");
        let mut leaf = self.actor(owner).call_op(Operation::Posting,move |s| {
            let leaf = s.leaves.remove(&id).expect("owner leaf missing");
            s.physical -= leaf.ids.len();
            leaf
        });
        leaf.parent_id = metadata.parent_id;
        leaf.centroid_code = metadata.centroid_code;
        leaf
    }
    pub(super) fn snapshot(&self, id: u32) -> Option<LeafNode> {
        let owner = self.leaf_owner.get(&id).map(|v| *v)?;
        self.actor(owner)
            .call_op(Operation::Posting,move |s| s.leaves.get(&id).map(clone_leaf))
            .inspect(|v| {
                self.snapshot_copy_bytes.fetch_add(
                    (v.centroid.len() * 4 + v.centroid_code.len() + v.ids.len() * 5 + v.codes.len())
                        as u64,
                    Ordering::Relaxed,
                );
            })
    }
    pub(super) fn register(
        &self,
        leaf: u32,
        id: u32,
        version: u8,
        embedding: &[f32],
    ) -> Option<usize> {
        #[cfg(test)]
        {let pause={self.first_registration_pause.lock().take()};if let Some((entered,resume))=pause{entered.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(10)).expect("test registration retry resume");return None;}}
        let owner = self.leaf_owner.get(&leaf).map(|v| *v)?;
        self.registration_vector_copy_bytes
            .fetch_add((embedding.len() * 4) as u64, Ordering::Relaxed);
        let values: Arc<[f32]> = Arc::from(embedding);
        let lock = self.lock_for(id);
        let _guard = lock.lock();
        let reservation=self.authority.reserve(id.into()).expect("authority admission before registration");
        let staged=if self.vector_store().is_some() && !self.authority.contains_key(&id){Some(self.staged_vector(id).expect("durable new-vector staging"))}else{None};
        let initial=staged.is_some();
        let capacity=self.physical_capacity.load(Ordering::Relaxed);
        let length = self.actor(owner).call_op(Operation::Registration,move |s| {
            assert!(s.physical < capacity, "owner physical capacity");
            let leaf = s.leaves.get_mut(&leaf)?;
            let code = Code::<1>::quantize(&values, &leaf.centroid);
            leaf.ids.push(id);
            leaf.versions.push(version);
            leaf.codes.extend_from_slice(code.as_ref());
            leaf.length += 1;
            s.physical += 1;
            if let Some(handle)=staged{assert!(s.current.write().insert(id,version).is_none());assert!(s.vectors.insert(id,handle).is_none());s.dirty_current.insert(id);}
            Some(leaf.length)
        })?;
        if initial{reservation.activate(owner);}else{drop(reservation);}
        let from = *self.authority.get(&id).expect("current owner missing");
        if from != owner {
            let current = self.actor(from).call_op(Operation::Registration,move |s| s.current.read().get(&id).copied());
            if current == Some(version) {
                let handle=self.actor(from).call_op(Operation::Registration,move |s| {
                    s.current.write().remove(&id);s.vectors.remove(&id)
                });
                #[cfg(test)]
                if let Some((removed,resume))=self.transfer_pause.lock().take(){removed.send(()).unwrap();resume.recv().unwrap();}
                self.actor(owner).call_op(Operation::Registration,move |s| {
                    s.current.write().insert(id, version);if let Some(handle)=handle{s.vectors.insert(id,handle);}
                });
                self.authority.insert(id, owner);
            }
        }
        self.registrations.fetch_add(1, Ordering::Relaxed);
        Some(length)
    }
    /// The split caller holds tree_lock after removing its source leaf. ID locks
    /// precede owner reads; actors never acquire ID/node/tree locks or call peers.
    /// Preserve physical row order and the reference's missing-to-zero comparison.
    /// No version observations escape this bounded filter; no mutation runs here.
    pub(super) fn split_input_flags(&self, ids:&[u32], stored:&[u8])->Vec<bool> {
        assert!(ids.len()==stored.len() && ids.len()<=64,"split input capacity/extent");
        self.split_input_rows.fetch_add(ids.len() as u64,Ordering::Relaxed);
        if ids.is_empty(){return vec![];}
        let flags=if self.split_input_policy.load(Ordering::Relaxed)==0 {
            self.split_input_scalar_calls.fetch_add(ids.len() as u64,Ordering::Relaxed);
            ids.iter().zip(stored).map(|(id,v)|*v==self.current(*id).unwrap_or(0)).collect::<Vec<_>>()
        }else{
            self.split_input_batches.fetch_add(1,Ordering::Relaxed);
            self.split_input_lengths.lock()[ids.len()]+=1;
            self.current_requests.fetch_add(ids.len() as u64,Ordering::Relaxed);
            let mut unique=ids.to_vec();unique.sort_unstable();unique.dedup();
            let locks=self.locks_for(&unique);
            let _guards:Vec<_>=locks.iter().map(|lock|lock.lock()).collect();
            let mut grouped:BTreeMap<usize,Vec<(usize,u32)>>=BTreeMap::new();
            for (slot,id) in ids.iter().enumerate(){if let Some(owner)=self.authority.get(id){grouped.entry(*owner).or_default().push((slot,*id));}}
            let mut versions=vec![None;ids.len()];
            for (owner,rows) in grouped {
                self.split_input_owner_calls.fetch_add(1,Ordering::Relaxed);
                self.split_input_owner_rows.fetch_add(rows.len() as u64,Ordering::Relaxed);
                let values=self.actor(owner).call_op(Operation::Current,move |s| {
                    let current=s.current.read();rows.into_iter().map(|(slot,id)|(slot,current.get(&id).copied())).collect::<Vec<_>>()
                });
                for (slot,current) in values{versions[slot]=current;}
            }
            stored.iter().zip(versions).map(|(v,current)|*v==current.unwrap_or(0)).collect()
        };
        self.split_input_retained.fetch_add(flags.iter().filter(|v|**v).count() as u64,Ordering::Relaxed);
        flags
    }
    pub(super) fn live_prefix_enabled(&self)->bool {self.live_prefix_policy.load(Ordering::Relaxed)!=0}
    /// Consume a bounded, ordered NPA prefix while its versions cannot change.
    /// Caller holds no node/structural guard. Actor jobs only read owner state.
    /// All guards and unused suffix values drop before the returned action runs.
    pub(super) fn live_npa_prefix(&self,rows:&[(u32,u8,Option<bool>)],evaluated:&mut std::collections::HashSet<u32>)->LivePrefix {
        assert!(rows.len()<=64,"live prefix capacity");
        self.live_prefix_batches.fetch_add(1,Ordering::Relaxed);
        self.live_prefix_rows.fetch_add(rows.len() as u64,Ordering::Relaxed);
        self.current_requests.fetch_add(rows.len() as u64,Ordering::Relaxed);
        let mut ids:Vec<_>=rows.iter().map(|v|v.0).collect();ids.sort_unstable();ids.dedup();
        let locks=self.locks_for(&ids);
        let _guards:Vec<_>=locks.iter().map(|lock|lock.lock()).collect();
        let mut grouped:BTreeMap<usize,Vec<(usize,u32)>>=BTreeMap::new();
        for (slot,(id,_,_)) in rows.iter().enumerate() {if let Some(owner)=self.authority.get(id){grouped.entry(*owner).or_default().push((slot,*id));}}
        let mut versions=vec![None;rows.len()];
        for (owner,requested) in grouped {
            self.live_prefix_owner_calls.fetch_add(1,Ordering::Relaxed);
            self.live_prefix_request_rows.fetch_add(requested.len() as u64,Ordering::Relaxed);
            let values=self.actor(owner).call_op(Operation::Current,move |state| {
                let current=state.current.read();requested.into_iter().map(|(slot,id)|(slot,current.get(&id).copied())).collect::<Vec<_>>()
            });
            for (slot,value) in values {versions[slot]=value;}
        }
        let mut result=LivePrefix{consumed:0,evaluated:0,action:None};
        for (slot,(id,version,predicate)) in rows.iter().enumerate() {
            result.consumed+=1;
            // Preserve the reference's missing=>0 byte comparison and dedup order.
            if *version!=versions[slot].unwrap_or(0) || !evaluated.insert(*id) {continue;}
            let Some(eligible)=predicate else {continue;};result.evaluated+=1;
            if *eligible {result.action=Some(slot);break;}
        }
        self.live_prefix_consumed.fetch_add(result.consumed as u64,Ordering::Relaxed);
        self.live_prefix_actions.fetch_add(u64::from(result.action.is_some()),Ordering::Relaxed);
        let mut lengths=self.live_prefix_lengths.lock();lengths.0[rows.len()]+=1;lengths.1[result.consumed]+=1;
        result
    }
    /// The caller retains the source node's mutable guard through this whole
    /// operation. No callback takes node/ID locks or waits on another owner.
    fn validate_scrub_batch(&self, rows:&[(u32,u8)])->Vec<bool> {
        assert!(rows.len()<=64,"scrub validation capacity");
        self.scrub_validation_batches.fetch_add(1,Ordering::Relaxed);
        self.scrub_validation_rows.fetch_add(rows.len() as u64,Ordering::Relaxed);
        self.current_requests.fetch_add(rows.len() as u64,Ordering::Relaxed);
        let mut ids:Vec<_>=rows.iter().map(|v|v.0).collect();ids.sort_unstable();ids.dedup();
        let locks=self.locks_for(&ids);
        let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();
        let mut grouped:BTreeMap<usize,Vec<(usize,u32)>>=BTreeMap::new();
        for (slot,(id,_)) in rows.iter().enumerate(){if let Some(owner)=self.authority.get(id){grouped.entry(*owner).or_default().push((slot,*id));}}
        let mut versions=vec![None;rows.len()];
        for (owner,requested) in grouped {
            self.scrub_validation_request_rows.fetch_add(requested.len() as u64,Ordering::Relaxed);
            self.scrub_validation_owner_calls.fetch_add(1,Ordering::Relaxed);
            let values=self.actor(owner).call_op(Operation::Current,move |s| {
                let versions=s.current.read();requested.into_iter().map(|(slot,id)|(slot,versions.get(&id).copied())).collect::<Vec<_>>()
            });
            for (slot,value) in values {versions[slot]=value;}
        }
        rows.iter().zip(versions).map(|((_,v),current)|current==Some(*v)).collect()
    }
    pub(super) fn scrub(&self, leaf: u32, code_size: usize) -> Option<usize> {
        let owner = self.leaf_owner.get(&leaf).map(|v| *v)?;
        let rows = self.actor(owner).call_op(Operation::Posting,move |s| {
            s.leaves.get(&leaf).map(|v| {
                v.ids
                    .iter()
                    .copied()
                    .zip(v.versions.iter().copied())
                    .collect::<Vec<_>>()
            })
        })?;
        #[cfg(test)]
        if self.scrub_batch_policy.load(Ordering::Relaxed)!=0 {
            if let Some((captured,resume))=self.scrub_pause.lock().take(){captured.send(()).unwrap();resume.recv().unwrap();}
        }
        let valid: Vec<_> = if self.scrub_batch_policy.load(Ordering::Relaxed)!=0 {
            rows.chunks(64).flat_map(|v|self.validate_scrub_batch(v)).collect()
        } else {
            rows.iter().map(|(id,v)|self.current(*id)==Some(*v)).collect()
        };
        self.actor(owner).call_op(Operation::Posting,move |s| {
            let leaf = s.leaves.get_mut(&leaf)?;
            let old = leaf.ids.len();
            let mut i = 0;
            let mut flags = valid;
            while i < leaf.ids.len() {
                if flags[i] {
                    i += 1;
                } else {
                    leaf.ids.swap_remove(i);
                    leaf.versions.swap_remove(i);
                    flags.swap_remove(i);
                    let last = leaf.codes.len() / code_size - 1;
                    if i != last {
                        leaf.codes
                            .copy_within(last * code_size..(last + 1) * code_size, i * code_size);
                    }
                    leaf.codes.truncate(last * code_size);
                }
            }
            leaf.length = leaf.ids.len();
            s.physical -= old - leaf.length;
            Some(leaf.length)
        })
    }
    pub(super) fn prepare_predicate(
        &self,
        metric: DistanceFunction,
        centroid: &[f32],
        old: &[f32],
        left: &[f32],
        right: &[f32],
    ) -> Arc<Predicate> {
        self.predicate_center_copy_bytes.fetch_add(
            ((centroid.len() + old.len() + left.len() + right.len()) * 4) as u64,
            Ordering::Relaxed,
        );
        Arc::new(Predicate {
            metric,
            centroid: centroid.to_vec(),
            old: old.to_vec(),
            left: left.to_vec(),
            right: right.to_vec(),
        })
    }
    pub(super) fn predicate_batching_enabled(&self) -> bool { self.predicate_batching.load(Ordering::Relaxed) != 0 }
    /// Computes only immutable distance predicates. Version reads, deduplication
    /// and immediate reassignment stay in the caller at their original positions.
    pub(super) fn predicate_batch(
        &self,
        leaf: u32,
        values: Vec<Option<Arc<[f32]>>>,
        predicate: Arc<Predicate>,
        own_cluster: bool,
    ) -> Option<Vec<Option<bool>>> {
        if self.predicate_batching.load(Ordering::Relaxed) == 0 {
            return None;
        }
        assert!(values.len() <= 64, "predicate batch capacity");
        if values.is_empty() { return Some(vec![]); }
        let owner = *self.leaf_owner.get(&leaf).expect("predicate owner missing");
        self.predicate_batch_callbacks.fetch_add(1, Ordering::Relaxed);
        self.predicate_batch_rows.fetch_add(values.iter().filter(|v|v.is_some()).count() as u64, Ordering::Relaxed);
        Some(self.actor(owner).call_op(Operation::Predicate,move |_s| values.into_iter().map(|v| v.map(|v| {
            if own_cluster {
                let old = predicate.metric.distance(&v, &predicate.old);
                let new = predicate.metric.distance(&v, &predicate.left);
                new > old
            } else {
                super::eligibility::fp_eligible(&predicate.metric, &v, &predicate.centroid, &predicate.old, &predicate.left, &predicate.right)
            }
        })).collect()))
    }
    pub(super) fn fp_predicate(
        &self,
        leaf: u32,
        values: Arc<[f32]>,
        predicate: Arc<Predicate>,
    ) -> bool {
        let owner = *self.leaf_owner.get(&leaf).expect("predicate owner missing");
        self.predicate_calls.fetch_add(1, Ordering::Relaxed);
        self.actor(owner).call_op(Operation::Predicate,move |_s| {
            super::eligibility::fp_eligible(
                &predicate.metric,
                &values,
                &predicate.centroid,
                &predicate.old,
                &predicate.left,
                &predicate.right,
            )
        })
    }
    pub(super) fn self_predicate(
        &self,
        leaf: u32,
        values: Arc<[f32]>,
        predicate: Arc<Predicate>,
    ) -> bool {
        let owner = *self.leaf_owner.get(&leaf).expect("predicate owner missing");
        self.predicate_calls.fetch_add(1, Ordering::Relaxed);
        self.actor(owner).call_op(Operation::Predicate,move |_s| {
            let old = predicate.metric.distance(&values, &predicate.old);
            let new = predicate.metric.distance(&values, &predicate.left);
            new > old
        })
    }
    pub(super) fn record_orphans(&self, count: usize) {
        self.orphan_adoptions
            .fetch_add(count as u64, Ordering::Relaxed);
    }
    pub(super) fn record_parent_edit(&self) {
        self.parent_edits.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn record_schedule(&self, assignments: &[Vec<u32>]) {
        self.schedule_rounds.fetch_add(1, Ordering::Relaxed);
        if assignments.iter().map(Vec::len).sum::<usize>() <= 256 {
            *self.last_schedule.lock() = assignments.to_vec();
        } else {
            self.schedule_sample_overflow.store(true, Ordering::Relaxed);
        }
    }
    pub(super) fn subtree_span(&self) -> WorkSpan {
        WorkSpan::new(&self.subtrees)
    }
    pub(super) fn critical_span(&self, start: Instant) -> CriticalSpan {
        self.critical_wait_ns
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let outer = CRITICAL_DEPTH.with(|v| {
            let mut v = v.borrow_mut();
            let d = v.entry(self.key).or_default();
            let outer = *d == 0;
            *d += 1;
            outer
        });
        CriticalSpan {
            key: self.key,
            span: outer.then(|| WorkSpan::new(&self.critical)),
        }
    }
    pub(super) fn noncritical_span(&self) -> WorkSpan {
        WorkSpan::new(&self.noncritical)
    }
    pub(super) fn navigation_span(&self) -> WorkSpan {
        WorkSpan::new(&self.add_navigation)
    }
    pub(super) fn balance_span(&self) -> WorkSpan {
        WorkSpan::new(&self.balance_leaf)
    }
}
impl HierarchicalSpannWriter {
    pub fn construction_shape(&self)->serde_json::Value {
        let mut pending=vec![(self.root_id(),0usize)];let mut seen=std::collections::HashSet::new();let mut depth=0;let mut occupancy=vec![];let mut children=vec![];
        while let Some((id,d))=pending.pop(){assert!(seen.insert(id),"shape cycle");depth=depth.max(d);let node=self.nodes.get(&id).expect("shape missing");match node.value(){TreeNode::Leaf(v)=>occupancy.push(v.length),TreeNode::Internal(v)=>{children.push(v.children.len());for child in &v.children {pending.push((*child,d+1));}}}}
        serde_json::json!({"nodes":self.nodes.len(),"reachable_nodes":seen.len(),"depth":depth,"leaf_physical_occupancy":occupancy,"internal_child_counts":children,"pending_balance":self.pending_balance_leaves()})
    }
    pub(super) fn install_reference_node(&self, id: u32, node: TreeNode) -> Option<TreeNode> {
        let node = match (&self.owner_backend, node) {
            (Some(b), TreeNode::Leaf(leaf)) => {
                // Reference leaf installation uses fresh IDs or IDs already
                // removed from nodes. Reject a live overwrite before owner data
                // changes, so a held scrub guard protects the owner payload too.
                if b.scrub_batch_policy.load(Ordering::Relaxed)!=0 {
                    assert!(!self.nodes.contains_key(&id),"batched scrub cannot overwrite a live leaf");
                }
                TreeNode::Leaf(b.install(id, leaf))
            },
            (_, n) => n,
        };
        self.nodes.insert(id, node)
    }
    pub(super) fn take_reference_node(&self, id: u32) -> Option<(u32, TreeNode)> {
        self.nodes.remove(&id).map(|(id, node)| {
            let node = match (&self.owner_backend, node) {
                (Some(b), TreeNode::Leaf(leaf)) => TreeNode::Leaf(b.take(id, leaf)),
                (_, n) => n,
            };
            (id, node)
        })
    }
}

/// This wrapper admits new IDs only and exposes joined construction phases.
/// The underlying reference kernel is private so callers cannot bypass ownership.
pub struct ConcurrentOwnerWriter {
    kernel: HierarchicalSpannWriter,
    max_rows: usize,
    failed:bool,
    next: u32,
    ready: bool,
    phase_reports: Vec<serde_json::Value>,
    insertion_remaining: usize,
    insertion_workers: usize,
    phase_count: u64,
    add_seconds_total: f64,
    balance_seconds_total: f64,
    publication:Mutex<PublicationState>,
    publication_busy:std::sync::atomic::AtomicBool,
    #[cfg(test)] publication_fail_stage:AtomicUsize,
}
#[derive(Default)]
struct PublicationState{roots:Option<super::HierarchicalSpannIds>,generation:u64,noops:u64,last:serde_json::Value}
struct PublicationGuard<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for PublicationGuard<'_>{fn drop(&mut self){self.0.store(false,Ordering::Release);}}
impl ConcurrentOwnerWriter {
    pub fn new(
        dim: usize,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        owners: usize,
        max_rows: usize,
    ) -> Result<Self> {
        ensure!(
            owners > 0 && owners <= 8 && max_rows > 0 && max_rows < u32::MAX as usize && dim > 0 && dim <= 768,
            "owner adapter capacity"
        );
        ensure!(
            config.write_navigation == super::super::config::NavigationMode::Fp
                && config.fp_npa
                && config.max_replicas == 1,
            "owner adapter requires explicit FP and one current copy"
        );
        let mut kernel = HierarchicalSpannWriter::new(dim, metric, config);
        let backend = Arc::new(Backend::with_authority_capacity(owners, max_rows));
        let (_, node) = kernel.nodes.remove(&0).unwrap();
        let TreeNode::Leaf(leaf) = node else {
            unreachable!()
        };
        kernel
            .nodes
            .insert(0, TreeNode::Leaf(backend.install(0, leaf)));
        kernel.owner_backend = Some(backend);
        Ok(Self {
            kernel,
            max_rows,
            failed:false,
            next: 0,
            ready: true,
            phase_reports: vec![],
            insertion_remaining:0,insertion_workers:0,phase_count:0,add_seconds_total:0.,balance_seconds_total:0.,
            publication:Mutex::new(PublicationState::default()),publication_busy:std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)] publication_fail_stage:AtomicUsize::new(0),
        })
    }
    pub fn enable_bounded_vectors(&mut self,provider:chroma_blockstore::provider::BlockfileProvider,limits:super::owner_vector_store::VectorLimits)->Result<()> {
        ensure!(self.ready && self.next==0 && self.kernel.embeddings.is_empty() && self.kernel.live_owner_hook.lock().is_none(),"bounded vectors require empty append-only writer");
        let backend=self.kernel.owner_backend.as_ref().unwrap();ensure!(backend.vector_store().is_none(),"vector policy already configured");ensure!(self.kernel.config.split_threshold<=limits.split_rows,"split trigger exceeds admitted tuple capacity");
        *backend.vector_store.lock()=Some(Arc::new(super::owner_vector_store::OwnerVectorStore::new(provider,self.kernel.dim,backend.actors.len(),limits)?));Ok(())
    }
    /// The opt-in policy changes pure predicate dispatch only at a joined boundary.
    pub fn set_predicate_batching(&mut self, enabled: bool) -> Result<()> {
        ensure!(enabled || !self.kernel.owner_backend.as_ref().unwrap().live_prefix_enabled(),"live prefix requires predicate batching");
        ensure!(self.ready, "predicate policy requires joined phase");
        self.kernel.owner_backend.as_ref().unwrap().predicate_batching.store(usize::from(enabled), Ordering::Relaxed);
        Ok(())
    }
    pub fn set_construction_observation(&mut self, enabled:bool)->Result<()> {
        ensure!(self.ready,"observation requires joined phase");
        self.kernel.stats.observe_current_versions.store(enabled,Ordering::Relaxed);
        self.kernel.owner_backend.as_ref().unwrap().message_timing.enabled.store(enabled,Ordering::Relaxed);Ok(())
    }
    pub fn construction_counts(&self)->serde_json::Value {let mut value=self.kernel.stats.construction_counts();value["kernel_current_version_requests"]=value["current_version_requests"].clone();value["current_version_requests"]=serde_json::json!(self.kernel.owner_backend.as_ref().unwrap().current_requests.load(Ordering::Relaxed));value}
    pub fn message_observation(&self)->serde_json::Value {self.kernel.owner_backend.as_ref().unwrap().message_timing.snapshot()}
    /// Enables a process-local synchronized read of the owner's original map.
    /// All mutations still execute on that owner; placement remains joined.
    pub fn set_direct_current_reads(&mut self, enabled:bool)->Result<()> {
        ensure!(self.ready,"current read policy requires joined phase");
        ensure!(!enabled || (!self.kernel.owner_backend.as_ref().unwrap().live_prefix_enabled() && self.kernel.owner_backend.as_ref().unwrap().split_input_policy.load(Ordering::Relaxed)==0),"batched version policies require actor reads");
        self.kernel.owner_backend.as_ref().unwrap().direct_current_policy.store(usize::from(enabled),Ordering::Relaxed);Ok(())
    }
    pub fn set_batched_split_input_validation(&mut self,enabled:bool)->Result<()> {
        ensure!(self.ready,"split input policy requires joined phase");let b=self.kernel.owner_backend.as_ref().unwrap();
        ensure!(!enabled || b.direct_current_policy.load(Ordering::Relaxed)==0,"split input batching requires actor current reads");
        b.split_input_policy.store(usize::from(enabled),Ordering::Relaxed);Ok(())
    }
    pub fn set_live_prefix_validation(&mut self,enabled:bool)->Result<()> {
        ensure!(self.ready,"live prefix requires joined phase");let backend=self.kernel.owner_backend.as_ref().unwrap();
        ensure!(!enabled || (backend.predicate_batching_enabled() && backend.direct_current_policy.load(Ordering::Relaxed)==0 && self.kernel.live_owner_hook.lock().is_none()),"live prefix requires immutable predicate batching, actor reads and no live trace hook");
        backend.live_prefix_policy.store(usize::from(enabled),Ordering::Relaxed);Ok(())
    }
    pub fn set_batched_scrub_validation(&mut self, enabled:bool)->Result<()> {
        ensure!(self.ready,"scrub policy requires joined phase");
        self.kernel.owner_backend.as_ref().unwrap().scrub_batch_policy.store(usize::from(enabled),Ordering::Relaxed);Ok(())
    }
    pub fn configure_phase(&mut self, config: HierarchicalSpannConfig) -> Result<()> {
        ensure!(
            self.ready
                && config.write_navigation == super::super::config::NavigationMode::Fp
                && config.fp_npa
                && config.max_replicas == 1
                && config.branching_factor >= 2
                && config.merge_threshold < config.split_threshold,
            "phase configuration differs"
        );
        if let Some(store)=self.kernel.owner_backend.as_ref().unwrap().vector_store(){ensure!(config.split_threshold<=store.limits.split_rows,"split trigger exceeds tuple capacity");}
        self.kernel.config = config;
        Ok(())
    }
    /// Capacity is an explicit physical-row limit, including stale copies.
    pub fn set_physical_capacity(&mut self, rows:usize)->Result<()> {
        ensure!(self.ready && self.next==0 && rows>0 && rows<u32::MAX as usize,"physical capacity requires empty writer");
        self.kernel.owner_backend.as_ref().unwrap().physical_capacity.store(rows,Ordering::Relaxed);Ok(())
    }
    /// Reserve the whole logical cut before any chunk becomes authoritative.
    pub fn begin_add_phase(&mut self, rows:usize,workers:usize)->Result<()> {
        ensure!(self.ready && !self.failed && rows>0 && workers>0 && workers<=8 && (self.next as usize).checked_add(rows).is_some_and(|n|n<=self.max_rows),"add phase capacity/state");
        let b=self.kernel.owner_backend.as_ref().unwrap();
        if let Some(store)=b.vector_store(){ensure!(workers<=store.limits.workers && self.kernel.live_owner_hook.lock().is_none(),"bounded append workers/trace");}
        let reserve=rows.checked_mul(2).ok_or_else(||anyhow::anyhow!("physical reservation overflow"))?;
        let capacity=b.physical_capacity.load(Ordering::Relaxed);
        for actor in &b.actors {ensure!(actor.call(|s|s.physical).checked_add(reserve).is_some_and(|n|n<=capacity),"owner physical reservation exhausted");}
        self.ready=false;self.insertion_remaining=rows;self.insertion_workers=workers;self.phase_count+=1;
        if self.phase_reports.len()==32{self.phase_reports.remove(0);}
        self.phase_reports.push(serde_json::json!({"phase":self.phase_count,"rows_through":self.next,"reserved_rows":rows,"storage_chunks":0,"add_seconds":0.,"add_workers":workers}));Ok(())
    }
    /// Join each storage chunk without balancing or moving ownership between chunks.
    pub fn add_chunk(&mut self,rows:&[(u32,Vec<f32>)])->Result<()> {
        ensure!(!self.ready && !self.failed && !rows.is_empty() && rows.len()<=self.insertion_remaining,"append chunk state/extent");
        let b=self.kernel.owner_backend.as_ref().unwrap();
        if let Some(store)=b.vector_store(){ensure!(rows.len()<=store.limits.append_rows,"append chunk capacity");}
        ensure!(rows.iter().enumerate().all(|(i,(id,v))|*id==self.next+i as u32 && v.len()==self.kernel.dim && v.iter().all(|x|x.is_finite())),"new dense IDs/vector shape");
        let started=Instant::now();
        if let Some(store)=b.vector_store(){if let Err(e)=store.stage(rows){self.failed=true;return Err(e);}}
        let size=rows.len().div_ceil(self.insertion_workers);
        let outcome=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||std::thread::scope(|scope|{
            for rows in rows.chunks(size){let kernel=&self.kernel;scope.spawn(move||for (id,v) in rows{kernel.add(*id,v);});}
        })));
        if outcome.is_err(){self.failed=true;return Err(anyhow::anyhow!("append phase failed; writer cannot resume or publish"));}
        if let Some(store)=b.vector_store(){store.finish_stage();assert!(self.kernel.embeddings.is_empty(),"no full-vector mirror");}
        self.next+=rows.len() as u32;self.insertion_remaining-=rows.len();let elapsed=started.elapsed().as_secs_f64();self.add_seconds_total+=elapsed;
        let phase=self.phase_reports.last_mut().unwrap();phase["rows_through"]=self.next.into();phase["storage_chunks"]=(phase["storage_chunks"].as_u64().unwrap()+1).into();phase["add_seconds"]=(phase["add_seconds"].as_f64().unwrap()+elapsed).into();Ok(())
    }
    pub fn add_phase(&mut self,rows:&[(u32,Vec<f32>)],workers:usize)->Result<()> {
        ensure!(!rows.is_empty() && rows.iter().enumerate().all(|(i,(id,v))|*id==self.next+i as u32 && v.len()==self.kernel.dim && v.iter().all(|x|x.is_finite())),"new dense IDs/vector shape");
        self.begin_add_phase(rows.len(),workers)?;
        let chunk=self.kernel.owner_backend.as_ref().unwrap().vector_store().map(|s|s.limits.append_rows).unwrap_or(rows.len());
        for rows in rows.chunks(chunk){self.add_chunk(rows)?;}Ok(())
    }
    pub fn balance_phase(&mut self, workers: usize) -> Result<()> {
        ensure!(
            !self.ready && !self.failed && self.insertion_remaining==0 && workers > 0 && workers <= 8,
            "balance phase state"
        );
        if let Some(store)=self.kernel.owner_backend.as_ref().unwrap().vector_store(){ensure!(workers<=store.limits.workers,"bounded balance workers");}
        let started = Instant::now();
        let work=||self.kernel.balance_index_parallel(workers);let outcome=if self.kernel.bounded_vectors(){std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))}else{work();Ok(())};
        if outcome.is_err(){self.failed=true;return Err(anyhow::anyhow!("balance failed; writer cannot resume or publish"));}
        let elapsed=started.elapsed().as_secs_f64();self.balance_seconds_total+=elapsed;
        let phase = self.phase_reports.last_mut().unwrap();
        phase["balance_seconds"] = serde_json::json!(elapsed);
        phase["balance_workers"] = serde_json::json!(workers);
        phase["pending_balance"] = serde_json::json!(self.kernel.pending_balance_leaves());
        self.ready = true;
        Ok(())
    }
    /// Moves only existing leaf payload and its current-version authority. No
    /// centroid, ID, posting row or full-vector bytes are changed by placement.
    pub fn reshard(&mut self, move_above: usize) -> Result<usize> {
        ensure!(
            self.ready && move_above > 0,
            "reshard requires joined phase"
        );
        let b = self.kernel.owner_backend.as_ref().unwrap();
        let mut loads = vec![0usize; b.actors.len()];
        let leaves: Vec<_> = self
            .kernel
            .nodes
            .iter()
            .filter_map(|v| {
                if matches!(v.value(), TreeNode::Leaf(_)) {
                    Some(*v.key())
                } else {
                    None
                }
            })
            .collect();
        for id in &leaves {
            let owner = *b.leaf_owner.get(id).unwrap();
            let l = b.snapshot(*id).unwrap();
            loads[owner] += l
                .ids
                .iter()
                .zip(&l.versions)
                .filter(|(id, v)| b.current(**id) == Some(**v))
                .count();
        }
        let mut moved = 0;
        for leaf in leaves {
            let source = *b.leaf_owner.get(&leaf).unwrap();
            if loads[source] <= move_above {
                continue;
            }
            let dest = (0..loads.len()).min_by_key(|i| loads[*i]).unwrap();
            if dest == source {
                continue;
            }
            let capacity=b.physical_capacity.load(Ordering::Relaxed);
            let source_rows=b.actor(source).call(move |s|s.leaves.get(&leaf).expect("move leaf missing").ids.len());
            ensure!(b.actor(dest).call(|s|s.physical).checked_add(source_rows).is_some_and(|n|n<=capacity),"destination physical reservation exhausted");
            let payload = b.actor(source).call(move |s| {
                let v = s.leaves.remove(&leaf).expect("move leaf missing");
                s.physical -= v.ids.len();
                v
            });
            let mut current = vec![];
            for (&id, &version) in payload.ids.iter().zip(&payload.versions) {
                let owner = b.authority.get(&id).map(|v| *v);
                if owner == Some(source) && b.current(id) == Some(version) {
                    let value = b
                        .actor(source)
                        .call(move |s| (s.current.write().remove(&id).unwrap(),s.vectors.remove(&id)));
                    current.push((id, value));
                }
            }
            b.moved_posting_payload_bytes.fetch_add(
                (payload.ids.len() * 5
                    + payload.codes.len()
                    + payload.centroid.len() * 4
                    + payload.centroid_code.len()) as u64,
                Ordering::Relaxed,
            );
            let count = current.len();
            for (id, _) in &current {
                b.authority.insert(*id, dest);
            }
            let capacity=b.physical_capacity.load(Ordering::Relaxed);
            b.actor(dest).call(move |s| {
                s.physical += payload.ids.len();
                assert!(s.physical <= capacity, "destination physical capacity");
                assert!(s.leaves.insert(leaf, payload).is_none());
                for (id, (version,handle)) in current {
                    assert!(s.current.write().insert(id, version).is_none());if let Some(handle)=handle{assert!(s.vectors.insert(id,handle).is_none());}
                }
            });
            b.leaf_owner.insert(leaf, dest);
            loads[source] -= count;
            loads[dest] += count;
            moved += 1;
            b.moves.fetch_add(1, Ordering::Relaxed);
        }
        Ok(moved)
    }
    /// Return one complete durable root tuple from a joined construction cut.
    /// Authoritative dirty marks survive commit/flush errors; no partial tuple escapes.
    pub async fn save(&self,provider:&chroma_blockstore::provider::BlockfileProvider)->Result<super::HierarchicalSpannIds>{
        ensure!(self.ready && !self.failed,"save requires joined balance");
        ensure!(self.publication_busy.compare_exchange(false,true,Ordering::Acquire,Ordering::Relaxed).is_ok(),"publication already active");let _guard=PublicationGuard(&self.publication_busy);
        let started=Instant::now();let b=self.kernel.owner_backend.as_ref().unwrap();let parent=self.publication.lock().roots.clone();
        let vector_parent=if let Some(store)=b.vector_store(){Some(store.latest()?)}else{None};
        let dirty_versions=b.publication_has_dirty_versions();
        if let Some(ids)=&parent{if self.kernel.dirty_nodes.is_empty() && self.kernel.tombstones.is_empty() && self.kernel.dirty_versions.is_empty() && !dirty_versions && self.kernel.dirty_embeddings.is_empty() && self.kernel.dirty_deleted_embeddings.is_empty() && vector_parent.is_none_or(|root|root==ids.vector_data_id){self.publication.lock().noops+=1;return Ok(ids.clone());}}
        let flusher=self.kernel.commit_owner_delta(provider,parent.as_ref(),vector_parent,OWNER_PUBLICATION_LEAF_BYTES,self.next.checked_sub(1)).await.map_err(|e|anyhow::anyhow!("owner delta commit: {e}"))?;
        let metrics=flusher.owner_delta.clone();
        #[cfg(test)] let fail=self.publication_fail_stage.swap(0,Ordering::Relaxed);
        #[cfg(not(test))] let fail=0;
        let roots=flusher.flush_owner(fail).await.map_err(|e|anyhow::anyhow!("owner delta flush: {e}"))?;
        // Everything below is infallible and runs before the complete tuple is returned.
        // Existing content handles remain pinned to their historical immutable roots.
        if let Some(store)=b.vector_store(){store.acknowledge_published_root(roots.vector_data_id);store.publication_completed(started.elapsed().as_nanos() as u64);}
        b.acknowledge_publication();self.kernel.tombstones.clear();self.kernel.dirty_nodes.clear();self.kernel.dirty_versions.clear();self.kernel.dirty_embeddings.clear();self.kernel.dirty_deleted_embeddings.clear();
        let mut publication=self.publication.lock();publication.generation+=1;publication.roots=Some(roots.clone());publication.last=serde_json::json!({"metrics":metrics,"elapsed_ns":started.elapsed().as_nanos() as u64,"scope":"Changed-key metadata and one admitted owner-leaf clone at a time. Blockfile writer dirty buffers/provider/root indexes and global routing remain outside that payload bound; no physicalIO/RSS claim."});Ok(roots)
    }
    #[cfg(test)]
    async fn save_full_oracle(
        &self,
        provider: &chroma_blockstore::provider::BlockfileProvider,
    ) -> Result<super::HierarchicalSpannIds> {
        ensure!(self.ready, "save requires joined balance");
        let publication_started=Instant::now();
        let b = self.kernel.owner_backend.as_ref().unwrap();
        let mut snapshot = HierarchicalSpannWriter::new(
            self.kernel.dim,
            self.kernel.distance_fn.clone(),
            self.kernel.config.clone(),
        );
        snapshot.nodes.clear();
        for entry in self.kernel.nodes.iter() {
            let id = *entry.key();
            let node = match entry.value() {
                TreeNode::Leaf(meta) => {
                    let mut leaf = b.snapshot(id).expect("saved leaf missing");
                    leaf.centroid = meta.centroid.clone();
                    leaf.centroid_code = meta.centroid_code.clone();
                    leaf.parent_id = meta.parent_id;
                    TreeNode::Leaf(leaf)
                }
                TreeNode::Internal(node) => {
                    TreeNode::Internal(super::super::common::InternalNode {
                        centroid: node.centroid.clone(),
                        centroid_code: node.centroid_code.clone(),
                        children: node.children.clone(),
                        parent_id: node.parent_id,
                    })
                }
            };
            snapshot.nodes.insert(id, node);
            snapshot.mark_node_dirty(id);
        }
        snapshot
            .root_id
            .store(self.kernel.root_id(), Ordering::Relaxed);
        snapshot.next_node_id.store(
            self.kernel.next_node_id.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        for actor in &b.actors {
            let versions = actor.call(|s| s.current.read().clone());
            for (id, v) in versions {
                ensure!(
                    snapshot.versions.insert(id, v).is_none(),
                    "duplicate owner current authority"
                );
                snapshot.mark_version_dirty(id);
            }
        }
        if let Some(store)=b.vector_store(){ensure!(self.kernel.embeddings.is_empty(),"no central vectors at save");snapshot.vector_base_root=Some(store.latest()?);}
        for value in self.kernel.embeddings.iter() {
            snapshot
                .embeddings
                .insert(*value.key(), value.value().clone());
            snapshot.mark_embedding_dirty(*value.key());
        }
        let flushed = snapshot
            .commit(provider, None)
            .await
            .map_err(|e| anyhow::anyhow!("snapshot commit: {e}"))?
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("snapshot flush: {e}"))?;
        Ok(flushed)
    }
    pub fn report(&self) -> serde_json::Value {
        let b = self.kernel.owner_backend.as_ref().unwrap();
        let mut owners = vec![];
        for (id, actor) in b.actors.iter().enumerate() {
            let counts = actor.call(|s| {
                assert_eq!(
                    s.physical,
                    s.leaves.values().map(|v| v.ids.len()).sum::<usize>()
                );
                (s.leaves.len(), s.current.read().len(), s.physical)
            });
            owners.push(serde_json::json!({"owner":id,"leaves":counts.0,"current":counts.1,"physical":counts.2}));
        }
        let mut stack = vec![(self.kernel.root_id(), 0usize)];
        let mut depths = BTreeMap::new();
        let mut occupancy = vec![];
        let mut internal_children = vec![];
        while let Some((id, depth)) = stack.pop() {
            assert!(
                depths.insert(id, depth).is_none(),
                "routing cycle/duplicate"
            );
            let node = self.kernel.nodes.get(&id).expect("missing reachable node");
            match node.value() {
                TreeNode::Leaf(leaf) => occupancy.push(leaf.length),
                TreeNode::Internal(inner) => {
                    internal_children.push(inner.children.len());
                    for child in &inner.children {
                        let c = self.kernel.nodes.get(child).expect("missing child");
                        assert_eq!(
                            match c.value() {
                                TreeNode::Leaf(v) => v.parent_id,
                                TreeNode::Internal(v) => v.parent_id,
                            },
                            Some(id)
                        );
                        stack.push((*child, depth + 1));
                    }
                }
            }
        }
        assert_eq!(
            depths.len(),
            self.kernel.nodes.len(),
            "unreachable routing node"
        );
        let mut report = serde_json::json!({"depth":depths.values().max(),"leaf_physical_occupancy":occupancy,"internal_child_counts":internal_children,"rows":self.next,"phases":self.phase_reports,"phase_count":self.phase_count,"phase_reports_retained":self.phase_reports.len(),"phase_reports_dropped":self.phase_count.saturating_sub(self.phase_reports.len() as u64),"add_seconds_total":self.add_seconds_total,"balance_seconds_total":self.balance_seconds_total,"physical_capacity_per_owner":b.physical_capacity.load(Ordering::Relaxed),"owners":owners,"nodes":self.kernel.nodes.len(),"pending_balance":self.kernel.pending_balance_leaves(),"scheduler_rounds":b.schedule_rounds.load(Ordering::Relaxed),"last_reference_subtree_assignments":*b.last_schedule.lock(),"schedule_sample_overflow":b.schedule_sample_overflow.load(Ordering::Relaxed),"actual_subtree_jobs":b.subtrees.calls.load(Ordering::Relaxed)});
        report.as_object_mut().unwrap().extend(serde_json::json!({"peak_distinct_subtree_workers":b.subtrees.peak.load(Ordering::Relaxed),"reference_splits":self.kernel.stats.splits.load(Ordering::Relaxed),"reference_merges":self.kernel.stats.merges.load(Ordering::Relaxed),"reference_balance_rounds":self.kernel.stats.balance_rounds.load(Ordering::Relaxed),"orphan_adoptions":b.orphan_adoptions.load(Ordering::Relaxed),"parent_edits":b.parent_edits.load(Ordering::Relaxed),"reference_missing_retries":self.kernel.stats.add_missing_nodes.load(Ordering::Relaxed),"central_mutable_posting_rows":self.kernel.nodes.iter().map(|v|match v.value(){TreeNode::Leaf(v)=>v.ids.len(),_=>0}).sum::<usize>(),"central_current_versions":self.kernel.versions.len(),"add_navigation_calls":b.add_navigation.calls.load(Ordering::Relaxed),"peak_add_navigation_spans":b.add_navigation.peak.load(Ordering::Relaxed),"balance_leaf_calls":b.balance_leaf.calls.load(Ordering::Relaxed)}).as_object().unwrap().clone());
        report.as_object_mut().unwrap().extend(serde_json::json!({"peak_balance_leaf_spans":b.balance_leaf.peak.load(Ordering::Relaxed),"registrations":b.registrations.load(Ordering::Relaxed),"peak_actual_owner_callbacks":b.actor_work.peak.load(Ordering::Relaxed),"critical_outer_entries":b.critical.calls.load(Ordering::Relaxed),"critical_outer_hold_ns":b.critical.nanos.load(Ordering::Relaxed),"critical_wait_ns":b.critical_wait_ns.load(Ordering::Relaxed),"peak_distinct_critical_threads":b.critical.peak.load(Ordering::Relaxed),"noncritical_npa_entries":b.noncritical.calls.load(Ordering::Relaxed),"noncritical_npa_inclusive_ns":b.noncritical.nanos.load(Ordering::Relaxed),"peak_npa_contexts":b.noncritical.peak.load(Ordering::Relaxed),"predicate_centers_copied_once_per_scope_bytes":b.predicate_center_copy_bytes.load(Ordering::Relaxed),"registration_vector_copy_bytes":b.registration_vector_copy_bytes.load(Ordering::Relaxed)}).as_object().unwrap().clone());
        report["phase_failed"]=serde_json::json!(self.failed);
        {let publication=self.publication.lock();report["incremental_publication"]=serde_json::json!({"generation":publication.generation,"no_op_reuses":publication.noops,"last":publication.last,"leaf_owned_payload_admission_bytes":OWNER_PUBLICATION_LEAF_BYTES});}
        report["bounded_vector_store"]=b.vector_store().map(|s|s.report()).unwrap_or(serde_json::Value::Null);
        report.as_object_mut().unwrap().extend(serde_json::json!({"owner_snapshot_copy_bytes":b.snapshot_copy_bytes.load(Ordering::Relaxed),"moved_posting_logical_payload_bytes":b.moved_posting_payload_bytes.load(Ordering::Relaxed),"shared_embedding_cache_logical_bytes":self.kernel.embeddings.iter().map(|v|v.value().len()*4).sum::<usize>(),"owner_predicates":b.predicate_calls.load(Ordering::Relaxed),"predicate_batch_callbacks":b.predicate_batch_callbacks.load(Ordering::Relaxed),"predicate_batch_computed_rows":b.predicate_batch_rows.load(Ordering::Relaxed),"actor_callbacks":b.actor_work.calls.load(Ordering::Relaxed),"live_version_requests":b.current_requests.load(Ordering::Relaxed),"direct_current_reads":b.direct_current_reads.load(Ordering::Relaxed),"direct_current_policy":b.direct_current_policy.load(Ordering::Relaxed)!=0,"predicate_batching":b.predicate_batching_enabled(),"placement_moves":b.moves.load(Ordering::Relaxed),"scope":"Actual local owner actors; reference kernel routing/scheduler and existing structural lock. Distinct-thread spans de-duplicate recursive entries; NPA elapsed includes nested critical work and waits, not CPU. Critical hold spans count outermost same-thread lock ownership only. Shared immutable embedding cache remains central/local. Publication streams changed nodes and owner versions, while global changed-key metadata and blockfile writer/provider buffers remain. No cross-host claim."}).as_object().unwrap().clone());
        report["scrub_validation_batches"]=serde_json::json!(b.scrub_validation_batches.load(Ordering::Relaxed));
        report["scrub_validation_request_rows"]=serde_json::json!(b.scrub_validation_request_rows.load(Ordering::Relaxed));
        report["scrub_validation_rows"]=serde_json::json!(b.scrub_validation_rows.load(Ordering::Relaxed));
        report["scrub_validation_owner_calls"]=serde_json::json!(b.scrub_validation_owner_calls.load(Ordering::Relaxed));
        report["batched_scrub_validation"]=serde_json::json!(b.scrub_batch_policy.load(Ordering::Relaxed)!=0);
        let lengths=b.live_prefix_lengths.lock();
        report["split_input_validation"]=serde_json::json!({"enabled":b.split_input_policy.load(Ordering::Relaxed)!=0,"physical_rows":b.split_input_rows.load(Ordering::Relaxed),"retained_rows":b.split_input_retained.load(Ordering::Relaxed),"scalar_current_calls":b.split_input_scalar_calls.load(Ordering::Relaxed),"batches":b.split_input_batches.load(Ordering::Relaxed),"owner_calls":b.split_input_owner_calls.load(Ordering::Relaxed),"owner_requested_rows":b.split_input_owner_rows.load(Ordering::Relaxed),"batch_lengths":b.split_input_lengths.lock().to_vec(),"scope":"Only split-input current checks; at most64 rows per grouped window under tree_lock and sorted ID locks. Original physical order/missing-to-zero equality; no observations reused across batches. Logical payload estimates exclude transport; callback sums overlap. Kernel current counter counts logical rows here, not scalar function entries."});
        report["live_prefix_validation"]=serde_json::json!({"enabled":b.live_prefix_enabled(),"batches":b.live_prefix_batches.load(Ordering::Relaxed),"requested_rows":b.live_prefix_rows.load(Ordering::Relaxed),"consumed_rows":b.live_prefix_consumed.load(Ordering::Relaxed),"actions":b.live_prefix_actions.load(Ordering::Relaxed),"owner_calls":b.live_prefix_owner_calls.load(Ordering::Relaxed),"owner_requested_rows":b.live_prefix_request_rows.load(Ordering::Relaxed),"requested_lengths":lengths.0.to_vec(),"consumed_lengths":lengths.1.to_vec(),"scope":"Bounded64-ID authoritative reads under sorted per-ID locks; release before each reassignment and resample suffix. Request rows can exceed consumed rows; no cached versions or direct pointer reads. Logical request/reply payload only, not physicalnetwork."});
        report["message_timing"]=b.message_timing.snapshot();
        if b.vector_store().is_some(){report["scope"]=serde_json::json!("Local owner authority with bounded immutable vector fetch/cache and changed-key publication. Reference global routing, u32 ID directories and structural lock remain. Sorted changed-key metadata and blockfile writer/provider buffers are not covered by the one-leaf copy admission. Provider cache policy is caller-supplied and excluded from owned-vector cache capacity; no total RSS or cross-host claim.");}
        report
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::reader::{AuditLimits, HierarchicalSpannReader};
    use super::*;
    fn provider(path: &std::path::Path) -> chroma_blockstore::provider::BlockfileProvider {
        use chroma_blockstore::arrow::{
            config::BlockManagerConfig, provider::ArrowBlockfileProvider,
        };
        use chroma_cache::new_cache_for_test;
        use chroma_storage::{local::LocalStorage, Storage};
        chroma_blockstore::provider::BlockfileProvider::ArrowBlockfileProvider(
            ArrowBlockfileProvider::new(
                Storage::Local(LocalStorage::new(path.to_str().unwrap())),
                BlockManagerConfig::default().max_block_size_bytes,
                new_cache_for_test(),
                new_cache_for_test(),
                2,
            ),
        )
    }
    async fn reader(
        p: &chroma_blockstore::provider::BlockfileProvider,
        ids: super::super::HierarchicalSpannIds,
        c: HierarchicalSpannConfig,
        n: usize,
    ) -> HierarchicalSpannReader {
        let reader = HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, c)
            .await
            .unwrap();
        let census = reader
            .audit_reachable_ids(
                n as u64,
                AuditLimits {
                    max_bitmap_bytes: (n as u64).div_ceil(8),
                    max_nodes: 10000,
                    max_leaf_entries: 1048576,
                },
            )
            .await
            .unwrap();
        census.require_single_current_copy().unwrap();
        assert_eq!(census.unique_live_rows, n as u64);
        assert_eq!(census.unreachable_nodes, 0);
        reader
    }
    #[test]
    fn split_input_fixed_order_mutation_and_bounded_messages() {
        let mut outputs=vec![];
        for grouped in [false,true] {
            let b=current_fixture(false);b.split_input_policy.store(usize::from(grouped),Ordering::Relaxed);
            for id in 1..130 {assert_eq!(b.bump(id,None),Some(1));assert!(b.register(10,id,1,&[id as f32,-0.,2.,3.]).is_some());}
            assert!(b.register(20,65,1,&[65.,-0.,2.,3.]).is_some());
            b.actor(0).call(|s|{s.current.write().insert(10,129);});
            let mut rows:Vec<_>=(0..130).map(|id|(id,1)).collect();rows.extend([(7,1),(7,0),(10,129),(999,0),(999,1)]);
            let mut kept=vec![];let mut flags_all=vec![];
            let start=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed);
            for (batch,chunk) in rows.chunks(64).enumerate() {
                if batch==1 {assert_eq!(b.bump(70,Some(1)),Some(2));assert!(b.register(20,70,2,&[70.,-0.,2.,3.]).is_some());}
                let ids:Vec<_>=chunk.iter().map(|v|v.0).collect();let versions:Vec<_>=chunk.iter().map(|v|v.1).collect();
                let flags=b.split_input_flags(&ids,&versions);
                kept.extend(chunk.iter().zip(&flags).filter(|(_,valid)|**valid).map(|(row,_)|*row));flags_all.extend(flags);
            }
            assert!(!kept.contains(&(70,1)));assert!(kept.contains(&(10,129)));assert!(kept.contains(&(999,0)));assert_eq!(kept.iter().filter(|v|**v==(7,1)).count(),2);
            let calls=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed)-start;
            if grouped {let h=b.split_input_lengths.lock();assert_eq!(h.iter().enumerate().map(|(n,v)|n as u64*v).sum::<u64>(),rows.len() as u64);assert_eq!(h.iter().sum::<u64>(),3);}
            outputs.push((kept,flags_all,b.snapshot(10).unwrap().codes,b.snapshot(20).unwrap().codes,calls));
        }
        assert_eq!((&outputs[0].0,&outputs[0].1,&outputs[0].2,&outputs[0].3),(&outputs[1].0,&outputs[1].1,&outputs[1].2,&outputs[1].3));
        assert!(outputs[1].4<outputs[0].4);println!("SPLIT_INPUT_FIXED scalar={} grouped={} rows=135",outputs[0].4,outputs[1].4);
        let b=current_fixture(false);assert!(b.split_input_flags(&[],&[]).is_empty());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.split_input_flags(&[0],&[]))).is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.split_input_flags(&[0;65],&[1;65]))).is_err());
        b.split_input_policy.store(1,Ordering::Relaxed);b.authority.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.split_input_flags(&[0],&[1]))).is_err());
    }

    #[test]
    fn split_input_tree_lock_transfer_visibility_and_sorted_progress() {
        let config=HierarchicalSpannConfig {fp_npa:true,max_replicas:1,..Default::default()};
        let mut owner=ConcurrentOwnerWriter::new(4,DistanceFunction::Euclidean,config,2,32).unwrap();owner.set_batched_split_input_validation(true).unwrap();assert!(owner.set_direct_current_reads(true).is_err());
        let b=owner.kernel.owner_backend.as_ref().unwrap().clone();
        b.leaf_owner.insert(20,1);b.install(20,LeafNode {centroid:vec![0.;4],centroid_code:vec![],ids:vec![],versions:vec![],codes:vec![],length:0,parent_id:None});
        assert_eq!(b.bump(0,None),Some(1));assert!(b.register(0,0,1,&[1.,-0.,2.,3.]).is_some());assert_eq!(b.bump(1,None),Some(1));assert!(b.register(0,1,1,&[2.,-0.,2.,3.]).is_some());assert_eq!(b.bump(0,Some(1)),Some(2));
        let (removed_tx,removed_rx)=mpsc::channel();let (resume_tx,resume_rx)=mpsc::channel();*b.transfer_pause.lock()=Some((removed_tx,resume_rx));
        // A real coordinator structural guard stays held while the reader waits
        // for a transfer that only owns an ID lock and executes owner callbacks.
        let _structural=owner.kernel.tree_lock.lock();
        std::thread::scope(|scope| {
            let w=b.clone();let transfer=scope.spawn(move||w.register(20,0,2,&[1.,-0.,2.,3.]));removed_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let before=b.current_requests.load(Ordering::Relaxed);let (tx,rx)=mpsc::channel();let w=b.clone();let read=scope.spawn(move||tx.send(w.split_input_flags(&[0,1,0],&[2,1,1])).unwrap());
            let start=Instant::now();while b.current_requests.load(Ordering::Relaxed)==before {assert!(start.elapsed()<std::time::Duration::from_secs(5));std::thread::yield_now();}
            assert!(matches!(rx.try_recv(),Err(mpsc::TryRecvError::Empty)));resume_tx.send(()).unwrap();assert!(transfer.join().unwrap().is_some());assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),vec![true,true,false]);read.join().unwrap();
        });
        std::thread::scope(|scope| {
            let barrier=Arc::new(std::sync::Barrier::new(3));let (tx,rx)=mpsc::channel();
            for reverse in [false,true] {let w=b.clone();let barrier=barrier.clone();let tx=tx.clone();scope.spawn(move||{barrier.wait();for _ in 0..64 {let (ids,versions)=if reverse{([1,0],[1,2])}else{([0,1],[2,1])};assert_eq!(w.split_input_flags(&ids,&versions),vec![true,true]);}tx.send(()).unwrap();});}
            barrier.wait();for _ in 0..2 {rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();}
        });
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn split_input_integrated_scheduler_saved_census_and_old_reader() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig{branching_factor:4,split_threshold:16,merge_threshold:4,write_navigation:super::super::super::config::NavigationMode::Fp,fp_npa:true,max_replicas:1,..Default::default()};
        let input:Vec<Vec<f32>>=(0..256).map(|id|(0..32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.}).collect()).collect();
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config.clone(),4,256).unwrap();owner.set_predicate_batching(true).unwrap();owner.set_batched_scrub_validation(true).unwrap();owner.set_live_prefix_validation(true).unwrap();owner.set_batched_split_input_validation(true).unwrap();
        assert!(owner.set_direct_current_reads(true).is_err());assert!(owner.set_predicate_batching(false).is_err());
        for start in [0,128] {
            let rows:Vec<_>=(start..start+128).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();
            if start==0 {
                let ids=owner.save(&p).await.unwrap();let old=reader(&p,ids,config.clone(),128).await;let before=old.rerank_selected(&input[0],&(0..128).collect::<Vec<_>>()).await.unwrap();assert!(owner.reshard(32).unwrap()>0);
                let rows:Vec<_>=(128..256).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();let ids=owner.save(&p).await.unwrap();let current=reader(&p,ids,config.clone(),256).await;for (id,v) in input.iter().enumerate(){assert!(current.verify_vector_bits(id as u32,v).await.unwrap());}assert_eq!(old.rerank_selected(&input[0],&(0..128).collect::<Vec<_>>()).await.unwrap(),before);break;
            }
        }
        let report=owner.report();assert!(report["reference_splits"].as_u64().unwrap()>0);assert!(report["actual_subtree_jobs"].as_u64().unwrap()>=2);let work=&report["split_input_validation"];assert!(work["physical_rows"].as_u64().unwrap()>0);assert!(work["owner_calls"].as_u64().unwrap()>0);assert_eq!(report["central_current_versions"],0);assert_eq!(report["central_mutable_posting_rows"],0);println!("SPLIT_INPUT_INTEGRATED {}",report);
    }

    #[test]
    fn live_prefix_fixed_decisions_resample_mutated_suffix_and_reduce_calls() {
        let mut results=vec![];
        for batched in [false,true] {
            let b=current_fixture(false);
            for id in 1..130 {assert_eq!(b.bump(id,None),Some(1));assert!(b.register(10,id,1,&[id as f32,-0.,2.,3.]).is_some());}
            b.actor(0).call(|s|{s.current.write().insert(10,129);});
            let mut rows:Vec<_>=(0..130).map(|id|(id,1,Some([5,20,90].contains(&id)))).collect();
            rows.extend([(7,1,Some(true)),(999,1,Some(true)),(998,0,None)]);
            let before=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed);
            let mut seen=std::collections::HashSet::new();let mut actions=vec![];let mut evaluations=0;let mut offset=0;
            while offset<rows.len() {
                let (consumed,evaluated,action)=if batched {
                    let step=b.live_npa_prefix(&rows[offset..rows.len().min(offset+64)],&mut seen);(step.consumed,step.evaluated,step.action)
                }else{
                    let (id,version,predicate)=rows[offset];let mut count=0;let mut action=None;
                    if version==b.current(id).unwrap_or(0) && seen.insert(id) {if let Some(eligible)=predicate {count=1;if eligible {action=Some(0);}}}
                    (1,count,action)
                };
                evaluations+=evaluated;
                if let Some(slot)=action {
                    let id=rows[offset+slot].0;actions.push(id);
                    if id==5 {assert_eq!(b.bump(20,Some(1)),Some(2));assert!(b.register(20,20,2,&[20.,-0.,2.,3.]).is_some());}
                }
                assert!(consumed>0);offset+=consumed;
            }
            let calls=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed)-before;
            assert_eq!(actions,vec![5,90]);assert!(!seen.contains(&20));assert!(seen.contains(&998));
            if batched {assert!(b.live_prefix_rows.load(Ordering::Relaxed)>b.live_prefix_consumed.load(Ordering::Relaxed));let h=b.live_prefix_lengths.lock();assert_eq!(h.1.iter().enumerate().map(|(i,n)|i as u64*n).sum::<u64>(),rows.len() as u64);}
            results.push((actions,seen,evaluations,b.snapshot(10).unwrap().codes,b.snapshot(20).unwrap().codes,calls));
        }
        assert_eq!((&results[0].0,&results[0].1,results[0].2,&results[0].3,&results[0].4),(&results[1].0,&results[1].1,results[1].2,&results[1].3,&results[1].4));
        assert!(results[1].5<results[0].5);println!("LIVE_PREFIX_FIXED scalar_current_calls={} batch_owner_calls={}",results[0].5,results[1].5);
    }

    #[test]
    fn live_prefix_empty_duplicates_capacity_and_authority_rejection() {
        let b=current_fixture(false);let mut seen=std::collections::HashSet::new();let empty=b.live_npa_prefix(&[],&mut seen);assert_eq!((empty.consumed,empty.evaluated,empty.action),(0,0,None));
        let rows=[(0,1,Some(false)),(0,1,Some(true)),(0,0,Some(true))];let step=b.live_npa_prefix(&rows,&mut seen);assert_eq!((step.consumed,step.evaluated,step.action),(3,1,None));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.live_npa_prefix(&vec![(0,1,Some(false));65],&mut seen))).is_err());
        b.authority.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.live_npa_prefix(&[(0,1,Some(false))],&mut seen))).is_err());b.authority.insert(0,0);
        let step=b.live_npa_prefix(&[(0,1,Some(true))],&mut std::collections::HashSet::new());assert_eq!(step.action,Some(0));
    }

    #[test]
    fn live_prefix_transfer_visibility_and_opposite_order_progress() {
        let b=current_fixture(false);assert_eq!(b.bump(1,None),Some(1));assert!(b.register(10,1,1,&[2.,-0.,2.,3.]).is_some());
        assert_eq!(b.bump(0,Some(1)),Some(2));let (removed_tx,removed_rx)=mpsc::channel();let (resume_tx,resume_rx)=mpsc::channel();*b.transfer_pause.lock()=Some((removed_tx,resume_rx));
        std::thread::scope(|scope| {
            let w=b.clone();let transfer=scope.spawn(move||w.register(20,0,2,&[1.,-0.,2.,3.]));removed_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let (entered_tx,entered_rx)=mpsc::channel();let (done_tx,done_rx)=mpsc::channel();let w=b.clone();let read=scope.spawn(move||{entered_tx.send(()).unwrap();let mut seen=std::collections::HashSet::new();let v=w.live_npa_prefix(&[(0,2,Some(true))],&mut seen);done_tx.send(v.action).unwrap();});entered_rx.recv().unwrap();assert!(matches!(done_rx.try_recv(),Err(mpsc::TryRecvError::Empty)));resume_tx.send(()).unwrap();assert!(transfer.join().unwrap().is_some());assert_eq!(done_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),Some(0));read.join().unwrap();
        });
        std::thread::scope(|scope| {
            let barrier=Arc::new(std::sync::Barrier::new(3));let (tx,rx)=mpsc::channel();
            for reversed in [false,true] {let w=b.clone();let barrier=barrier.clone();let tx=tx.clone();scope.spawn(move||{barrier.wait();let rows=if reversed {vec![(1,1,Some(false)),(0,2,Some(false))]}else{vec![(0,2,Some(false)),(1,1,Some(false))]};for _ in 0..64 {let step=w.live_npa_prefix(&rows,&mut std::collections::HashSet::new());assert_eq!(step.evaluated,2);}tx.send(()).unwrap();});}
            barrier.wait();for _ in 0..2 {rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();}
        });
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn live_prefix_integrated_scheduler_saved_census_and_old_reader() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let config=HierarchicalSpannConfig{branching_factor:4,split_threshold:16,merge_threshold:4,write_navigation:super::super::super::config::NavigationMode::Fp,fp_npa:true,max_replicas:1,..Default::default()};
        let input:Vec<Vec<f32>>=(0..256).map(|id|(0..32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.}).collect()).collect();
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config.clone(),4,256).unwrap();owner.set_predicate_batching(true).unwrap();owner.set_batched_scrub_validation(true).unwrap();owner.set_live_prefix_validation(true).unwrap();
        assert!(owner.set_direct_current_reads(true).is_err());assert!(owner.set_predicate_batching(false).is_err());
        for start in [0,128] {
            let rows:Vec<_>=(start..start+128).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();
            if start==0 {
                let ids=owner.save(&p).await.unwrap();let old=reader(&p,ids,config.clone(),128).await;let before=old.rerank_selected(&input[0],&(0..128).collect::<Vec<_>>()).await.unwrap();assert!(owner.reshard(32).unwrap()>0);
                let rows:Vec<_>=(128..256).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();let ids=owner.save(&p).await.unwrap();let current=reader(&p,ids,config.clone(),256).await;for (id,v) in input.iter().enumerate(){assert!(current.verify_vector_bits(id as u32,v).await.unwrap());}assert_eq!(old.rerank_selected(&input[0],&(0..128).collect::<Vec<_>>()).await.unwrap(),before);break;
            }
        }
        let report=owner.report();assert!(report["reference_splits"].as_u64().unwrap()>0);assert!(report["actual_subtree_jobs"].as_u64().unwrap()>=2);let work=&report["live_prefix_validation"];assert!(work["consumed_rows"].as_u64().unwrap()>0);assert!(work["owner_calls"].as_u64().unwrap()>0);assert_eq!(report["central_current_versions"],0);assert_eq!(report["central_mutable_posting_rows"],0);println!("LIVE_PREFIX_INTEGRATED {}",report);
    }

    #[test]
    fn reference_owner_scrub_live_install_rejects_before_owner_mutation() {
        let c=HierarchicalSpannConfig {fp_npa:true,max_replicas:1,..Default::default()};
        let mut owner=ConcurrentOwnerWriter::new(4,DistanceFunction::Euclidean,c,1,32).unwrap();owner.set_batched_scrub_validation(true).unwrap();
        let b=owner.kernel.owner_backend.as_ref().unwrap();let before=b.snapshot(0).unwrap();let mut replacement=clone_leaf(&before);replacement.centroid=vec![99.;4];
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||owner.kernel.install_reference_node(0,TreeNode::Leaf(replacement)))).is_err());
        assert_eq!(b.snapshot(0).unwrap().centroid,before.centroid);assert!(owner.kernel.nodes.contains_key(&0));
    }

    #[test]
    fn reference_owner_scrub_duplicate_ids_and_capacity() {
        let b=current_fixture(false);
        let before=b.scrub_validation_owner_calls.load(Ordering::Relaxed);
        assert_eq!(b.validate_scrub_batch(&[(0,1),(0,0),(0,1)]),vec![true,false,true]);
        assert_eq!(b.scrub_validation_owner_calls.load(Ordering::Relaxed)-before,1);
        assert!(b.validate_scrub_batch(&[]).is_empty());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.validate_scrub_batch(&vec![(0,1);65]))).is_err());
        assert_eq!(b.current(0),Some(1));
    }

    #[test]
    fn reference_owner_scrub_fixed_oracle_order_codes_and_callbacks() {
        let mut outputs=vec![];
        for batched in [false,true] {
            let b=current_fixture(false);let mut expected=BTreeMap::new();
            for id in 0..130u32 {
                if id!=0 {assert_eq!(b.bump(id,None),Some(1));assert!(b.register(10,id,1,&[id as f32,-0.,2.,3.]).is_some());}
                let mut v=1;
                if id%3==0||id%5==0 {v=2;assert_eq!(b.bump(id,Some(1)),Some(2));}
                if id%5==0 {assert!(b.register(20,id,v,&[id as f32,-0.,2.,3.]).is_some());}
                expected.insert(id,v);
            }
            b.authority.remove(&7);expected.remove(&7);
            let original=b.snapshot(10).unwrap();let size=original.codes.len()/original.ids.len();
            let mut positions:Vec<_>=(0..original.ids.len()).collect();let mut i=0;
            while i<positions.len(){let old=positions[i];if expected.get(&original.ids[old])!=Some(&original.versions[old]){positions.swap_remove(i);}else{i+=1;}}
            let ids:Vec<_>=positions.iter().map(|&i|original.ids[i]).collect();let versions:Vec<_>=positions.iter().map(|&i|original.versions[i]).collect();let codes:Vec<_>=positions.iter().flat_map(|&i|original.codes[i*size..(i+1)*size].iter().copied()).collect();
            b.scrub_batch_policy.store(usize::from(batched),Ordering::Relaxed);
            let before=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed);
            assert_eq!(b.scrub(10,size),Some(ids.len()));let calls=b.message_timing.buckets[Operation::Current as usize].calls.load(Ordering::Relaxed)-before;
            let actual=b.snapshot(10).unwrap();assert_eq!((actual.ids.clone(),actual.versions.clone(),actual.codes.clone()),(ids,versions,codes));
            if batched {assert_eq!(b.scrub_validation_batches.load(Ordering::Relaxed),3);assert!(calls<=6);assert_eq!(b.scrub_validation_rows.load(Ordering::Relaxed),130);}else{assert_eq!(calls,129);}
            assert_eq!(b.direct_current_reads.load(Ordering::Relaxed),0);
            outputs.push((actual.ids,actual.versions,actual.codes));
            println!("SCRUB_FIXED_WORK {}",serde_json::json!({"batched":batched,"physical_rows":130,"current_owner_callbacks":calls,"validation_batches":b.scrub_validation_batches.load(Ordering::Relaxed)}));
        }
        assert_eq!(outputs[0],outputs[1]);
    }
    #[test]
    fn reference_owner_scrub_waits_for_actual_transfer_visibility() {
        let b=current_fixture(false);b.scrub_batch_policy.store(1,Ordering::Relaxed);assert_eq!(b.bump(0,Some(1)),Some(2));
        let (removed_tx,removed_rx)=mpsc::channel();let (resume_tx,resume_rx)=mpsc::channel();*b.transfer_pause.lock()=Some((removed_tx,resume_rx));
        std::thread::scope(|scope| {
            let w=b.clone();let mutation=scope.spawn(move||w.register(20,0,2,&[1.,-0.,2.,3.]));removed_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let size=b.snapshot(10).unwrap().codes.len();let (tx,rx)=mpsc::channel();let w=b.clone();let scrub=scope.spawn(move||{tx.send(w.scrub(10,size)).unwrap();});
            let start=Instant::now();while b.scrub_validation_batches.load(Ordering::Relaxed)==0 {assert!(start.elapsed()<std::time::Duration::from_secs(5));std::thread::yield_now();}
            assert!(matches!(rx.try_recv(),Err(mpsc::TryRecvError::Empty)));resume_tx.send(()).unwrap();assert!(mutation.join().unwrap().is_some());assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),Some(0));scrub.join().unwrap();
        });assert_eq!(b.current(0),Some(2));assert_eq!(b.snapshot(20).unwrap().ids,vec![0]);
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn reference_owner_scrub_source_guard_blocks_leaf_change_and_saves() {
        let c=HierarchicalSpannConfig {branching_factor:2,split_threshold:100,merge_threshold:0,fp_npa:true,max_replicas:1,..Default::default()};
        let mut owner=ConcurrentOwnerWriter::new(4,DistanceFunction::Euclidean,c.clone(),2,32).unwrap();owner.set_batched_scrub_validation(true).unwrap();
        let rows:Vec<_>=(0..8u32).map(|id|(id,vec![id as f32,1.,-0.,0.])).collect();owner.add_phase(&rows,2).unwrap();
        let b=owner.kernel.owner_backend.as_ref().unwrap().clone();let (captured_tx,captured_rx)=mpsc::channel();let (resume_tx,resume_rx)=mpsc::channel();*b.scrub_pause.lock()=Some((captured_tx,resume_rx));
        std::thread::scope(|scope| {
            let kernel=&owner.kernel;let scrub=scope.spawn(move||kernel.scrub_owner_fixture(0));captured_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let (entered_tx,entered_rx)=mpsc::channel();let (tx,rx)=mpsc::channel();let kernel=&owner.kernel;
            let change=scope.spawn(move||{entered_tx.send(()).unwrap();tx.send(kernel.take_reference_node(0)).unwrap();});entered_rx.recv().unwrap();
            assert!(matches!(rx.try_recv(),Err(mpsc::TryRecvError::Empty)));resume_tx.send(()).unwrap();scrub.join().unwrap();let (id,node)=rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap().unwrap();change.join().unwrap();owner.kernel.install_reference_node(id,node);
        });owner.balance_phase(2).unwrap();let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let saved=owner.save(&p).await.unwrap();let _reader=reader(&p,saved,c,8).await;
    }

    fn current_fixture(direct:bool)->Arc<Backend> {
        let b=Arc::new(Backend::new(2));
        b.direct_current_policy.store(usize::from(direct),Ordering::Relaxed);
        b.message_timing.enabled.store(true,Ordering::Relaxed);
        for (leaf,owner) in [(10,0),(20,1)] {
            b.leaf_owner.insert(leaf,owner);
            b.install(leaf,LeafNode {centroid:vec![0.;4],centroid_code:vec![],ids:vec![],versions:vec![],codes:vec![],length:0,parent_id:None});
        }
        assert_eq!(b.bump(0,None),Some(1));assert!(b.register(10,0,1,&[1.,-0.,2.,3.]).is_some());b
    }
    #[test]
    fn reference_owner_current_fixed_decisions_same_authority_no_messages() {
        let mut results=vec![];
        for direct in [false,true] {
            let b=current_fixture(direct);let before=b.actor_work.calls.load(Ordering::Relaxed);
            for _ in 0..256 {assert_eq!(b.current(0),Some(1));}
            let calls=b.actor_work.calls.load(Ordering::Relaxed)-before;assert_eq!(calls,if direct {0}else{256});
            assert_eq!(b.current(999),None);assert_eq!(b.bump(0,Some(1)),Some(2));assert!(b.register(20,0,2,&[1.,-0.,2.,3.]).is_some());
            assert_eq!(b.current(0),Some(2));assert!(b.actor(0).versions.read().get(&0).is_none());assert_eq!(b.actor(1).versions.read().get(&0),Some(&2));
            let leaf=b.snapshot(20).unwrap();results.push((leaf.ids,leaf.versions,leaf.codes));
            b.actor(1).call(|s|{s.current.write().insert(0,2|DELETED_BIT);});assert_eq!(b.current(0),Some(2|DELETED_BIT));
            assert_eq!(b.bump(0,Some(2)),None);
            b.authority.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.current(0))).is_err());b.authority.insert(0,1);
            println!("CURRENT_FIXED_WORK {}",serde_json::json!({"direct":direct,"reads":256,"current_callbacks":calls,"same_owner_map":Arc::ptr_eq(&b.actor(1).versions,&b.actor(1).call(|s|s.current.clone()))}));
        }
        assert_eq!(results[0],results[1]);
    }
    #[test]
    fn reference_owner_current_blocks_across_actual_transfer_gap() {
        for direct in [false,true] {
            let b=current_fixture(direct);assert_eq!(b.bump(0,Some(1)),Some(2));
            let (removed_tx,removed_rx)=mpsc::channel();let (resume_tx,resume_rx)=mpsc::channel();*b.transfer_pause.lock()=Some((removed_tx,resume_rx));
            std::thread::scope(|scope| {
                let w=b.clone();let mutation=scope.spawn(move||w.register(20,0,2,&[1.,-0.,2.,3.]));
                removed_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
                assert_eq!(*b.authority.get(&0).unwrap(),0);assert!(b.actor(0).versions.read().get(&0).is_none());assert!(b.actor(1).versions.read().get(&0).is_none());
                let before=b.current_requests.load(Ordering::Relaxed);let (tx,rx)=mpsc::channel();let w=b.clone();let read=scope.spawn(move||{tx.send(w.current(0)).unwrap();});
                let start=Instant::now();while b.current_requests.load(Ordering::Relaxed)==before {assert!(start.elapsed()<std::time::Duration::from_secs(5));std::thread::yield_now();}
                assert!(matches!(rx.try_recv(),Err(mpsc::TryRecvError::Empty)));
                resume_tx.send(()).unwrap();assert!(mutation.join().unwrap().is_some());assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),Some(2));read.join().unwrap();
            });
            assert_eq!(*b.authority.get(&0).unwrap(),1);assert_eq!(b.current(0),Some(2));
        }
    }

    #[test]
    fn reference_owner_message_observer_fixed_work_on_off() {
        let b=Backend::new(1);let mut samples=vec![];
        for enabled in [false,true,true,false] {
            b.message_timing.enabled.store(enabled,Ordering::Relaxed);
            let before=b.actor_work.calls.load(Ordering::Relaxed);let t=Instant::now();let mut sum=0u64;
            for id in 0..1024u64 {sum+=b.actor(0).call_op(Operation::Current,move |_|id);}
            assert_eq!(sum,1023*1024/2);assert_eq!(b.actor_work.calls.load(Ordering::Relaxed)-before,1024);
            samples.push(serde_json::json!({"enabled":enabled,"calls":1024,"elapsed_ns":t.elapsed().as_nanos()}));
        }
        let observed=b.message_timing.snapshot();assert_eq!(observed["queued"],0);assert_eq!(observed["operations"]["current"]["calls"],2048);
        assert!(observed["operations"]["current"]["caller_ns"].as_u64().unwrap()>0);
        for op in [Operation::Other,Operation::VersionMutation,Operation::Posting,Operation::Predicate,Operation::Registration] {
            b.message_timing.enabled.store(true,Ordering::Relaxed);assert_eq!(b.actor(0).call_op(op,|_|7),7);
        }
        let all=b.message_timing.snapshot();for n in OP_NAMES {assert!(all["operations"][n]["calls"].as_u64().unwrap()>0);}
        println!("MESSAGE_OBSERVER_FIXED_WORK {}",serde_json::json!({"samples":samples,"counters":all,"scope":"Same1024nooprequests percell, on/offobserver overhead only; no construction speed claim."}));
    }

    #[test]
    fn reference_owner_predicate_batch_fixed_inputs_and_callback_reduction() {
        let backend = Backend::new(2);
        backend.leaf_owner.insert(7, 1);
        let values: Vec<Option<Arc<[f32]>>> = (0..130).map(|i| if i==63 {None} else {Some(Arc::from(vec![i as f32 / 17., -0., 1., -2.]))}).collect();
        let predicate = backend.prepare_predicate(DistanceFunction::Euclidean, &[1.,0.,2.,3.], &[0.,0.,0.,0.], &[2.,0.,0.,0.], &[-2.,0.,0.,0.]);
        assert!(backend.predicate_batch(7, vec![], predicate.clone(), false).is_none());
        for own in [false,true] {
            let before = backend.actor_work.calls.load(Ordering::Relaxed);
            let expected: Vec<_> = values.iter().map(|v|v.as_ref().map(|v|if own {backend.self_predicate(7,v.clone(),predicate.clone())} else {backend.fp_predicate(7,v.clone(),predicate.clone())})).collect();
            assert_eq!(backend.actor_work.calls.load(Ordering::Relaxed)-before,129);
            backend.predicate_batching.store(1,Ordering::Relaxed);
            let before = backend.actor_work.calls.load(Ordering::Relaxed);
            let actual: Vec<_> = values.chunks(64).flat_map(|v|backend.predicate_batch(7,v.to_vec(),predicate.clone(),own).unwrap()).collect();
            assert_eq!(actual,expected);
            assert_eq!(backend.actor_work.calls.load(Ordering::Relaxed)-before,3);
            assert_eq!(backend.predicate_batch(7,vec![],predicate.clone(),own),Some(vec![]));
            backend.predicate_batching.store(0,Ordering::Relaxed);
        }
        assert_eq!(backend.predicate_batch_rows.load(Ordering::Relaxed),258);
        assert_eq!(backend.predicate_batch_callbacks.load(Ordering::Relaxed),6);
        // Precomputed pure decisions do not cache current versions. A mutation
        // between decision computation and consumption is observed by current().
        backend.authority.insert(5,1);
        backend.actor(1).call(|s|s.current.write().insert(5,1));
        assert_eq!(backend.current(5),Some(1));
        assert_eq!(backend.bump(5,Some(1)),Some(2));
        assert_eq!(backend.current(5),Some(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reference_owner_parallel_regions_moves_conflicts_and_saved_readers() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let config = HierarchicalSpannConfig {
            branching_factor: 4,
            split_threshold: 16,
            merge_threshold: 4,
            write_beam_min: 8,
            write_beam_max: 16,
            reassign_neighbor_count: 16,
            max_replicas: 1,
            write_navigation: super::super::super::config::NavigationMode::Fp,
            fp_npa: true,
            ..Default::default()
        };
        let rows: Vec<_> = (0..1024u32)
            .map(|id| {
                let mut v = vec![0.; 32];
                for (d, x) in v.iter_mut().enumerate() {
                    let q = id
                        .wrapping_mul(1664525)
                        .wrapping_add((d as u32 + 1) * 1013904223u32.wrapping_div(32));
                    *x = ((q >> 8) % 1024) as f32 / 128.;
                }
                v[0] += (id % 16) as f32 * 16.;
                v[2] = -0.;
                v
            })
            .collect();
        let mut owner =
            ConcurrentOwnerWriter::new(32, DistanceFunction::Euclidean, config.clone(), 4, 1024)
                .unwrap();
        owner.set_direct_current_reads(false).unwrap();
        owner.set_batched_scrub_validation(true).unwrap();
        owner.set_predicate_batching(true).unwrap();
        let mut reference =
            HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config.clone());
        let first: Vec<_> = (0..512u32)
            .map(|id| (id, rows[id as usize].clone()))
            .collect();
        owner.add_phase(&first, 4).unwrap();
        owner.balance_phase(4).unwrap();
        std::thread::scope(|scope| {
            for group in first.chunks(128) {
                let w = &reference;
                scope.spawn(move || {
                    for (id, v) in group {
                        w.add(*id, v);
                    }
                });
            }
        });
        reference.balance_index_parallel(4);
        let old_ids = owner.save(&p).await.unwrap();
        let old = reader(&p, old_ids, config.clone(), 512).await;
        let old_query = rows[17].clone();
        let old_results = old
            .rerank_selected(&old_query, &(0..512u32).collect::<Vec<_>>())
            .await
            .unwrap();
        let before = owner.report();
        let moves = owner.reshard(128).unwrap();
        assert!(moves > 0);
        let moved = owner.report();
        assert_eq!(moved["rows"], before["rows"]);
        assert_eq!(moved["central_mutable_posting_rows"], 0);
        assert_eq!(moved["central_current_versions"], 0);
        assert!(
            moved["owners"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| v["current"].as_u64().unwrap() > 0)
                .count()
                > 1
        );
        let final_config = HierarchicalSpannConfig {
            split_threshold: 64,
            merge_threshold: 24,
            ..config.clone()
        };
        owner.configure_phase(final_config.clone()).unwrap();
        reference.config = final_config.clone();
        let second: Vec<_> = (512..1024u32)
            .map(|id| (id, rows[id as usize].clone()))
            .collect();
        assert!(owner.add_phase(&[(999, rows[0].clone())], 4).is_err());
        owner.add_phase(&second, 4).unwrap();
        assert!(owner.reshard(128).is_err());
        owner.balance_phase(4).unwrap();
        std::thread::scope(|scope| {
            for group in second.chunks(128) {
                let w = &reference;
                scope.spawn(move || {
                    for (id, v) in group {
                        w.add(*id, v);
                    }
                });
            }
        });
        reference.balance_index_parallel(4);
        let saved = owner.save(&p).await.unwrap();
        let postings = p
            .read::<u32, chroma_types::hierarchical_spann::HierarchicalSpannPostingList<'static>>(
                chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                    saved.posting_list_id,
                    String::new(),
                ),
            )
            .await
            .unwrap();
        for entry in owner.kernel.nodes.iter() {
            if let TreeNode::Leaf(node) = entry.value() {
                let posting = postings.get("", *entry.key()).await.unwrap().unwrap();
                assert_eq!(
                    posting.codes.len(),
                    posting.ids.len() * owner.kernel.code_size()
                );
                for (slot, &id) in posting.ids.iter().enumerate() {
                    let expected = Code::<1>::quantize(&rows[id as usize], &node.centroid);
                    assert_eq!(
                        &posting.codes[slot * owner.kernel.code_size()
                            ..(slot + 1) * owner.kernel.code_size()],
                        expected.as_ref()
                    );
                }
            }
        }
        let owned = reader(&p, saved, final_config.clone(), 1024).await;
        for (id, vector) in rows.iter().enumerate() {
            assert!(owned.verify_vector_bits(id as u32, vector).await.unwrap());
        }
        let expected = reference
            .commit(&p, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let baseline = reader(&p, expected, final_config.clone(), 1024).await;
        let ids: Vec<_> = (0..1024).collect();
        assert_eq!(
            owned.rerank_selected(&old_query, &ids).await.unwrap(),
            baseline.rerank_selected(&old_query, &ids).await.unwrap()
        );
        assert_eq!(
            old.rerank_selected(&old_query, &(0..512).collect::<Vec<_>>())
                .await
                .unwrap(),
            old_results
        );
        let report = owner.report();
        assert!(report["reference_merges"].as_u64().unwrap() > 0);
        assert_eq!(report["central_mutable_posting_rows"], 0);
        assert_eq!(report["central_current_versions"], 0);
        assert!(report["peak_add_navigation_spans"].as_u64().unwrap() > 1);
        assert!(report["peak_balance_leaf_spans"].as_u64().unwrap() > 1);
        assert!(report["peak_npa_contexts"].as_u64().unwrap() > 1);
        assert!(report["peak_npa_contexts"].as_u64().unwrap() <= 4);
        assert!(report["peak_balance_leaf_spans"].as_u64().unwrap() <= 4);
        assert_eq!(report["peak_distinct_critical_threads"], 1);
        assert!(report["peak_distinct_subtree_workers"].as_u64().unwrap() > 1);
        assert_eq!(report["schedule_sample_overflow"], false);
        assert_eq!(report["owner_predicates"], 0);
        assert!(report["predicate_batch_callbacks"].as_u64().unwrap() > 0);
        assert!(report["predicate_batch_computed_rows"].as_u64().unwrap() > 0);
        assert_eq!(report["direct_current_reads"],0);
        assert!(report["scrub_validation_batches"].as_u64().unwrap()>0);
        println!("REFERENCE_OWNER_REPORT {report}");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reference_owner_missing_parent_adopts_concurrent_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let p = provider(dir.path());
        let c = HierarchicalSpannConfig {
            branching_factor: 2,
            split_threshold: 4,
            merge_threshold: 0,
            max_replicas: 1,
            fp_npa: true,
            ..Default::default()
        };
        let mut owner =
            ConcurrentOwnerWriter::new(4, DistanceFunction::Euclidean, c.clone(), 2, 32).unwrap();
        let input: Vec<_> = (0..16u32)
            .map(|id| (id, vec![id as f32, 1., -0., 0.]))
            .collect();
        owner.add_phase(&input, 2).unwrap();
        owner.balance_phase(2).unwrap();
        let mut orphans = vec![];
        for x in [-1., 20.] {
            let id = owner.kernel.alloc_node_id();
            let centroid = vec![x, 1., 0., 0.];
            let centroid_code = owner.kernel.quantize_origin(&centroid);
            owner.kernel.install_reference_node(
                id,
                TreeNode::Leaf(LeafNode {
                    centroid,
                    centroid_code,
                    ids: vec![],
                    versions: vec![],
                    codes: vec![],
                    length: 0,
                    parent_id: Some(u32::MAX),
                }),
            );
            orphans.push(id);
        }
        std::thread::scope(|scope| {
            for id in orphans {
                let kernel = &owner.kernel;
                scope.spawn(move || kernel.replace_child(u32::MAX, u32::MAX, &[id]));
            }
        });
        let report = owner.report();
        assert_eq!(report["orphan_adoptions"], 2);
        assert!(report["parent_edits"].as_u64().unwrap() >= 2);
        let saved = owner.save(&p).await.unwrap();
        let _ = reader(&p, saved, c, 16).await;
        println!("REFERENCE_ORPHAN_REPORT {report}");
    }
    #[test]
    fn reference_owner_missing_wrong_owner_and_configuration_reject() {
        let mut c = HierarchicalSpannConfig::default();
        c.fp_npa = false;
        assert!(ConcurrentOwnerWriter::new(4, DistanceFunction::Euclidean, c, 2, 64).is_err());
        let b = Backend::new(2);
        let leaf = LeafNode {
            centroid: vec![0.; 4],
            centroid_code: vec![],
            ids: vec![],
            versions: vec![],
            codes: vec![],
            parent_id: None,
            length: 0,
        };
        b.install(0, leaf);
        assert_eq!(b.bump(0, None), Some(1));
        b.leaf_owner.insert(0, 1);
        assert!(b.register(0, 0, 1, &[1.; 4]).is_none());
        assert!(b.snapshot(0).is_none());
        assert_eq!(b.current(0), Some(1));
        b.leaf_owner.insert(0, 99);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| b.snapshot(0))).is_err());
        b.leaf_owner.insert(0, 0);
        assert_eq!(b.snapshot(0).unwrap().length, 0);
    }
}

#[cfg(test)]
mod matched_16k_capacity_tests {
    use super::*;
    #[test]
    fn matched_16k_capacity_admits_exact_bound_without_loading_vectors() {
        let mut config=HierarchicalSpannConfig::default();
        config.write_navigation=super::super::super::config::NavigationMode::Fp;config.fp_npa=true;
        for owners in [1,4] {
            let writer=ConcurrentOwnerWriter::new(768,DistanceFunction::InnerProduct,config.clone(),owners,16384).unwrap();
            assert_eq!(writer.max_rows,16384);assert_eq!(writer.next,0);assert!(writer.ready);
        }
        assert!(ConcurrentOwnerWriter::new(768,DistanceFunction::InnerProduct,config.clone(),4,1_000_000).is_ok());
        assert!(ConcurrentOwnerWriter::new(768,DistanceFunction::InnerProduct,config,4,u32::MAX as usize).is_err());
    }
}

#[cfg(test)]
mod bounded_vector_tests {
    use super::*;
    use super::super::owner_vector_store::VectorLimits;
    use super::super::super::{reader::{HierarchicalSpannReader,AuditLimits},persistance::PREFIX_VERSION};
    fn provider(path:&std::path::Path)->chroma_blockstore::provider::BlockfileProvider {
        use chroma_blockstore::arrow::{config::BlockManagerConfig,provider::ArrowBlockfileProvider};
        chroma_blockstore::provider::BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(path.to_str().unwrap())),BlockManagerConfig::default().max_block_size_bytes,super::super::persistence::block_on_for_sync_writer(chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)).unwrap(),super::super::persistence::block_on_for_sync_writer(chroma_cache::from_config_persistent(&chroma_cache::CacheConfig::Nop)).unwrap(),2))
    }
    fn config()->HierarchicalSpannConfig{HierarchicalSpannConfig{branching_factor:4,split_threshold:32,merge_threshold:8,write_beam_min:8,write_beam_max:16,reassign_neighbor_count:16,max_replicas:1,write_navigation:super::super::super::config::NavigationMode::Fp,fp_npa:true,..Default::default()}}
    fn bits(v:&[f32])->Vec<u32>{v.iter().map(|v|v.to_bits()).collect()}
    async fn checked_reader(p:&chroma_blockstore::provider::BlockfileProvider,ids:super::super::HierarchicalSpannIds,n:usize)->HierarchicalSpannReader{
        let r=HierarchicalSpannReader::open(p,ids,DistanceFunction::Euclidean,config()).await.unwrap();let census=r.audit_reachable_ids(n as u64,AuditLimits{max_bitmap_bytes:(n as u64).div_ceil(8),max_nodes:10000,max_leaf_entries:1048576}).await.unwrap();census.require_single_current_copy().unwrap();assert_eq!(census.unique_live_rows,n as u64);r
    }
    fn publication_file_hashes(root:&std::path::Path)->BTreeMap<std::path::PathBuf,String>{
        use sha2::{Digest,Sha256};let mut files=BTreeMap::new();let mut pending=vec![root.to_path_buf()];while let Some(dir)=pending.pop(){for item in std::fs::read_dir(dir).unwrap(){let path=item.unwrap().path();if path.is_dir(){pending.push(path);}else{files.insert(path.clone(),format!("{:x}",Sha256::digest(std::fs::read(path).unwrap())));}}}files
    }
    fn root_bits(ids:&super::super::HierarchicalSpannIds)->[uuid::Uuid;5]{[ids.posting_list_id,ids.scalar_metadata_id,ids.vector_data_id,ids.leaf_node_id,ids.internal_node_id]}
    fn publication_metrics(owner:&ConcurrentOwnerWriter,label:&str){let p=owner.publication.lock();println!("PUBLICATION_GENERATION label={} {}",label,serde_json::json!({"generation":p.generation,"no_op_reuses":p.noops,"last":p.last}));}
    async fn publication_tombstones_absent(p:&chroma_blockstore::provider::BlockfileProvider,ids:&super::super::HierarchicalSpannIds,removed:&[u32]){
        use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
        use chroma_types::hierarchical_spann::{HierarchicalSpannPostingList,HierarchicalLeafNode,HierarchicalInternalNode};
        let posting=p.read::<u32,HierarchicalSpannPostingList<'_>>(BlockfileReaderOptions::new(ids.posting_list_id,String::new())).await.unwrap();let leaf=p.read::<u32,HierarchicalLeafNode<'_>>(BlockfileReaderOptions::new(ids.leaf_node_id,String::new())).await.unwrap();let internal=p.read::<u32,HierarchicalInternalNode<'_>>(BlockfileReaderOptions::new(ids.internal_node_id,String::new())).await.unwrap();let vectors=p.read::<u32,&[f32]>(BlockfileReaderOptions::new(ids.vector_data_id,String::new())).await.unwrap();
        for id in removed{assert!(posting.get("",*id).await.unwrap().is_none());assert!(leaf.get("",*id).await.unwrap().is_none());assert!(internal.get("",*id).await.unwrap().is_none());assert!(vectors.get(super::super::super::persistance::PREFIX_CENTROID,*id).await.unwrap().is_none());}
    }
    async fn publication_equivalent(p:&chroma_blockstore::provider::BlockfileProvider,a:super::super::HierarchicalSpannIds,b:super::super::HierarchicalSpannIds,owner:&ConcurrentOwnerWriter,input:&[Vec<f32>]){
        use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
        use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
        use super::super::super::persistance::PREFIX_CENTROID;
        let ar=checked_reader(p,a.clone(),input.len()).await;let br=checked_reader(p,b.clone(),input.len()).await;
        let av=p.read::<u32,u32>(BlockfileReaderOptions::new(a.scalar_metadata_id,String::new())).await.unwrap();let bv=p.read::<u32,u32>(BlockfileReaderOptions::new(b.scalar_metadata_id,String::new())).await.unwrap();
        let ap=p.read::<u32,HierarchicalSpannPostingList<'_>>(BlockfileReaderOptions::new(a.posting_list_id,String::new())).await.unwrap();let bp=p.read::<u32,HierarchicalSpannPostingList<'_>>(BlockfileReaderOptions::new(b.posting_list_id,String::new())).await.unwrap();
        let ac=p.read::<u32,&[f32]>(BlockfileReaderOptions::new(a.vector_data_id,String::new())).await.unwrap();let bc=p.read::<u32,&[f32]>(BlockfileReaderOptions::new(b.vector_data_id,String::new())).await.unwrap();
        for(id,v) in input.iter().enumerate(){assert!(ar.verify_vector_bits(id as u32,v).await.unwrap());assert!(br.verify_vector_bits(id as u32,v).await.unwrap());assert_eq!(av.get(PREFIX_VERSION,id as u32).await.unwrap(),bv.get(PREFIX_VERSION,id as u32).await.unwrap());}
        for node in owner.kernel.nodes.iter(){let id=*node.key();let x=ac.get(PREFIX_CENTROID,id).await.unwrap().unwrap();let y=bc.get(PREFIX_CENTROID,id).await.unwrap().unwrap();assert_eq!(bits(x),bits(y));
            if matches!(node.value(),TreeNode::Leaf(_)){let x=ap.get("",id).await.unwrap();let y=bp.get("",id).await.unwrap();match(x,y){(Some(x),Some(y))=>{assert_eq!(x.ids,y.ids);assert_eq!(x.versions,y.versions);assert_eq!(x.codes,y.codes);},(None,None)=>{},_=>panic!("posting presence differs")}}
        }
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_incremental_publication_multigeneration_failures_and_reuse(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let input:Vec<_>=(0..512u32).map(|id|(0..32u32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.+if d==0{(id%16) as f32*16.}else{0.}}).collect::<Vec<_>>()).collect();
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,512).unwrap();owner.enable_bounded_vectors(p.clone(),VectorLimits{split_rows:1024,..Default::default()}).unwrap();owner.set_predicate_batching(true).unwrap();owner.set_batched_scrub_validation(true).unwrap();owner.set_batched_split_input_validation(true).unwrap();
        for start in (0..256).step_by(64){let rows:Vec<_>=(start..start+64).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();}
        let first=owner.save(&p).await.unwrap();publication_metrics(&owner,"bootstrap");let old_objects=publication_file_hashes(dir.path());let old=checked_reader(&p,first.clone(),256).await;let old_hits=old.rerank_selected(&input[0],&(0..256).collect::<Vec<_>>()).await.unwrap();
        assert_eq!(root_bits(&owner.save(&p).await.unwrap()),root_bits(&first),"no-op reuses every root");assert!(owner.reshard(64).unwrap()>0);assert_eq!(root_bits(&owner.save(&p).await.unwrap()),root_bits(&first),"metadata-only placement does not rewrite query pages");
        // Modify one internal centroid only, proving inherited unchanged centroids.
        let root=owner.kernel.root_id();{let mut node=owner.kernel.nodes.get_mut(&root).unwrap();let TreeNode::Internal(v)=node.value_mut()else{panic!("internal fixture root")};v.centroid[0]+=0.125;v.centroid_code=Code::<1>::quantize(&v.centroid,&vec![0.;32]).as_ref().to_vec();}owner.kernel.mark_node_dirty(root);
        let metadata=owner.save(&p).await.unwrap();publication_metrics(&owner,"metadata-only");let metrics=owner.report()["incremental_publication"]["last"]["metrics"].clone();assert_eq!(metrics["changed_node_keys"],1);assert_eq!(metrics["copied_posting_rows"],0);assert_eq!(metrics["version_rows"],0);
        let full=owner.save_full_oracle(&p).await.unwrap();publication_equivalent(&p,metadata.clone(),full,&owner,&input[..256]).await;
        for start in (256..512).step_by(64){let rows:Vec<_>=(start..start+64).map(|id|(id as u32,input[id].clone())).collect();owner.add_phase(&rows,4).unwrap();owner.balance_phase(4).unwrap();}
        let b=owner.kernel.owner_backend.as_ref().unwrap().clone();let dirty=(owner.kernel.dirty_nodes.len(),owner.kernel.tombstones.len(),owner.kernel.dirty_versions.len(),b.publication_dirty_versions());let staged=b.vector_store().unwrap().latest().unwrap();
        for stage in 1..=6{owner.publication_fail_stage.store(stage,Ordering::Relaxed);assert!(owner.save(&p).await.is_err());assert_eq!(root_bits(owner.publication.lock().roots.as_ref().unwrap()),root_bits(&metadata));assert_eq!(b.vector_store().unwrap().latest().unwrap(),staged);assert_eq!((owner.kernel.dirty_nodes.len(),owner.kernel.tombstones.len(),owner.kernel.dirty_versions.len(),b.publication_dirty_versions()),dirty);assert!(!owner.publication_busy.load(Ordering::Acquire));}
        let next=owner.save(&p).await.unwrap();publication_metrics(&owner,"append-after-failed-flushes");assert_eq!(b.vector_store().unwrap().latest().unwrap(),next.vector_data_id);assert!(owner.kernel.dirty_nodes.is_empty());assert!(b.publication_dirty_versions().is_empty());
        let full=owner.save_full_oracle(&p).await.unwrap();publication_equivalent(&p,next.clone(),full,&owner,&input).await;assert_eq!(old.rerank_selected(&input[0],&(0..256).collect::<Vec<_>>()).await.unwrap(),old_hits);
        // Reconfiguration can merge existing leaves without staging new vectors.
        let minimum=owner.kernel.nodes.iter().filter_map(|n|if let TreeNode::Leaf(v)=n.value(){(v.parent_id.is_some() && v.length>0).then_some(v.length)}else{None}).min().unwrap();
        let mut c=config();c.split_threshold=256;c.merge_threshold=minimum+1;assert!(c.merge_threshold<c.split_threshold);owner.configure_phase(c).unwrap();assert!(owner.kernel.pending_balance_leaves()>0,"explicit undersized merge precondition");owner.ready=false;owner.insertion_remaining=0;owner.balance_phase(4).unwrap();let removed:Vec<_>=owner.kernel.tombstones.iter().map(|v|*v).collect();assert!(!removed.is_empty());let merged=owner.save(&p).await.unwrap();publication_metrics(&owner,"merge");publication_tombstones_absent(&p,&merged,&removed).await;println!("PUBLICATION_TOMBSTONES checked={}",removed.len());let full=owner.save_full_oracle(&p).await.unwrap();publication_equivalent(&p,merged.clone(),full,&owner,&input).await;
        assert!(owner.report()["reference_merges"].as_u64().unwrap()>0);assert_eq!(root_bits(&owner.save(&p).await.unwrap()),root_bits(&merged));
        assert_eq!(old.rerank_selected(&input[0],&(0..256).collect::<Vec<_>>()).await.unwrap(),old_hits);
        // Fixed authoritative reassignment empties one persisted posting. This
        // exercises delete-vs-inherit without changing vector content or deleting IDs.
        let mut leaves:Vec<_>=owner.kernel.nodes.iter().filter_map(|n|if let TreeNode::Leaf(v)=n.value(){(v.length>0).then_some((*n.key(),v.length))}else{None}).collect();leaves.sort_by_key(|v|v.1);assert!(leaves.len()>=2);let(source,_)=leaves[0];let(target,_)=leaves[1];let source_rows=b.snapshot(source).unwrap();
        {let _structure=owner.kernel.tree_lock.lock();for(&id,&version) in source_rows.ids.iter().zip(&source_rows.versions){if b.current(id)!=Some(version){continue;}let next=b.bump(id,Some(version)).unwrap();owner.kernel.mark_version_dirty(id);let mut node=owner.kernel.nodes.get_mut(&target).unwrap();let TreeNode::Leaf(meta)=node.value_mut()else{unreachable!()};meta.length=b.register(target,id,next,&input[id as usize]).unwrap();drop(node);owner.kernel.mark_node_dirty(target);}
            let mut node=owner.kernel.nodes.get_mut(&source).unwrap();let TreeNode::Leaf(meta)=node.value_mut()else{unreachable!()};meta.length=b.scrub(source,Code::<1>::size(32)).unwrap();assert_eq!(meta.length,0);drop(node);owner.kernel.mark_node_dirty(source);}
        let empty=owner.save(&p).await.unwrap();publication_metrics(&owner,"empty-posting");let full=owner.save_full_oracle(&p).await.unwrap();publication_equivalent(&p,empty,full,&owner,&input).await;
        let after_objects=publication_file_hashes(dir.path());for(path,hash) in old_objects{assert_eq!(after_objects.get(&path),Some(&hash),"old immutable object changed: {}",path.display());}
        println!("INCREMENTAL_PUBLICATION {}",owner.report());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_configured_staging_grain_fixed_input_publications_and_saved_bits(){
        let input:Vec<_>=(0..513u32).map(|id|(id,(0..32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.+if d==0{(id%2) as f32*1000.}else{0.}}).collect::<Vec<_>>())).collect();
        let mut outcomes=vec![];
        for grain in [64,256]{
            let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut c=config();c.split_threshold=2048;c.merge_threshold=16;
            let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,c,2,2048).unwrap();
            owner.enable_bounded_vectors(p.clone(),VectorLimits{append_rows:grain,staging_working_bytes:1024*1024,split_rows:2048,split_working_bytes:4*1024*1024,..Default::default()}).unwrap();
            // A fixed valid two-leaf topology avoids random clustering and the
            // unrelated oversized-single-posting persistence path retained in negative logs.
            let (_,node)=owner.kernel.take_reference_node(0).unwrap();let TreeNode::Leaf(mut first)=node else{unreachable!()};first.parent_id=Some(1);owner.kernel.install_reference_node(0,TreeNode::Leaf(first));
            let center=vec![1000.;32];owner.kernel.install_reference_node(2,TreeNode::Leaf(LeafNode{centroid:center.clone(),centroid_code:Code::<1>::quantize(&center,&vec![0.;32]).as_ref().to_vec(),ids:vec![],versions:vec![],codes:vec![],length:0,parent_id:Some(1)}));
            owner.kernel.install_reference_node(1,TreeNode::Internal(super::super::super::common::InternalNode{centroid:vec![0.;32],centroid_code:Code::<1>::quantize(&vec![0.;32],&vec![0.;32]).as_ref().to_vec(),children:vec![0,2],parent_id:None}));
            owner.kernel.root_id.store(1,Ordering::Relaxed);owner.kernel.next_node_id.store(3,Ordering::Relaxed);owner.kernel.mark_node_dirty(1);owner.kernel.mark_node_dirty(2);
            owner.add_phase(&input[..257],4).unwrap();owner.balance_phase(4).unwrap();let ids=owner.save(&p).await.unwrap();let old=checked_reader(&p,ids,257).await;let old_hits=old.rerank_selected(&input[0].1,&(0..257).collect::<Vec<_>>()).await.unwrap();
            assert!(owner.reshard(128).unwrap()>0,"actual move between completed logical cuts");assert!(owner.kernel.owner_backend.as_ref().unwrap().authority.iter().any(|v|*v.value()==1));
            owner.add_phase(&input[257..],4).unwrap();owner.balance_phase(4).unwrap();
            let ids=owner.save(&p).await.unwrap();let saved=checked_reader(&p,ids,513).await;for(id,v) in &input{assert!(saved.verify_vector_bits(*id,v).await.unwrap());}
            assert_eq!(old.rerank_selected(&input[0].1,&(0..257).collect::<Vec<_>>()).await.unwrap(),old_hits);
            let b=owner.kernel.owner_backend.as_ref().unwrap();let size=Code::<1>::size(32);let mut rows=vec![];for leaf_id in [0,2]{let leaf=b.snapshot(leaf_id).unwrap();rows.extend(leaf.ids.iter().enumerate().map(|(i,id)|(*id,leaf.versions[i],leaf.codes[i*size..(i+1)*size].to_vec())));}rows.sort();
            assert!(rows.iter().all(|(id,v,_)|b.current(*id)==Some(*v)));let report=owner.report();let vectors=&report["bounded_vector_store"];
            assert_eq!(vectors["staged_storage_chunks"],if grain==64{9}else{3});assert_eq!(vectors["staged_logical_bytes"],513*32*4);assert_eq!(report["phase_count"],2);assert_eq!(report["reference_splits"],0);assert_eq!(report["reference_merges"],0);assert!(vectors["peak_pending_handles"].as_u64().unwrap()<=grain as u64);
            println!("STAGING_GRAIN_FIXED grain={} {}",grain,report);outcomes.push(rows);
        }
        assert_eq!(outcomes[0],outcomes[1],"same current IDs, versions and per-ID codes in fixed no-split decisions");
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_staging_byte_budget_rejects_before_root_or_authority(){
        use super::super::owner_vector_store::staging_working_bytes;
        assert_eq!(staging_working_bytes(8192,768).unwrap(),102825984);
        assert!(staging_working_bytes(usize::MAX,768).is_err());
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,64).unwrap();
        let bad=VectorLimits{append_rows:8192,staging_working_bytes:1,..Default::default()};assert!(owner.enable_bounded_vectors(p.clone(),bad).is_err());assert!(owner.kernel.owner_backend.as_ref().unwrap().vector_store().is_none());assert!(owner.ready);
        owner.enable_bounded_vectors(p,VectorLimits{append_rows:1,..Default::default()}).unwrap();owner.begin_add_phase(2,2).unwrap();assert!(owner.add_chunk(&[(0,vec![0.;32]),(1,vec![1.;32])]).is_err());
        let b=owner.kernel.owner_backend.as_ref().unwrap();assert!(b.authority.is_empty());assert!(b.vector_store().unwrap().latest().is_err());assert!(owner.save(&provider(dir.path())).await.is_err());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_chunked_logical_cuts_bounded_history_and_saved_readers(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut c=config();c.split_threshold=128;c.merge_threshold=16;
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,c.clone(),2,4096).unwrap();
        let limits=VectorLimits{split_rows:1024,split_working_bytes:4*1024*1024,..Default::default()};
        owner.enable_bounded_vectors(p.clone(),limits).unwrap();owner.set_batched_scrub_validation(true).unwrap();owner.set_predicate_batching(true).unwrap();owner.set_batched_split_input_validation(true).unwrap();
        let input:Vec<_>=(0..743u32).map(|id|(id,(0..32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.}).collect::<Vec<_>>())).collect();
        // 641 rows crosses ten storage boundaries and exceeds the old256 split cap.
        owner.begin_add_phase(641,4).unwrap();
        assert!(owner.add_chunk(&[(1,vec![0.;32])]).is_err());assert!(owner.kernel.owner_backend.as_ref().unwrap().authority.is_empty());
        assert!(owner.balance_phase(4).is_err());assert!(owner.save(&p).await.is_err());
        for chunk in input[..641].chunks(64){owner.add_chunk(chunk).unwrap();}
        assert_eq!(owner.phase_reports[0]["storage_chunks"],11);assert_eq!(owner.insertion_remaining,0);
        owner.balance_phase(4).unwrap();let ids=owner.save(&p).await.unwrap();let old=checked_reader(&p,ids,641).await;
        let q=&input[0].1;let old_hits=old.rerank_selected(q,&(0..641).collect::<Vec<_>>()).await.unwrap();
        for start in (641..743).step_by(3){owner.add_phase(&input[start..(start+3).min(743)],4).unwrap();owner.balance_phase(4).unwrap();owner.reshard(256).unwrap();}
        let r=owner.report();assert_eq!(r["phase_count"],35);assert_eq!(r["phase_reports_retained"],32);assert_eq!(r["phase_reports_dropped"],3);assert_eq!(r["bounded_vector_store"]["split_working_active_bytes"],0);assert!(r["bounded_vector_store"]["split_working_peak_reserved_bytes"].as_u64().unwrap()>0);
        assert!(owner.kernel.embeddings.is_empty());let ids=owner.save(&p).await.unwrap();let saved=checked_reader(&p,ids,743).await;
        for (id,v) in &input{assert!(saved.verify_vector_bits(*id,v).await.unwrap());}
        assert_eq!(old.rerank_selected(q,&(0..641).collect::<Vec<_>>()).await.unwrap(),old_hits);
        for node in owner.kernel.nodes.iter(){if let TreeNode::Leaf(_)=node.value(){let leaf=owner.kernel.owner_backend.as_ref().unwrap().snapshot(*node.key()).unwrap();for (i,id) in leaf.ids.iter().enumerate(){let expected=Code::<1>::quantize(&input[*id as usize].1,&leaf.centroid);assert_eq!(&leaf.codes[i*expected.as_ref().len()..(i+1)*expected.as_ref().len()],expected.as_ref());}}}
        println!("CHUNKED_CONSTRUCTION {}",r);
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_phase_reservation_and_split_budget_reject_before_mutation(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,1_000_000).unwrap();
        owner.set_physical_capacity(128).unwrap();owner.enable_bounded_vectors(p,VectorLimits::default()).unwrap();
        assert!(owner.begin_add_phase(65,4).is_err());assert!(owner.ready);assert_eq!(owner.next,0);let b=owner.kernel.owner_backend.as_ref().unwrap();assert!(b.authority.is_empty());assert!(b.vector_store().unwrap().latest().is_err());
        let store=b.vector_store().unwrap();let first=store.reserve_split(256).unwrap();assert!(store.reserve_split(257).is_err());drop(first);assert_eq!(store.report()["split_working_active_bytes"],0);
        let admitted=VectorLimits{split_working_bytes:200_000,..Default::default()};let limited=super::super::owner_vector_store::OwnerVectorStore::new(provider(dir.path()),32,2,admitted).unwrap();let active=limited.reserve_split(1).unwrap();assert!(limited.reserve_split(1).is_err());drop(active);assert!(limited.reserve_split(1).is_ok());
        let limits=VectorLimits{split_rows:100_000,split_working_bytes:1,..Default::default()};assert!(super::super::owner_vector_store::OwnerVectorStore::new(provider(dir.path()),768,2,limits).is_err());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_cache_crossing_move_mutation_full_saved_census_and_old_reader(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());
        let rows:Vec<_>=(0..1024u32).map(|id|(0..32u32).map(|d|if d==2{-0.}else{((id*17+d*13)%97) as f32/19.+if d==0{(id%16) as f32*16.}else{0.}}).collect::<Vec<_>>()).collect();
        let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,1024).unwrap();owner.enable_bounded_vectors(p.clone(),VectorLimits::default()).unwrap();owner.set_batched_scrub_validation(true).unwrap();owner.set_predicate_batching(true).unwrap();owner.set_live_prefix_validation(true).unwrap();owner.set_batched_split_input_validation(true).unwrap();
        let mut reference=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,config());let mut old=None;let mut old_result=None;let mut moves=0;let mut splits_at_move=0;
        for start in (0..1024).step_by(64){
            if start==960{let mut merged=config();merged.split_threshold=64;merged.merge_threshold=24;owner.configure_phase(merged.clone()).unwrap();reference.config=merged;} 
            let batch:Vec<_>=(start..start+64).map(|id|(id as u32,rows[id].clone())).collect();owner.add_phase(&batch,4).unwrap();owner.balance_phase(4).unwrap();
            std::thread::scope(|scope|{for batch in batch.chunks(16){let w=&reference;scope.spawn(move||for (id,v) in batch{w.add(*id,v);});}});reference.balance_index_parallel(4);
            assert!(owner.kernel.embeddings.is_empty());
            if start==448{let ids=owner.save(&p).await.unwrap();let r=checked_reader(&p,ids,512).await;old_result=Some(r.rerank_selected(&rows[0],&(0..512).collect::<Vec<_>>()).await.unwrap());old=Some(r);}
            let moved=owner.reshard(96).unwrap();if moved>0 && splits_at_move==0{splits_at_move=owner.report()["reference_splits"].as_u64().unwrap();}moves+=moved;
        }
        assert!(moves>0);assert!(owner.report()["reference_splits"].as_u64().unwrap()>splits_at_move,"structural mutation after actual move");
        let saved=owner.save(&p).await.unwrap();let reader=checked_reader(&p,saved.clone(),1024).await;
        let reference_saved=reference.commit(&p,None).await.unwrap().flush().await.unwrap();let reference_reader=checked_reader(&p,reference_saved,1024).await;
        for (id,v) in rows.iter().enumerate(){assert!(reader.verify_vector_bits(id as u32,v).await.unwrap());assert!(reference_reader.verify_vector_bits(id as u32,v).await.unwrap());}
        assert_eq!(reader.rerank_selected(&rows[0],&(0..1024).collect::<Vec<_>>()).await.unwrap(),reference_reader.rerank_selected(&rows[0],&(0..1024).collect::<Vec<_>>()).await.unwrap());
        assert_eq!(old.unwrap().rerank_selected(&rows[0],&(0..512).collect::<Vec<_>>()).await.unwrap(),old_result.unwrap());
        let b=owner.kernel.owner_backend.as_ref().unwrap();let pl=p.read::<u32,chroma_types::hierarchical_spann::HierarchicalSpannPostingList<'_>>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(saved.posting_list_id,String::new())).await.unwrap();
        let versions=p.read::<u32,u32>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(saved.scalar_metadata_id,String::new())).await.unwrap();
        for entry in owner.kernel.nodes.iter(){if matches!(entry.value(),TreeNode::Leaf(_)){let leaf=b.snapshot(*entry.key()).unwrap();let persisted=pl.get("",*entry.key()).await.unwrap().unwrap();assert_eq!(persisted.ids,&leaf.ids);assert_eq!(persisted.versions,&leaf.versions);assert_eq!(persisted.codes,&leaf.codes);for (i,(&id,&v)) in leaf.ids.iter().zip(&leaf.versions).enumerate(){assert_eq!(versions.get(PREFIX_VERSION,id).await.unwrap().unwrap() as u8,b.current(id).unwrap());let code=Code::<1>::quantize(&rows[id as usize],&leaf.centroid);assert_eq!(&leaf.codes[i*code.as_ref().len()..(i+1)*code.as_ref().len()],code.as_ref());let _=v;}}}
        let fetched=b.fetch_vectors(&[19,1,19,1023]).unwrap();for (v,id) in fetched.iter().zip([19,1,19,1023]){assert_eq!(bits(v),bits(&rows[id]));}
        // An exact recorded reference decision also validates owned tuple order.
        let ordered=[19u32,1,57,200,1023];let fetched=b.fetch_vectors(&ordered).unwrap();
        let expected:Vec<chroma_index::spann::utils::EmbeddingPoint>=ordered.iter().map(|id|(*id,b.current(*id).unwrap() as u32,Arc::from(rows[*id as usize].clone()))).collect();
        let mut expected_result=None;
        let decisions=super::super::live_owner_tests::record_splits(||{expected_result=Some(super::super::live_owner_tests::reference_split(expected,&DistanceFunction::Euclidean));});
        assert_eq!(decisions.len(),1);
        let actual=super::super::live_owner_tests::replay_one_split(decisions[0].clone(),ordered.iter().zip(fetched).map(|(id,v)|(*id,b.current(*id).unwrap() as u32,v)).collect(),&DistanceFunction::Euclidean);
        let expected=expected_result.unwrap();assert_eq!(bits(&actual.0),bits(&expected.0));assert_eq!(bits(&actual.2),bits(&expected.2));
        for (a,e) in [(&actual.1,&expected.1),(&actual.3,&expected.3)]{assert_eq!(a.iter().map(|v|(v.0,v.1,bits(&v.2))).collect::<Vec<_>>(),e.iter().map(|v|(v.0,v.1,bits(&v.2))).collect::<Vec<_>>());}
        let report=owner.report();assert!(report["reference_merges"].as_u64().unwrap()>0,"supported merge vector path");assert_eq!(report["shared_embedding_cache_logical_bytes"],0);let vectors=&report["bounded_vector_store"];assert!(vectors["reader_opens"].as_u64().unwrap()>32);assert!(vectors["retained_cache_owned_payload_bytes"].as_u64().unwrap()<=8192);assert!(vectors["peak_fetch_workers"].as_u64().unwrap()<=4);assert!(vectors["peak_fetch_slots_per_owner"].as_u64().unwrap()<=2);assert_eq!(vectors["fetch_errors"],0);assert!(vectors["provider_traffic"]["storage_callbacks"].as_u64().unwrap()>0);assert!(vectors["provider_traffic"]["decode_successes"].as_u64().unwrap()>0);println!("BOUNDED_VECTOR_SEQUENCE provider_cache_policy=nop {}",report);
        let store=b.vector_store().unwrap();let root=store.latest().unwrap();assert!(owner.add_phase(&[(0,rows[0].clone())],1).is_err());assert_eq!(store.latest().unwrap(),root);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||owner.kernel.insert_embedding(0,Arc::from(rows[0].clone())))).is_err());assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||owner.kernel.delete(0))).is_err());assert!(owner.kernel.embeddings.is_empty());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_immutable_fetch_transfer_visibility_and_lock_progress(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let b=Arc::new(Backend::new(2));let store=Arc::new(super::super::owner_vector_store::OwnerVectorStore::new(p,32,2,VectorLimits::default()).unwrap());*b.vector_store.lock()=Some(store.clone());
        let input=vec![(0,vec![-0.;32]),(1,vec![3.;32])];store.stage(&input).unwrap();assert!(b.current(0).is_none(),"durable staging is not current authority");
        for leaf in [10,20]{b.leaf_owner.insert(leaf,usize::from(leaf==20));b.install(leaf,LeafNode{centroid:vec![0.;32],centroid_code:Code::<1>::quantize(&vec![0.;32],&vec![0.;32]).as_ref().to_vec(),ids:vec![],versions:vec![],codes:vec![],parent_id:None,length:0});}
        for (id,v) in &input{assert!(b.register(10,*id,1,v).is_some());}store.finish_stage();let (source,handle)=b.resolve_vector(0).unwrap();assert_eq!(source,0);
        let(entered_tx,entered_rx)=mpsc::channel();let(resume_tx,resume_rx)=mpsc::channel();store.pause_next_fetch(entered_tx,resume_rx);
        std::thread::scope(|scope|{let w=b.clone();let fetch=scope.spawn(move||w.fetch_vectors(&[0]).unwrap());entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            assert_eq!(b.bump(0,Some(1)),Some(2));assert!(b.register(20,0,2,&input[0].1).is_some());assert_eq!(b.current(0),Some(2));resume_tx.send(()).unwrap();assert_eq!(bits(&fetch.join().unwrap()[0]),bits(&input[0].1));});
let (destination,current_handle)=b.resolve_vector(0).unwrap();assert_eq!(destination,1);assert_eq!(handle,current_handle);assert_eq!(bits(&store.fetch(source,&[handle.clone()]).unwrap()[0]),bits(&input[0].1));
        std::thread::scope(|scope|{let(tx,rx)=mpsc::channel();for reverse in [false,true]{let b=b.clone();let tx=tx.clone();scope.spawn(move||{for _ in 0..32{let ids=if reverse{[1,0]}else{[0,1]};assert_eq!(b.fetch_vectors(&ids).unwrap().len(),2);}tx.send(()).unwrap();});}for _ in 0..2{rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();}});
        let mut missing=handle.clone();missing.key=999;assert!(store.fetch(0,&[missing]).is_err());b.authority.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.resolve_vector(0))).is_err());b.authority.insert(0,1);b.actor(1).call(|s|s.vectors.remove(&0));assert!(b.resolve_vector(0).is_err());
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_capacity_and_integrity_failure_cannot_publish(){
        for missing in [false,true]{
            let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut c=config();c.split_threshold=8;c.merge_threshold=2;
            let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,c,2,128).unwrap();let mut limits=VectorLimits::default();if !missing{limits.split_rows=16;}
            owner.enable_bounded_vectors(p.clone(),limits).unwrap();
            let rows:Vec<_>=(0..64).map(|id|(id,vec![id as f32;32])).collect();
            assert!(owner.add_phase(&vec![(0,vec![0.;32]);65],4).is_err());assert!(owner.kernel.owner_backend.as_ref().unwrap().vector_store().unwrap().latest().is_err());
            owner.add_phase(&rows,4).unwrap();let b=owner.kernel.owner_backend.as_ref().unwrap();
            if missing{b.actor(0).call(|s|s.vectors.remove(&0));}
            assert!(owner.balance_phase(4).is_err());assert!(owner.failed);assert!(!owner.ready);assert!(owner.save(&p).await.is_err());assert!(owner.balance_phase(4).is_err());assert!(owner.reshard(32).is_err());
            if !missing{assert_eq!(owner.kernel.nodes.len(),1);assert!(owner.kernel.nodes.contains_key(&0));assert!(owner.kernel.tombstones.is_empty(),"capacity rejected before source detach");}
        }
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_missing_first_registration_preserves_unpublished_authority_and_version(){
        let mut outcomes=vec![];
        for bounded in [false,true]{
            let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let mut owner=ConcurrentOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,64).unwrap();let vector:Vec<_>=(0..32).map(|i|if i==3{-0.}else{i as f32/7.}).collect();
            if bounded{owner.enable_bounded_vectors(p,VectorLimits::default()).unwrap();owner.kernel.owner_backend.as_ref().unwrap().vector_store().unwrap().stage(&[(0,vector.clone())]).unwrap();}
            let b=owner.kernel.owner_backend.as_ref().unwrap().clone();let(entered_tx,entered_rx)=mpsc::channel();let(resume_tx,resume_rx)=mpsc::channel();*b.first_registration_pause.lock()=Some((entered_tx,resume_rx));
            std::thread::scope(|scope|{let kernel=&owner.kernel;let v=&vector;let add=scope.spawn(move||kernel.add(0,v));entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
                assert!(b.snapshot(0).unwrap().ids.is_empty(),"failed target adds no posting");
                if bounded{assert!(b.current(0).is_none());assert!(!b.authority.contains_key(&0));for actor in &b.actors{assert!(actor.call(|s|s.current.read().is_empty() && s.vectors.is_empty()));}assert!(owner.kernel.embeddings.is_empty());}
                else{assert_eq!(b.current(0),Some(1));}
                resume_tx.send(()).unwrap();add.join().unwrap();});
            assert_eq!(owner.kernel.stats.add_missing_nodes.load(Ordering::Relaxed),1);assert_eq!(b.current(0),Some(2));let leaf=b.snapshot(0).unwrap();assert_eq!(leaf.ids,vec![0]);assert_eq!(leaf.versions,vec![2]);
            if bounded{assert!(b.resolve_vector(0).is_ok());assert_eq!(bits(&b.fetch_vectors(&[0]).unwrap()[0]),bits(&vector));assert!(owner.kernel.embeddings.is_empty());}
            outcomes.push((leaf.ids,leaf.versions,leaf.codes));
        }
        assert_eq!(outcomes[0],outcomes[1],"same eventual reference version/posting/code after one missing target");
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn bounded_vectors_fixed_tape_batch_reuse_and_alternating_roots_pin_budget(){
        use super::super::owner_vector_store::OwnerVectorStore;
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let b=Arc::new(Backend::new(2));let creator=Arc::new(OwnerVectorStore::new(p.clone(),32,2,VectorLimits::default()).unwrap());*b.vector_store.lock()=Some(creator.clone());
        for leaf in [10,20]{b.leaf_owner.insert(leaf,usize::from(leaf==20));b.install(leaf,LeafNode{centroid:vec![0.;32],centroid_code:Code::<1>::quantize(&vec![0.;32],&vec![0.;32]).as_ref().to_vec(),ids:vec![],versions:vec![],codes:vec![],parent_id:None,length:0});}
        let rows:Vec<_>=(0..192u32).map(|id|(0..32).map(|d|if d==2{-0.}else{id as f32+d as f32/17.}).collect::<Vec<_>>()).collect();
        for start in (0..192).step_by(64){let batch:Vec<_>=(start..start+64).map(|id|(id as u32,rows[id].clone())).collect();creator.stage(&batch).unwrap();for (id,v) in &batch{assert!(b.register(10,*id,1,v).is_some());}creator.finish_stage();}
        let old=b.resolve_vector(0).unwrap().1;
        for id in 0..16{assert_eq!(b.bump(id,Some(1)),Some(2));assert!(b.register(20,id,2,&rows[id as usize]).is_some());}
        assert_eq!(b.resolve_vector(0).unwrap(),(1,old.clone()));
        let contiguous:Vec<(usize,Vec<u32>)>=vec![(0,(16..32).collect()),(0,(64..80).collect()),(0,(128..144).collect()),(0,(32..48).collect()),(1,(0..16).collect())];
        let alternating=vec![(0,(0..16u32).map(|i|if i%2==0{48+i/2}else{96+i/2}).collect::<Vec<_>>())];
        for (name,tape) in [("contiguous",contiguous),("alternating",alternating)]{
            let mut reports=vec![];let mut results=vec![];
            for reuse in [false,true]{
                let store=OwnerVectorStore::new(p.clone(),32,2,VectorLimits::default()).unwrap();let mut output=vec![];
                for (owner,ids) in &tape{let handles:Vec<_>=ids.iter().map(|id|{let(actual,h)=b.resolve_vector(*id).unwrap();assert_eq!(actual,*owner);h}).collect();let values=if reuse{store.fetch(*owner,&handles)}else{store.fetch_point_reference(*owner,&handles)}.unwrap();for (id,v) in ids.iter().zip(values){assert_eq!(bits(&v),bits(&rows[*id as usize]));output.push((*id,bits(&v)));}}
                let report=store.report();assert_eq!(report["fetch_errors"],0);assert_eq!(report["reader_pin_admission_rejections"],0);assert!(report["peak_single_reader_pinned_bytes"].as_u64().unwrap()<=262144);assert!(report["retained_cache_owned_payload_bytes"].as_u64().unwrap()<=8192);assert_eq!(report["active_fetch_workers"],0);reports.push(report);results.push(output);
            }
            assert_eq!(results[0],results[1]);assert_eq!(reports[0]["logical_gets"],reports[1]["logical_gets"]);assert_eq!(reports[0]["cache_hits"],reports[1]["cache_hits"]);assert_eq!(reports[0]["copied_return_bytes"],reports[1]["copied_return_bytes"]);
            if name=="contiguous"{assert!(reports[1]["reader_opens"].as_u64().unwrap()<reports[0]["reader_opens"].as_u64().unwrap());assert!(reports[1]["provider_traffic"]["decode_successes"].as_u64().unwrap()<reports[0]["provider_traffic"]["decode_successes"].as_u64().unwrap());}
            else{assert_eq!(reports[0]["reader_opens"],reports[1]["reader_opens"]);assert_eq!(reports[0]["provider_traffic"],reports[1]["provider_traffic"]);}
            println!("BOUNDED_VECTOR_FIXED_REUSE {}",serde_json::json!({"case":name,"requests":tape,"point_reader":reports[0],"batch_reuse":reports[1]}));
        }
        // A small admission budget rejects after decode without hiding that
        // transient. No copied result/cache entry escapes the rejected fetch.
        let mut small=VectorLimits::default();small.reader_pin_bytes=1024;let store=OwnerVectorStore::new(p.clone(),32,2,small).unwrap();assert!(store.fetch(1,&[old.clone()]).is_err());let report=store.report();assert!(report["peak_single_reader_pinned_bytes"].as_u64().unwrap()>1024);assert_eq!(report["retained_cache_owned_payload_bytes"],0);assert_eq!(report["active_fetch_workers"],0);assert_eq!(report["provider_traffic_complete"],false);assert_eq!(report["reader_pin_admission_rejections"],1);println!("BOUNDED_VECTOR_PIN_REJECTION {}",report);
        let store=OwnerVectorStore::new(p,32,2,VectorLimits::default()).unwrap();let mut wrong=b.resolve_vector(96).unwrap().1;wrong.root=old.root;assert!(store.fetch(0,&[wrong]).is_err(),"no fallback to a later generation/root");
    }

}

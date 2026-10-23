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
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

type Job = Box<dyn FnOnce(&mut OwnerState) + Send>;
#[derive(Default)]
struct OwnerState {
    leaves: BTreeMap<u32, LeafNode>,
    current: Arc<parking_lot::RwLock<BTreeMap<u32, u8>>>,
    physical: usize,
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
pub(crate) struct Backend {
    key: usize,
    actors: Vec<Actor>,
    leaf_owner: DashMap<u32, usize>,
    id_owner: DashMap<u32, usize>,
    id_locks: DashMap<u32, Arc<Mutex<()>>>,
    add_navigation: Arc<Work>,
    balance_leaf: Arc<Work>,
    registrations: AtomicU64,
    predicate_calls: AtomicU64,
    current_requests: AtomicU64,
    direct_current_reads: AtomicU64,
    direct_current_policy: AtomicUsize,
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
    fn new(owners: usize) -> Self {
        let actor_work = Arc::new(Work::default());
        let message_timing=Arc::new(MessageTiming::default());
        Self {
            key: NEXT_BACKEND.fetch_add(1, Ordering::Relaxed),
            actors: (0..owners)
                .map(|_| Actor::new(actor_work.clone(),message_timing.clone()))
                .collect(),
            leaf_owner: DashMap::new(),
            id_owner: DashMap::new(),
            id_locks: DashMap::new(),
            add_navigation: Arc::default(),
            balance_leaf: Arc::default(),
            registrations: AtomicU64::new(0),
            predicate_calls: AtomicU64::new(0),
            current_requests: AtomicU64::new(0),
            direct_current_reads: AtomicU64::new(0),
            direct_current_policy: AtomicUsize::new(0),
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
    fn lock_for(&self, id: u32) -> Arc<Mutex<()>> {
        self.id_locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
        let owner = self.id_owner.get(&id).map(|v| *v)?;
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
        let owner = *self.id_owner.entry(id).or_insert(0);
        self.actor(owner).call_op(Operation::VersionMutation,move |s| {
            let mut versions=s.current.write();
            let value = versions.entry(id).or_insert(expected.unwrap_or(0));
            if *value & DELETED_BIT != 0 || expected.is_some_and(|v| v != *value) {
                return None;
            }
            *value = value.wrapping_add(1) & 0x7f;
            Some(*value)
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
        self.actor(owner).call_op(Operation::Posting,move |s| {
            let old = s.leaves.get(&id).map(|l| l.ids.len()).unwrap_or(0);
            let count = s.physical - old + leaf.ids.len();
            assert!(count <= 1048576, "owner physical capacity");
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
        let owner = self.leaf_owner.get(&leaf).map(|v| *v)?;
        self.registration_vector_copy_bytes
            .fetch_add((embedding.len() * 4) as u64, Ordering::Relaxed);
        let values: Arc<[f32]> = Arc::from(embedding);
        let lock = self.lock_for(id);
        let _guard = lock.lock();
        let length = self.actor(owner).call_op(Operation::Registration,move |s| {
            assert!(s.physical < 1048576, "owner physical capacity");
            let leaf = s.leaves.get_mut(&leaf)?;
            let code = Code::<1>::quantize(&values, &leaf.centroid);
            leaf.ids.push(id);
            leaf.versions.push(version);
            leaf.codes.extend_from_slice(code.as_ref());
            leaf.length += 1;
            s.physical += 1;
            Some(leaf.length)
        })?;
        let from = *self.id_owner.get(&id).expect("current owner missing");
        if from != owner {
            let current = self.actor(from).call_op(Operation::Registration,move |s| s.current.read().get(&id).copied());
            if current == Some(version) {
                self.actor(from).call_op(Operation::Registration,move |s| {
                    s.current.write().remove(&id);
                });
                #[cfg(test)]
                if let Some((removed,resume))=self.transfer_pause.lock().take(){removed.send(()).unwrap();resume.recv().unwrap();}
                self.actor(owner).call_op(Operation::Registration,move |s| {
                    s.current.write().insert(id, version);
                });
                self.id_owner.insert(id, owner);
            }
        }
        self.registrations.fetch_add(1, Ordering::Relaxed);
        Some(length)
    }
    /// The caller retains the source node's mutable guard through this whole
    /// operation. No callback takes node/ID locks or waits on another owner.
    fn validate_scrub_batch(&self, rows:&[(u32,u8)])->Vec<bool> {
        assert!(rows.len()<=64,"scrub validation capacity");
        self.scrub_validation_batches.fetch_add(1,Ordering::Relaxed);
        self.scrub_validation_rows.fetch_add(rows.len() as u64,Ordering::Relaxed);
        self.current_requests.fetch_add(rows.len() as u64,Ordering::Relaxed);
        let mut ids:Vec<_>=rows.iter().map(|v|v.0).collect();ids.sort_unstable();ids.dedup();
        let locks:Vec<_>=ids.iter().map(|id|self.lock_for(*id)).collect();
        let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();
        let mut grouped:BTreeMap<usize,Vec<(usize,u32)>>=BTreeMap::new();
        for (slot,(id,_)) in rows.iter().enumerate(){if let Some(owner)=self.id_owner.get(id){grouped.entry(*owner).or_default().push((slot,*id));}}
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
    next: u32,
    ready: bool,
    phase_reports: Vec<serde_json::Value>,
}
impl ConcurrentOwnerWriter {
    pub fn new(
        dim: usize,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        owners: usize,
        max_rows: usize,
    ) -> Result<Self> {
        ensure!(
            owners > 0 && owners <= 8 && max_rows > 0 && max_rows <= 8192 && dim > 0 && dim <= 768,
            "owner adapter capacity"
        );
        ensure!(
            config.write_navigation == super::super::config::NavigationMode::Fp
                && config.fp_npa
                && config.max_replicas == 1,
            "owner adapter requires explicit FP and one current copy"
        );
        let mut kernel = HierarchicalSpannWriter::new(dim, metric, config);
        let backend = Arc::new(Backend::new(owners));
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
            next: 0,
            ready: true,
            phase_reports: vec![],
        })
    }
    /// The opt-in policy changes pure predicate dispatch only at a joined boundary.
    pub fn set_predicate_batching(&mut self, enabled: bool) -> Result<()> {
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
        self.kernel.owner_backend.as_ref().unwrap().direct_current_policy.store(usize::from(enabled),Ordering::Relaxed);Ok(())
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
        self.kernel.config = config;
        Ok(())
    }
    pub fn add_phase(&mut self, rows: &[(u32, Vec<f32>)], workers: usize) -> Result<()> {
        ensure!(
            self.ready
                && self.phase_reports.len() < 32
                && workers > 0
                && workers <= 8
                && !rows.is_empty()
                && self.next as usize + rows.len() <= self.max_rows,
            "add phase capacity/state"
        );
        for (offset, (id, vector)) in rows.iter().enumerate() {
            ensure!(
                *id == self.next + offset as u32
                    && vector.len() == self.kernel.dim
                    && vector.iter().all(|v| v.is_finite()),
                "new dense IDs/vector shape"
            );
        }
        let backend = self.kernel.owner_backend.as_ref().unwrap();
        for actor in &backend.actors {
            let physical = actor.call(|s| s.physical);
            ensure!(
                physical
                    .checked_add(rows.len() * 2)
                    .is_some_and(|v| v <= 1048576),
                "owner physical reservation exhausted"
            );
        }
        self.ready = false;
        let started = Instant::now();
        let size = rows.len().div_ceil(workers);
        std::thread::scope(|scope| {
            for rows in rows.chunks(size) {
                let kernel = &self.kernel;
                scope.spawn(move || {
                    for (id, v) in rows {
                        kernel.add(*id, v);
                    }
                });
            }
        });
        self.next += rows.len() as u32;
        self.phase_reports.push(serde_json::json!({"rows_through":self.next,"add_seconds":started.elapsed().as_secs_f64(),"add_workers":workers}));
        Ok(())
    }
    pub fn balance_phase(&mut self, workers: usize) -> Result<()> {
        ensure!(
            !self.ready && workers > 0 && workers <= 8,
            "balance phase state"
        );
        let started = Instant::now();
        self.kernel.balance_index_parallel(workers);
        let phase = self.phase_reports.last_mut().unwrap();
        phase["balance_seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
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
            let payload = b.actor(source).call(move |s| {
                let v = s.leaves.remove(&leaf).expect("move leaf missing");
                s.physical -= v.ids.len();
                v
            });
            let mut current = vec![];
            for (&id, &version) in payload.ids.iter().zip(&payload.versions) {
                let owner = b.id_owner.get(&id).map(|v| *v);
                if owner == Some(source) && b.current(id) == Some(version) {
                    let value = b
                        .actor(source)
                        .call(move |s| s.current.write().remove(&id).unwrap());
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
                b.id_owner.insert(*id, dest);
            }
            b.actor(dest).call(move |s| {
                s.physical += payload.ids.len();
                assert!(s.physical <= 1048576, "destination physical capacity");
                assert!(s.leaves.insert(leaf, payload).is_none());
                for (id, version) in current {
                    assert!(s.current.write().insert(id, version).is_none());
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
    /// A joined snapshot is the only place that materializes all owner postings
    /// together. It is publication cost, never a mutable construction fallback.
    pub async fn save(
        &self,
        provider: &chroma_blockstore::provider::BlockfileProvider,
    ) -> Result<super::HierarchicalSpannIds> {
        ensure!(self.ready, "save requires joined balance");
        let b = self.kernel.owner_backend.as_ref().unwrap();
        let snapshot = HierarchicalSpannWriter::new(
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
        let mut report = serde_json::json!({"depth":depths.values().max(),"leaf_physical_occupancy":occupancy,"internal_child_counts":internal_children,"rows":self.next,"phases":self.phase_reports,"owners":owners,"nodes":self.kernel.nodes.len(),"pending_balance":self.kernel.pending_balance_leaves(),"scheduler_rounds":b.schedule_rounds.load(Ordering::Relaxed),"last_reference_subtree_assignments":*b.last_schedule.lock(),"schedule_sample_overflow":b.schedule_sample_overflow.load(Ordering::Relaxed),"actual_subtree_jobs":b.subtrees.calls.load(Ordering::Relaxed)});
        report.as_object_mut().unwrap().extend(serde_json::json!({"peak_distinct_subtree_workers":b.subtrees.peak.load(Ordering::Relaxed),"reference_splits":self.kernel.stats.splits.load(Ordering::Relaxed),"reference_merges":self.kernel.stats.merges.load(Ordering::Relaxed),"reference_balance_rounds":self.kernel.stats.balance_rounds.load(Ordering::Relaxed),"orphan_adoptions":b.orphan_adoptions.load(Ordering::Relaxed),"parent_edits":b.parent_edits.load(Ordering::Relaxed),"reference_missing_retries":self.kernel.stats.add_missing_nodes.load(Ordering::Relaxed),"central_mutable_posting_rows":self.kernel.nodes.iter().map(|v|match v.value(){TreeNode::Leaf(v)=>v.ids.len(),_=>0}).sum::<usize>(),"central_current_versions":self.kernel.versions.len(),"add_navigation_calls":b.add_navigation.calls.load(Ordering::Relaxed),"peak_add_navigation_spans":b.add_navigation.peak.load(Ordering::Relaxed),"balance_leaf_calls":b.balance_leaf.calls.load(Ordering::Relaxed)}).as_object().unwrap().clone());
        report.as_object_mut().unwrap().extend(serde_json::json!({"peak_balance_leaf_spans":b.balance_leaf.peak.load(Ordering::Relaxed),"registrations":b.registrations.load(Ordering::Relaxed),"peak_actual_owner_callbacks":b.actor_work.peak.load(Ordering::Relaxed),"critical_outer_entries":b.critical.calls.load(Ordering::Relaxed),"critical_outer_hold_ns":b.critical.nanos.load(Ordering::Relaxed),"critical_wait_ns":b.critical_wait_ns.load(Ordering::Relaxed),"peak_distinct_critical_threads":b.critical.peak.load(Ordering::Relaxed),"noncritical_npa_entries":b.noncritical.calls.load(Ordering::Relaxed),"noncritical_npa_inclusive_ns":b.noncritical.nanos.load(Ordering::Relaxed),"peak_npa_contexts":b.noncritical.peak.load(Ordering::Relaxed),"predicate_centers_copied_once_per_scope_bytes":b.predicate_center_copy_bytes.load(Ordering::Relaxed),"registration_vector_copy_bytes":b.registration_vector_copy_bytes.load(Ordering::Relaxed)}).as_object().unwrap().clone());
        report.as_object_mut().unwrap().extend(serde_json::json!({"owner_snapshot_copy_bytes":b.snapshot_copy_bytes.load(Ordering::Relaxed),"moved_posting_logical_payload_bytes":b.moved_posting_payload_bytes.load(Ordering::Relaxed),"shared_embedding_cache_logical_bytes":self.kernel.embeddings.iter().map(|v|v.value().len()*4).sum::<usize>(),"owner_predicates":b.predicate_calls.load(Ordering::Relaxed),"predicate_batch_callbacks":b.predicate_batch_callbacks.load(Ordering::Relaxed),"predicate_batch_computed_rows":b.predicate_batch_rows.load(Ordering::Relaxed),"actor_callbacks":b.actor_work.calls.load(Ordering::Relaxed),"live_version_requests":b.current_requests.load(Ordering::Relaxed),"direct_current_reads":b.direct_current_reads.load(Ordering::Relaxed),"direct_current_policy":b.direct_current_policy.load(Ordering::Relaxed)!=0,"predicate_batching":b.predicate_batching_enabled(),"placement_moves":b.moves.load(Ordering::Relaxed),"scope":"Actual local owner actors; reference kernel routing/scheduler and existing structural lock. Distinct-thread spans de-duplicate recursive entries; NPA elapsed includes nested critical work and waits, not CPU. Critical hold spans count outermost same-thread lock ownership only. Shared immutable embedding cache remains central/local. Snapshot publication materializes all joined owner postings. No cross-host claim."}).as_object().unwrap().clone());
        report["scrub_validation_batches"]=serde_json::json!(b.scrub_validation_batches.load(Ordering::Relaxed));
        report["scrub_validation_request_rows"]=serde_json::json!(b.scrub_validation_request_rows.load(Ordering::Relaxed));
        report["scrub_validation_rows"]=serde_json::json!(b.scrub_validation_rows.load(Ordering::Relaxed));
        report["scrub_validation_owner_calls"]=serde_json::json!(b.scrub_validation_owner_calls.load(Ordering::Relaxed));
        report["batched_scrub_validation"]=serde_json::json!(b.scrub_batch_policy.load(Ordering::Relaxed)!=0);
        report["message_timing"]=b.message_timing.snapshot();
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
            b.id_owner.remove(&7);expected.remove(&7);
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
            b.id_owner.insert(0,99);assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||b.current(0))).is_err());b.id_owner.insert(0,1);
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
                assert_eq!(*b.id_owner.get(&0).unwrap(),0);assert!(b.actor(0).versions.read().get(&0).is_none());assert!(b.actor(1).versions.read().get(&0).is_none());
                let before=b.current_requests.load(Ordering::Relaxed);let (tx,rx)=mpsc::channel();let w=b.clone();let read=scope.spawn(move||{tx.send(w.current(0)).unwrap();});
                let start=Instant::now();while b.current_requests.load(Ordering::Relaxed)==before {assert!(start.elapsed()<std::time::Duration::from_secs(5));std::thread::yield_now();}
                assert!(matches!(rx.try_recv(),Err(mpsc::TryRecvError::Empty)));
                resume_tx.send(()).unwrap();assert!(mutation.join().unwrap().is_some());assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),Some(2));read.join().unwrap();
            });
            assert_eq!(*b.id_owner.get(&0).unwrap(),1);assert_eq!(b.current(0),Some(2));
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
        backend.id_owner.insert(5,1);
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

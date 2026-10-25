//! Owner-local mutable records and protocol-backed operations. Payloads stay at
//! the authority that mutates them; global navigation exchanges routing scores.
use super::observability::{counted_contains, Counter, Scope, WorkProfile, WorkSnapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Weak,
};
use std::time::{Duration, Instant};

use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use super::super::{common::ReadBeamPolicy, config::HierarchicalSpannConfig};
use super::protocol::envelope::{Envelope, Phase};
use super::protocol::transfer::{
    DriverDecision, Role, TransferDriver, TransferPayload, TransferProof, TransferRecord,
    TransferState, TransferStep,
};
use super::protocol::{self, AttemptId, AuthorityId, NodeId};
use super::routing::{
    self, Child, ExpandReply, ExpandRequest, ExpansionBackend, ExpansionStatus, NodeRef,
    RoutingNode, ScoredChild,
};
use super::transport::{RequestClass, RpcClient, TransportError};

/// Immutable vector location is independent of the mutable posting owner.
pub trait VectorSource: Send + Sync {
    fn get(&self, id: u64) -> Result<Arc<[f32]>, String>;
}
impl<F> VectorSource for F
where
    F: Fn(u64) -> Result<Arc<[f32]>, String> + Send + Sync,
{
    fn get(&self, id: u64) -> Result<Arc<[f32]>, String> {
        self(id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostingList {
    pub ids: Vec<u64>,
    pub codes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredNode {
    pub routing: RoutingNode,
    pub postings: Option<PostingList>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OwnerCounts {
    pub process_cpu_seconds: Option<f64>,
    pub rss_bytes: u64,
    pub process_peak_rss_bytes: u64,
    pub owner: AuthorityId,
    pub nodes: usize,
    pub leaves: usize,
    pub vectors: u64,
    pub splits: u64,
    pub transfers: u64,
    pub expansions: u64,
    pub distances: u64,
    pub active_peak: u64,
    pub active_workers: u64,
    pub scheduler: BTreeMap<String, u64>,
    pub jobs_started: u64,
    pub jobs_completed: u64,
    pub vector_fetches: u64,
    pub ingestion_id_ranges: usize,
    #[serde(default)]
    pub work: WorkSnapshot,
    #[serde(default)]
    pub reservation_state: BTreeMap<String, u64>,
    #[serde(default)]
    pub clustering_process: serde_json::Value,
    #[serde(default)]
    pub centroid_refresh_longest: Vec<super::shared_runtime::CentroidRefreshDiagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EngineRequest {
    Expand(Vec<ExpandRequest>),
    OwnerTransfer(super::batched_transfer::BatchRequest),
    CurrentRoot,
    CurrentView,
    CurrentViewIfChanged {
        known_sequence: u64,
    },
    Placement(super::placement_runtime::PlacementRequest),
    Shared(super::shared_runtime::SharedRequest),
    BoundaryReady(super::construction::BoundaryPreparation),
    BoundaryFinish {
        envelope: Envelope,
        published: bool,
    },
    BoundaryStatus {
        envelope: Envelope,
    },
    NeighborReassign {
        envelope: Envelope,
        neighbor: NodeRef,
        old: Vec<f32>,
        left: Vec<f32>,
        right: Vec<f32>,
    },
    BalanceJob(super::construction::BalanceJob),
    ReassignJob(super::construction::ReassignJob),
    EnqueueReassign(super::construction::ReassignJob),
    EnqueueNavigationRetry {
        job: super::construction::ReassignJob,
        producer: Option<AttemptId>,
    },
    ScheduledJob(super::construction::WorkJob),
    Eligible,
    Enqueue(super::construction::BalanceJob),
    EnqueueNeighbor(super::construction::NeighborJob),
    Structure(super::construction::StructuralEdit),
    Summary {
        root: NodeRef,
    },
    Handoff {
        envelope: Envelope,
        root: NodeRef,
        destination: AuthorityId,
        new_placement: u64,
    },
    Append {
        envelope: Envelope,
        leaf: NodeRef,
        expected_revision: u64,
        ids: Vec<u64>,
    },
    PrepareTransfer {
        envelope: Envelope,
        payload: TransferPayload,
        role: Role,
        codes: Vec<u8>,
    },
    TransferStep {
        envelope: Envelope,
        role: Role,
        step: TransferStep,
        proof: TransferProof,
    },
    TransferStatus {
        envelope: Envelope,
        role: Role,
    },
    Root {
        root: NodeRef,
    },
    Node {
        node: NodeRef,
    },
    Counts,
    Membership,
    ValidateWriter,
    Snapshot,
    SaveOwner(super::save_runtime::SaveOwnerRequest),
    WriterCheckpoint {
        run: u128,
        epoch: u64,
        path: std::path::PathBuf,
        next_input: u64,
        input_sha256: String,
        balance_pending: bool,
    },
    ReadSavedObject(super::save_runtime::ReadSavedObjectRequest),
    ListNodes,
    SnapshotNodes {
        nodes: Vec<NodeRef>,
    },
    OpenEpoch {
        run: u128,
        epoch: u64,
    },
    CloseEpoch {
        run: u128,
        epoch: u64,
    },
    SealEpoch {
        run: u128,
        epoch: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EngineResponse {
    #[serde(rename = "ExpandedMetadataV2")]
    Expanded(#[serde(with = "super::routing::expansion_codec::batch")] Vec<ExpandReply>),
    Eligible(Vec<(NodeRef, usize, bool)>),
    Structure(super::construction::PublicationResult),
    Summary(Vec<super::placement::OwnershipUnit>),
    Ack,
    Placement(super::placement_runtime::PlacementResponse),
    Shared(super::shared_runtime::SharedResponse),
    BoundaryStatus {
        preparing: bool,
        ready: bool,
        finished: bool,
    },
    Routing(RoutingNode),
    ViewUnchanged {
        sequence: u64,
    },
    View(super::protocol::publication::PublishedView<super::construction::BoundaryView>),
    Transfer(TransferState),
    Counts(OwnerCounts),
    Membership(Vec<(NodeId, Vec<u64>)>),
    Snapshot(Vec<StoredNode>),
    OwnerSaved(super::save_runtime::OwnerSaveReceipt),
    SavedObject(super::save_runtime::SavedObjectChunk),
    Nodes(Vec<NodeRef>),
}

#[derive(Clone, Debug, PartialEq)]
enum Operation {
    Append {
        leaf: NodeRef,
        revision: u64,
        ids: Vec<u64>,
    },
    Placement(Vec<u8>),
    Transfer(TransferPayload),
    Job(Vec<u8>),
}
struct TransferEndpoint {
    record: TransferRecord,
    codes: Vec<u8>,
}
struct TransferReservation {
    envelope: Envelope,
    payload: TransferPayload,
    manager: Arc<super::protocol::reservation::RegionManager>,
    request: super::protocol::reservation::RegionRequest,
    prepare_lock: Arc<Mutex<()>>,
}
struct PhaseOperations {
    phase: Phase<Operation>,
    transfers: BTreeMap<(AttemptId, u8), TransferEndpoint>,
    transfer_regions: BTreeMap<AttemptId, TransferReservation>,
}
pub(super) struct Resident {
    pub(super) routing: Arc<RoutingNode>,
    pub(super) postings: Option<PostingList>,
    pub(super) reserved: Option<AttemptId>,
    pub(super) preparing_split: bool,
    pub(super) transfer_source: Option<NodeId>,
    pub(super) claim_producer: Option<AttemptId>,
}

pub struct OwnerService {
    pub owner: AuthorityId,
    pub run: u128,
    pub(super) batch_transfers:
        Mutex<BTreeMap<AttemptId, Arc<Mutex<super::batched_transfer::BatchEndpoint>>>>,
    #[cfg(test)]
    pub(super) batch_fault: AtomicU64,
    #[cfg(test)]
    pub(super) epoch_open_hook: Mutex<Option<Arc<dyn Fn(bool) + Send + Sync>>>,
    #[cfg(test)]
    pub(super) epoch_open_wait_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    pub(super) batch_chunk_hook: Mutex<Option<Arc<dyn Fn(usize) + Send + Sync>>>,
    pub dim: usize,
    pub metric: DistanceFunction,
    pub config: HierarchicalSpannConfig,
    pub vectors: Arc<dyn VectorSource>,
    pub(super) observability: Arc<WorkProfile>,
    pub(super) nodes: DashMap<NodeId, Arc<RwLock<Resident>>>,
    pub(super) runtime: RwLock<Weak<Cluster>>,
    pub(super) placement: Mutex<super::placement_runtime::OwnerPlacementState>,
    pub(super) shared_state: Mutex<super::shared_runtime::SharedOwnerState>,
    pub(super) boundaries: Mutex<BTreeMap<AttemptId, super::construction::PendingBoundary>>,
    job_records: Mutex<BTreeMap<AttemptId, Arc<Mutex<Option<Result<(), String>>>>>>,
    active_jobs: AtomicU64,
    job_peak: AtomicU64,
    jobs_started: AtomicU64,
    jobs_completed: AtomicU64,
    vector_fetches: AtomicU64,
    pub(super) regions: RwLock<Arc<super::protocol::reservation::RegionManager>>,
    pub(super) routing_gate: RwLock<()>,
    /// Navigation pins immutable routing metadata without waiting for postings.
    pub(super) routing_images: RwLock<BTreeMap<NodeId, Arc<RoutingNode>>>,
    operations: Mutex<PhaseOperations>,
    #[cfg(test)]
    pub(super) transfer_capture_hook: Mutex<Option<Arc<dyn Fn(AttemptId) + Send + Sync>>>,
    #[cfg(test)]
    scheduler_queue_wait_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    pub(super) save_cache: Mutex<Option<super::save_runtime::OwnerSaveCache>>,
    pub(super) save_requests: Mutex<BTreeMap<String, super::save_runtime::SaveOwnerRequest>>,
    pub(super) epoch: AtomicU64,
    sequence: AtomicU64,
    poisoned: AtomicBool,
    poison_reason: Mutex<Option<String>>,
    pub(super) splits: AtomicU64,
    pub(super) transfers: AtomicU64,
    expansions: AtomicU64,
    distances: AtomicU64,
}

impl OwnerService {
    pub fn new(
        owner: AuthorityId,
        run: u128,
        dim: usize,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        vectors: Arc<dyn VectorSource>,
    ) -> Self {
        let observability = Arc::new(WorkProfile::default());
        Self {
            observability: observability.clone(),
            owner,
            run,
            batch_transfers: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            batch_fault: AtomicU64::new(0),
            #[cfg(test)]
            epoch_open_hook: Mutex::new(None),
            #[cfg(test)]
            epoch_open_wait_hook: Mutex::new(None),
            #[cfg(test)]
            batch_chunk_hook: Mutex::new(None),
            dim,
            metric,
            config,
            vectors,
            nodes: DashMap::new(),
            runtime: RwLock::new(Weak::new()),
            placement: Mutex::new(Default::default()),
            shared_state: Mutex::new(Default::default()),
            boundaries: Mutex::new(BTreeMap::new()),
            job_records: Mutex::new(BTreeMap::new()),
            active_jobs: AtomicU64::new(0),
            job_peak: AtomicU64::new(0),
            jobs_started: AtomicU64::new(0),
            jobs_completed: AtomicU64::new(0),
            vector_fetches: AtomicU64::new(0),
            regions: RwLock::new(Arc::new(
                super::protocol::reservation::RegionManager::new_profiled(
                    BTreeMap::new(),
                    1_000_000,
                    observability,
                )
                .expect("empty local forest"),
            )),
            routing_gate: RwLock::new(()),
            routing_images: RwLock::new(BTreeMap::new()),
            operations: Mutex::new(PhaseOperations {
                phase: Phase::new(run, 0, 1_000_000),
                transfers: BTreeMap::new(),
                transfer_regions: BTreeMap::new(),
            }),
            #[cfg(test)]
            transfer_capture_hook: Mutex::new(None),
            #[cfg(test)]
            scheduler_queue_wait_hook: Mutex::new(None),
            save_cache: Mutex::new(None),
            save_requests: Mutex::new(BTreeMap::new()),
            epoch: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            poison_reason: Mutex::new(None),
            splits: AtomicU64::new(0),
            transfers: AtomicU64::new(0),
            expansions: AtomicU64::new(0),
            distances: AtomicU64::new(0),
        }
    }
    #[cfg(test)]
    fn observe_scheduler_queue_wait(&self) {
        let hook = self.scheduler_queue_wait_hook.lock().clone();
        if let Some(hook) = hook {
            hook()
        }
    }
    pub(super) fn admit_placement(&self, e: &Envelope, identity: Vec<u8>) -> Result<bool, String> {
        self.operations
            .lock()
            .phase
            .admit(e.clone(), Operation::Placement(identity))
            .map_err(|x| x.to_string())
    }
    pub(super) fn terminal_placement(&self, e: &Envelope) -> Result<(), String> {
        self.operations
            .lock()
            .phase
            .terminal(e)
            .map_err(|x| x.to_string())
    }
    pub(super) fn vector(&self, id: u64) -> Result<Arc<[f32]>, String> {
        self.vector_fetches.fetch_add(1, Ordering::Relaxed);
        self.vectors.get(id)
    }
    fn enqueue_reassignment(
        &self,
        job: super::construction::ReassignJob,
        navigation_producer: Option<Option<AttemptId>>,
    ) -> Result<EngineResponse, String> {
        let cluster = self.cluster()?;
        if self.owner != cluster.coordinator {
            return Err("only coordinator queues deferred reassignment".into());
        }
        #[cfg(test)]
        self.observe_scheduler_queue_wait();
        // Close uses this same queue -> phase order. Waiting on the queue never
        // holds mutation admission, and accepted retries prevent a false drain.
        {
            let mut queue = cluster.jobs.lock();
            let ops = self.operations.lock();
            ops.phase.check(&job.envelope).map_err(|e| e.to_string())?;
            if ops.phase.state() != super::protocol::envelope::PhaseState::Open {
                return Err("cannot queue work in a closed phase".into());
            }
            match navigation_producer {
                None => queue.enqueue_reassign(job, self)?,
                Some(producer) => queue.enqueue_navigation_retry(job, producer, self)?,
            }
        }
        cluster.jobs_changed.notify_all();
        Ok(EngineResponse::Ack)
    }
    pub(super) fn execute_job(
        &self,
        envelope: &Envelope,
        payload: Vec<u8>,
        action: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        self.check(envelope)?;
        let execution = {
            let mut records = self.job_records.lock();
            if !records.contains_key(&envelope.attempt) && records.len() >= 1_000_000 {
                return Err("construction job record capacity reached".into());
            }
            Arc::clone(
                records
                    .entry(envelope.attempt)
                    .or_insert_with(|| Arc::new(Mutex::new(None))),
            )
        };
        let mut result = execution.lock();
        {
            let mut ops = self.operations.lock();
            ops.phase
                .admit(envelope.clone(), Operation::Job(payload))
                .map_err(|e| e.to_string())?;
            if let Some(done) = &*result {
                return done.clone();
            }
            let active = self.active_jobs.fetch_add(1, Ordering::AcqRel) + 1;
            self.job_peak.fetch_max(active, Ordering::Relaxed);
            self.jobs_started.fetch_add(1, Ordering::Relaxed);
        }
        struct Flight<'a> {
            owner: &'a OwnerService,
            finished: bool,
        }
        impl Drop for Flight<'_> {
            fn drop(&mut self) {
                self.owner.active_jobs.fetch_sub(1, Ordering::AcqRel);
                if !self.finished {
                    self.owner.poisoned.store(true, Ordering::Release);
                }
            }
        }
        let mut flight = Flight {
            owner: self,
            finished: false,
        };
        let _job_work = self.observability.operation(Scope::BalanceJob);
        let done = action();
        self.operations
            .lock()
            .phase
            .terminal(envelope)
            .map_err(|e| e.to_string())?;
        if let Err(error) = &done {
            self.poison_reason.lock().get_or_insert(error.clone());
            self.poisoned.store(true, Ordering::Release);
            eprintln!("construction job {:?} failed: {error}", envelope.attempt);
        }
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
        *result = Some(done.clone());
        flight.finished = true;
        done
    }
    pub fn bind_cluster(&self, cluster: &Arc<Cluster>) {
        *self.runtime.write() = Arc::downgrade(cluster);
    }
    pub(super) fn cluster(&self) -> Result<Arc<Cluster>, String> {
        self.runtime
            .read()
            .upgrade()
            .ok_or_else(|| "owner runtime is not bound".into())
    }
    pub(super) fn ensure_save_quiescent(&self, run: u128, epoch: u64) -> Result<(), String> {
        if run != self.run
            || epoch != self.epoch.load(Ordering::Acquire)
            || self.poisoned.load(Ordering::Acquire)
        {
            return Err("save requires the current healthy writer phase".into());
        }
        if self.operations.lock().phase.state() != super::protocol::envelope::PhaseState::Sealed {
            return Err("save requires sealed mutation admission".into());
        }
        self.ensure_batches_quiescent()?;
        self.boundary_save_quiescent()?;
        self.placement.lock().ensure_save_quiescent()?;
        self.shared_state.lock().ensure_quiescent()?;
        Ok(())
    }
    pub(super) fn save_quiescent_snapshot(
        &self,
        run: u128,
        epoch: u64,
        previous: &BTreeMap<u128, super::save_runtime::SavedResident>,
    ) -> Result<(Vec<super::save_runtime::SaveSnapshotNode>, u64), String> {
        self.ensure_save_quiescent(run, epoch)?;
        let ops = self.operations.lock();
        if ops.phase.state() != super::protocol::envelope::PhaseState::Sealed
            || epoch != self.epoch.load(Ordering::Acquire)
        {
            return Err("save epoch changed before snapshot".into());
        }
        let mut snapshot = vec![];
        let mut ids = BTreeSet::new();
        let mut scanned = 0u64;
        for node in self.nodes.iter() {
            let n = node.read();
            if n.reserved.is_some() {
                return Err("save has reserved membership".into());
            }
            if let Some(postings) = &n.postings {
                if postings.ids.len().checked_mul(self.code_size()) != Some(postings.codes.len()) {
                    return Err("invalid posting payload".into());
                }
                for id in &postings.ids {
                    if !ids.insert(*id) {
                        return Err("duplicate vector in owner snapshot".into());
                    }
                }
                scanned += postings.ids.len() as u64;
            }
            let resident = Arc::downgrade(node.value());
            // Membership/code edits bump or replace the immutable routing image.
            // Cache holds an Arc so Arc::make_mut cannot modify it in place.
            // A replaced/handed-off resident cannot match even with equal revisions.
            let reused = previous
                .get(node.key())
                .filter(|saved| {
                    std::sync::Weak::ptr_eq(&saved.resident, &resident)
                        && Arc::ptr_eq(&saved.routing, &n.routing)
                })
                .map(|saved| saved.posting.clone());
            snapshot.push(super::save_runtime::SaveSnapshotNode {
                node: StoredNode {
                    routing: (*n.routing).clone(),
                    postings: if reused.is_some() {
                        None
                    } else {
                        n.postings.clone()
                    },
                },
                resident,
                routing: n.routing.clone(),
                reused,
            });
        }
        snapshot.sort_unstable_by_key(|n| n.node.routing.target.id);
        Ok((snapshot, scanned))
    }
    pub fn initial_root(&self) -> StoredNode {
        let zero = vec![0.; self.dim];
        StoredNode {
            routing: RoutingNode {
                navigation: Default::default(),
                target: NodeRef {
                    id: (self.owner as u128) << 64,
                    authority: self.owner,
                    placement: 0,
                },
                parent: None,
                centroid: zero.clone(),
                centroid_code: Code::<4>::quantize(&zero, &zero).as_ref().to_vec(),
                children: vec![],
                leaf: true,
                revision: 0,
            },
            postings: Some(PostingList::default()),
        }
    }
    fn navigation_attempts(&self, count: usize) -> Result<u128, String> {
        let count = u64::try_from(count.max(1))
            .map_err(|_| "navigation batch identity capacity exhausted")?;
        let base = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(count)
            })
            .map_err(|_| "navigation attempt capacity exhausted")?
            + 1;
        Ok(((self.owner as u128) << 64) | base as u128)
    }
    pub fn attempt(&self) -> Result<AttemptId, String> {
        let sequence = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| "operation identity exhausted")?
            + 1;
        Ok(AttemptId {
            authority: self.owner,
            sequence,
        })
    }
    pub fn envelope(&self, kind: &str, placements: Vec<(NodeId, u64)>) -> Result<Envelope, String> {
        Ok(Envelope {
            run: self.run,
            epoch: self.epoch.load(Ordering::Acquire),
            attempt: self.attempt()?,
            kind: kind.into(),
            placements,
        })
    }
    pub(super) fn fail_unresolved(&self, error: String) {
        self.poison_reason.lock().get_or_insert(error);
        self.poisoned.store(true, Ordering::Release);
    }
    pub(super) fn ensure_healthy(&self) -> Result<(), String> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(format!(
                "owner has an unresolved mutation and cannot continue: {}",
                self.poison_reason
                    .lock()
                    .as_deref()
                    .unwrap_or("a transfer or mutation exited before resolution")
            ));
        }
        Ok(())
    }
    pub(super) fn check(&self, e: &Envelope) -> Result<(), String> {
        self.ensure_healthy()?;
        self.operations
            .lock()
            .phase
            .check(e)
            .map_err(|e| e.to_string())
    }
    pub(super) fn resident(&self, node: NodeRef) -> Result<Arc<RwLock<Resident>>, String> {
        if node.authority != self.owner {
            return Err("request addressed to the wrong owner".into());
        }
        let resident = self
            .nodes
            .get(&node.id)
            .map(|r| Arc::clone(r.value()))
            .ok_or("stale or retired node")?;
        if resident.read().routing.target != node {
            return Err("stale placement".into());
        }
        Ok(resident)
    }
    pub fn node(&self, node: NodeRef) -> Result<RoutingNode, String> {
        if node.authority != self.owner {
            return Err("request addressed to the wrong owner".into());
        }
        let image = {
            let _gate = self.routing_gate.read();
            self.routing_images
                .read()
                .get(&node.id)
                .cloned()
                .ok_or("stale or retired node")?
        };
        if image.target != node {
            return Err("stale placement".into());
        }
        Ok((*image).clone())
    }
    /// Call after preparing images and acquiring any required resident protection.
    /// This critical section never waits for posting locks or calls another owner.
    pub(super) fn publish_images(
        &self,
        images: Vec<RoutingNode>,
        retired: &[NodeId],
    ) -> Result<(), String> {
        let prepared: Vec<_> = images
            .into_iter()
            .map(|mut image| {
                image.prepare_navigation()?;
                Ok((image.target.id, Arc::new(image)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let _gate = self.routing_gate.write();
        let mut current = self.routing_images.write();
        for id in retired {
            current.remove(id);
        }
        for (id, image) in prepared {
            current.insert(id, image);
        }
        Ok(())
    }
    pub(super) fn code_size(&self) -> usize {
        Code::<1>::quantize(&vec![0.; self.dim], &vec![0.; self.dim])
            .as_ref()
            .len()
    }
    fn validate_nodes(&self, nodes: &[StoredNode]) -> Result<(), String> {
        let size = self.code_size();
        let mut seen = BTreeSet::new();
        for n in nodes {
            if !seen.insert(n.routing.target.id)
                || n.routing.target.authority != self.owner
                || n.routing.centroid.len() != self.dim
                || n.routing.centroid.iter().any(|x| !x.is_finite())
            {
                return Err("invalid staged routing identity or centroid".into());
            }
            match (&n.postings, n.routing.leaf) {
                (Some(p), true)
                    if p.codes.len() == p.ids.len() * size
                        && p.ids.iter().collect::<BTreeSet<_>>().len() == p.ids.len() => {}
                (None, false) => {}
                _ => return Err("invalid posting representation".into()),
            }
        }
        Ok(())
    }
    pub fn install_initial(&self, mut nodes: Vec<StoredNode>) -> Result<(), String> {
        for node in &mut nodes {
            node.routing.prepare_navigation()?;
        }
        self.validate_nodes(&nodes)?;
        let resident_ids = nodes
            .iter()
            .map(|n| n.routing.target.id)
            .collect::<BTreeSet<_>>();
        let forest = nodes
            .iter()
            .map(|n| {
                (
                    n.routing.target.id,
                    super::protocol::reservation::RegionNode {
                        parent: n
                            .routing
                            .parent
                            .filter(|p| p.authority == self.owner && resident_ids.contains(&p.id))
                            .map(|p| p.id),
                        placement: n.routing.target.placement,
                        revision: n.routing.revision,
                        live: true,
                    },
                )
            })
            .collect();
        let manager = Arc::new(
            super::protocol::reservation::RegionManager::new_profiled(
                forest,
                1_000_000,
                self.observability.clone(),
            )
            .map_err(|e| e.to_string())?,
        );
        let images = nodes
            .iter()
            .map(|n| (n.routing.target.id, Arc::new(n.routing.clone())))
            .collect();
        let _gate = self.routing_gate.write();
        if !self.nodes.is_empty() {
            return Err("initial tree already installed".into());
        }
        for n in nodes {
            self.nodes.insert(
                n.routing.target.id,
                Arc::new(RwLock::new(Resident {
                    routing: Arc::new(n.routing),
                    postings: n.postings,
                    reserved: None,
                    preparing_split: false,
                    transfer_source: None,
                    claim_producer: None,
                })),
            );
        }
        *self.routing_images.write() = images;
        *self.regions.write() = manager;
        Ok(())
    }
    fn expand(&self, request: ExpandRequest) -> Result<ExpandReply, String> {
        if request.run != self.run || request.epoch != self.epoch.load(Ordering::Acquire) {
            return Err("stale expansion run or phase".into());
        }
        if request.query.len() != self.dim || request.query.iter().any(|x| !x.is_finite()) {
            return Err("invalid expansion vector".into());
        }
        let images = {
            let _gate = self.routing_gate.read();
            let published = self.routing_images.read();
            request
                .nodes
                .iter()
                .map(|n| {
                    published
                        .get(&n.id)
                        .filter(|image| image.target == *n && n.authority == self.owner)
                        .cloned()
                        .ok_or("stale routing image")
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let mut reply = ExpandReply {
            authority: self.owner,
            run: request.run,
            epoch: request.epoch,
            shared_sequence: request.shared_sequence,
            child_counts: vec![],
            navigation: request.navigation,
            step: request.step,
            query_slot: request.query_slot,
            nodes: request.nodes,
            status: ExpansionStatus::Complete,
            children: vec![],
        };
        let Ok(images) = images else {
            reply.status = ExpansionStatus::Stale;
            return Ok(reply);
        };
        // Copy immutable image handles under the gate, then score without it.
        let mut observed = self.observability.operation(Scope::OwnedRoutingScoring);
        let mut successful_distances = 0_u64;
        let result = (|| {
            for image in images {
                if image.leaf {
                    return Err("Expand requires an internal routing node".into());
                }
                reply
                    .child_counts
                    .push((image.target.id, image.children.len()));
                self.expansions.fetch_add(1, Ordering::Relaxed);
                observed.add(Counter::OwnedRoutingDistances, image.children.len() as u64);
                for (ordinal, target, leaf, centroid) in image.scored_children()? {
                    let distance = self.metric.distance(&request.query, centroid);
                    if !distance.is_finite() {
                        return Err("nonfinite centroid distance".into());
                    }
                    reply.children.push(ScoredChild {
                        parent: image.target.id,
                        ordinal,
                        target,
                        leaf,
                        distance,
                        observed_revision: image.revision,
                    });
                    successful_distances += 1;
                }
            }
            Ok(reply)
        })();
        // Preserve successful-prefix accounting on errors without an atomic
        // update for every scored child. Images and row order are unchanged.
        self.distances
            .fetch_add(successful_distances, Ordering::Relaxed);
        result
    }
    fn append(
        &self,
        e: Envelope,
        leaf: NodeRef,
        revision: u64,
        ids: Vec<u64>,
    ) -> Result<EngineResponse, String> {
        self.check(&e)?;
        let operation = Operation::Append {
            leaf,
            revision,
            ids: ids.clone(),
        };
        // Only committed appends enter the phase table. Resolve a duplicate
        // before consulting a routing revision changed by its original append.
        {
            let ops = self.operations.lock();
            match ops.phase.lookup(&e) {
                Ok(old) if old == &operation => return Ok(EngineResponse::Ack),
                Ok(_) => return Err("conflicting append duplicate".into()),
                Err(protocol::ProtocolError::Unknown) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        // Full vectors are resolved before taking either bookkeeping or node
        // edit protection; the resolver may read shared durable vector storage.
        let image = self.node(leaf)?;
        if !image.leaf || image.revision != revision {
            return Err("append destination changed".into());
        }
        let capacity = ids
            .len()
            .checked_mul(self.code_size())
            .ok_or("append code capacity overflow")?;
        let mut codes = Vec::with_capacity(capacity);
        let mut seen = BTreeSet::new();
        for id in &ids {
            if !seen.insert(*id) {
                return Err("duplicate new vector ID in append".into());
            }
            let vector = self.vector(*id)?;
            if vector.len() != self.dim || vector.iter().any(|x| !x.is_finite()) {
                return Err("invalid immutable vector".into());
            }
            codes.extend_from_slice(Code::<1>::quantize(&vector, &image.centroid).as_ref());
        }
        let resident = self.resident(leaf)?;
        let mut node = resident.write();
        let mut ops = self.operations.lock();
        match ops.phase.lookup(&e) {
            Ok(old) if old == &operation => return Ok(EngineResponse::Ack),
            Ok(_) => return Err("conflicting append duplicate".into()),
            Err(protocol::ProtocolError::Unknown) => {}
            Err(error) => return Err(error.to_string()),
        }
        if node.reserved.is_some() {
            return Err("append conflicts with protected membership operation".into());
        }
        if node.routing.target != leaf {
            return Err("append placement changed".into());
        }
        if node.routing.revision != revision {
            return Err("append dependency changed".into());
        }
        let next_revision = node
            .routing
            .revision
            .checked_add(1)
            .ok_or("node revision exhausted")?;
        let posting = node.postings.as_mut().ok_or("append target is internal")?;
        if ids.iter().any(|id| posting.ids.contains(id)) {
            return Err("duplicate current vector ID".into());
        }
        // Every fallible validation precedes admission and membership mutation.
        ops.phase
            .admit(e.clone(), operation)
            .map_err(|x| x.to_string())?;
        posting.ids.extend(ids);
        posting.codes.extend(codes);
        let routing = Arc::make_mut(&mut node.routing);
        routing.revision = next_revision;
        ops.phase.terminal(&e).map_err(|x| x.to_string())?;
        drop(ops);
        self.publish_images(vec![(*node.routing).clone()], &[])?;
        Ok(EngineResponse::Ack)
    }
    fn transfer_prepare(
        &self,
        e: Envelope,
        payload: TransferPayload,
        role: Role,
        codes: Vec<u8>,
    ) -> Result<EngineResponse, String> {
        let mut observed = self.observability.operation(Scope::TransferPrepare);
        let (mut probes, mut comparison_bound) = (0, 0);
        self.check(&e)?;
        let key = (e.attempt, if role == Role::Source { 0 } else { 1 });
        {
            let ops = self.operations.lock();
            if let Some(existing) = ops.transfers.get(&key) {
                ops.phase.lookup(&e).map_err(|x| x.to_string())?;
                if existing.codes != codes || existing.record.payload != payload {
                    return Err("conflicting transfer staging duplicate".into());
                }
                return Ok(EngineResponse::Transfer(existing.record.state()));
            }
        }
        let (owner, id, placement) = if role == Role::Source {
            (
                payload.source_owner,
                payload.source_leaf,
                payload.source_placement,
            )
        } else {
            (
                payload.destination_owner,
                payload.destination_leaf,
                payload.destination_placement,
            )
        };
        if owner != self.owner {
            return Err("transfer endpoint owner mismatch".into());
        }
        observed.add(
            Counter::TransferPayloadIdBytesCloned,
            (payload.vectors.len() * std::mem::size_of::<u64>()) as u64,
        );
        let record = TransferRecord::new(role, payload.clone()).map_err(|x| x.to_string())?;
        let (manager, request, prepare_lock) = {
            let mut ops = self.operations.lock();
            ops.phase.check(&e).map_err(|x| x.to_string())?;
            if let Some(existing) = ops.transfer_regions.get(&e.attempt) {
                if existing.envelope != e || existing.payload != payload {
                    return Err("conflicting transfer reservation duplicate".into());
                }
            } else {
                if ops.transfer_regions.len() >= 1_000_000 {
                    return Err("transfer reservation capacity exceeded".into());
                }
                let manager = self.regions.read().clone();
                let mut placements = vec![];
                if payload.source_owner == self.owner {
                    placements.push((payload.source_leaf, payload.source_placement));
                }
                if payload.destination_owner == self.owner {
                    placements.push((payload.destination_leaf, payload.destination_placement));
                }
                placements.sort_unstable();
                placements.dedup();
                let request = super::protocol::reservation::RegionRequest {
                    roots: placements.iter().map(|(id, _)| *id).collect(),
                    placements,
                    topology_sequence: manager.sequence(),
                };
                ops.transfer_regions.insert(
                    e.attempt,
                    TransferReservation {
                        envelope: e.clone(),
                        payload: payload.clone(),
                        manager,
                        request,
                        prepare_lock: Arc::new(Mutex::new(())),
                    },
                );
            }
            let reservation = &ops.transfer_regions[&e.attempt];
            (
                reservation.manager.clone(),
                reservation.request.clone(),
                reservation.prepare_lock.clone(),
            )
        };
        #[cfg(test)]
        if let Some(hook) = self.transfer_capture_hook.lock().clone() {
            hook(e.attempt);
        }
        let _preparing = prepare_lock.lock();
        {
            let ops = self.operations.lock();
            if let Some(existing) = ops.transfers.get(&key) {
                ops.phase.lookup(&e).map_err(|x| x.to_string())?;
                if existing.codes != codes || existing.record.payload != payload {
                    return Err("conflicting transfer staging duplicate".into());
                }
                return Ok(EngineResponse::Transfer(existing.record.state()));
            }
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut status = manager
            .request_current(e.attempt, request)
            .map_err(|x| x.to_string())?;
        let initial_status = status;
        let queue_started = Instant::now();
        while status == super::protocol::reservation::ReservationStatus::Queued {
            if let Err(error) = self.ensure_healthy() {
                // This request has not edited either endpoint. Cancel its queued
                // protection, or release an unmutated grant that won the race.
                if manager.cancel(e.attempt).is_err()
                    && manager.status(e.attempt).ok()
                        == Some(super::protocol::reservation::ReservationStatus::Granted)
                {
                    manager.release(e.attempt).map_err(|x| x.to_string())?;
                }
                return Err(error);
            }
            if Instant::now() >= deadline {
                if manager.cancel(e.attempt) == Err(protocol::ProtocolError::Prerequisite) {
                    manager.release(e.attempt).map_err(|x| x.to_string())?;
                }
                return Err("transfer region acquisition deadline expired".into());
            }
            std::thread::sleep(Duration::from_millis(1));
            status = manager.status(e.attempt).map_err(|x| x.to_string())?;
        }
        if initial_status == super::protocol::reservation::ReservationStatus::Queued {
            observed.add(
                Counter::ReservationQueuedWaitNanos,
                queue_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        if status != super::protocol::reservation::ReservationStatus::Granted {
            return Err(format!("transfer region is not granted: attempt={:?} role={role:?} source={} destination={} initial={initial_status:?}; {}",e.attempt,payload.source_leaf,payload.destination_leaf,manager.diagnostic(e.attempt)));
        }
        let result = (|| {
            let resident = self.resident(NodeRef {
                id,
                authority: owner,
                placement,
            })?;
            let mut node = resident.write();
            let mut ops = self.operations.lock();
            // A concurrent retry may wait for the original endpoint's node lock.
            // Its terminal result wins over already removed source membership.
            if let Some(existing) = ops.transfers.get(&key) {
                ops.phase.lookup(&e).map_err(|x| x.to_string())?;
                if existing.codes != codes || existing.record.payload != payload {
                    return Err("conflicting transfer staging duplicate".into());
                }
                return Ok(EngineResponse::Transfer(existing.record.state()));
            }
            if node.routing.target
                != (NodeRef {
                    id,
                    authority: owner,
                    placement,
                })
            {
                return Err("transfer placement changed".into());
            }
            if node.reserved.is_some_and(|id| id != e.attempt) {
                let error = if node.preparing_split {
                    "transfer leaf is protected by a split preparation claim".into()
                } else if let Some(source) = node.transfer_source {
                    format!("transfer leaf is protected by a local transfer claim source={source}")
                } else {
                    "transfer leaf is already protected".into()
                };
                return Err(super::construction::append_claim_producers(
                    error,
                    node.claim_producer,
                ));
            }
            let expected_centroid = if role == Role::Source {
                payload.source_centroid_digest
            } else {
                payload.destination_centroid_digest
            };
            if centroid_digest(&node.routing.centroid) != expected_centroid {
                return Err("transfer centroid dependency changed".into());
            }
            let postings = node.postings.as_ref().ok_or("transfer requires a leaf")?;
            if role == Role::Source
                && (!codes.is_empty()
                    || payload.vectors.iter().any(|id| {
                        !counted_contains(&postings.ids, id, &mut probes, &mut comparison_bound)
                    }))
            {
                return Err("source membership changed before transfer protection".into());
            }
            let code_bytes = payload
                .vectors
                .len()
                .checked_mul(self.code_size())
                .ok_or("transfer code capacity overflow")?;
            if role == Role::Destination && codes.len() != code_bytes {
                return Err(format!(
                    "invalid destination code payload: {} bytes for {} expected",
                    codes.len(),
                    code_bytes
                ));
            }
            if role == Role::Destination
                && payload.vectors.iter().any(|id| {
                    counted_contains(&postings.ids, id, &mut probes, &mut comparison_bound)
                })
            {
                return Err("destination membership changed before transfer preparation".into());
            }
            ops.phase
                .admit(e.clone(), Operation::Transfer(payload.clone()))
                .map_err(|x| x.to_string())?;
            node.reserved = Some(e.attempt);
            ops.transfers
                .insert(key, TransferEndpoint { record, codes });
            Ok(EngineResponse::Transfer(TransferState::Prepared))
        })();
        if result.is_err() {
            let has_endpoint = self
                .operations
                .lock()
                .transfers
                .keys()
                .any(|(id, _)| *id == e.attempt);
            if !has_endpoint {
                manager.release(e.attempt).map_err(|x| {
                    self.poisoned.store(true, Ordering::Release);
                    x.to_string()
                })?;
            }
        }
        observed.add(Counter::TransferMembershipProbes, probes);
        observed.add(
            Counter::TransferMembershipComparisonUpperBound,
            comparison_bound,
        );
        result
    }
    fn transfer_step(
        &self,
        e: Envelope,
        role: Role,
        step: TransferStep,
        proof: TransferProof,
    ) -> Result<EngineResponse, String> {
        let mut observed = self.observability.operation(Scope::TransferStep);
        let (mut probes, mut comparison_bound) = (0, 0);
        self.check(&e)?;
        let key = (e.attempt, if role == Role::Source { 0 } else { 1 });
        // Obtain the identity without holding bookkeeping while waiting for a
        // node lock. The phase retains this record until sealing.
        let payload = {
            let ops = self.operations.lock();
            ops.phase.lookup(&e).map_err(|x| x.to_string())?;
            ops.transfers
                .get(&key)
                .ok_or("unknown transfer endpoint")?
                .record
                .payload
                .clone()
        };
        let leaf = if role == Role::Source {
            NodeRef {
                id: payload.source_leaf,
                authority: payload.source_owner,
                placement: payload.source_placement,
            }
        } else {
            NodeRef {
                id: payload.destination_leaf,
                authority: payload.destination_owner,
                placement: payload.destination_placement,
            }
        };
        let resident = self.resident(leaf)?;
        let mut node = resident.write();
        let mut ops = self.operations.lock();
        let endpoint = ops
            .transfers
            .get_mut(&key)
            .ok_or("unknown transfer endpoint")?;
        let already_terminal = matches!(
            endpoint.record.state(),
            TransferState::Released | TransferState::Aborted
        );
        let previous_state = endpoint.record.state();
        if !already_terminal && node.reserved != Some(e.attempt) {
            return Err("transfer protection was lost".into());
        }
        let vectors = &payload.vectors;
        let codes = &endpoint.codes;
        let code_size = self.code_size();
        let state = endpoint
            .record
            .apply(step, &proof, || {
                // Exhaustion is detected before editing either posting list. A
                // duplicate step skips this closure and keeps its accepted result.
                let next_revision = if matches!(
                    step,
                    TransferStep::RemoveSource | TransferStep::ActivateDestination
                ) {
                    Some(
                        node.routing
                            .revision
                            .checked_add(1)
                            .ok_or(protocol::ProtocolError::Exhausted)?,
                    )
                } else {
                    None
                };
                match step {
                    TransferStep::RemoveSource => {
                        let _compaction = self.observability.operation(Scope::TransferCompaction);
                        observed.add(Counter::TransferCompactions, 1);
                        let postings = node
                            .postings
                            .as_mut()
                            .ok_or(protocol::ProtocolError::Conflict)?;
                        if vectors.iter().any(|id| {
                            !counted_contains(&postings.ids, id, &mut probes, &mut comparison_bound)
                        }) {
                            return Err(protocol::ProtocolError::Conflict);
                        }
                        let removed = vectors.iter().copied().collect::<BTreeSet<_>>();
                        let mut kept = PostingList {
                            ids: Vec::with_capacity(postings.ids.len() - vectors.len()),
                            codes: Vec::with_capacity(postings.codes.len()),
                        };
                        observed.add(
                            Counter::TransferSourceEntriesScanned,
                            postings.ids.len() as u64,
                        );
                        for (slot, id) in postings.ids.iter().enumerate() {
                            if !removed.contains(id) {
                                kept.ids.push(*id);
                                kept.codes.extend_from_slice(
                                    &postings.codes[slot * code_size..(slot + 1) * code_size],
                                );
                            }
                        }
                        observed.add(Counter::TransferCopiedIdBytes, (kept.ids.len() * 8) as u64);
                        observed.add(Counter::TransferCopiedCodeBytes, kept.codes.len() as u64);
                        *postings = kept;
                    }
                    TransferStep::ActivateDestination => {
                        let postings = node
                            .postings
                            .as_mut()
                            .ok_or(protocol::ProtocolError::Conflict)?;
                        if vectors.iter().any(|id| {
                            counted_contains(&postings.ids, id, &mut probes, &mut comparison_bound)
                        }) {
                            return Err(protocol::ProtocolError::Conflict);
                        }
                        observed.add(Counter::TransferCopiedIdBytes, (vectors.len() * 8) as u64);
                        observed.add(Counter::TransferCopiedCodeBytes, codes.len() as u64);
                        postings.ids.extend(vectors);
                        postings.codes.extend_from_slice(codes);
                    }
                    TransferStep::Release | TransferStep::Abort => node.reserved = None,
                    _ => {}
                }
                if let Some(revision) = next_revision {
                    Arc::make_mut(&mut node.routing).revision = revision;
                }
                Ok(())
            })
            .map_err(|x| x.to_string())?;
        let mut release_region = None;
        if matches!(state, TransferState::Released | TransferState::Aborted) {
            let both_terminal = ops
                .transfers
                .iter()
                .filter(|((id, _), _)| *id == e.attempt)
                .all(|(_, r)| {
                    matches!(
                        r.record.state(),
                        TransferState::Released | TransferState::Aborted
                    )
                });
            if both_terminal {
                ops.phase.terminal(&e).map_err(|x| x.to_string())?;
                release_region = ops
                    .transfer_regions
                    .get(&e.attempt)
                    .map(|r| r.manager.clone());
            }
        }
        drop(ops);
        if state != previous_state
            && matches!(
                step,
                TransferStep::RemoveSource | TransferStep::ActivateDestination
            )
        {
            self.publish_images(vec![(*node.routing).clone()], &[])?;
        }
        if let Some(manager) = release_region {
            manager.release(e.attempt).map_err(|error| {
                self.poisoned.store(true, Ordering::Release);
                error.to_string()
            })?;
        }
        observed.add(Counter::TransferMembershipProbes, probes);
        observed.add(
            Counter::TransferMembershipComparisonUpperBound,
            comparison_bound,
        );
        observed.add(
            Counter::TransferPayloadIdBytesCloned,
            (vectors.len() * 8) as u64,
        );
        Ok(EngineResponse::Transfer(state))
    }
    pub fn handle(&self, request: EngineRequest) -> Result<EngineResponse, String> {
        match request {
            EngineRequest::CurrentViewIfChanged { known_sequence } => {
                let view = self.cluster()?.publication.view();
                // The version and immutable view share one atomic publication.
                // A match acknowledges exactly the view the caller already has.
                if view.sequence == known_sequence {
                    Ok(EngineResponse::ViewUnchanged {
                        sequence: view.sequence,
                    })
                } else {
                    Ok(EngineResponse::View((*view).clone()))
                }
            }
            EngineRequest::ValidateWriter => {
                let mut ids = BTreeSet::new();
                for entry in self.nodes.iter() {
                    let node = entry.read();
                    if let Some(postings) = &node.postings {
                        if postings.codes.len() != postings.ids.len() * self.code_size() {
                            return Err("writer posting length mismatch".into());
                        }
                        for (offset, id) in postings.ids.iter().enumerate() {
                            if !ids.insert(*id) {
                                return Err("duplicate writer membership".into());
                            }
                            let vector = self.vectors.get(*id)?;
                            let code = Code::<1>::quantize(&vector, &node.routing.centroid);
                            if code.as_ref()
                                != &postings.codes
                                    [offset * self.code_size()..(offset + 1) * self.code_size()]
                            {
                                return Err("writer posting code mismatch".into());
                            }
                        }
                    }
                }
                Ok(EngineResponse::Membership(vec![(
                    0,
                    ids.into_iter().collect(),
                )]))
            }
            EngineRequest::WriterCheckpoint {
                run,
                epoch,
                path,
                next_input,
                input_sha256,
                balance_pending,
            } => {
                self.write_checkpoint(run, epoch, &path, next_input, input_sha256, balance_pending)?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::OwnerTransfer(request) => self
                .handle_owner_transfer(request)
                .map(EngineResponse::Transfer),
            EngineRequest::SaveOwner(request) => self
                .save_owner_runtime(request)
                .map(EngineResponse::OwnerSaved),
            EngineRequest::ReadSavedObject(request) => self
                .read_saved_object(request)
                .map(EngineResponse::SavedObject),
            EngineRequest::CurrentRoot => {
                Ok(EngineResponse::Routing(self.cluster()?.refresh_root()?))
            }
            EngineRequest::CurrentView => Ok(EngineResponse::View(
                (*self.cluster()?.publication.view()).clone(),
            )),
            EngineRequest::Shared(request) => {
                self.handle_shared(request).map(EngineResponse::Shared)
            }
            EngineRequest::Placement(request) => self
                .handle_placement(request)
                .map(EngineResponse::Placement),
            EngineRequest::BoundaryStatus { envelope } => {
                self.check(&envelope)?;
                let records = self.boundaries.lock();
                let p = records
                    .get(&envelope.attempt)
                    .ok_or("unknown boundary attempt")?;
                if p.request.envelope != envelope {
                    return Err("conflicting boundary envelope".into());
                }
                Ok(EngineResponse::BoundaryStatus {
                    preparing: p.preparing,
                    ready: p.ready,
                    finished: p.finished,
                })
            }
            EngineRequest::BoundaryReady(prepared) => {
                self.boundary_ready(prepared)?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::BoundaryFinish {
                envelope,
                published,
            } => {
                self.boundary_finish(envelope, published)?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::ScheduledJob(job) => {
                self.execute_scheduled_job(job)?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::NeighborReassign {
                envelope,
                neighbor,
                old,
                left,
                right,
            } => {
                let identity = bincode::serialize(&(neighbor, &old, &left, &right))
                    .map_err(|e| e.to_string())?;
                self.execute_job(&envelope, identity, || {
                    self.cluster()?
                        .neighbor_reassign_for_job(
                            neighbor,
                            &old,
                            &left,
                            &right,
                            &mut BTreeSet::new(),
                            None,
                        )
                        .map_err(|e| format!("neighbor reassignment {}: {e}", neighbor.id))
                })?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::ReassignJob(job) => {
                if job.source.authority != self.owner {
                    return Err("reassignment dispatched to wrong owner".into());
                }
                let envelope = job.envelope.clone();
                let identity = bincode::serialize(&job).map_err(|e| e.to_string())?;
                self.execute_job(&envelope, identity, || {
                    self.cluster()?.run_reassign_job(job, None)
                })?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::EnqueueReassign(job) => self.enqueue_reassignment(job, None),
            EngineRequest::EnqueueNavigationRetry { job, producer } => {
                self.enqueue_reassignment(job, Some(producer))
            }
            EngineRequest::BalanceJob(job) => {
                let envelope = job.envelope.clone();
                let identity = bincode::serialize(&job).map_err(|e| e.to_string())?;
                self.execute_job(&envelope, identity, || self.cluster()?.run_job(job))?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::Eligible => Ok(EngineResponse::Eligible(self.eligible()?)),
            EngineRequest::EnqueueNeighbor(job) => {
                let cluster = self.cluster()?;
                #[cfg(test)]
                self.observe_scheduler_queue_wait();
                // Waiting for the scheduler must not monopolize mutation admission.
                // Close uses this same queue -> phase order, so validation and
                // insertion remain one atomic decision with respect to closing.
                {
                    let mut queue = cluster.jobs.lock();
                    let ops = self.operations.lock();
                    ops.phase.check(&job.envelope).map_err(|e| e.to_string())?;
                    if ops.phase.state() != super::protocol::envelope::PhaseState::Open {
                        return Err("cannot queue work in a closed phase".into());
                    }
                    queue.enqueue_neighbor(job);
                }
                cluster.jobs_changed.notify_all();
                Ok(EngineResponse::Ack)
            }
            EngineRequest::Enqueue(job) => {
                let cluster = self.cluster()?;
                #[cfg(test)]
                self.observe_scheduler_queue_wait();
                // Waiting for the scheduler must not monopolize mutation admission.
                // Close uses this same queue -> phase order, so validation and
                // insertion remain one atomic decision with respect to closing.
                {
                    let mut queue = cluster.jobs.lock();
                    let ops = self.operations.lock();
                    ops.phase.check(&job.envelope).map_err(|e| e.to_string())?;
                    if ops.phase.state() != super::protocol::envelope::PhaseState::Open {
                        return Err("cannot queue work in a closed phase".into());
                    }
                    queue.enqueue(job);
                }
                cluster.jobs_changed.notify_all();
                Ok(EngineResponse::Ack)
            }
            EngineRequest::Structure(edit) => Ok(EngineResponse::Structure(
                self.cluster()?.publish_structure(edit)?,
            )),
            EngineRequest::Summary { root } => {
                Ok(EngineResponse::Summary(self.subtree_children(root)?))
            }
            EngineRequest::Handoff {
                envelope,
                root,
                destination,
                new_placement,
            } => {
                self.cluster()?
                    .handoff(envelope, root, destination, new_placement)?;
                Ok(EngineResponse::Ack)
            }
            EngineRequest::Expand(requests) => Ok(EngineResponse::Expanded(
                requests
                    .into_iter()
                    .map(|r| self.expand(r))
                    .collect::<Result<_, _>>()?,
            )),
            EngineRequest::Root { root } | EngineRequest::Node { node: root } => {
                if let Ok(cluster) = self.cluster() {
                    let view = cluster.current_view();
                    if let Some(image) = view.value.shared.get(&root.id) {
                        if image.target != root {
                            return Err("stale shared routing target".into());
                        }
                        return Ok(EngineResponse::Routing(image.clone()));
                    }
                }
                Ok(EngineResponse::Routing(self.node(root)?))
            }
            EngineRequest::Append {
                envelope,
                leaf,
                expected_revision,
                ids,
            } => self.append(envelope, leaf, expected_revision, ids),
            EngineRequest::PrepareTransfer {
                envelope,
                payload,
                role,
                codes,
            } => self.transfer_prepare(envelope, payload, role, codes),
            EngineRequest::TransferStep {
                envelope,
                role,
                step,
                proof,
            } => self.transfer_step(envelope, role, step, proof),
            EngineRequest::TransferStatus { envelope, role } => {
                self.check(&envelope)?;
                let ops = self.operations.lock();
                ops.phase.lookup(&envelope).map_err(|x| x.to_string())?;
                Ok(EngineResponse::Transfer(
                    ops.transfers
                        .get(&(envelope.attempt, if role == Role::Source { 0 } else { 1 }))
                        .ok_or("unknown transfer")?
                        .record
                        .state(),
                ))
            }
            EngineRequest::Counts => {
                let memory = super::super::mem_probe::read_self();
                let mut counts = OwnerCounts {
                    work: self.observability.snapshot(),
                    centroid_refresh_longest: super::shared_runtime::centroid_refresh_diagnostics(
                        self.owner,
                    ),
                    reservation_state: self.regions.read().state_snapshot(),
                    clustering_process: serde_json::to_value(
                        chroma_index::spann::utils::clustering_profile_snapshot(),
                    )
                    .map_err(|e| e.to_string())?,
                    process_cpu_seconds: super::metrics::cpu_seconds(),
                    rss_bytes: memory.rss,
                    process_peak_rss_bytes: memory.rss_peak,
                    owner: self.owner,
                    nodes: self.nodes.len(),
                    splits: self.splits.load(Ordering::Relaxed),
                    transfers: self.transfers.load(Ordering::Relaxed),
                    expansions: self.expansions.load(Ordering::Relaxed),
                    distances: self.distances.load(Ordering::Relaxed),
                    active_workers: self.active_jobs.load(Ordering::Relaxed),
                    scheduler: self
                        .cluster()
                        .map_or_else(|_| BTreeMap::new(), |c| c.jobs.lock().diagnostic_counts()),
                    active_peak: self.job_peak.load(Ordering::Relaxed),
                    jobs_started: self.jobs_started.load(Ordering::Relaxed),
                    jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
                    vector_fetches: self.vector_fetches.load(Ordering::Relaxed),
                    ingestion_id_ranges: self
                        .cluster()
                        .map_or(0, |cluster| cluster.inserted_ids.lock().ranges()),
                    ..Default::default()
                };
                for handle in self
                    .nodes
                    .iter()
                    .map(|node| Arc::clone(node.value()))
                    .collect::<Vec<_>>()
                {
                    let n = handle.read();
                    if let Some(p) = &n.postings {
                        counts.leaves += 1;
                        counts.vectors += p.ids.len() as u64;
                    }
                }
                Ok(EngineResponse::Counts(counts))
            }
            EngineRequest::Membership => {
                let mut leaves = vec![];
                for handle in self
                    .nodes
                    .iter()
                    .map(|node| Arc::clone(node.value()))
                    .collect::<Vec<_>>()
                {
                    let n = handle.read();
                    if n.reserved.is_some() {
                        return Err("membership validation requires drained transfers".into());
                    }
                    if let Some(p) = &n.postings {
                        leaves.push((n.routing.target.id, p.ids.clone()));
                    }
                }
                Ok(EngineResponse::Membership(leaves))
            }
            EngineRequest::ListNodes => {
                let mut nodes = self
                    .routing_images
                    .read()
                    .values()
                    .map(|image| image.target)
                    .collect::<Vec<_>>();
                nodes.sort_unstable_by_key(|n| n.id);
                Ok(EngineResponse::Nodes(nodes))
            }
            EngineRequest::SnapshotNodes { nodes } => {
                if nodes.len() > 64 {
                    return Err("snapshot page exceeds64nodes".into());
                }
                if self.poisoned.load(Ordering::Acquire) {
                    return Err("snapshot forbidden after unresolved mutation".into());
                }
                let mut saved = Vec::with_capacity(nodes.len());
                for target in nodes {
                    let handle = self.resident(target)?;
                    let n = handle.read();
                    if n.reserved.is_some() {
                        return Err("snapshot requires drained mutations".into());
                    }
                    saved.push(StoredNode {
                        routing: (*n.routing).clone(),
                        postings: n.postings.clone(),
                    });
                }
                Ok(EngineResponse::Snapshot(saved))
            }
            EngineRequest::Snapshot => {
                let mut nodes = vec![];
                for handle in self
                    .nodes
                    .iter()
                    .map(|node| Arc::clone(node.value()))
                    .collect::<Vec<_>>()
                {
                    let n = handle.read();
                    if n.reserved.is_some() {
                        return Err("save requires drained transfers".into());
                    }
                    nodes.push(StoredNode {
                        routing: (*n.routing).clone(),
                        postings: n.postings.clone(),
                    });
                }
                Ok(EngineResponse::Snapshot(nodes))
            }
            EngineRequest::OpenEpoch { run, epoch } => {
                if run != self.run || epoch <= self.epoch.load(Ordering::Acquire) {
                    return Err("stale phase opening".into());
                }
                #[cfg(test)]
                {
                    let hook = self.epoch_open_wait_hook.lock().clone();
                    if let Some(hook) = hook {
                        hook();
                    }
                }
                let mut ops = self.operations.lock();
                if run != self.run || epoch <= self.epoch.load(Ordering::Acquire) {
                    return Err("stale phase opening".into());
                }
                if ops.phase.state() != super::protocol::envelope::PhaseState::Sealed {
                    return Err("previous epoch is not sealed".into());
                }
                // Retire the sealed epoch's endpoint registry before publishing
                // the new phase. Registry lookups never retain their map guard
                // while admitting a phase operation, so this order has no inverse.
                self.batch_transfers.lock().clear();
                #[cfg(test)]
                if let Some(hook) = self.epoch_open_hook.lock().clone() {
                    let available = self.operations.try_lock().is_some();
                    hook(available);
                }
                ops.phase = Phase::new(run, epoch, 1_000_000);
                ops.transfers.clear();
                ops.transfer_regions.clear();
                self.epoch.store(epoch, Ordering::Release);
                Ok(EngineResponse::Ack)
            }
            EngineRequest::CloseEpoch { run, epoch } => {
                let cluster = self.cluster().ok();
                #[cfg(test)]
                if cluster.is_some() {
                    self.observe_scheduler_queue_wait();
                }
                // Enqueue and close agree on queue -> phase ordering. A queued
                // waiter holds no phase lock, and no accepted work can appear
                // between the drained check and the transition to Closing.
                let queue = cluster.as_ref().map(|c| c.jobs.lock());
                let mut ops = self.operations.lock();
                // A request may have waited while another caller sealed and
                // opened an epoch. Validate its identity after acquiring locks.
                if run != self.run || epoch != self.epoch.load(Ordering::Acquire) {
                    return Err("stale phase close".into());
                }
                if self.active_jobs.load(Ordering::Acquire) != 0 {
                    return Err("construction jobs are still active; phase remains open".into());
                }
                if queue.as_ref().is_some_and(|q| !q.is_drained()) {
                    return Err("construction queue is not drained; phase remains open".into());
                }
                ops.phase.close();
                Ok(EngineResponse::Ack)
            }
            EngineRequest::SealEpoch { run, epoch } => {
                if run != self.run || epoch != self.epoch.load(Ordering::Acquire) {
                    return Err("stale phase seal".into());
                }
                let mut ops = self.operations.lock();
                if ops.phase.state() != super::protocol::envelope::PhaseState::Sealed {
                    ops.phase.seal(true).map_err(|x| x.to_string())?;
                }
                Ok(EngineResponse::Ack)
            }
        }
    }
}

/// A construction caller has direct access to its local authority and RPC
/// clients for other owners. It holds only the shared upper-tree routing view.
pub struct Cluster {
    pub local: Arc<OwnerService>,
    pub peers: BTreeMap<AuthorityId, RpcClient>,
    pub root: RwLock<RoutingNode>,
    pub shared: RwLock<BTreeMap<NodeId, RoutingNode>>,
    pub shared_sequence: AtomicU64,
    pub deadline: Duration,
    pub shutdown_deadline: Option<Instant>,
    pub coordinator: AuthorityId,
    pub publication: super::protocol::publication::Publication<super::construction::BoundaryView>,
    pub installed: RwLock<
        Option<Arc<super::protocol::publication::PublishedView<super::construction::BoundaryView>>>,
    >,
    pub(super) jobs: Mutex<super::construction::JobQueue>,
    pub(super) jobs_changed: parking_lot::Condvar,
    #[cfg(test)]
    navigation_snapshot_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    pub(super) navigation_images_hook: Mutex<Option<Arc<dyn Fn(&[NodeRef]) + Send + Sync>>>,
    pub(super) input_lock: Mutex<()>,
    pub(super) inserted_ids: Mutex<super::ingestion::InsertedIds>,
    /// Frozen predecessor per save attempt prevents an old replay rolling latest back.
    pub(super) save_predecessors: Mutex<BTreeMap<String, Option<String>>>,
    pub(super) structural_records:
        Mutex<BTreeMap<AttemptId, Arc<Mutex<super::construction::StructuralRecord>>>>,
    /// Saves also write each owner's full vectors as a blockfile in the store.
    pub save_vector_store: std::sync::atomic::AtomicBool,
}
impl Cluster {
    pub fn new(
        local: Arc<OwnerService>,
        peers: BTreeMap<AuthorityId, RpcClient>,
        mut root: RoutingNode,
    ) -> Self {
        root.prepare_navigation()
            .expect("initial root has valid child centroids");
        Self {
            local,
            peers,
            root: RwLock::new(root.clone()),
            shared: RwLock::new(BTreeMap::new()),
            shared_sequence: AtomicU64::new(0),
            deadline: Duration::from_secs(60),
            shutdown_deadline: None,
            coordinator: 1,
            installed: RwLock::new(None),
            publication: super::protocol::publication::Publication::new(
                super::construction::BoundaryView {
                    navigation_topology: None,
                    root: root.clone(),
                    shared: BTreeMap::new(),
                    units: std::iter::once((root.target.id, root.target)).collect(),
                }
                .prepared()
                .expect("initial routing view is valid"),
                1_000_000,
            ),
            jobs: Mutex::new(super::construction::JobQueue::default()),
            jobs_changed: parking_lot::Condvar::new(),
            structural_records: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            navigation_snapshot_hook: Mutex::new(None),
            #[cfg(test)]
            navigation_images_hook: Mutex::new(None),
            input_lock: Mutex::new(()),
            save_predecessors: Mutex::new(BTreeMap::new()),
            inserted_ids: Mutex::new(Default::default()),
            save_vector_store: std::sync::atomic::AtomicBool::new(false),
        }
    }
    pub(super) fn current_view(
        &self,
    ) -> Arc<super::protocol::publication::PublishedView<super::construction::BoundaryView>> {
        if self.local.owner == self.coordinator {
            return self.publication.view();
        }
        self.installed
            .read()
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.publication.view())
    }
    pub fn request(
        &self,
        authority: AuthorityId,
        request: EngineRequest,
    ) -> Result<EngineResponse, String> {
        self.request_until(authority, request, Instant::now() + self.deadline)
    }
    pub fn request_until(
        &self,
        authority: AuthorityId,
        request: EngineRequest,
        deadline: Instant,
    ) -> Result<EngineResponse, String> {
        if Instant::now() >= deadline {
            return Err("operation deadline expired".into());
        }
        let mut observed = self.local.observability.operation(Scope::EngineRequest);
        observed.add(
            if authority == self.local.owner {
                Counter::LocalEngineCalls
            } else {
                Counter::RemoteEngineCalls
            },
            1,
        );
        if matches!(
            &request,
            EngineRequest::OwnerTransfer(..)
                | EngineRequest::PrepareTransfer { .. }
                | EngineRequest::TransferStep { .. }
                | EngineRequest::TransferStatus { .. }
        ) {
            observed.add(Counter::TransferProtocolCalls, 1);
        }
        if authority == self.local.owner {
            return self.local.handle(request);
        }
        let class = match &request {
            EngineRequest::BalanceJob(_)
            | EngineRequest::ReassignJob(_)
            | EngineRequest::NeighborReassign { .. }
            | EngineRequest::ScheduledJob(_)
            | EngineRequest::Structure(_)
            | EngineRequest::Handoff { .. }
            | EngineRequest::Append { .. }
            | EngineRequest::Snapshot
            | EngineRequest::SnapshotNodes { .. }
            | EngineRequest::SaveOwner(_)
            | EngineRequest::ReadSavedObject(_) => RequestClass::Data,
            EngineRequest::Shared(super::shared_runtime::SharedRequest::RefreshCentroid {
                ..
            }) => RequestClass::Data,
            EngineRequest::Placement(
                super::placement_runtime::PlacementRequest::Export { .. }
                | super::placement_runtime::PlacementRequest::Chunk { .. },
            ) => RequestClass::Data,
            _ => RequestClass::Control,
        };
        let peer = self.peers.get(&authority).ok_or("unknown owner endpoint")?;
        if matches!(
            &request,
            EngineRequest::SaveOwner(_)
                | EngineRequest::WriterCheckpoint { .. }
                | EngineRequest::ValidateWriter
        ) {
            peer.call_with_timeout(
                &request,
                class,
                deadline,
                super::save_runtime::save_operation_timeout(),
            )
        } else {
            peer.call(&request, class, deadline)
        }
        .map_err(|e| e.to_string())
    }
    pub fn node(&self, target: NodeRef) -> Result<RoutingNode, String> {
        let view = self.current_view();
        if let Some(shared) = view.value.shared.get(&target.id) {
            if shared.target != target {
                return Err("stale shared routing target".into());
            }
            return Ok(shared.clone());
        }
        match self.request(target.authority, EngineRequest::Node { node: target })? {
            EngineResponse::Routing(n) => Ok(n),
            _ => Err("invalid node response".into()),
        }
    }
    pub fn navigate(
        &self,
        queries: &[Vec<f32>],
        policy: &ReadBeamPolicy,
        excluded: Option<NodeId>,
    ) -> Result<Vec<Vec<(NodeRef, f32)>>, String> {
        let mut observed = self.local.observability.operation(Scope::Navigation);
        observed.add(Counter::NavigationCalls, 1);
        observed.add(Counter::NavigationQueryRows, queries.len() as u64);
        let started = Instant::now();
        let mut last_rejection = String::new();
        let mut pinned_sequence = 0;
        for _ in 0..8 {
            observed.add(Counter::NavigationAttempts, 1);
            let (view, root) = match self.refreshed_routing_view() {
                Ok(result) => result,
                Err(error) if error.contains("stale") || error.contains("retired") => {
                    last_rejection = error;
                    std::thread::yield_now();
                    continue;
                }
                Err(error) => return Err(error),
            };
            pinned_sequence = view.sequence;
            if queries
                .iter()
                .any(|q| q.len() != self.local.dim || q.iter().any(|x| !x.is_finite()))
            {
                return Err(
                    "navigation requires finite vectors matching the index dimension".into(),
                );
            }
            if root.leaf {
                if Some(root.target.id) != excluded {
                    observed.add(Counter::RootLeafDistances, queries.len() as u64);
                    self.local
                        .distances
                        .fetch_add(queries.len() as u64, Ordering::Relaxed);
                }
                return Ok(queries
                    .iter()
                    .map(|q| {
                        if Some(root.target.id) == excluded {
                            vec![]
                        } else {
                            vec![(root.target, self.local.metric.distance(q, &root.centroid))]
                        }
                    })
                    .collect());
            }
            #[cfg(test)]
            if let Some(hook) = self.navigation_snapshot_hook.lock().clone() {
                hook();
            }
            let first = self.local.navigation_attempts(queries.len())?;
            let attempt = NavigationAttempt {
                cluster: self,
                view: view.clone(),
            };
            let result = routing::navigate_batch(
                &attempt,
                &root,
                queries,
                policy,
                self.local.run,
                self.local.epoch.load(Ordering::Acquire),
                view.sequence,
                first,
                excluded,
            )
            .and_then(|result| {
                attempt.check_topology()?;
                Ok(result)
            });
            match result {
                Ok((rows, _)) => return Ok(rows),
                Err(error)
                    if error.contains("route changed") || error.contains("stale expansion") =>
                {
                    last_rejection = error;
                    std::thread::yield_now();
                }
                Err(error) => return Err(error),
            }
        }
        Err(format!("construction navigation exceeded its whole-attempt retry budget; attempts=8 query_rows={} elapsed_seconds={:.6} pinned_sequence={pinned_sequence} current_sequence={} last_rejection={last_rejection}", queries.len(), started.elapsed().as_secs_f64(), self.current_view().sequence))
    }
    /// Protect endpoints in owner-ID order. The driver records its completion
    /// decision once and waits for both endpoint acknowledgements at each barrier.
    pub fn transfer(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
    ) -> Result<(), String> {
        self.transfer_for_job(source, destination, ids, None)
    }
    pub(super) fn transfer_for_job(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        if source == destination || ids.is_empty() {
            return Ok(());
        }
        if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err("duplicate vector ID in a posting transfer".into());
        }
        if source.authority == self.local.owner && destination.authority == self.local.owner {
            return self.transfer_local_exact_for_job(source, destination, ids, producer);
        }
        self.transfer_with_protocol(source, destination, ids)
    }
    pub(super) fn transfer_with_protocol(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
    ) -> Result<(), String> {
        // JSON can use four bytes for each code byte. A 512 KiB raw-code
        // budget leaves ample room for IDs, routing dependencies and framing.
        // Each chunk completes both release barriers before the next snapshots
        // current source/destination revisions and centroid codes.
        let rows_per_chunk = (512 * 1024 / self.local.code_size().max(1)).max(1);
        for chunk in ids.chunks(rows_per_chunk) {
            self.transfer_chunk(source, destination, chunk.to_vec())?;
        }
        Ok(())
    }
    fn transfer_chunk(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
    ) -> Result<(), String> {
        let mut observed = self.local.observability.operation(Scope::TransferDriver);
        observed.add(Counter::TransferGroups, 1);
        observed.add(Counter::TransferProposedRows, ids.len() as u64);
        let source_image = self.node(source)?;
        let dest = self.node(destination)?;
        if !dest.leaf {
            return Err("transfer destination is internal".into());
        }
        let quantize = self.local.observability.operation(Scope::TransferQuantize);
        let mut codes = Vec::new();
        for id in &ids {
            let vector = self.local.vector(*id)?;
            codes.extend_from_slice(Code::<1>::quantize(&vector, &dest.centroid).as_ref());
        }
        drop(quantize);
        // Bind the immutable input with a deterministic digest; payload equality
        // additionally checks the complete ordered ID list at each endpoint.
        let bytes = bincode::serialize(&(
            source,
            destination,
            source_image.revision,
            dest.revision,
            &ids,
            &codes,
        ))
        .map_err(|e| e.to_string())?;
        let digest = sha256_bytes(&bytes);
        let payload = TransferPayload {
            source_owner: source.authority,
            destination_owner: destination.authority,
            source_leaf: source.id,
            destination_leaf: destination.id,
            source_placement: source.placement,
            destination_placement: destination.placement,
            source_revision: source_image.revision,
            destination_revision: dest.revision,
            source_centroid_digest: centroid_digest(&source_image.centroid),
            destination_centroid_digest: centroid_digest(&dest.centroid),
            vectors: ids,
            content_digest: digest,
        };
        let e = self.local.envelope(
            "posting-transfer",
            vec![
                (source.id, source.placement),
                (destination.id, destination.placement),
            ],
        )?;
        let mut endpoints = vec![
            (source.authority, Role::Source),
            (destination.authority, Role::Destination),
        ];
        endpoints.sort_by_key(|(owner, role)| {
            (
                *owner,
                if *role == Role::Source {
                    source.id
                } else {
                    destination.id
                },
            )
        });
        struct TransferFlight<'a> {
            owner: &'a OwnerService,
            resolved: bool,
        }
        impl Drop for TransferFlight<'_> {
            fn drop(&mut self) {
                if !self.resolved {
                    self.owner.poisoned.store(true, Ordering::Release);
                }
            }
        }
        let mut flight = TransferFlight {
            owner: &self.local,
            resolved: false,
        };
        let mut accepted = Vec::new();
        for (owner, role) in &endpoints {
            match self.request(
                *owner,
                EngineRequest::PrepareTransfer {
                    envelope: e.clone(),
                    payload: payload.clone(),
                    role: *role,
                    codes: if *role == Role::Destination {
                        codes.clone()
                    } else {
                        vec![]
                    },
                },
            ) {
                Ok(EngineResponse::Transfer(state)) if state != TransferState::Aborted => {
                    accepted.push((*owner, *role))
                }
                outcome => {
                    let known_rejection = matches!(&outcome, Err(error) if !error.starts_with("transport "))
                        || matches!(
                            &outcome,
                            Ok(EngineResponse::Transfer(TransferState::Aborted))
                        );
                    let reason = match outcome {
                        Err(e) => e,
                        Ok(_) => "invalid transfer preparation acknowledgement".into(),
                    };
                    let proof = TransferProof {
                        abort_decided: true,
                        ..Default::default()
                    };
                    for (authority, endpoint_role) in accepted {
                        match self.request(
                            authority,
                            EngineRequest::TransferStep {
                                envelope: e.clone(),
                                role: endpoint_role,
                                step: TransferStep::Abort,
                                proof,
                            },
                        ) {
                            Ok(EngineResponse::Transfer(state))
                                if validate_transfer_ack(
                                    endpoint_role,
                                    TransferStep::Abort,
                                    state,
                                )
                                .is_ok() => {}
                            _ => {
                                return Err(format!(
                                    "unresolved transfer preparation/abort: {reason}"
                                ));
                            }
                        }
                    }
                    // Transport failure may have accepted the endpoint whose
                    // acknowledgement was lost; absence from `accepted` is not
                    // proof that it holds no protection. Keep the run poisoned.
                    if known_rejection {
                        flight.resolved = true;
                    }
                    observed.add(Counter::TransferRejectedGroups, 1);
                    return Err(reason);
                }
            }
        }
        let mut driver = TransferDriver::default();
        let perform =
            |role: Role, step: TransferStep, driver: &mut TransferDriver| -> Result<(), String> {
                let owner = if role == Role::Source {
                    source.authority
                } else {
                    destination.authority
                };
                let mut last = None;
                for retry in 0..3 {
                    if retry > 0 {
                        let mut retry_work =
                            self.local.observability.operation(Scope::TransferRetry);
                        retry_work.add(Counter::TransferRetryCalls, 1);
                    }
                    match self.request(
                        owner,
                        EngineRequest::TransferStep {
                            envelope: e.clone(),
                            role,
                            step,
                            proof: driver.proof(),
                        },
                    ) {
                        Ok(EngineResponse::Transfer(state)) => {
                            validate_transfer_ack(role, step, state)?;
                            return driver
                                .acknowledge(role, step)
                                .map_err(|error| error.to_string());
                        }
                        Ok(_) => return Err("invalid transfer acknowledgement".into()),
                        Err(error) => {
                            let retry = error.starts_with("transport ");
                            last = Some(error);
                            if !retry {
                                break;
                            }
                        }
                    }
                }
                Err(format!(
                    "unresolved transfer {step:?} {role:?}: {}",
                    last.unwrap_or_default()
                ))
            };
        perform(Role::Destination, TransferStep::Stage, &mut driver)?;
        driver
            .decide(DriverDecision::Complete)
            .map_err(|x| x.to_string())?;
        for role in [Role::Source, Role::Destination] {
            perform(role, TransferStep::BeginCompletion, &mut driver)?;
        }
        perform(Role::Source, TransferStep::RemoveSource, &mut driver)?;
        perform(
            Role::Destination,
            TransferStep::ActivateDestination,
            &mut driver,
        )?;
        for role in [Role::Source, Role::Destination] {
            perform(role, TransferStep::Complete, &mut driver)?;
        }
        for (_, role) in endpoints.iter().rev() {
            perform(*role, TransferStep::Release, &mut driver)?;
        }
        if !driver.success() {
            return Err("transfer did not receive every release acknowledgement".into());
        }
        observed.add(Counter::TransferCompletedGroups, 1);
        flight.resolved = true;
        self.local
            .transfers
            .fetch_add(payload.vectors.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}
impl Cluster {
    fn expand_local_nodes_in_view(
        &self,
        view: &super::protocol::publication::PublishedView<super::construction::BoundaryView>,
        nodes: &[NodeRef],
        query: &[f32],
        run: u128,
        epoch: u64,
        shared_sequence: u64,
        children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        if nodes.is_empty() {
            return Ok(());
        }
        self.local.ensure_healthy()?;
        if run != self.local.run || epoch != self.local.epoch.load(Ordering::Acquire) {
            return Err("stale expansion run or phase".into());
        }
        if view.sequence != shared_sequence {
            return Err("navigation route changed; discard the complete attempt and retry".into());
        }
        enum Image<'a> {
            Shared(&'a RoutingNode),
            Owned(Arc<RoutingNode>),
        }
        let mut images = Vec::with_capacity(nodes.len());
        {
            // Retain only immutable image handles. No posting list is loaded or
            // resident membership lock held while scoring these packed rows.
            let _gate = self.local.routing_gate.read();
            let published = self.local.routing_images.read();
            for node in nodes {
                if let Some(image) = view.value.shared.get(&node.id) {
                    if image.target != *node {
                        return Err("navigation route changed: shared placement".into());
                    }
                    images.push(Image::Shared(image));
                } else if node.authority == self.local.owner {
                    let image = published
                        .get(&node.id)
                        .filter(|image| image.target == *node)
                        .cloned()
                        .ok_or_else(|| format!("navigation route changed: stale local image; target={node:?} current_target={:?}", published.get(&node.id).map(|image|image.target)))?;
                    images.push(Image::Owned(image));
                } else {
                    remote.push(*node);
                }
            }
        }
        #[cfg(test)]
        if let Some(hook) = self.navigation_images_hook.lock().clone() {
            hook(nodes);
        }
        let mut owned = images
            .iter()
            .any(|image| matches!(image, Image::Owned(_)))
            .then(|| {
                self.local
                    .observability
                    .operation(Scope::OwnedRoutingScoring)
            });
        let mut shared = images
            .iter()
            .any(|image| matches!(image, Image::Shared(_)))
            .then(|| {
                self.local
                    .observability
                    .operation(Scope::SharedRoutingScoring)
            });
        let mut successful_owned_distances = 0_u64;
        let mut owned_expansions = 0_u64;
        let result = (|| {
            for image in images {
                let (image, owner_local) = match &image {
                    Image::Shared(image) => (*image, false),
                    Image::Owned(image) => (image.as_ref(), true),
                };
                if image.leaf {
                    return Err("navigation route changed: selected internal became leaf".into());
                }
                if owner_local {
                    owned_expansions += 1;
                    owned
                        .as_mut()
                        .unwrap()
                        .add(Counter::OwnedRoutingDistances, image.children.len() as u64);
                } else {
                    shared
                        .as_mut()
                        .unwrap()
                        .add(Counter::SharedRoutingDistances, image.children.len() as u64);
                }
                children.reserve(image.children.len());
                for (ordinal, target, leaf, centroid) in image.scored_children()? {
                    let distance = self.local.metric.distance(query, centroid);
                    if !distance.is_finite() {
                        return Err("nonfinite centroid distance".into());
                    }
                    children.push(ScoredChild {
                        parent: image.target.id,
                        ordinal,
                        target,
                        leaf,
                        distance,
                        observed_revision: image.revision,
                    });
                    if owner_local {
                        successful_owned_distances += 1;
                    }
                }
            }
            Ok(())
        })();
        self.local
            .expansions
            .fetch_add(owned_expansions, Ordering::Relaxed);
        self.local
            .distances
            .fetch_add(successful_owned_distances, Ordering::Relaxed);
        result
    }
    fn expand_in_view(
        &self,
        view: &super::protocol::publication::PublishedView<super::construction::BoundaryView>,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        let mut observed = self.local.observability.operation(Scope::ExpansionBackend);
        let mut replies = vec![];
        let mut remote = vec![];
        // Only shared upper-tree records are answered from this cache. Owned
        // subtree records always expand through their actual authority.
        for request in requests {
            let mut local_children = vec![];
            let mut selected = vec![];
            let mut pending = vec![];
            {
                if view.sequence != request.shared_sequence {
                    replies.push(ExpandReply {
                        authority,
                        run: request.run,
                        epoch: request.epoch,
                        shared_sequence: request.shared_sequence,
                        child_counts: vec![],
                        navigation: request.navigation,
                        step: request.step,
                        query_slot: request.query_slot,
                        nodes: request.nodes.clone(),
                        status: ExpansionStatus::Stale,
                        children: vec![],
                    });
                    continue;
                }
                let shared = &view.value.shared;
                for node in &request.nodes {
                    if let Some(image) = shared.get(&node.id) {
                        selected.push(*node);
                        let _scoring = self
                            .local
                            .observability
                            .operation(Scope::SharedRoutingScoring);
                        observed.add(Counter::SharedRoutingDistances, image.children.len() as u64);
                        for (ordinal, target, leaf, centroid) in image.scored_children()? {
                            local_children.push(ScoredChild {
                                parent: node.id,
                                ordinal,
                                target,
                                leaf,
                                distance: self.local.metric.distance(&request.query, centroid),
                                observed_revision: image.revision,
                            });
                        }
                    } else {
                        pending.push(*node)
                    }
                }
            }
            if !selected.is_empty() {
                let child_counts = selected
                    .iter()
                    .map(|node| {
                        (
                            node.id,
                            local_children
                                .iter()
                                .filter(|child| child.parent == node.id)
                                .count(),
                        )
                    })
                    .collect();
                replies.push(ExpandReply {
                    authority,
                    run: request.run,
                    epoch: request.epoch,
                    shared_sequence: request.shared_sequence,
                    child_counts,
                    navigation: request.navigation,
                    step: request.step,
                    query_slot: request.query_slot,
                    nodes: selected,
                    status: ExpansionStatus::Complete,
                    children: local_children,
                });
            }
            if !pending.is_empty() {
                remote.push(ExpandRequest {
                    nodes: pending,
                    ..request
                });
            }
        }
        if !remote.is_empty() {
            if authority == self.local.owner {
                match self.request(authority, EngineRequest::Expand(remote))? {
                    EngineResponse::Expanded(mut rows) => replies.append(&mut rows),
                    _ => return Err("invalid expansion response".into()),
                }
            } else {
                replies.append(&mut self.expand_remote_bounded(authority, remote)?);
            }
        }
        Ok(replies)
    }
    fn expand_remote_bounded(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        let client = self
            .peers
            .get(&authority)
            .ok_or("unknown expansion owner endpoint")?;
        let deadline = client.bounded_deadline(Instant::now() + self.deadline);
        self.expand_remote_until(authority, requests, deadline)
    }
    fn expand_remote_until(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
        deadline: Instant,
    ) -> Result<Vec<ExpandReply>, String> {
        static CONCURRENCY: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let concurrency = *CONCURRENCY.get_or_init(|| {
            match std::env::var("SPANN_EXPANSION_GROUP_CONCURRENCY").as_deref() {
                Ok("2") => 2,
                _ => 1,
            }
        });
        self.expand_remote_with_concurrency(authority, requests, deadline, concurrency)
    }

    fn expand_remote_with_concurrency(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
        deadline: Instant,
        concurrency: usize,
    ) -> Result<Vec<ExpandReply>, String> {
        assert!((1..=2).contains(&concurrency));
        // Detailed diagnostics distinguish owner fanout from sequential chunks
        // within one owner. Despite its retained telemetry name, the scope
        // measures the whole invocation, including overlapped groups and splits.
        let initial_groups = requests.len().div_ceil(64);
        let mut group_work = super::observability::detailed().then(|| {
            let mut work = self
                .local
                .observability
                .operation(Scope::ExpansionSequentialGroups);
            work.add(Counter::ExpansionRemoteCalls, 1);
            work.add(Counter::ExpansionInitialGroups, initial_groups as u64);
            let bucket = match initial_groups {
                0 => None,
                1 => Some(Counter::ExpansionGroups1),
                2 => Some(Counter::ExpansionGroups2),
                3..=4 => Some(Counter::ExpansionGroups3To4),
                5..=8 => Some(Counter::ExpansionGroups5To8),
                _ => Some(Counter::ExpansionGroups9Plus),
            };
            if let Some(bucket) = bucket {
                work.add(bucket, 1);
            }
            if initial_groups > 1 {
                work.add(Counter::ExpansionMultiGroupCalls, 1);
            }
            work
        });
        let mut input = requests.into_iter();
        let mut pending = std::collections::VecDeque::new();
        loop {
            // Preserve the existing insertion window. A balance may have many
            // more queries, but no individual RPC starts with more than 64.
            let group = input.by_ref().take(64).collect::<Vec<_>>();
            if group.is_empty() {
                break;
            }
            pending.push_back(group);
        }
        // Each invocation creates at most one extra worker, independent of its
        // group count. Both lanes use the same absolute deadline and inherit
        // the initiating job's attribution. A one-group insertion stays inline.
        let cancelled = AtomicBool::new(false);
        let run = |groups: Vec<(usize, Vec<ExpandRequest>)>| {
            let mut results = Vec::new();
            let mut subcalls = 0;
            let mut splits = 0;
            for (index, group) in groups {
                if cancelled.load(Ordering::Acquire) {
                    break;
                }
                let result = self.expand_remote_group(
                    authority,
                    group,
                    deadline,
                    &mut subcalls,
                    &mut splits,
                );
                if result.is_err() {
                    cancelled.store(true, Ordering::Release);
                }
                results.push((index, result));
            }
            (results, subcalls, splits)
        };
        let groups: Vec<_> = pending.into_iter().enumerate().collect();
        let (mut results, subcalls, splits) = if concurrency == 2 && groups.len() > 1 {
            let (left, right): (Vec<_>, Vec<_>) =
                groups.into_iter().partition(|(index, _)| index % 2 == 0);
            let context = super::observability::current_context();
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    let _context = super::observability::install_context(context);
                    run(right)
                });
                let (mut first, first_calls, first_splits) = run(left);
                let (second, second_calls, second_splits) = worker.join().unwrap_or_else(|_| {
                    (
                        vec![(usize::MAX, Err("expansion worker panicked".into()))],
                        0,
                        0,
                    )
                });
                first.extend(second);
                (
                    first,
                    first_calls + second_calls,
                    first_splits + second_splits,
                )
            })
        } else {
            run(groups)
        };
        if let Some(work) = group_work.as_mut() {
            work.add(Counter::ExpansionSubcalls, subcalls);
            work.add(Counter::ExpansionSizeSplits, splits);
        }
        results.sort_unstable_by_key(|(index, _)| *index);
        let mut replies = Vec::new();
        for (_, result) in results {
            replies.extend(result?);
        }
        // Join all started work before exposing complete, original-order rows.
        Ok(replies)
    }

    fn expand_remote_group(
        &self,
        authority: AuthorityId,
        group: Vec<ExpandRequest>,
        deadline: Instant,
        subcalls: &mut u64,
        splits: &mut u64,
    ) -> Result<Vec<ExpandReply>, String> {
        let client = self
            .peers
            .get(&authority)
            .ok_or("unknown expansion owner endpoint")?;
        let mut pending = std::collections::VecDeque::from([group]);
        let mut replies = vec![];
        while let Some(group) = pending.pop_front() {
            *subcalls += 1;
            let mut observed = self.local.observability.operation(Scope::EngineRequest);
            observed.add(Counter::RemoteEngineCalls, 1);
            let request = EngineRequest::Expand(group);
            let result =
                client.call::<_, EngineResponse>(&request, RequestClass::Control, deadline);
            let EngineRequest::Expand(mut group) = request else {
                unreachable!()
            };
            match result {
                Ok(EngineResponse::Expanded(mut rows)) => replies.append(&mut rows),
                Ok(_) => return Err("invalid expansion response".into()),
                Err(TransportError::FrameTooLarge | TransportError::ReplyTooLarge) => {
                    let (left, right) = if group.len() > 1 {
                        let right = group.split_off(group.len() / 2);
                        (group, right)
                    } else {
                        let mut left = group.pop().ok_or("empty oversized expansion group")?;
                        if left.nodes.len() <= 1 {
                            return Err(format!("one complete parent expansion exceeds the transport or codec size limit; owner={authority} query_slot={} nodes={:?}", left.query_slot, left.nodes));
                        }
                        let right_nodes = left.nodes.split_off(left.nodes.len() / 2);
                        observed.add(
                            Counter::NavigationQueryBytesCopied,
                            left.query.len() as u64 * 4,
                        );
                        let right = ExpandRequest {
                            navigation: left.navigation,
                            step: left.step,
                            query_slot: left.query_slot,
                            query: left.query.clone(),
                            nodes: right_nodes,
                            run: left.run,
                            epoch: left.epoch,
                            shared_sequence: left.shared_sequence,
                        };
                        (vec![left], vec![right])
                    };
                    *splits += 1;
                    pending.push_front(right);
                    pending.push_front(left);
                }
                // An EOF, application rejection or arbitrary codec failure has
                // no size proof. Return it without resubmitting any operation.
                Err(error) => return Err(error.to_string()),
            }
        }
        // No caller receives a prefix when a later group fails. The navigation
        // driver still validates and globally selects only after every reply.
        Ok(replies)
    }
}

/// Each attempt uses one immutable shared scoring view. Publications that only
/// change centroids cannot invalidate its live owner identities. Structural or
/// placement changes restart the complete global beam instead of mixing views.
struct NavigationAttempt<'a> {
    cluster: &'a Cluster,
    view: Arc<super::protocol::publication::PublishedView<super::construction::BoundaryView>>,
}
impl NavigationAttempt<'_> {
    fn check_topology(&self) -> Result<(), String> {
        let current = self.cluster.current_view();
        let pinned = self
            .view
            .value
            .navigation_topology
            .ok_or("unprepared navigation topology")?;
        let latest = current
            .value
            .navigation_topology
            .ok_or("unprepared current navigation topology")?;
        if pinned != latest {
            return Err(format!("navigation route changed: shared topology or placement; pinned_sequence={} current_sequence={}", self.view.sequence, current.sequence));
        }
        Ok(())
    }
}
impl ExpansionBackend for NavigationAttempt<'_> {
    fn observer(&self) -> Option<&WorkProfile> {
        Some(&self.cluster.local.observability)
    }
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        run: u128,
        epoch: u64,
        shared_sequence: u64,
        children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        self.check_topology()?;
        self.cluster.expand_local_nodes_in_view(
            &self.view,
            nodes,
            query,
            run,
            epoch,
            shared_sequence,
            children,
            remote,
        )
    }
    fn expand(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        self.check_topology()?;
        self.cluster.expand_in_view(&self.view, authority, requests)
    }
}
impl ExpansionBackend for Cluster {
    fn observer(&self) -> Option<&WorkProfile> {
        Some(&self.local.observability)
    }
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        run: u128,
        epoch: u64,
        shared_sequence: u64,
        children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        self.expand_local_nodes_in_view(
            &self.current_view(),
            nodes,
            query,
            run,
            epoch,
            shared_sequence,
            children,
            remote,
        )
    }
    fn expand(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        self.expand_in_view(&self.current_view(), authority, requests)
    }
}

// A small local SHA-256 implementation is intentionally not introduced here;
// use the workspace's existing hash crate for upload and transfer identities.
pub(super) fn centroid_digest(centroid: &[f32]) -> [u8; 32] {
    use sha2::Digest;
    let mut hash = sha2::Sha256::new();
    hash.update((centroid.len() as u64).to_le_bytes());
    for coordinate in centroid {
        hash.update(coordinate.to_bits().to_le_bytes());
    }
    hash.finalize().into()
}
pub(super) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).into()
}

#[cfg(test)]
#[path = "engine_endpoint_tests.rs"]
mod endpoint_tests;

/// An acknowledgement proves that this step, or a later idempotent state,
/// actually committed at the endpoint before the driver builds its next proof.
pub(super) fn validate_transfer_ack(
    role: Role,
    step: TransferStep,
    state: TransferState,
) -> Result<(), String> {
    use TransferState::*;
    let valid = match step {
        TransferStep::Stage => {
            role == Role::Destination
                && matches!(state, Staged | Completing | Applied | Completed | Released)
        }
        TransferStep::BeginCompletion => {
            matches!(state, Completing | Applied | Completed | Released)
        }
        TransferStep::RemoveSource => {
            role == Role::Source && matches!(state, Applied | Completed | Released)
        }
        TransferStep::ActivateDestination => {
            role == Role::Destination && matches!(state, Applied | Completed | Released)
        }
        TransferStep::Complete => matches!(state, Completed | Released),
        TransferStep::Release => state == Released,
        TransferStep::Abort => state == Aborted,
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid transfer {role:?} {step:?} acknowledgement: {state:?}"
        ))
    }
}
#[cfg(test)]
mod transfer_ack_tests {
    use super::*;
    #[test]
    fn earlier_or_aborted_endpoint_state_cannot_prove_application() {
        for state in [
            TransferState::Prepared,
            TransferState::Staged,
            TransferState::Completing,
            TransferState::Aborted,
        ] {
            assert!(
                validate_transfer_ack(Role::Source, TransferStep::RemoveSource, state).is_err()
            );
            assert!(validate_transfer_ack(
                Role::Destination,
                TransferStep::ActivateDestination,
                state
            )
            .is_err());
        }
        assert!(validate_transfer_ack(
            Role::Source,
            TransferStep::RemoveSource,
            TransferState::Applied
        )
        .is_ok());
        assert!(validate_transfer_ack(
            Role::Destination,
            TransferStep::Complete,
            TransferState::Released
        )
        .is_ok());
        assert!(validate_transfer_ack(
            Role::Source,
            TransferStep::Release,
            TransferState::Completed
        )
        .is_err());
    }
}

/// A writer checkpoint retains full-precision routing and exact posting bytes.
/// Restore requires a fresh process and advances the sealed mutation epoch.
#[derive(Serialize, Deserialize)]
pub struct WriterCheckpoint {
    version: u32,
    owner: u64,
    run: u128,
    epoch: u64,
    sequence: u64,
    shared_sequence: u64,
    dimensions: usize,
    metric: String,
    config: HierarchicalSpannConfig,
    pub next_input: u64,
    #[serde(default)]
    pub balance_pending: bool,
    pub input_sha256: String,
    nodes: Vec<StoredNode>,
    view: super::protocol::publication::PublishedView<super::construction::BoundaryView>,
    inserted: super::ingestion::InsertedIds,
}
impl WriterCheckpoint {
    pub fn read(path: &std::path::Path) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
        let expected =
            std::fs::read_to_string(path.with_extension("sha256")).map_err(|e| e.to_string())?;
        if format!("{:x}", Sha256::digest(&bytes)) != expected.trim() {
            return Err("writer checkpoint checksum mismatch".into());
        }
        let value: Self = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if value.version != 1 && value.version != 2 {
            return Err("unsupported writer checkpoint".into());
        }
        Ok(value)
    }
    pub fn root(&self) -> RoutingNode {
        self.view.value.root.clone()
    }
}
impl OwnerService {
    fn write_checkpoint(
        &self,
        run: u128,
        epoch: u64,
        path: &std::path::Path,
        next_input: u64,
        input_sha256: String,
        balance_pending: bool,
    ) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let (nodes, _) = self.save_quiescent_snapshot(run, epoch, &BTreeMap::new())?;
        let cluster = self.cluster()?;
        let checkpoint = WriterCheckpoint {
            version: 2,
            owner: self.owner,
            run,
            epoch,
            sequence: self.sequence.load(Ordering::Acquire),
            shared_sequence: cluster.shared_sequence.load(Ordering::Acquire),
            dimensions: self.dim,
            metric: format!("{:?}", self.metric),
            config: self.config.clone(),
            next_input,
            balance_pending,
            input_sha256,
            nodes: nodes.into_iter().map(|n| n.node).collect(),
            view: (*cluster.current_view()).clone(),
            inserted: cluster.inserted_ids.lock().clone(),
        };
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent).map_err(|e| e.to_string())?; }
        let bytes = serde_json::to_vec(&checkpoint).map_err(|e| e.to_string())?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        let mut checksum = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.with_extension("sha256"))
            .map_err(|e| e.to_string())?;
        checksum
            .write_all(format!("{:x}", Sha256::digest(&bytes)).as_bytes())
            .and_then(|_| checksum.sync_all())
            .map_err(|e| e.to_string())?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent).and_then(|file| file.sync_all()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
impl Cluster {
    /// Restore before binding/listening. Terminal operation histories are retired
    /// because the checkpoint's epoch is sealed and the new process opens the next.
    pub fn restore_checkpoint(
        &mut self,
        mut checkpoint: WriterCheckpoint,
        input_sha256: &str,
    ) -> Result<(), String> {
        let owner = &self.local;
        if checkpoint.owner != owner.owner
            || checkpoint.run != owner.run
            || checkpoint.dimensions != owner.dim
            || checkpoint.metric != format!("{:?}", owner.metric)
            || checkpoint.config != owner.config
            || checkpoint.input_sha256 != input_sha256
        {
            return Err("writer checkpoint identity/configuration/input mismatch".into());
        }
        if !owner.nodes.is_empty() || owner.sequence.load(Ordering::Acquire) != 0 {
            return Err("checkpoint restore requires a fresh owner".into());
        }
        let epoch = checkpoint
            .epoch
            .checked_add(1)
            .ok_or("checkpoint epoch exhausted")?;
        checkpoint.view.value = checkpoint.view.value.prepared()?;
        owner.install_initial(checkpoint.nodes)?;
        owner.sequence.store(checkpoint.sequence, Ordering::Release);
        owner.operations.lock().phase = Phase::new(owner.run, epoch, 1_000_000);
        owner.epoch.store(epoch, Ordering::Release);
        self.shared_sequence
            .store(checkpoint.shared_sequence, Ordering::Release);
        *self.inserted_ids.lock() = checkpoint.inserted;
        self.install_view(&checkpoint.view)?;
        self.publication =
            super::protocol::publication::Publication::from_checkpoint(checkpoint.view, 1_000_000);
        Ok(())
    }
    pub fn checkpoint_completed(
        &self,
        directory: &std::path::Path,
        next_input: u64,
        input_sha256: &str,
    ) -> Result<(), String> {
        self.checkpoint_phase(directory, next_input, input_sha256, false, false)
    }

    /// Save a quiescent phase boundary and optionally reopen admission. A marker
    /// is published only after every owner has durably written its snapshot.
    pub fn checkpoint_phase(
        &self,
        directory: &std::path::Path,
        next_input: u64,
        input_sha256: &str,
        balance_pending: bool,
        continue_build: bool,
    ) -> Result<(), String> {
        let run = self.local.run;
        let epoch = self.local.epoch.load(Ordering::Acquire);
        let owners: Vec<_> = std::iter::once(self.local.owner)
            .chain(self.peers.keys().copied())
            .collect();
        for &owner in &owners {
            self.request(owner, EngineRequest::CloseEpoch { run, epoch })?;
        }
        for &owner in &owners {
            self.request(owner, EngineRequest::SealEpoch { run, epoch })?;
        }
        for &owner in &owners {
            self.request_until(
                owner,
                EngineRequest::WriterCheckpoint {
                    run,
                    epoch,
                    path: directory.join(format!("owner-{owner}.json")),
                    next_input,
                    input_sha256: input_sha256.into(),
                    balance_pending,
                },
                Instant::now() + Duration::from_secs(600),
            )?;
        }
        use std::io::Write;
        let mut marker = std::fs::OpenOptions::new().write(true).create_new(true)
            .open(directory.join("complete.json.tmp")).map_err(|e| e.to_string())?;
        marker.write_all(serde_json::to_string(&serde_json::json!({
            "next_input": next_input, "balance_pending": balance_pending,
            "input_sha256": input_sha256, "owners": owners, "epoch": epoch,
            "run": run.to_string()
        })).map_err(|e| e.to_string())?.as_bytes()).map_err(|e| e.to_string())?;
        marker.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(directory.join("complete.json.tmp"), directory.join("complete.json")).map_err(|e| e.to_string())?;
        std::fs::File::open(directory).and_then(|file| file.sync_all()).map_err(|e| e.to_string())?;
        if continue_build {
            let next_epoch = epoch.checked_add(1).ok_or("checkpoint epoch exhausted")?;
            for owner in owners {
                self.request(owner, EngineRequest::OpenEpoch { run, epoch: next_epoch })?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod writer_checkpoint_tests {
    use super::*;
    fn owner() -> Arc<OwnerService> {
        Arc::new(OwnerService::new(
            1,
            3030,
            17,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|id| Ok(Arc::from(vec![id as f32 / 113.; 17]))),
        ))
    }
    #[test]
    fn restored_owned_root_can_have_a_shared_parent() {
        let service = owner();
        let mut root = service.initial_root();
        root.routing.parent = Some(NodeRef {
            id: root.routing.target.id + 100,
            authority: 1,
            placement: 0,
        });
        service.install_initial(vec![root]).unwrap();
    }
    #[test]
    fn oversized_leaf_checkpoint_restores_exact_state_then_balances() {
        let directory = tempfile::tempdir().unwrap();
        let make_owner = || {
            Arc::new(OwnerService::new(
                1,
                3030,
                17,
                DistanceFunction::Euclidean,
                HierarchicalSpannConfig {
                    split_threshold: 16,
                    merge_threshold: 4,
                    branching_factor: 4,
                    ..Default::default()
                },
                Arc::new(|id| Ok(Arc::from(vec![id as f32 / 113.; 17]))),
            ))
        };
        let original = make_owner();
        let root = original.initial_root();
        original.install_initial(vec![root.clone()]).unwrap();
        let cluster = Arc::new(Cluster::new(
            original.clone(),
            BTreeMap::new(),
            root.routing,
        ));
        original.bind_cluster(&cluster);
        cluster.add_batch(&(0..64).collect::<Vec<_>>(), 2).unwrap();
        let before = match original.handle(EngineRequest::Snapshot).unwrap() {
            EngineResponse::Snapshot(s) => s,
            _ => panic!(),
        };
        assert_eq!(
            before.len(),
            1,
            "insertion leaves the oversized root unsplit"
        );
        cluster
            .checkpoint_phase(directory.path(), 64, "fixture", true, true)
            .unwrap();
        assert_eq!(original.epoch.load(Ordering::Acquire), 1);
        assert!(directory.path().join("complete.json").exists());
        let checkpoint = WriterCheckpoint::read(&directory.path().join("owner-1.json")).unwrap();
        assert_eq!(checkpoint.nodes, before);
        assert_eq!(checkpoint.next_input, 64);
        assert!(checkpoint.balance_pending);
        // The original process continues after saving; its mutations cannot
        // affect the immutable snapshot restored by the second process below.
        cluster.balance(2).unwrap();
        cluster.add_batch(&[64], 1).unwrap();
        let next = directory.path().join("next");
        cluster.checkpoint_phase(&next, 65, "fixture", false, true).unwrap();
        assert_eq!(original.epoch.load(Ordering::Acquire), 2);
        assert!(!WriterCheckpoint::read(&next.join("owner-1.json")).unwrap().balance_pending);
        let sequence = checkpoint.sequence;
        let shared_sequence = checkpoint.shared_sequence;
        let view = serde_json::to_vec(&checkpoint.view).unwrap();
        let restored_owner = make_owner();
        let mut restored = Cluster::new(restored_owner.clone(), BTreeMap::new(), checkpoint.root());
        restored.restore_checkpoint(checkpoint, "fixture").unwrap();
        let restored = Arc::new(restored);
        restored_owner.bind_cluster(&restored);
        let snapshot = match restored_owner.handle(EngineRequest::Snapshot).unwrap() {
            EngineResponse::Snapshot(s) => s,
            _ => panic!(),
        };
        assert_eq!(
            snapshot, before,
            "IDs, revisions, full centroids and posting bytes must survive"
        );
        assert_eq!(restored_owner.sequence.load(Ordering::Acquire), sequence);
        assert_eq!(
            restored.shared_sequence.load(Ordering::Acquire),
            shared_sequence
        );
        assert_eq!(serde_json::to_vec(&*restored.current_view()).unwrap(), view);
        assert!(
            restored.add_batch(&[0], 1).is_err(),
            "inserted-ID admission survives"
        );
        restored.balance(2).unwrap();
        let groups = match restored_owner
            .handle(EngineRequest::ValidateWriter)
            .unwrap()
        {
            EngineResponse::Membership(groups) => groups,
            _ => panic!(),
        };
        let mut ids: Vec<_> = groups.into_iter().flat_map(|(_, ids)| ids).collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
        let after = match restored_owner.handle(EngineRequest::Snapshot).unwrap() {
            EngineResponse::Snapshot(s) => s,
            _ => panic!(),
        };
        assert!(
            after.len() > before.len(),
            "balance discovers and splits the oversized leaf"
        );
    }
    #[test]
    fn checkpoint_restores_exact_writer_and_continues_admission() {
        let directory = tempfile::tempdir().unwrap();
        let original = owner();
        let root = original.initial_root();
        original.install_initial(vec![root.clone()]).unwrap();
        let cluster = Arc::new(Cluster::new(
            original.clone(),
            BTreeMap::new(),
            root.routing,
        ));
        original.bind_cluster(&cluster);
        cluster.add_batch(&(0..20).collect::<Vec<_>>(), 2).unwrap();
        cluster.balance(2).unwrap();
        assert!(original
            .write_checkpoint(
                3030,
                0,
                &directory.path().join("live.json"),
                20,
                "fixture".into(),
                false,
            )
            .is_err());
        cluster
            .checkpoint_completed(directory.path(), 20, "fixture")
            .unwrap();
        let path = directory.path().join("owner-1.json");
        let checkpoint = WriterCheckpoint::read(&path).unwrap();
        let before = checkpoint.nodes.clone();
        let next_sequence = checkpoint.sequence;
        let restored_owner = owner();
        let mut restored = Cluster::new(restored_owner.clone(), BTreeMap::new(), checkpoint.root());
        restored.restore_checkpoint(checkpoint, "fixture").unwrap();
        let restored = Arc::new(restored);
        restored_owner.bind_cluster(&restored);
        let snapshot = match restored_owner.handle(EngineRequest::Snapshot).unwrap() {
            EngineResponse::Snapshot(s) => s,
            _ => panic!(),
        };
        assert_eq!(before, snapshot);
        assert!(restored_owner.attempt().unwrap().sequence > next_sequence);
        assert!(restored.add_batch(&[0], 1).is_err());
        restored.add_batch(&[20, 21], 2).unwrap();
        restored.balance(2).unwrap();
        assert!(
            restored_owner.epoch.load(Ordering::Acquire) > original.epoch.load(Ordering::Acquire)
        );
        assert!(cluster
            .checkpoint_completed(directory.path(), 20, "fixture")
            .is_err());
        std::fs::write(&path, b"corrupt").unwrap();
        assert!(WriterCheckpoint::read(&path).is_err());
    }
}

#[cfg(test)]
mod conditional_view_tests {
    use super::*;
    #[test]
    fn unchanged_view_acknowledges_only_the_exact_published_sequence() {
        let owner = Arc::new(OwnerService::new(
            1,
            9,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_| Ok(Arc::from(vec![0.; 8]))),
        ));
        let root = owner.initial_root().routing;
        let mut cluster = Cluster::new(owner.clone(), BTreeMap::new(), root);
        let mut view = (*cluster.publication.view()).clone();
        view.sequence = 7;
        cluster.publication =
            super::super::protocol::publication::Publication::from_checkpoint(view, 100);
        let cluster = Arc::new(cluster);
        owner.bind_cluster(&cluster);
        for sequence in [6, 8] {
            match owner
                .handle(EngineRequest::CurrentViewIfChanged {
                    known_sequence: sequence,
                })
                .unwrap()
            {
                EngineResponse::View(view) => assert_eq!(view.sequence, 7),
                _ => panic!("different version must return the complete view"),
            }
        }
        assert!(matches!(
            owner
                .handle(EngineRequest::CurrentViewIfChanged { known_sequence: 7 })
                .unwrap(),
            EngineResponse::ViewUnchanged { sequence: 7 }
        ));
    }
    #[test]
    fn peer_reuses_exact_view_and_installs_changed_view_over_tcp() {
        use super::super::transport::{RpcServer, TransportConfig};
        let coordinator = Arc::new(OwnerService::new(
            1,
            9,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_| Ok(Arc::from(vec![0.; 8]))),
        ));
        let root = coordinator.initial_root().routing;
        let source = Cluster::new(coordinator, BTreeMap::new(), root.clone());
        let published = Arc::new(RwLock::new((*source.publication.view()).clone()));
        let handler_view = published.clone();
        let config = TransportConfig::default();
        let mut server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "conditional-view-test-token".into(),
            config.clone(),
            Arc::new(move |request: EngineRequest| {
                let view = handler_view.read();
                match request {
                    EngineRequest::CurrentViewIfChanged { known_sequence }
                        if known_sequence == view.sequence =>
                    {
                        Ok(EngineResponse::ViewUnchanged {
                            sequence: view.sequence,
                        })
                    }
                    EngineRequest::CurrentView | EngineRequest::CurrentViewIfChanged { .. } => {
                        Ok(EngineResponse::View(view.clone()))
                    }
                    _ => Err("unexpected request".into()),
                }
            }),
        )
        .unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "conditional-view-test-token".into(),
            config,
        )
        .unwrap();
        let owner = Arc::new(OwnerService::new(
            2,
            9,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_| Ok(Arc::from(vec![0.; 8]))),
        ));
        let cluster = Arc::new(Cluster::new(
            owner.clone(),
            BTreeMap::from([(1, client.clone())]),
            root,
        ));
        owner.bind_cluster(&cluster);
        let first = cluster.refreshed_routing_view().unwrap().0;
        let bytes_before = client.counters().bytes_received;
        let unchanged = cluster.refreshed_routing_view().unwrap().0;
        let unchanged_bytes = client.counters().bytes_received - bytes_before;
        assert!(Arc::ptr_eq(&first, &unchanged));
        {
            let mut view = published.write();
            view.sequence += 1;
            view.value.root.revision += 1;
        }
        let bytes_before = client.counters().bytes_received;
        let changed = cluster.refreshed_routing_view().unwrap().0;
        assert_eq!(changed.sequence, 1);
        assert_eq!(changed.value.root.revision, first.value.root.revision + 1);
        assert!(!Arc::ptr_eq(&first, &changed));
        assert!(client.counters().bytes_received - bytes_before > unchanged_bytes);
        server.shutdown();
    }
    #[test]
    fn empty_peer_cache_rejects_an_unchanged_acknowledgement() {
        use super::super::transport::{RpcServer, TransportConfig};
        let config = TransportConfig::default();
        let mut server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "conditional-view-test-token".into(),
            config.clone(),
            Arc::new(|_: EngineRequest| Ok(EngineResponse::ViewUnchanged { sequence: 0 })),
        )
        .unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "conditional-view-test-token".into(),
            config,
        )
        .unwrap();
        let owner = Arc::new(OwnerService::new(
            2,
            9,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_| Ok(Arc::from(vec![0.; 8]))),
        ));
        let mut root = owner.initial_root().routing;
        root.target.authority = 1;
        let cluster = Arc::new(Cluster::new(
            owner.clone(),
            BTreeMap::from([(1, client)]),
            root,
        ));
        owner.bind_cluster(&cluster);
        assert!(cluster
            .refreshed_routing_view()
            .unwrap_err()
            .contains("unchanged routing acknowledgement"));
        server.shutdown();
    }
}

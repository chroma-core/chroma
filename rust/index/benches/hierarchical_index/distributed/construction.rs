//! Construction over the same logical tree. Owners run clustering and posting
//! edits locally; the coordinator schedules node jobs and publishes boundaries.
use super::super::common::ReadBeamPolicy;
use super::engine::{
    Cluster, EngineRequest, EngineResponse, OwnerService, PostingList, Resident, StoredNode,
};
use super::observability::{Counter, Scope};
use super::placement::{OwnerBudget, OwnershipUnit};
use super::protocol::envelope::Envelope;
use super::protocol::reservation::{RegionManager, RegionNode, RegionRequest, ReservationStatus};
use super::protocol::{AttemptId, AuthorityId, NodeId};
use super::routing::{Child, NodeRef, RoutingNode};
use chroma_index::{
    quantization::Code,
    spann::utils::{self, EmbeddingPoint},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

/// Limit on chained balancing, as in the reference writer: work that a job
/// triggers through moving vectors (the leaves its reassignments and merges
/// touch) is one level deeper than the job, and work deeper
/// than this limit is dropped for the current pass. Every balancing pass
/// starts its out-of-threshold nodes again at depth 0, so dropped work is
/// reconsidered after the next batch has changed the tree. Without the limit
/// a pass can chain splits and reassignments indefinitely.
pub(super) const MAX_BALANCE_DEPTH: u32 = 4;
thread_local! {
    /// Depth of the balancing job running on this thread, if any.
    static JOB_DEPTH: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}
/// Depth of the job running on this thread; 0 outside balancing.
pub(super) fn current_depth() -> u32 {
    JOB_DEPTH.with(|d| d.get().unwrap_or(0))
}
/// Depth for work the running job triggers: one deeper than the job, or 0
/// for work that insertion or a pass start schedules.
pub(super) fn follow_up_depth() -> u32 {
    JOB_DEPTH.with(|d| d.get().map_or(0, |d| d.saturating_add(1)))
}
/// Marks a job's depth on this thread while the job runs.
struct DepthScope(Option<u32>);
impl DepthScope {
    fn enter(depth: u32) -> Self {
        Self(JOB_DEPTH.with(|d| d.replace(Some(depth))))
    }
}
impl Drop for DepthScope {
    fn drop(&mut self) {
        JOB_DEPTH.with(|d| d.set(self.0));
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BalanceJob {
    pub envelope: Envelope,
    pub node: NodeRef,
    pub depth: u32,
    pub internal: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NeighborJob {
    pub envelope: Envelope,
    pub node: NodeRef,
    pub old: Vec<f32>,
    pub left: Vec<f32>,
    pub right: Vec<f32>,
    /// Balancing depth of the split that produced this job.
    #[serde(default)]
    pub depth: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReassignJob {
    pub envelope: Envelope,
    pub source: NodeRef,
    pub ids: Vec<u64>,
    pub excluded: Option<NodeId>,
    pub blocked: Vec<NodeId>,
    /// Exact scheduled attempts whose membership claims prevented this work.
    #[serde(default)]
    pub blocked_jobs: Vec<AttemptId>,
    /// Balancing depth of the job whose reassignment this continues.
    #[serde(default)]
    pub depth: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkJob {
    Balance(BalanceJob),
    Neighbor(NeighborJob),
    Reassign(ReassignJob),
}
impl WorkJob {
    fn attempt(&self) -> AttemptId {
        match self {
            Self::Balance(j) => j.envelope.attempt,
            Self::Neighbor(j) => j.envelope.attempt,
            Self::Reassign(j) => j.envelope.attempt,
        }
    }
    fn node(&self) -> NodeRef {
        match self {
            Self::Balance(j) => j.node,
            Self::Neighbor(j) => j.node,
            Self::Reassign(j) => j.source,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ReassignmentCoalescing {
    pub dispatches: u64,
    pub merged_jobs: u64,
    /// Duplicate IDs removed between combined jobs, excluding normalization
    /// within each original incoming request.
    pub duplicate_pending_ids: u64,
}
const MAX_REASSIGN_IDS: usize = 32_768;
const MAX_REASSIGN_REQUESTS: usize = 1_000_000;
/// Samples occur at dispatch attempts at most ten times per second. They are
/// observations, not time-weighted occupancy; gaps with no scheduler calls are
/// absent. The 300-second bound matches the balance progress deadline.
struct SchedulerDiagnostics {
    started: Instant,
    last_sample: Option<Instant>,
    counts: BTreeMap<String, u64>,
    active_owners: BTreeMap<AttemptId, AuthorityId>,
}
impl Default for SchedulerDiagnostics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            last_sample: None,
            counts: BTreeMap::new(),
            active_owners: BTreeMap::new(),
        }
    }
}
#[derive(Default)]
pub struct JobQueue {
    ready: VecDeque<WorkJob>,
    pending: BTreeSet<NodeId>,
    neighbor_seen: BTreeSet<AttemptId>,
    reassign_seen: BTreeMap<AttemptId, [u8; 32]>,
    active_ids: BTreeSet<NodeId>,
    active_attempts: BTreeSet<AttemptId>,
    active_structural_attempts: BTreeSet<AttemptId>,
    active: usize,
    pub completed: usize,
    pub producer_dependency_skips: u64,
    coalescing: ReassignmentCoalescing,
    pub peak: usize,
    pub failed: Option<String>,
    diagnostics: Option<SchedulerDiagnostics>,
}
impl JobQueue {
    pub(super) fn diagnostic_counts(&self) -> BTreeMap<String, u64> {
        let mut counts = self.queue_eligibility();
        counts.insert("active".into(), self.active as u64);
        counts.insert("completed".into(), self.completed as u64);
        if let Some(diagnostics) = &self.diagnostics {
            counts.extend(diagnostics.counts.clone());
        }
        counts
    }
    /// Queue eligibility applies only to dispatch: downstream reservations and
    /// remote admission happen after dispatch and are measured by job scopes.
    fn queue_eligibility(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for job in &self.ready {
            let node_busy = self.active_ids.contains(&job.node().id);
            let producer_busy = matches!(job, WorkJob::Reassign(j) if j.blocked_jobs.iter().any(|id| self.active_attempts.contains(id)));
            let owner = job.node().authority;
            let reason = match (node_busy, producer_busy) {
                (false, false) => "dispatch_eligible",
                (true, false) => "blocked_node_only",
                (false, true) => "blocked_producer_only",
                (true, true) => "blocked_node_and_producer",
            };
            *counts.entry(format!("owner_{owner}_{reason}")).or_default() += 1;
            *counts
                .entry(
                    if node_busy || producer_busy {
                        "blocked"
                    } else {
                        "ready"
                    }
                    .into(),
                )
                .or_default() += 1;
        }
        counts.entry("ready".into()).or_default();
        counts.entry("blocked".into()).or_default();
        counts
    }
    fn sample_scheduler(&mut self) {
        if !super::observability::detailed() {
            return;
        }
        let diagnostics = self
            .diagnostics
            .get_or_insert_with(SchedulerDiagnostics::default);
        let now = Instant::now();
        if diagnostics
            .last_sample
            .is_some_and(|last| now.duration_since(last) < Duration::from_millis(100))
        {
            return;
        }
        let second = now.duration_since(diagnostics.started).as_secs();
        if second >= 300 {
            return;
        }
        let mut snapshot = self.queue_eligibility();
        snapshot.insert("active".into(), self.active as u64);
        snapshot.insert("completed".into(), self.completed as u64);
        let diagnostics = self.diagnostics.as_mut().unwrap();
        for owner in diagnostics.active_owners.values() {
            *snapshot.entry(format!("owner_{owner}_active")).or_default() += 1;
        }
        diagnostics.last_sample = Some(now);
        *diagnostics
            .counts
            .entry(format!("sample_second_{second}_count"))
            .or_default() += 1;
        for (key, value) in snapshot {
            *diagnostics
                .counts
                .entry(format!("sample_second_{second}_{key}_sum"))
                .or_default() += value;
        }
    }
    fn diagnostic_add(&mut self, owner: AuthorityId, key: &str, value: u64) {
        if let Some(diagnostics) = &mut self.diagnostics {
            *diagnostics
                .counts
                .entry(format!("owner_{owner}_{key}"))
                .or_default() += value;
        }
    }
    #[cfg(test)]
    pub(super) fn queued_balance_nodes(&self) -> Vec<NodeRef> {
        self.ready
            .iter()
            .filter_map(|job| {
                if let WorkJob::Balance(job) = job {
                    Some(job.node)
                } else {
                    None
                }
            })
            .collect()
    }
    pub(super) fn is_drained(&self) -> bool {
        self.ready.is_empty() && self.active == 0
    }
    pub fn enqueue(&mut self, job: BalanceJob) {
        if self.pending.insert(job.node.id) {
            self.ready.push_back(WorkJob::Balance(job))
        }
    }
    pub fn enqueue_neighbor(&mut self, job: NeighborJob) {
        if self.neighbor_seen.insert(job.envelope.attempt) {
            self.ready.push_back(WorkJob::Neighbor(job))
        }
    }
    pub(super) fn enqueue_reassign(
        &mut self,
        job: ReassignJob,
        coordinator: &OwnerService,
    ) -> Result<(), String> {
        self.enqueue_reassign_with_limit(job, coordinator, MAX_REASSIGN_REQUESTS)
    }
    fn enqueue_reassign_with_limit(
        &mut self,
        job: ReassignJob,
        coordinator: &OwnerService,
        request_limit: usize,
    ) -> Result<(), String> {
        self.enqueue_reassign_request(job, coordinator, request_limit, None)
    }
    pub(super) fn enqueue_navigation_retry(
        &mut self,
        job: ReassignJob,
        producer: Option<AttemptId>,
        coordinator: &OwnerService,
    ) -> Result<(), String> {
        self.enqueue_reassign_request(job, coordinator, MAX_REASSIGN_REQUESTS, Some(producer))
    }
    fn enqueue_reassign_request(
        &mut self,
        mut job: ReassignJob,
        coordinator: &OwnerService,
        request_limit: usize,
        navigation_producer: Option<Option<AttemptId>>,
    ) -> Result<(), String> {
        if job.ids.is_empty() || job.ids.len() > MAX_REASSIGN_IDS {
            return Err("invalid deferred reassignment payload bound".into());
        }
        let body: [u8; 32] = Sha256::digest(
            bincode::serialize(&(navigation_producer, &job)).map_err(|e| e.to_string())?,
        )
        .into();
        if let Some(prior) = self.reassign_seen.get(&job.envelope.attempt) {
            return if *prior == body {
                Ok(())
            } else {
                Err("conflicting deferred reassignment duplicate".into())
            };
        }
        if self.reassign_seen.len() >= request_limit {
            return Err("retained reassignment request bound reached".into());
        }
        // The incoming request remains immutable for replay. Its fresh execution
        // envelope is private to the queue until dispatch, so combining pending
        // IDs never changes a body that an owner may already have executed.
        if coordinator.run != job.envelope.run
            || coordinator.epoch.load(Ordering::Acquire) != job.envelope.epoch
        {
            return Err("reassignment execution epoch differs from request".into());
        }
        if let Some(producer) = navigation_producer {
            // Waiting is justified only by a different admitted structural job.
            // Reassignment-only retries cannot manufacture their own progress
            // dependency. The sender returns after enqueue and holds no claim.
            let blockers = self
                .active_structural_attempts
                .iter()
                .copied()
                .filter(|attempt| Some(*attempt) != producer)
                .collect::<Vec<_>>();
            if blockers.is_empty() {
                return Err("navigation retry has no other active structural producer".into());
            }
            job.blocked_jobs.extend(blockers);
            job.blocked_jobs.sort_unstable();
            job.blocked_jobs.dedup();
        }
        let execution = coordinator.envelope(
            "reassign-batch",
            vec![(job.source.id, job.source.placement)],
        )?;
        if execution.run != job.envelope.run || execution.epoch != job.envelope.epoch {
            return Err("reassignment execution epoch differs from request".into());
        }
        let ingress_attempt = job.envelope.attempt;
        job.ids.sort_unstable();
        job.ids.dedup();
        job.envelope = execution;
        self.reassign_seen.insert(ingress_attempt, body);
        self.ready.push_back(WorkJob::Reassign(job));
        Ok(())
    }
    fn coalesce_ready_reassignments(&mut self, selected: &mut ReassignJob) {
        let mut source_barrier = false;
        let active = &self.active_attempts;
        let mut merged_jobs = 0;
        let mut duplicates = 0;
        self.ready.retain(|candidate| {
            // A structural or neighbor decision for this source remains between
            // the preceding and following retry batches, even when it is blocked.
            if candidate.node().id == selected.source.id
                && matches!(candidate, WorkJob::Balance(_) | WorkJob::Neighbor(_))
            {
                source_barrier = true;
            }
            let WorkJob::Reassign(other) = candidate else {
                return true;
            };
            if source_barrier
                || other.source != selected.source
                || other.envelope.run != selected.envelope.run
                || other.envelope.epoch != selected.envelope.epoch
                || other.excluded != selected.excluded
                || other.blocked_jobs.iter().any(|id| active.contains(id))
            {
                return true;
            }
            // Each input is bounded. Sorting this temporary union preserves
            // reference retry order and needs no persistent per-vector state.
            let mut union = selected.ids.clone();
            union.extend_from_slice(&other.ids);
            union.sort_unstable();
            union.dedup();
            if union.len() > MAX_REASSIGN_IDS {
                return true;
            }
            duplicates += (selected.ids.len() + other.ids.len() - union.len()) as u64;
            selected.ids = union;
            // The combined job continues the shallower chain.
            selected.depth = selected.depth.min(other.depth);
            merged_jobs += 1;
            false
        });
        self.coalescing.dispatches += 1;
        self.coalescing.merged_jobs += merged_jobs;
        self.coalescing.duplicate_pending_ids += duplicates;
    }
    fn next(&mut self, owner: AuthorityId) -> Option<WorkJob> {
        self.sample_scheduler();
        self.diagnostic_add(owner, "next_calls", 1);
        for _ in 0..self.ready.len() {
            let mut job = self.ready.pop_front()?;
            let node = job.node();
            let producer_blocked = matches!(&job, WorkJob::Reassign(j) if j.blocked_jobs.iter().any(|id| self.active_attempts.contains(id)));
            if producer_blocked {
                self.producer_dependency_skips += 1;
            }
            // These counts describe visits, not distinct jobs or elapsed time.
            self.diagnostic_add(owner, "queue_visits", 1);
            if node.authority != owner {
                self.diagnostic_add(owner, "wrong_owner_visits", 1);
            }
            if self.active_ids.contains(&node.id) {
                self.diagnostic_add(owner, "active_node_visits", 1);
            }
            if producer_blocked {
                self.diagnostic_add(owner, "active_producer_visits", 1);
            }
            if node.authority != owner || self.active_ids.contains(&node.id) || producer_blocked {
                self.ready.push_back(job);
                continue;
            }
            if let WorkJob::Reassign(reassign) = &mut job {
                self.coalesce_ready_reassignments(reassign);
            }
            if matches!(job, WorkJob::Balance(_)) {
                self.pending.remove(&node.id);
            }
            self.active_ids.insert(node.id);
            self.active_attempts.insert(job.attempt());
            if matches!(job, WorkJob::Balance(_)) {
                self.active_structural_attempts.insert(job.attempt());
            }
            self.active += 1;
            if let Some(diagnostics) = &mut self.diagnostics {
                diagnostics.active_owners.insert(job.attempt(), owner);
            }
            self.diagnostic_add(owner, "dispatches", 1);
            self.peak = self.peak.max(self.active);
            return Some(job);
        }
        None
    }
    fn finish(&mut self, job: &WorkJob, result: Result<(), String>) {
        self.active_ids.remove(&job.node().id);
        self.active_attempts.remove(&job.attempt());
        self.active_structural_attempts.remove(&job.attempt());
        if let Some(diagnostics) = &mut self.diagnostics {
            diagnostics.active_owners.remove(&job.attempt());
        }
        self.active -= 1;
        self.completed += 1;
        if let Err(e) = result {
            self.failed.get_or_insert(e);
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StructuralEdit {
    pub source_claim: Option<AttemptId>,
    pub envelope: Envelope,
    pub source: NodeRef,
    pub expected_revision: u64,
    pub replacements: Vec<RoutingNode>,
    pub dependencies: Vec<(NodeRef, u64)>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublicationResult {
    pub root: RoutingNode,
    pub sequence: u64,
}

pub(super) struct StructuralRecord {
    pub edit: StructuralEdit,
    pub outcome: Option<Result<PublicationResult, String>>,
}

/// A leaf claim prevents posting edits while preparation uses its membership.
/// It holds no node or region lock; other mutation paths reject this token.
pub(super) struct SourceLeafClaim {
    resident: Arc<parking_lot::RwLock<Resident>>,
    token: AttemptId,
}
impl Drop for SourceLeafClaim {
    fn drop(&mut self) {
        let mut node = self.resident.write();
        if node.reserved == Some(self.token) {
            node.reserved = None;
            node.preparing_split = false;
            node.claim_producer = None;
        }
    }
}
impl OwnerService {
    pub(super) fn execute_scheduled_job(&self, job: WorkJob) -> Result<(), String> {
        let _job_context = self.observability.job(
            match &job {
                WorkJob::Balance(_) => super::observability::JobKind::Balance,
                WorkJob::Neighbor(_) => super::observability::JobKind::Neighbor,
                WorkJob::Reassign(_) => super::observability::JobKind::Reassign,
            },
            self.owner,
            job.node().id,
            job.attempt(),
        );
        let _timing = self.observability.operation(match &job {
            WorkJob::Balance(_) => Scope::ScheduledBalance,
            WorkJob::Neighbor(_) => Scope::ScheduledNeighbor,
            WorkJob::Reassign(_) => Scope::ScheduledReassign,
        });
        if job.node().authority != self.owner {
            return Err("scheduled work dispatched to wrong owner".into());
        }
        let envelope = match &job {
            WorkJob::Balance(j) => j.envelope.clone(),
            WorkJob::Neighbor(j) => j.envelope.clone(),
            WorkJob::Reassign(j) => j.envelope.clone(),
        };
        let identity = bincode::serialize(&("scheduled", &job)).map_err(|e| e.to_string())?;
        let depth = match &job {
            WorkJob::Balance(j) => j.depth,
            WorkJob::Neighbor(j) => j.depth,
            WorkJob::Reassign(j) => j.depth,
        };
        self.execute_job(&envelope, identity, || {
            let _depth = DepthScope::enter(depth);
            let cluster = self.cluster()?;
            match job {
                WorkJob::Balance(j) => cluster.run_job_for_producer(j, Some(envelope.attempt)),
                WorkJob::Neighbor(j) => cluster.neighbor_reassign_for_job(
                    j.node,
                    &j.old,
                    &j.left,
                    &j.right,
                    &mut BTreeSet::new(),
                    Some(envelope.attempt),
                ),
                WorkJob::Reassign(j) => cluster.run_reassign_job(j, Some(envelope.attempt)),
            }
        })
    }
    fn claim_source_leaf(&self, source: NodeRef) -> Result<Option<SourceLeafClaim>, String> {
        self.claim_source_leaf_for_job(source, None)
    }
    pub(super) fn claim_source_leaf_for_job(
        &self,
        source: NodeRef,
        producer: Option<AttemptId>,
    ) -> Result<Option<SourceLeafClaim>, String> {
        let handle = match self.resident(source) {
            Ok(h) => h,
            Err(_) => return Ok(None),
        };
        let token = self.attempt()?;
        {
            let mut node = handle.write();
            if node.reserved.is_some() || node.postings.is_none() {
                return Ok(None);
            }
            node.reserved = Some(token);
            node.preparing_split = true;
            node.claim_producer = producer;
        }
        Ok(Some(SourceLeafClaim {
            resident: handle,
            token,
        }))
    }
}
struct RegionGuard {
    manager: Arc<RegionManager>,
    attempt: AttemptId,
}
impl Drop for RegionGuard {
    fn drop(&mut self) {
        let _ = self.manager.release(self.attempt);
    }
}
impl OwnerService {
    pub(super) fn rebuild_regions_drained(&self) -> Result<(), String> {
        let mut graph = BTreeMap::new();
        for entry in self.nodes.iter() {
            let node = entry.read();
            let r = &node.routing;
            graph.insert(
                r.target.id,
                RegionNode {
                    parent: r
                        .parent
                        .filter(|p| p.authority == self.owner && self.nodes.contains_key(&p.id))
                        .map(|p| p.id),
                    placement: r.target.placement,
                    revision: r.revision,
                    live: true,
                },
            );
        }
        *self.regions.write() = Arc::new(
            RegionManager::new_profiled(graph, 1_000_000, self.observability.clone())
                .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?,
        );
        Ok(())
    }
    fn reserve(&self, roots: Vec<NodeRef>) -> Result<RegionGuard, String> {
        let mut queue_work = self.observability.operation(Scope::ReservationQueueWait);
        let manager = Arc::clone(&self.regions.read());
        let attempt = self.attempt()?;
        let request = RegionRequest {
            roots: roots.iter().map(|r| r.id).collect(),
            topology_sequence: manager.sequence(),
            placements: roots.iter().map(|r| (r.id, r.placement)).collect(),
        };
        let mut status = manager
            .request(attempt, request)
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        let was_queued = status == ReservationStatus::Queued;
        let queue_started = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(30);
        while status == ReservationStatus::Queued {
            if let Err(error) = self.ensure_healthy() {
                if manager.cancel(attempt).is_err()
                    && manager.status(attempt).ok() == Some(ReservationStatus::Granted)
                {
                    manager.release(attempt).map_err(|e| e.to_string())?;
                }
                return Err(error);
            }
            if Instant::now() >= deadline {
                if manager.cancel(attempt).is_err()
                    && manager.status(attempt).ok() == Some(ReservationStatus::Granted)
                {
                    // A grant can win the race with cancellation. No data edit
                    // has begun, so release that authoritative grant here.
                    manager.release(attempt).map_err(|e| e.to_string())?;
                }
                return Err("region reservation progress deadline".into());
            }
            std::thread::yield_now();
            status = manager
                .status(attempt)
                .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        }
        if was_queued {
            queue_work.add(
                Counter::ReservationQueuedWaitNanos,
                queue_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
        }
        if status != ReservationStatus::Granted {
            return Err("region dependencies changed; replan".into());
        }
        Ok(RegionGuard { manager, attempt })
    }
    pub(super) fn eligible(&self) -> Result<Vec<(NodeRef, usize, bool)>, String> {
        let cluster = self.cluster()?;
        let root = cluster.current_view().value.root.target;
        let mut jobs = vec![];
        for handle in self
            .nodes
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect::<Vec<_>>()
        {
            let n = handle.read();
            let r = &n.routing;
            if let Some(p) = &n.postings {
                if p.ids.len() > self.config.split_threshold
                    || (r.target != root
                        && !p.ids.is_empty()
                        && p.ids.len() < self.config.merge_threshold)
                {
                    jobs.push((r.target, p.ids.len(), false))
                }
            } else if r.children.len() > self.config.branching_factor
                || r.children.is_empty()
                || (r.target == root && r.children.len() == 1)
            {
                jobs.push((r.target, r.children.len(), true))
            }
        }
        jobs.sort_unstable_by_key(|(node, len, _)| (std::cmp::Reverse(*len), node.id));
        Ok(jobs)
    }
    pub(super) fn subtree_children(&self, root: NodeRef) -> Result<Vec<OwnershipUnit>, String> {
        let image = self.node(root)?;
        image
            .children
            .iter()
            .map(|c| {
                Ok(OwnershipUnit {
                    root: c.target,
                    vectors: self.subtree_count(c.target)?,
                })
            })
            .collect()
    }
    pub(super) fn subtree_count(&self, root: NodeRef) -> Result<u64, String> {
        let mut total = 0_u64;
        let mut todo = vec![root];
        let mut seen = BTreeSet::new();
        while let Some(node) = todo.pop() {
            if !seen.insert(node.id) {
                return Err("cycle in owned subtree".into());
            }
            let r = self.resident(node)?;
            let r = r.read();
            if let Some(p) = &r.postings {
                total = total
                    .checked_add(p.ids.len() as u64)
                    .ok_or("vector count overflow")?
            } else {
                todo.extend(r.routing.children.iter().map(|c| c.target));
            }
        }
        Ok(total)
    }
    pub(super) fn subtree_snapshot(&self, root: NodeRef) -> Result<Vec<StoredNode>, String> {
        let mut nodes = vec![];
        let mut todo = vec![root];
        let mut seen = BTreeSet::new();
        while let Some(target) = todo.pop() {
            if !seen.insert(target.id) {
                return Err("cycle in handoff source".into());
            }
            let r = self.resident(target)?;
            let n = r.read();
            if n.reserved.is_some() {
                return Err("handoff requires drained mutations".into());
            }
            todo.extend(n.routing.children.iter().map(|c| c.target));
            nodes.push(StoredNode {
                routing: (*n.routing).clone(),
                postings: n.postings.clone(),
            });
        }
        Ok(nodes)
    }
    fn alloc_node(&self) -> Result<NodeRef, String> {
        let id = self.attempt()?;
        Ok(NodeRef {
            id: ((self.owner as u128) << 64) | id.sequence as u128,
            authority: self.owner,
            placement: 0,
        })
    }
    fn fresh_leaf(
        &self,
        target: NodeRef,
        center: Vec<f32>,
        ids: Vec<u64>,
    ) -> Result<StoredNode, String> {
        let zero = vec![0.; self.dim];
        let mut codes = Vec::with_capacity(ids.len() * self.code_size());
        for id in &ids {
            codes.extend_from_slice(Code::<1>::quantize(&self.vector(*id)?, &center).as_ref());
        }
        Ok(StoredNode {
            routing: RoutingNode {
                navigation: Default::default(),
                target,
                parent: None,
                centroid_code: Code::<4>::quantize(&center, &zero).as_ref().to_vec(),
                centroid: center,
                children: vec![],
                leaf: true,
                revision: 0,
            },
            postings: Some(PostingList { ids, codes }),
        })
    }
    /// Split preparation calls the same two-way clustering routine as the writer.
    /// Its temporary u32 labels are slots, not truncated persistent vector IDs.
    fn cluster_points(
        &self,
        points: Vec<(u64, Arc<[f32]>)>,
    ) -> Result<(Vec<f32>, Vec<u64>, Vec<f32>, Vec<u64>), String> {
        if points.len() > u32::MAX as usize {
            return Err("one clustering job exceeds the slot label capacity".into());
        }
        let ids = points.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let input: Vec<EmbeddingPoint> = points
            .into_iter()
            .enumerate()
            .map(|(i, (_, v))| (i as u32, 0, v))
            .collect();
        let (a, left, b, right) = utils::split(input, &self.metric);
        Ok((
            a.to_vec(),
            left.into_iter()
                .map(|(slot, _, _)| ids[slot as usize])
                .collect(),
            b.to_vec(),
            right
                .into_iter()
                .map(|(slot, _, _)| ids[slot as usize])
                .collect(),
        ))
    }
    fn split_leaf(&self, source: NodeRef, depth: u32) -> Result<(), String> {
        self.split_leaf_for_job(source, depth, None)
    }
    fn split_leaf_for_job(
        &self,
        source: NodeRef,
        depth: u32,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        let Some(claim) = self.claim_source_leaf_for_job(source, producer)? else {
            self.cluster()?.schedule(source, depth)?;
            return Ok(());
        };
        let (old, ids) = {
            let node = claim.resident.read();
            let posting = node
                .postings
                .as_ref()
                .ok_or("leaf split target is internal")?;
            if posting.ids.len() <= self.config.split_threshold {
                return Ok(());
            }
            ((*node.routing).clone(), posting.ids.clone())
        };
        let mut preparation = self.observability.operation(Scope::SplitPreparation);
        preparation.add(Counter::SplitAttempts, 1);
        preparation.add(Counter::SplitInputRows, ids.len() as u64);
        let points = ids
            .iter()
            .map(|id| self.vector(*id).map(|v| (*id, v)))
            .collect::<Result<Vec<_>, _>>()?;
        let (left, left_ids, right, right_ids) = self.cluster_points(points)?;
        let mut a = self.alloc_node()?;
        a.placement = source.placement;
        let mut b = self.alloc_node()?;
        b.placement = source.placement;
        let replacements = vec![
            self.fresh_leaf(a, left.clone(), left_ids.clone())?,
            self.fresh_leaf(b, right.clone(), right_ids.clone())?,
        ];
        preparation.add(Counter::SplitPrepared, 1);
        preparation.add(Counter::SplitPreparedRows, ids.len() as u64);
        drop(preparation);
        let mut installation = self.observability.operation(Scope::SplitInstallation);
        let cluster = self.cluster()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut installed = false;
        for _ in 0..32 {
            self.ensure_healthy()?;
            // Ancestor splits may change this leaf's parent and routing revision.
            // The claim keeps its membership and scoring centroid unchanged.
            let current = {
                let node = claim.resident.read();
                if node.reserved != Some(claim.token)
                    || node.routing.target != source
                    || !node.routing.leaf
                    || node.routing.centroid != old.centroid
                {
                    return Err("claimed split source changed incompatibly".into());
                }
                (*node.routing).clone()
            };
            if cluster
                .install_replacements_with_claim(
                    &current,
                    replacements.clone(),
                    true,
                    Some(claim.token),
                )
                .map_err(|e| format!("split publication: {e}"))?
            {
                installed = true;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::yield_now();
        }
        if !installed {
            installation.add(Counter::SplitDiscarded, 1);
            installation.add(Counter::SplitDiscardedRows, ids.len() as u64);
            drop(claim);
            cluster.enqueue(BalanceJob {
                envelope: cluster
                    .local
                    .envelope("balance-job", vec![(source.id, source.placement)])?,
                node: source,
                depth,
                internal: false,
            })?;
            return Ok(());
        }
        // Installation takes over protection until the old node is unlinked.
        // Reassignment runs after this source membership claim is released.
        drop(claim);
        installation.add(Counter::SplitCommitted, 1);
        installation.add(Counter::SplitCommittedRows, ids.len() as u64);
        drop(installation);
        self.splits.fetch_add(1, Ordering::Relaxed);
        // LIRE neighbor reassignment follows the same full-precision eligibility
        // tests and globally navigated neighbor order as the reference writer.
        if depth < MAX_BALANCE_DEPTH {
            let mut evaluated = BTreeSet::new();
            for (from, group, center) in [(a, &left_ids, &left), (b, &right_ids, &right)] {
                let mut proposed = vec![];
                for id in group {
                    evaluated.insert(*id);
                    let v = self.vector(*id)?;
                    if self.metric.distance(&v, center) > self.metric.distance(&v, &old.centroid) {
                        proposed.push(*id)
                    }
                }
                cluster
                    .reassign_for_job(from, proposed, None, producer)
                    .map_err(|e| format!("split self reassignment from {}: {e}", from.id))?;
            }
            let neighbors = cluster
                .navigate(&[old.centroid.clone()], &cluster.write_policy(), None)?
                .pop()
                .unwrap_or_default();
            for (neighbor, _) in neighbors
                .into_iter()
                .take(self.config.reassign_neighbor_count)
            {
                if [source.id, a.id, b.id].contains(&neighbor.id) {
                    continue;
                }
                cluster.neighbor_reassign_for_job(
                    neighbor,
                    &old.centroid,
                    &left,
                    &right,
                    &mut evaluated,
                    producer,
                )?;
            }
        }
        // The children inherit this split's depth: splitting a leaf's own
        // children only refines it and ends after a few halvings. Chains that
        // travel through reassignments are the ones the limit stops.
        cluster.schedule(a, depth)?;
        cluster.schedule(b, depth)?;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReassignmentCompletion {
    Drained,
    Deferred,
}

impl Cluster {
    /// Cumulative queue visits withheld by an actual executing producer attempt.
    pub fn producer_dependency_skips(&self) -> u64 {
        self.jobs.lock().producer_dependency_skips
    }
    /// Queue work counts; duplicate pending IDs are not a measured query saving.
    pub fn reassignment_coalescing(&self) -> ReassignmentCoalescing {
        self.jobs.lock().coalescing
    }
    pub fn write_policy(&self) -> ReadBeamPolicy {
        ReadBeamPolicy::with_level_overrides(
            Some(self.local.config.write_beam_tau),
            self.local.config.write_beam_min,
            self.local.config.write_beam_max,
            self.local.config.write_level_taus.clone(),
            self.local.config.write_level_min_pcts.clone(),
            vec![],
        )
    }
    pub fn refresh_root(&self) -> Result<RoutingNode, String> {
        self.refreshed_routing_view().map(|(_, root)| root)
    }
    pub(super) fn refreshed_routing_view(
        &self,
    ) -> Result<
        (
            Arc<super::protocol::publication::PublishedView<BoundaryView>>,
            RoutingNode,
        ),
        String,
    > {
        if self.local.owner != self.coordinator {
            let known = self.installed.read().clone();
            let request = match &known {
                Some(view) => EngineRequest::CurrentViewIfChanged {
                    known_sequence: view.sequence,
                },
                None => EngineRequest::CurrentView,
            };
            match self.request(self.coordinator, request)? {
                EngineResponse::ViewUnchanged { sequence } => {
                    if known.as_ref().is_none_or(|view| view.sequence != sequence) {
                        return Err(
                            "unchanged routing acknowledgement differs from requested view".into(),
                        );
                    }
                    // A concurrent installer may already hold a newer view;
                    // current_view below retains it rather than reinstalling known.
                }
                EngineResponse::View(view) => {
                    if let Err(error) = self.install_view(&view) {
                        // A concurrent refresh can install a newer response
                        // while this request is in flight. Keep that newer view.
                        if error != "stale shared routing view" {
                            return Err(error);
                        }
                    }
                }
                _ => return Err("invalid root view".into()),
            }
        }
        let view = self.current_view();
        let root = view.value.root.clone();
        if root.target.authority == self.local.owner
            && !view.value.shared.contains_key(&root.target.id)
            && self.local.nodes.contains_key(&root.target.id)
        {
            let fresh = self.local.node(root.target)?;
            return Ok((view, fresh));
        }
        Ok((view, root))
    }
    fn enqueue(&self, job: BalanceJob) -> Result<(), String> {
        self.request(self.coordinator, EngineRequest::Enqueue(job))?;
        Ok(())
    }
    pub(super) fn schedule(&self, node: NodeRef, depth: u32) -> Result<(), String> {
        {
            let mut accounting = self.local.observability.operation(Scope::BalanceScheduling);
            if depth > MAX_BALANCE_DEPTH {
                accounting.add(Counter::BalanceDepthDeferred, 1);
                return Ok(());
            }
            accounting.add(
                [
                    Counter::BalanceScheduledDepth0,
                    Counter::BalanceScheduledDepth1,
                    Counter::BalanceScheduledDepth2,
                    Counter::BalanceScheduledDepth3,
                    Counter::BalanceScheduledDepth4,
                ][depth as usize],
                1,
            );
        }
        if node.authority != self.local.owner {
            self.enqueue(BalanceJob {
                envelope: self
                    .local
                    .envelope("balance-job", vec![(node.id, node.placement)])?,
                node,
                depth,
                internal: false,
            })?;
            return Ok(());
        }
        let resident = match self.local.resident(node) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };
        let n = resident.read();
        let internal = !n.routing.leaf;
        let eligible = if let Some(p) = &n.postings {
            p.ids.len() > self.local.config.split_threshold
                || (!p.ids.is_empty() && p.ids.len() < self.local.config.merge_threshold)
        } else {
            n.routing.children.len() > self.local.config.branching_factor
                || n.routing.children.is_empty()
        };
        drop(n);
        if eligible {
            self.enqueue(BalanceJob {
                envelope: self
                    .local
                    .envelope("balance-job", vec![(node.id, node.placement)])?,
                node,
                depth,
                internal,
            })?
        }
        Ok(())
    }
    pub fn add_batch(&self, ids: &[u64], workers: usize) -> Result<(), String> {
        if self.local.owner != self.coordinator {
            return Err("new input batches require the index ingestion authority".into());
        }
        let _input = self.input_lock.lock();
        let admitted = self.inserted_ids.lock().admit(ids)?;
        let envelope = self.local.envelope("ingestion-batch", vec![])?;
        use sha2::Digest;
        let identity =
            sha2::Sha256::digest(bincode::serialize(ids).map_err(|e| e.to_string())?).to_vec();
        self.local
            .execute_job(&envelope, identity, || self.add_batch_inner(ids, workers))?;
        *self.inserted_ids.lock() = admitted;
        Ok(())
    }
    fn add_batch_inner(&self, ids: &[u64], workers: usize) -> Result<(), String> {
        if workers == 0 {
            return Err("at least one insertion worker is required".into());
        }
        let workers = Self::insertion_workers(
            std::env::var("SPANN_INSERTION_WORKERS")
                .map(Some)
                .or_else(|error| match error {
                    std::env::VarError::NotPresent => Ok(None),
                    _ => Err("SPANN_INSERTION_WORKERS must be UTF-8".to_string()),
                })?
                .as_deref(),
            workers,
        )?;
        let navigation_window = Self::insertion_navigation_window(
            std::env::var("SPANN_INSERTION_NAVIGATION_WINDOW")
                .map(Some)
                .or_else(|error| match error {
                    std::env::VarError::NotPresent => Ok(None),
                    _ => Err("SPANN_INSERTION_NAVIGATION_WINDOW must be UTF-8".to_string()),
                })?
                .as_deref(),
        )?;
        self.refresh_root()?;
        let unique = ids.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != ids.len() {
            return Err("duplicate new vector IDs in logical input batch".into());
        }
        let chunk = ids.len().div_ceil(workers).max(1);
        let route_phase = self
            .local
            .observability
            .operation(Scope::InsertionRoutePhase);
        let groups = std::thread::scope(|scope| {
            let jobs = ids
                .chunks(chunk)
                .map(|part| {
                    scope.spawn(move || {
                        let mut grouped: BTreeMap<NodeRef, Vec<u64>> = BTreeMap::new();
                        for window in part.chunks(navigation_window) {
                            let route_work =
                                self.local.observability.operation(Scope::InsertionRoute);
                            let queries = window
                                .iter()
                                .map(|id| self.local.vector(*id).map(|v| v.to_vec()))
                                .collect::<Result<Vec<_>, _>>()?;
                            let routes = self.navigate(&queries, &self.write_policy(), None)?;
                            drop(route_work);
                            let _group_work =
                                self.local.observability.operation(Scope::InsertionGroup);
                            if routes.len() != window.len() {
                                return Err(
                                    "insertion navigation returned an incomplete batch".into()
                                );
                            }
                            for (id, route) in window.iter().zip(routes) {
                                let destination = route
                                    .first()
                                    .ok_or("navigation found no insertion destination")?
                                    .0;
                                grouped.entry(destination).or_default().push(*id);
                            }
                        }
                        Ok::<_, String>(grouped)
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .map(|j| j.join().map_err(|_| "add worker panicked".to_string())?)
                .collect::<Result<Vec<_>, _>>()
        })?;
        drop(route_phase);
        let group_work = self.local.observability.operation(Scope::InsertionGroup);
        let mut combined: BTreeMap<NodeRef, Vec<u64>> = BTreeMap::new();
        for group in groups {
            for (leaf, rows) in group {
                combined.entry(leaf).or_default().extend(rows)
            }
        }
        // The contribution boundary is the entire logical input batch. Storage and
        // transport windows above do not cause smaller posting-list append locks.
        let jobs = combined.into_iter().collect::<Vec<_>>();
        drop(group_work);
        let _append_phase = self
            .local
            .observability
            .operation(Scope::InsertionAppendPhase);
        let next = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let handles = (0..workers)
                .map(|_| {
                    scope.spawn(|| -> Result<(), String> {
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some((leaf, ids)) = jobs.get(i) else {
                                break;
                            };
                            let _append_work =
                                self.local.observability.operation(Scope::InsertionAppend);
                            let image = self.node(*leaf)?;
                            let envelope = self
                                .local
                                .envelope("append", vec![(leaf.id, leaf.placement)])?;
                            self.request(
                                leaf.authority,
                                EngineRequest::Append {
                                    envelope,
                                    leaf: *leaf,
                                    expected_revision: image.revision,
                                    ids: ids.clone(),
                                },
                            )?;
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for h in handles {
                h.join().map_err(|_| "append worker panicked")??;
            }
            Ok(())
        })
    }
    // Bound query copying and expansion requests independently of logical batch size.
    // Larger windows retain the same per-leaf, whole-batch append contribution.
    fn insertion_navigation_window(value: Option<&str>) -> Result<usize, String> {
        let Some(value) = value else {
            return Ok(64);
        };
        let window = value.parse::<usize>().map_err(|_| {
            "SPANN_INSERTION_NAVIGATION_WINDOW must be an integer in 1..=256".to_string()
        })?;
        if !(1..=256).contains(&window) {
            return Err("SPANN_INSERTION_NAVIGATION_WINDOW must be in 1..=256".into());
        }
        Ok(window)
    }
    // The override changes insertion concurrency only; balancing keeps its caller's limit.
    fn insertion_workers(value: Option<&str>, default: usize) -> Result<usize, String> {
        let Some(value) = value else {
            return Ok(default);
        };
        let workers = value
            .parse::<usize>()
            .map_err(|_| "SPANN_INSERTION_WORKERS must be an integer in 1..=64".to_string())?;
        if !(1..=64).contains(&workers) {
            return Err("SPANN_INSERTION_WORKERS must be in 1..=64".into());
        }
        Ok(workers)
    }
    pub fn balance_leaf_once_for_correctness(&self, node: NodeRef) -> Result<(), String> {
        let job = BalanceJob {
            envelope: self
                .local
                .envelope("balance-job", vec![(node.id, node.placement)])?,
            node,
            depth: 0,
            internal: false,
        };
        match self.request(node.authority, EngineRequest::BalanceJob(job))? {
            EngineResponse::Ack => Ok(()),
            _ => Err("invalid balance acknowledgement".into()),
        }
    }
    pub fn balance(&self, workers: usize) -> Result<(), String> {
        if workers == 0 {
            return Err("at least one balance worker per owner is required".into());
        }
        self.refresh_root()?;
        {
            let mut queue = self.jobs.lock();
            if queue.active != 0 {
                return Err("balance already has active jobs".into());
            }
            if let Some(error) = &queue.failed {
                return Err(error.clone());
            }
            queue.completed = 0;
            queue.peak = 0;
        }

        let owners = std::iter::once(self.local.owner)
            .chain(self.peers.keys().copied())
            .collect::<BTreeSet<_>>();
        for owner in &owners {
            match self.request(*owner, EngineRequest::Eligible)? {
                EngineResponse::Eligible(jobs) => {
                    for (node, _, internal) in jobs {
                        self.jobs.lock().enqueue(BalanceJob {
                            envelope: self
                                .local
                                .envelope("balance-job", vec![(node.id, node.placement)])?,
                            node,
                            depth: 0,
                            internal,
                        });
                    }
                }
                _ => return Err("invalid eligible-job response".into()),
            }
        }
        let worker_owners = owners
            .into_iter()
            .flat_map(|owner| std::iter::repeat_n(owner, workers))
            .collect::<Vec<_>>();
        self.schedule_shared_eligible()?;
        // One balance pass must finish within this many seconds (300 unless
        // SPANN_BALANCE_PASS_SECONDS overrides it). Large single-owner ladder
        // builds hit dependent split cascades that run longer.
        let pass_seconds = std::env::var("SPANN_BALANCE_PASS_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(300);
        let deadline = Instant::now() + Duration::from_secs(pass_seconds);
        std::thread::scope(|scope| {
            let handles = worker_owners
                .into_iter()
                .map(|owner| {
                    scope.spawn(move || -> Result<(), String> {
                        loop {
                            let job = {
                                let _dispatch = super::observability::detailed().then(|| {
                                    self.local.observability.operation(Scope::SchedulerDispatch)
                                });
                                let mut q = self.jobs.lock();
                                loop {
                                    if let Some(error) = &q.failed {
                                        return Err(error.clone());
                                    }
                                    if let Err(error) = self.local.ensure_healthy() {
                                        q.failed = Some(error.clone());
                                        self.jobs_changed.notify_all();
                                        return Err(error);
                                    }
                                    if q.completed > 1_000_000 {
                                        return Err("balance job budget exhausted".into());
                                    }
                                    if let Some(job) = q.next(owner) {
                                        break job;
                                    }
                                    if q.active == 0 && q.ready.is_empty() {
                                        return Ok(());
                                    }
                                    // A complete failed dispatch scan establishes that
                                    // this owner has no dispatch-eligible job now.
                                    let wait_started =
                                        super::observability::detailed().then(Instant::now);
                                    let wake = self
                                        .jobs_changed
                                        .wait_for(&mut q, Duration::from_millis(100));
                                    if let Some(started) = wait_started {
                                        q.diagnostic_add(owner, "condition_wait_calls", 1);
                                        q.diagnostic_add(
                                            owner,
                                            "condition_wait_nanos",
                                            started.elapsed().as_nanos().min(u64::MAX as u128)
                                                as u64,
                                        );
                                        q.diagnostic_add(
                                            owner,
                                            if wake.timed_out() {
                                                "condition_timeout_returns"
                                            } else {
                                                "condition_notified_returns"
                                            },
                                            1,
                                        );
                                    }
                                    if Instant::now() >= deadline {
                                        return Err("balance progress deadline".into());
                                    }
                                }
                            };
                            let request = EngineRequest::ScheduledJob(job.clone());
                            let execution = super::observability::detailed().then(|| {
                                self.local
                                    .observability
                                    .operation(Scope::SchedulerExecution)
                            });
                            let result = match self.request(owner, request) {
                                Ok(EngineResponse::Ack) => Ok(()),
                                Ok(_) => Err("invalid construction-job acknowledgement".into()),
                                Err(error) => Err(error),
                            };
                            drop(execution);
                            self.jobs.lock().finish(&job, result);
                            self.jobs_changed.notify_all();
                        }
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().map_err(|_| "balancing worker panicked")??;
            }
            Ok(())
        })
    }
    pub(super) fn run_job(&self, job: BalanceJob) -> Result<(), String> {
        self.run_job_for_producer(job, None)
    }
    pub(super) fn run_job_for_producer(
        &self,
        job: BalanceJob,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        if job.node.authority != self.local.owner {
            return Err("balancing dispatched to wrong owner".into());
        }
        if job.internal && self.current_view().value.shared.contains_key(&job.node.id) {
            return self
                .balance_shared_internal(job.node)
                .map_err(|e| format!("shared balance {}: {e}", job.node.id));
        }
        if job.internal {
            self.split_internal(job.node)
                .map_err(|e| format!("split internal {}: {e}", job.node.id))
        } else {
            let count = match self.local.resident(job.node) {
                Ok(n) => n.read().postings.as_ref().map_or(0, |p| p.ids.len()),
                Err(_) => return Ok(()),
            };
            if count > self.local.config.split_threshold {
                self.local
                    .split_leaf_for_job(job.node, job.depth, producer)
                    .map_err(|e| format!("split leaf {}: {e}", job.node.id))
            } else if count == 0 {
                self.retire_empty_leaf(job.node)
                    .map_err(|e| format!("retire empty leaf {}: {e}", job.node.id))
            } else if count < self.local.config.merge_threshold {
                self.merge_leaf_for_job(job.node, producer)
                    .map_err(|e| format!("merge leaf {}: {e}", job.node.id))
            } else {
                Ok(())
            }
        }
    }
    pub(super) fn reassign(
        &self,
        source: NodeRef,
        ids: Vec<u64>,
        excluded: Option<NodeId>,
    ) -> Result<(), String> {
        self.reassign_for_job(source, ids, excluded, None)
    }
    pub(super) fn reassign_for_job(
        &self,
        source: NodeRef,
        ids: Vec<u64>,
        excluded: Option<NodeId>,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        self.reassign_inner::<true>(source, ids, excluded, producer)
            .map(|_| ())
    }
    pub(super) fn run_reassign_job(
        &self,
        job: ReassignJob,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        let source = job.source;
        let evacuating = job.excluded == Some(source.id);
        let completion = self.reassign_inner::<true>(source, job.ids, job.excluded, producer)?;
        if evacuating && completion == ReassignmentCompletion::Drained {
            // A deferred merge carries its retirement continuation in the
            // existing source-exclusion flag. Fresh arrivals are rechecked by
            // retire_empty_leaf; an empty source cannot remain silently queued.
            self.retire_empty_leaf(source)?;
        }
        Ok(())
    }
    // The protocol-only instantiation keeps its admission/progress regressions
    // meaningful. Normal construction always uses the same-owner bulk path.
    fn reassign_path<const LOCAL_BULK: bool>(
        &self,
        source: NodeRef,
        ids: Vec<u64>,
        excluded: Option<NodeId>,
    ) -> Result<(), String> {
        self.reassign_inner::<LOCAL_BULK>(source, ids, excluded, None)
            .map(|_| ())
    }
    fn reassign_inner<const LOCAL_BULK: bool>(
        &self,
        source: NodeRef,
        ids: Vec<u64>,
        excluded: Option<NodeId>,
        producer: Option<AttemptId>,
    ) -> Result<ReassignmentCompletion, String> {
        let mut observed = self.local.observability.operation(Scope::TransferDriver);
        observed.add(Counter::ReassignmentInputRows, ids.len() as u64);
        let initial_ids = ids.len();
        let mut pending = ids;
        let mut last_rejection = String::new();
        let started = Instant::now();
        let mut rounds = 0;
        let mut completed_vectors = 0;
        let mut rejected_groups = 0;
        let mut revalidation_deadline = Instant::now() + Duration::from_secs(5);
        for _ in 0..1024 {
            rounds += 1;
            observed.add(Counter::ReassignmentRounds, 1);
            self.local.ensure_healthy()?;
            let resident = match self.local.resident(source) {
                Ok(r) => r,
                Err(_) => return Ok(ReassignmentCompletion::Drained),
            };
            let membership_work = super::observability::detailed().then(|| {
                self.local
                    .observability
                    .operation(Scope::ReassignmentMembershipScan)
            });
            let current = {
                let node = resident.read();
                let Some(posting) = node.postings.as_ref() else {
                    return Ok(ReassignmentCompletion::Drained);
                };
                posting.ids.iter().copied().collect::<BTreeSet<_>>()
            };
            pending.retain(|id| current.contains(id));
            drop(membership_work);
            if pending.is_empty() {
                return Ok(ReassignmentCompletion::Drained);
            }
            let query_work = super::observability::detailed().then(|| {
                self.local
                    .observability
                    .operation(Scope::ReassignmentQueryBuild)
            });
            let queries = pending
                .iter()
                .map(|id| self.local.vector(*id).map(|v| v.to_vec()))
                .collect::<Result<Vec<_>, _>>()?;
            observed.add(
                Counter::ReassignmentQueryBytesCopied,
                queries.iter().map(|q| q.len() as u64 * 4).sum(),
            );
            drop(query_work);
            let routes = match self.navigate(&queries, &self.write_policy(), excluded) {
                Ok(routes) => routes,
                Err(error) if exhausted_navigation_route(&error) => {
                    self.local.ensure_healthy()?;
                    // Membership remains current in its source. Keep the exact
                    // obligation in the queue before acknowledging this job.
                    // The coordinator requires another active structural job,
                    // so a stable broken edge remains a terminal error.
                    for ids in pending.chunks(MAX_REASSIGN_IDS) {
                        let job = ReassignJob {
                            envelope: self.local.envelope(
                                "navigation-reassign-retry",
                                vec![(source.id, source.placement)],
                            )?,
                            source,
                            ids: ids.to_vec(),
                            excluded,
                            blocked: vec![],
                            blocked_jobs: vec![],
                            depth: current_depth(),
                        };
                        match self.request(
                            self.coordinator,
                            EngineRequest::EnqueueNavigationRetry { job, producer },
                        ) {
                            Ok(EngineResponse::Ack) => {
                                observed.add(Counter::NavigationDeferredJobs, 1);
                            }
                            Ok(_) => return Err("invalid navigation retry acknowledgement".into()),
                            Err(rejected)
                                if rejected.contains(
                                    "navigation retry has no other active structural producer",
                                ) =>
                            {
                                return Err(error)
                            }
                            Err(rejected) => return Err(rejected),
                        }
                    }
                    return Ok(ReassignmentCompletion::Deferred);
                }
                Err(error) => return Err(error),
            };
            let source_centroid = match self.node(source) {
                Ok(r) => r.centroid,
                Err(_) => return Ok(ReassignmentCompletion::Drained),
            };
            let mut grouped: BTreeMap<NodeRef, Vec<u64>> = BTreeMap::new();
            for ((id, q), route) in pending.iter().copied().zip(queries).zip(routes) {
                if let Some(&(dest, distance)) = route.first() {
                    if dest != source
                        && (excluded.is_some()
                            || distance < self.local.metric.distance(&q, &source_centroid))
                    {
                        grouped.entry(dest).or_default().push(id)
                    }
                }
            }
            pending.clear();
            let mut claimed_dependencies = BTreeSet::new();
            let mut claimed_jobs = BTreeSet::new();
            let mut local_groups = BTreeMap::new();
            let mut remote_groups = BTreeMap::new();
            for (destination, ids) in grouped {
                if LOCAL_BULK && destination.authority == self.local.owner {
                    local_groups.insert(destination, ids);
                } else {
                    remote_groups.insert(destination, ids);
                }
            }
            if !local_groups.is_empty() {
                let local = self.local.transfer_local_many_for_job(
                    source,
                    local_groups,
                    excluded == Some(source.id),
                    producer,
                )?;
                if local.moved > 0 {
                    completed_vectors += local.moved;
                    revalidation_deadline = Instant::now() + Duration::from_secs(5);
                    self.schedule(source, follow_up_depth())?;
                    for destination in local.destinations {
                        self.schedule(destination, follow_up_depth())?;
                    }
                }
                for ids in local.retries.values() {
                    pending.extend(ids);
                }
                if !local.retries.is_empty() {
                    rejected_groups += local.retries.len();
                    observed.add(
                        Counter::ReassignmentRejectedGroups,
                        local.retries.len() as u64,
                    );
                    last_rejection = "local transfer claim or routing dependency changed".into();
                }
                claimed_dependencies.extend(local.blocked);
                claimed_jobs.extend(local.blocked_jobs);
            }
            let batches = if LOCAL_BULK {
                let mut owners: BTreeMap<AuthorityId, BTreeMap<NodeRef, Vec<u64>>> =
                    BTreeMap::new();
                for (target, ids) in remote_groups {
                    owners
                        .entry(target.authority)
                        .or_default()
                        .insert(target, ids);
                }
                owners.into_values().collect::<Vec<_>>()
            } else {
                remote_groups
                    .into_iter()
                    .map(|(target, ids)| BTreeMap::from([(target, ids)]))
                    .collect()
            };
            for groups in batches {
                let proposed = groups.values().map(Vec::len).sum::<usize>();
                let transfer = if LOCAL_BULK {
                    self.transfer_owner_batch_for_construction(
                        source,
                        groups.clone(),
                        producer,
                        excluded,
                    )
                } else {
                    let (dest, group) = groups.iter().next().unwrap();
                    self.transfer_with_protocol(source, *dest, group.clone())
                };
                match transfer {
                    Ok(()) => {
                        completed_vectors += proposed;
                        revalidation_deadline = Instant::now() + Duration::from_secs(5);
                        if !LOCAL_BULK {
                            self.schedule(source, follow_up_depth())?;
                            for dest in groups.keys() {
                                self.schedule(*dest, follow_up_depth())?;
                            }
                        }
                    }
                    Err(error) if retryable_transfer_dependency(&error) => {
                        self.local.ensure_healthy()?;
                        rejected_groups += groups.len();
                        observed.add(Counter::ReassignmentRejectedGroups, groups.len() as u64);
                        claimed_jobs.extend(claim_producers_from_error(&error));
                        last_rejection = error;
                        pending.extend(groups.into_values().flatten());
                    }
                    Err(error) => return Err(error),
                }
            }
            if pending.is_empty() {
                return Ok(ReassignmentCompletion::Drained);
            }
            if !claimed_jobs.is_empty() {
                // The owner job holding the claim is useful progress. Retain
                // these IDs once, free this worker and retry after that job.
                for ids in pending.chunks(32_768) {
                    let job = ReassignJob {
                        envelope: self
                            .local
                            .envelope("reassign-job", vec![(source.id, source.placement)])?,
                        source,
                        ids: ids.to_vec(),
                        excluded,
                        blocked: claimed_dependencies.iter().copied().collect(),
                        blocked_jobs: claimed_jobs.iter().copied().collect(),
                        depth: current_depth(),
                    };
                    self.request(self.coordinator, EngineRequest::EnqueueReassign(job))?;
                }
                return Ok(ReassignmentCompletion::Deferred);
            }
            if Instant::now() >= revalidation_deadline {
                break;
            }
            // Give the operation that invalidated this proposal a chance to
            // finish before rebuilding another identical candidate proposal.
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!("membership reassignment exceeded its revalidation progress budget; source {} rounds={rounds} initial_ids={initial_ids} pending_ids={} completed_vectors={completed_vectors} rejected_groups={rejected_groups} elapsed_seconds={:.6} last rejection: {last_rejection}", source.id,pending.len(),started.elapsed().as_secs_f64()))
    }
    pub(super) fn neighbor_reassign(
        &self,
        neighbor: NodeRef,
        old: &[f32],
        left: &[f32],
        right: &[f32],
        evaluated: &mut BTreeSet<u64>,
    ) -> Result<(), String> {
        self.neighbor_reassign_for_job(neighbor, old, left, right, evaluated, None)
    }
    pub(super) fn neighbor_reassign_for_job(
        &self,
        neighbor: NodeRef,
        old: &[f32],
        left: &[f32],
        right: &[f32],
        evaluated: &mut BTreeSet<u64>,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        // This executes at the posting owner. Only the split centroids are sent;
        // neighbor posting payloads are never gathered by the initiating owner.
        if neighbor.authority != self.local.owner {
            let job = NeighborJob {
                envelope: self
                    .local
                    .envelope("neighbor-job", vec![(neighbor.id, neighbor.placement)])?,
                node: neighbor,
                old: old.to_vec(),
                left: left.to_vec(),
                right: right.to_vec(),
                depth: current_depth(),
            };
            self.request(self.coordinator, EngineRequest::EnqueueNeighbor(job))?;
            return Ok(());
        }
        let resident = match self.local.resident(neighbor) {
            Ok(n) => n,
            Err(_) => return Ok(()),
        };
        let (ids, centroid) = {
            let node = resident.read();
            (
                node.postings
                    .as_ref()
                    .ok_or("neighbor is internal")?
                    .ids
                    .clone(),
                node.routing.centroid.clone(),
            )
        };
        let mut selection = super::observability::detailed()
            .then(|| self.local.observability.operation(Scope::NeighborSelection));
        let mut proposed = vec![];
        for id in ids {
            if !evaluated.insert(id) {
                continue;
            }
            let v = self.local.vector(id)?;
            let a = self.local.metric.distance(&v, left);
            let b = self.local.metric.distance(&v, right);
            let farther_from_current = self.local.metric.distance(&v, &centroid) > a.min(b);
            if let Some(selection) = &mut selection {
                selection.add(Counter::NeighborCandidateRows, 1);
                selection.add(
                    Counter::NeighborDistanceCalculations,
                    3 + u64::from(farther_from_current),
                );
            }
            if farther_from_current && self.local.metric.distance(&v, old) > a.min(b) {
                proposed.push(id)
            }
        }
        if let Some(selection) = &mut selection {
            selection.add(Counter::NeighborProposedRows, proposed.len() as u64);
        }
        drop(selection);
        self.reassign_for_job(neighbor, proposed, None, producer)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoundaryView {
    /// Rebuilt at publication/install time; centroid-only revisions retain it.
    #[serde(skip)]
    pub(super) navigation_topology: Option<[u8; 32]>,
    pub root: RoutingNode,
    pub shared: BTreeMap<NodeId, RoutingNode>,
    pub units: BTreeMap<NodeId, NodeRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoundaryPreparation {
    pub source_claim: Option<AttemptId>,
    pub envelope: Envelope,
    pub old: RoutingNode,
    pub replacements: Vec<RoutingNode>,
    pub dependencies: Vec<(NodeRef, u64)>,
}
pub(super) struct PendingBoundary {
    prepared: Vec<StoredNode>,
    pub(super) request: BoundaryPreparation,
    pub(super) execution: Arc<parking_lot::Mutex<()>>,
    pub(super) preparing: bool,
    guard: Option<RegionGuard>,
    pub(super) ready: bool,
    pub(super) finished: bool,
}
// These errors cross the existing endpoint string-error boundary. Only explicit
// scheduled producer identities create queue dependencies; untracked claims use
// the existing bounded revalidation path instead of a guessed node dependency.
fn claim_producers_from_error(error: &str) -> BTreeSet<AttemptId> {
    error
        .split("producer_jobs=")
        .nth(1)
        .into_iter()
        .flat_map(|tail| tail.split_whitespace().next().unwrap_or("").split(','))
        .filter_map(|item| {
            let (authority, sequence) = item.split_once(':')?;
            Some(AttemptId {
                authority: authority.parse().ok()?,
                sequence: sequence.parse().ok()?,
            })
        })
        .collect()
}
pub(super) fn append_claim_producers(
    mut error: String,
    jobs: impl IntoIterator<Item = AttemptId>,
) -> String {
    let jobs = jobs
        .into_iter()
        .map(|j| format!("{}:{}", j.authority, j.sequence))
        .collect::<Vec<_>>();
    if !jobs.is_empty() {
        error.push_str(&format!(" producer_jobs={}", jobs.join(",")));
    }
    error
}
fn exhausted_navigation_route(error: &str) -> bool {
    error.starts_with("construction navigation exceeded its whole-attempt retry budget;")
        && (error.contains("last_rejection=navigation route changed")
            || error.contains("last_rejection=stale expansion"))
}
fn retryable_transfer_dependency(error: &str) -> bool {
    let message = error.to_ascii_lowercase();
    if ["unresolved", "poison", "transport", "deadline", "invalid"]
        .iter()
        .any(|stop| message.contains(stop))
    {
        return false;
    }
    ["changed", "stale", "protected", "region"]
        .iter()
        .any(|reason| message.contains(reason))
}
fn as_region(r: &RoutingNode, owner: AuthorityId, live: bool) -> RegionNode {
    RegionNode {
        parent: r.parent.filter(|p| p.authority == owner).map(|p| p.id),
        placement: r.target.placement,
        revision: r.revision,
        live,
    }
}
fn child(r: &RoutingNode) -> Child {
    Child {
        target: r.target,
        centroid: r.centroid.clone(),
        leaf: r.leaf,
    }
}
fn revise(r: &mut RoutingNode) -> Result<(), String> {
    r.revision = r.revision.checked_add(1).ok_or("node revision exhausted")?;
    Ok(())
}
fn set_center(r: &mut RoutingNode, dim: usize) -> Result<(), String> {
    if r.children.is_empty() {
        return Ok(());
    }
    let mut centroid = vec![0.; dim];
    for child in &r.children {
        for (dst, x) in centroid.iter_mut().zip(&child.centroid) {
            *dst += x;
        }
    }
    let scale = 1.0 / r.children.len() as f32;
    for x in &mut centroid {
        *x *= scale;
    }
    r.centroid_code = Code::<4>::quantize(&centroid, &vec![0.; dim])
        .as_ref()
        .to_vec();
    r.centroid = centroid;
    Ok(())
}

impl PartialEq for BoundaryView {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.shared == other.shared && self.units == other.units
    }
}
impl BoundaryView {
    fn topology_digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        fn target(hash: &mut Sha256, node: NodeRef) {
            hash.update(node.id.to_le_bytes());
            hash.update(node.authority.to_le_bytes());
            hash.update(node.placement.to_le_bytes());
        }
        fn image(hash: &mut Sha256, node: &RoutingNode) {
            target(hash, node.target);
            hash.update([node.leaf as u8, node.parent.is_some() as u8]);
            if let Some(parent) = node.parent {
                target(hash, parent);
            }
            hash.update((node.centroid.len() as u64).to_le_bytes());
            hash.update((node.children.len() as u64).to_le_bytes());
            for child in &node.children {
                target(hash, child.target);
                hash.update([child.leaf as u8]);
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"construction-navigation-topology-v1");
        image(&mut hash, &self.root);
        hash.update((self.shared.len() as u64).to_le_bytes());
        for (id, node) in &self.shared {
            hash.update(id.to_le_bytes());
            image(&mut hash, node);
        }
        hash.update((self.units.len() as u64).to_le_bytes());
        for (id, node) in &self.units {
            hash.update(id.to_le_bytes());
            target(&mut hash, *node);
        }
        hash.finalize().into()
    }
    pub(super) fn prepared(mut self) -> Result<Self, String> {
        self.root.prepare_navigation()?;
        for image in self.shared.values_mut() {
            image.prepare_navigation()?;
        }
        self.navigation_topology = Some(self.topology_digest());
        Ok(self)
    }
}

impl Cluster {
    pub(super) fn install_view(
        &self,
        view: &super::protocol::publication::PublishedView<BoundaryView>,
    ) -> Result<(), String> {
        let mut installed = self.installed.write();
        if let Some(current) = installed.as_ref() {
            if view.sequence < current.sequence {
                return Err("stale shared routing view".into());
            }
            if view.sequence == current.sequence && **current != *view {
                return Err("conflicting shared routing view".into());
            }
        }
        let mut prepared = view.clone();
        prepared.value = prepared.value.prepared()?;
        *installed = Some(Arc::new(prepared));
        // These mirrors support diagnostics and scheduler setup. Navigation pins
        // the immutable view above and never assembles a view from these fields.
        *self.shared.write() = view.value.shared.clone();
        *self.root.write() = view.value.root.clone();
        self.shared_sequence.store(view.sequence, Ordering::Release);
        Ok(())
    }
    fn install_split(
        &self,
        old: &RoutingNode,
        replacements: Vec<StoredNode>,
    ) -> Result<bool, String> {
        self.install_replacements(old, replacements, true)
    }
    fn install_replacements(
        &self,
        old: &RoutingNode,
        replacements: Vec<StoredNode>,
        wrap_root: bool,
    ) -> Result<bool, String> {
        self.install_replacements_with_claim(old, replacements, wrap_root, None)
    }
    fn install_replacements_with_claim(
        &self,
        old: &RoutingNode,
        mut replacements: Vec<StoredNode>,
        wrap_root: bool,
        source_claim: Option<AttemptId>,
    ) -> Result<bool, String> {
        match self.local.node(old.target) {
            Ok(current) if current.revision == old.revision => {}
            _ => return Ok(false),
        }
        let local_parent = old
            .parent
            .filter(|p| p.authority == self.local.owner && self.local.nodes.contains_key(&p.id));
        if let Some(parent) = local_parent {
            let parent_snapshot = match self.local.node(parent) {
                Ok(n) => n,
                Err(_) => return Ok(false),
            };
            let region_root = parent_snapshot
                .parent
                .filter(|p| p.authority == self.local.owner && self.local.nodes.contains_key(&p.id))
                .unwrap_or(parent);
            let region = match self.local.reserve(vec![region_root]) {
                Ok(r) => r,
                Err(_) => return Ok(false),
            };
            let source_handle = match self.local.resident(old.target) {
                Ok(r) => r,
                Err(_) => return Ok(false),
            };
            let mut source = source_handle.write();
            if source.routing.revision != old.revision || source.reserved != source_claim {
                return Ok(false);
            }
            let parent_handle = match self.local.resident(parent) {
                Ok(r) => r,
                Err(_) => return Ok(false),
            };
            let mut parent_record = parent_handle.write();
            if parent_record.routing.parent != parent_snapshot.parent {
                return Ok(false);
            }
            if !parent_record
                .routing
                .children
                .iter()
                .any(|c| c.target == old.target)
            {
                return Ok(false);
            }
            let mut parent_image = (*parent_record.routing).clone();
            parent_image
                .children
                .retain(|c| c.target.id != old.target.id);
            for n in &mut replacements {
                n.routing.parent = Some(parent);
                parent_image.children.push(child(&n.routing));
            }
            revise(&mut parent_image)?;
            set_center(&mut parent_image, self.local.dim)?;
            // Prepare child-parent rewrites for internal splits before publication.
            let mut updates = vec![parent_image.clone()];
            let mut child_handles = vec![];
            for n in &replacements {
                for c in &n.routing.children {
                    if c.target.authority == self.local.owner {
                        child_handles.push((c.target.id, self.local.resident(c.target)?));
                    }
                }
            }
            child_handles.sort_unstable_by_key(|(id, _)| *id);
            let mut child_guards = child_handles
                .iter()
                .map(|(_, h)| h.write())
                .collect::<Vec<_>>();
            for guard in &mut child_guards {
                let mut image = (*guard.routing).clone();
                let parent = replacements
                    .iter()
                    .find(|n| {
                        n.routing
                            .children
                            .iter()
                            .any(|c| c.target.id == image.target.id)
                    })
                    .unwrap()
                    .routing
                    .target;
                image.parent = Some(parent);
                revise(&mut image)?;
                updates.push(image);
            }
            let grand = parent_image.parent.filter(|p| {
                p.authority == self.local.owner && self.local.nodes.contains_key(&p.id)
            });
            let grand_handle = grand.map(|p| self.local.resident(p)).transpose()?;
            let mut grand_guard = grand_handle.as_ref().map(|h| h.write());
            if let Some(guard) = grand_guard.as_ref() {
                let mut image = (*guard.routing).clone();
                if let Some(c) = image.children.iter_mut().find(|c| c.target.id == parent.id) {
                    c.centroid = parent_image.centroid.clone();
                }
                revise(&mut image)?;
                updates.push(image);
            }
            let mut retired = old.clone();
            revise(&mut retired)?;
            let mut changes = updates
                .iter()
                .map(|r| (r.target.id, as_region(r, self.local.owner, true)))
                .collect::<Vec<_>>();
            changes.push((old.target.id, as_region(&retired, self.local.owner, false)));
            changes.extend(replacements.iter().map(|n| {
                (
                    n.routing.target.id,
                    as_region(&n.routing, self.local.owner, true),
                )
            }));
            let fresh_ids = replacements
                .iter()
                .map(|n| n.routing.target.id)
                .collect::<BTreeSet<_>>();
            for (_, node) in &mut changes {
                if node
                    .parent
                    .is_some_and(|p| !self.local.nodes.contains_key(&p) && !fresh_ids.contains(&p))
                {
                    node.parent = None;
                }
            }
            // Routing dependencies have their own revisions. Region bookkeeping
            // changes only when ancestry, placement or liveness changes; a
            // centroid-only edit does not require protecting its unchanged parent.
            changes.retain(|(id, next)| {
                region.manager.snapshot(*id).map_or(true, |(old, _)| {
                    old.parent != next.parent
                        || old.placement != next.placement
                        || old.live != next.live
                })
            });
            for (id, node) in &mut changes {
                if let Ok((old, _)) = region.manager.snapshot(*id) {
                    node.revision = old
                        .revision
                        .checked_add(1)
                        .ok_or("region revision exhausted")?;
                }
            }
            region
                .manager
                .publish_updates(region.attempt, changes)
                .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
            parent_record.routing = Arc::new(parent_image.clone());
            for guard in &mut child_guards {
                let image = updates
                    .iter()
                    .find(|r| r.target.id == guard.routing.target.id)
                    .unwrap();
                guard.routing = Arc::new(image.clone());
            }
            if let Some(guard) = grand_guard.as_mut() {
                let image = updates
                    .iter()
                    .find(|r| r.target.id == guard.routing.target.id)
                    .unwrap();
                guard.routing = Arc::new(image.clone());
            }
            for n in &replacements {
                self.local.nodes.insert(
                    n.routing.target.id,
                    Arc::new(parking_lot::RwLock::new(Resident {
                        routing: Arc::new(n.routing.clone()),
                        postings: n.postings.clone(),
                        reserved: None,
                        preparing_split: false,
                        transfer_source: None,
                        claim_producer: None,
                    })),
                );
            }
            updates.extend(replacements.iter().map(|n| n.routing.clone()));
            // The old handle remains pinned and marked reserved until it is unlinked.
            source.reserved = Some(region.attempt);
            source.preparing_split = false;
            self.local.nodes.remove(&old.target.id);
            self.local.publish_images(updates, &[old.target.id])?;
            drop(source);
            drop(parent_record);
            drop(child_guards);
            drop(grand_guard);
            drop(region);
            if parent_image.parent.is_none() {
                *self.root.write() = parent_image.clone();
            }
            // Shared balancing can replace the captured parent after region
            // release. Ownership-unit identity remains the boundary to refresh.
            if self.current_view().value.units.get(&parent.id) == Some(&parent) {
                self.refresh_boundary_centroid(parent)?;
            }
            // A parent overfilled by this replacement splits as part of the
            // same structural change, bounded by tree height, so it keeps
            // this job's depth (the reference splits parents without one).
            self.schedule(parent, current_depth())?;
            Ok(true)
        } else {
            let envelope = self.local.envelope(
                "boundary-split",
                vec![(old.target.id, old.target.placement)],
            )?;
            if old.parent.is_none() && wrap_root {
                if replacements.is_empty() {
                    return Err("cannot retire the global root".into());
                }
                let target = self.local.alloc_node()?;
                for n in &mut replacements {
                    n.routing.parent = Some(target);
                }
                let mut root = RoutingNode {
                    navigation: Default::default(),
                    target,
                    parent: None,
                    centroid: vec![0.; self.local.dim],
                    centroid_code: vec![],
                    children: replacements.iter().map(|n| child(&n.routing)).collect(),
                    leaf: false,
                    revision: 0,
                };
                set_center(&mut root, self.local.dim)?;
                replacements.push(StoredNode {
                    routing: root,
                    postings: None,
                });
            } else {
                for n in &mut replacements {
                    n.routing.parent = old.parent;
                }
            }
            let roots = if old.parent.is_none() && wrap_root {
                vec![replacements.last().unwrap().routing.clone()]
            } else {
                replacements.iter().map(|n| n.routing.clone()).collect()
            };
            let dependencies = replacements
                .iter()
                .filter(|n| self.local.nodes.contains_key(&n.routing.target.id))
                .map(|n| {
                    Ok((
                        n.routing.target,
                        n.routing
                            .revision
                            .checked_sub(1)
                            .ok_or("existing replacement needs a fresh revision")?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let request = BoundaryPreparation {
                source_claim,
                dependencies: dependencies.clone(),
                envelope: envelope.clone(),
                old: old.clone(),
                replacements: roots,
            };
            self.local.admit_placement(
                &envelope,
                bincode::serialize(&request)
                    .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?,
            )?;
            self.local.boundaries.lock().insert(
                envelope.attempt,
                PendingBoundary {
                    prepared: replacements,
                    request: request.clone(),
                    execution: Arc::new(parking_lot::Mutex::new(())),
                    preparing: false,
                    guard: None,
                    ready: false,
                    finished: false,
                },
            );
            match self.request(
                self.coordinator,
                EngineRequest::Structure(StructuralEdit {
                    source_claim,
                    envelope: envelope.clone(),
                    source: old.target,
                    expected_revision: old.revision,
                    replacements: request.replacements,
                    dependencies,
                }),
            ) {
                Ok(EngineResponse::Structure(result)) => {
                    *self.root.write() = result.root;
                    Ok(true)
                }
                Err(error) => {
                    let reversible = {
                        let mut records = self.local.boundaries.lock();
                        let p = records.get_mut(&envelope.attempt).unwrap();
                        if p.guard.is_none() && !p.ready {
                            p.finished = true;
                            true
                        } else {
                            false
                        }
                    };
                    if reversible {
                        self.local.terminal_placement(&envelope)?;
                        let dependency = error.to_ascii_lowercase();
                        if dependency.contains("changed")
                            || dependency.contains("stale")
                            || dependency.contains("retired")
                        {
                            return Ok(false);
                        }
                    }
                    Err(error)
                }
                _ => Err("invalid structural publication response".into()),
            }
        }
    }
    pub(super) fn publish_structure(
        &self,
        edit: StructuralEdit,
    ) -> Result<PublicationResult, String> {
        self.local.check(&edit.envelope)?;
        let execution = {
            let mut records = self.structural_records.lock();
            if !records.contains_key(&edit.envelope.attempt) && records.len() >= 1_000_000 {
                return Err("shared publication record capacity exceeded".into());
            }
            Arc::clone(records.entry(edit.envelope.attempt).or_insert_with(|| {
                Arc::new(parking_lot::Mutex::new(StructuralRecord {
                    edit: edit.clone(),
                    outcome: None,
                }))
            }))
        };
        // One operation's retries serialize here; this is not the coordinator's
        // publication bookkeeping mutex and it does not block Status handlers.
        let mut record = execution.lock();
        self.local.check(&edit.envelope)?;
        if record.edit != edit {
            return Err("conflicting structural attempt payload".into());
        }
        if let Some(outcome) = &record.outcome {
            return outcome.clone();
        }
        let outcome = self.publish_structure_inner(edit);
        record.outcome = Some(outcome.clone());
        outcome
    }
    fn publish_structure_inner(&self, edit: StructuralEdit) -> Result<PublicationResult, String> {
        if self.local.owner != self.coordinator {
            return Err("shared publication reached a non-coordinator".into());
        }
        // Envelope fencing precedes status lookup, including terminal duplicates.
        self.local.check(&edit.envelope)?;
        let view = self.publication.view();
        if let Ok(status) = self.publication.status(edit.envelope.attempt) {
            match status {
                super::protocol::publication::PublicationStatus::Published { sequence } => {
                    return Err(format!(
                        "published structural outcome {sequence} has no retained operation result"
                    ))
                }
                super::protocol::publication::PublicationStatus::Aborted => {
                    return Err("structural attempt aborted".into())
                }
                _ => {}
            }
        }
        let old = self.node(edit.source)?;
        if old.revision != edit.expected_revision {
            return Err("boundary source revision changed".into());
        }
        let mut next = view.value.clone();
        if old.parent.is_none() {
            if next.root.target != old.target {
                return Err("global root changed".into());
            }
            if edit.replacements.len() != 1 {
                return Err("root replacement requires one prepared new root".into());
            }
            next.root = edit.replacements[0].clone();
            next.units.remove(&old.target.id);
            next.units.insert(next.root.target.id, next.root.target);
        } else {
            let parent = old.parent.unwrap();
            let image = next
                .shared
                .get_mut(&parent.id)
                .ok_or("boundary parent changed; replan")?;
            if !image.children.iter().any(|c| c.target == old.target) {
                return Err("boundary source no longer linked".into());
            }
            image.children.retain(|c| c.target.id != old.target.id);
            image.children.extend(edit.replacements.iter().map(child));
            revise(image)?;
            set_center(image, self.local.dim)?;
            if image.target == next.root.target {
                next.root = image.clone()
            }
            next.units.remove(&old.target.id);
            for r in &edit.replacements {
                next.units.insert(r.target.id, r.target);
            }
            let mut changed = parent;
            loop {
                let child_image = next
                    .shared
                    .get(&changed.id)
                    .ok_or("changed shared ancestor vanished")?
                    .clone();
                if let Some(ancestor) = child_image.parent {
                    let image = next
                        .shared
                        .get_mut(&ancestor.id)
                        .ok_or("shared ancestor parent missing")?;
                    let row = image
                        .children
                        .iter_mut()
                        .find(|c| c.target == changed)
                        .ok_or("shared ancestor linkage changed")?;
                    row.centroid = child_image.centroid.clone();
                    revise(image)?;
                    set_center(image, self.local.dim)?;
                    changed = ancestor;
                } else {
                    next.root = child_image;
                    break;
                }
            }
        }
        let required = std::iter::once(old.target.authority).collect();
        self.publication
            .prepare(
                edit.envelope.attempt,
                super::protocol::publication::PublicationProposal {
                    expected_sequence: view.sequence,
                    required_owners: required,
                    value: next.prepared()?,
                },
            )
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self
            .publication
            .acquire_turn(edit.envelope.attempt)
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?
        {
            if Instant::now() >= deadline {
                return Err("shared publication turn deadline".into());
            }
            std::thread::yield_now();
        }
        if self.publication.view().sequence != view.sequence {
            self.publication
                .abort(edit.envelope.attempt)
                .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
            self.publication
                .release_turn(edit.envelope.attempt)
                .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
            return Err("shared publication changed while waiting for its turn".into());
        }
        let prep = BoundaryPreparation {
            source_claim: edit.source_claim,
            envelope: edit.envelope.clone(),
            old,
            replacements: edit.replacements,
            dependencies: edit.dependencies,
        };
        if let Err(error) = self.request(edit.source.authority, EngineRequest::BoundaryReady(prep))
        {
            // A rejected Ready has not published. If the owner reports changed
            // preparation, its guard unwinds before replying, so this turn can
            // be cancelled and a fresh attempt can replan. An unknown transport
            // outcome keeps protection and fails the run instead.
            if error.contains("dependencies changed")
                || error.contains("region dependencies")
                || error.contains("stale")
                || error.contains("retired")
            {
                self.publication
                    .abort(edit.envelope.attempt)
                    .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
                self.publication
                    .release_turn(edit.envelope.attempt)
                    .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
            }
            return Err(format!("boundary Ready rejected: {error}"));
        }
        self.publication
            .ready(edit.envelope.attempt, edit.source.authority)
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        let outcome = self
            .publication
            .publish(edit.envelope.attempt)
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        let sequence = match outcome {
            super::protocol::publication::PublicationStatus::Published { sequence } => sequence,
            _ => return Err("boundary publication has no committed outcome".into()),
        };
        let published = self.publication.view();
        self.install_view(&published)?;
        self.request(
            edit.source.authority,
            EngineRequest::BoundaryFinish {
                envelope: edit.envelope.clone(),
                published: true,
            },
        )?;
        self.publication
            .release_turn(edit.envelope.attempt)
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        self.schedule_shared_eligible()?;
        Ok(PublicationResult {
            root: published.value.root.clone(),
            sequence,
        })
    }
}

impl OwnerService {
    pub(super) fn boundary_ready(&self, request: BoundaryPreparation) -> Result<(), String> {
        self.check(&request.envelope)?;
        let execution = {
            let records = self.boundaries.lock();
            Arc::clone(
                &records
                    .get(&request.envelope.attempt)
                    .ok_or("unknown boundary attempt")?
                    .execution,
            )
        };
        let _execution = execution.lock();
        self.check(&request.envelope)?;
        {
            let mut records = self.boundaries.lock();
            let pending = records.get_mut(&request.envelope.attempt).unwrap();
            if bincode::serialize(&pending.request).map_err(|e| e.to_string())?
                != bincode::serialize(&request).map_err(|e| e.to_string())?
            {
                return Err("conflicting boundary duplicate".into());
            }
            if pending.ready {
                return Ok(());
            }
            pending.preparing = true;
        }
        let result = self.boundary_ready_inner(request.clone());
        self.boundaries
            .lock()
            .get_mut(&request.envelope.attempt)
            .unwrap()
            .preparing = false;
        result
    }
    fn boundary_ready_inner(&self, request: BoundaryPreparation) -> Result<(), String> {
        self.check(&request.envelope)?;
        let (prepared, old) = {
            let records = self.boundaries.lock();
            let pending = records
                .get(&request.envelope.attempt)
                .ok_or("unknown prepared boundary")?;
            if bincode::serialize(&pending.request).unwrap()
                != bincode::serialize(&request).unwrap()
            {
                return Err("conflicting boundary duplicate".into());
            }
            if pending.ready {
                return Ok(());
            }
            (pending.prepared.clone(), pending.request.old.clone())
        };
        let region = self.reserve(vec![old.target])?;
        let handle = self.resident(old.target)?;
        let mut source = handle.write();
        if source.routing.revision != old.revision || source.reserved != request.source_claim {
            return Err("boundary dependencies changed".into());
        }
        for (node, expected) in &request.dependencies {
            if self.node(*node)?.revision != *expected {
                return Err("boundary replacement dependencies changed".into());
            }
        }
        let existing_prepared = prepared
            .iter()
            .filter(|n| self.nodes.contains_key(&n.routing.target.id))
            .map(|n| Ok((n.routing.target.id, self.resident(n.routing.target)?)))
            .collect::<Result<Vec<_>, String>>()?;
        let mut existing_guards = existing_prepared
            .iter()
            .map(|(_, h)| h.write())
            .collect::<Vec<_>>();
        let mut changes = vec![];
        for n in &prepared {
            changes.push((n.routing.target.id, as_region(&n.routing, self.owner, true)));
        }
        let mut retired = old.clone();
        revise(&mut retired)?;
        changes.push((old.target.id, as_region(&retired, self.owner, false)));
        // Internal children retain payloads and IDs; only their parent changes.
        let mut child_handles = vec![];
        for n in &prepared {
            for c in &n.routing.children {
                if self.nodes.contains_key(&c.target.id) {
                    child_handles.push((c.target.id, n.routing.target, self.resident(c.target)?));
                }
            }
        }
        child_handles.sort_by_key(|(id, _, _)| *id);
        let mut guards = child_handles
            .iter()
            .map(|(_, _, h)| h.write())
            .collect::<Vec<_>>();
        let mut updates = vec![];
        for ((_, parent, _), guard) in child_handles.iter().zip(&mut guards) {
            let mut r = (*guard.routing).clone();
            r.parent = Some(*parent);
            revise(&mut r)?;
            changes.push((r.target.id, as_region(&r, self.owner, true)));
            updates.push(r);
        }
        for (id, node) in &mut changes {
            if let Ok((old, _)) = region.manager.snapshot(*id) {
                node.revision = old
                    .revision
                    .checked_add(1)
                    .ok_or("region revision exhausted")?;
            }
        }
        let prepared_ids = prepared
            .iter()
            .map(|n| n.routing.target.id)
            .collect::<BTreeSet<_>>();
        for (_, node) in &mut changes {
            if node
                .parent
                .is_some_and(|p| !self.nodes.contains_key(&p) && !prepared_ids.contains(&p))
            {
                node.parent = None;
            }
        }
        region
            .manager
            .publish_root_replacements(
                region.attempt,
                changes,
                vec![(
                    old.target.id,
                    request.replacements.iter().map(|r| r.target.id).collect(),
                )],
            )
            .map_err(|e| format!("construction protocol at line {}: {e}", line!()))?;
        for n in &prepared {
            if let Some(guard) = existing_guards
                .iter_mut()
                .find(|g| g.routing.target.id == n.routing.target.id)
            {
                guard.routing = Arc::new(n.routing.clone());
                updates.push(n.routing.clone());
                continue;
            }
            self.nodes.insert(
                n.routing.target.id,
                Arc::new(parking_lot::RwLock::new(Resident {
                    routing: Arc::new(n.routing.clone()),
                    postings: n.postings.clone(),
                    reserved: None,
                    preparing_split: false,
                    transfer_source: None,
                    claim_producer: None,
                })),
            );
            updates.push(n.routing.clone());
        }
        for guard in &mut guards {
            let r = updates
                .iter()
                .find(|r| r.target.id == guard.routing.target.id)
                .unwrap();
            guard.routing = Arc::new(r.clone());
        }
        source.reserved = Some(request.envelope.attempt);
        source.preparing_split = false;
        self.publish_images(updates, &[])?;
        let mut pending = self.boundaries.lock();
        let p = pending.get_mut(&request.envelope.attempt).unwrap();
        p.guard = Some(region);
        p.ready = true;
        Ok(())
    }
    pub(super) fn boundary_finish(
        &self,
        envelope: Envelope,
        published: bool,
    ) -> Result<(), String> {
        self.check(&envelope)?;
        let execution = {
            let records = self.boundaries.lock();
            Arc::clone(
                &records
                    .get(&envelope.attempt)
                    .ok_or("unknown boundary attempt")?
                    .execution,
            )
        };
        let _execution = execution.lock();
        self.check(&envelope)?;
        if !published {
            return Err("a prepared boundary requires forward completion or a stopped run".into());
        }
        let cluster = self.cluster()?;
        let outcome = if self.owner == cluster.coordinator {
            cluster
                .publication
                .status(envelope.attempt)
                .map_err(|e| e.to_string())?
        } else {
            match cluster.placement_request(
                cluster.coordinator,
                super::placement_runtime::PlacementRequest::PublicationStatus {
                    run: self.run,
                    attempt: envelope.attempt,
                },
            )? {
                super::placement_runtime::PlacementResponse::Publication(status) => status,
                _ => return Err("invalid publication status response".into()),
            }
        };
        if !matches!(
            outcome,
            super::protocol::publication::PublicationStatus::Published { .. }
        ) {
            return Err(
                "boundary cleanup requires a confirmed published coordinator outcome".into(),
            );
        }
        let mut pending = self.boundaries.lock();
        let p = pending
            .get_mut(&envelope.attempt)
            .ok_or("unknown boundary completion")?;
        if p.finished {
            return Ok(());
        }
        if !published {
            return Err("a prepared boundary requires forward completion or a stopped run".into());
        }
        if !p.ready {
            return Err("boundary completion before Ready".into());
        }
        let old = p.request.old.target.id;
        self.nodes.remove(&old);
        self.publish_images(vec![], &[old])?;
        p.guard.take();
        p.finished = true;
        self.terminal_placement(&envelope)?;
        Ok(())
    }
}

impl Cluster {
    fn split_internal(&self, target: NodeRef) -> Result<(), String> {
        let mut work = super::observability::detailed()
            .then(|| self.local.observability.operation(Scope::InternalSplit));
        let old = match self.local.node(target) {
            Ok(n) => n,
            Err(_) => return Ok(()),
        };
        if old.leaf {
            return Ok(());
        }
        if old.parent.is_none() && old.children.len() == 1 {
            let target = old.children[0].target;
            let handle = match self.local.resident(target) {
                Ok(h) => h,
                Err(_) => return Ok(()),
            };
            let mut replacement = {
                let node = handle.read();
                StoredNode {
                    routing: (*node.routing).clone(),
                    postings: node.postings.clone(),
                }
            };
            replacement.routing.parent = None;
            revise(&mut replacement.routing)?;
            if !self.install_replacements(&old, vec![replacement], false)? {
                self.enqueue(BalanceJob {
                    envelope: self
                        .local
                        .envelope("balance-job", vec![(old.target.id, old.target.placement)])?,
                    node: old.target,
                    depth: current_depth(),
                    internal: true,
                })?;
            }
            return Ok(());
        }
        if old.children.is_empty() && old.parent.is_some() {
            if !self.install_split(&old, vec![])? {
                self.enqueue(BalanceJob {
                    envelope: self
                        .local
                        .envelope("balance-job", vec![(target.id, target.placement)])?,
                    node: target,
                    depth: current_depth(),
                    internal: true,
                })?;
            }
            return Ok(());
        }
        if old.children.len() <= self.local.config.branching_factor {
            return Ok(());
        }
        if let Some(work) = &mut work {
            work.add(
                Counter::InternalSplitInputChildren,
                old.children.len() as u64,
            );
        }
        let input: Vec<EmbeddingPoint> = old
            .children
            .iter()
            .enumerate()
            .map(|(i, c)| (i as u32, 0, Arc::from(c.centroid.as_slice())))
            .collect();
        let (a, left, b, right) = utils::split(input, &self.local.metric);
        let zero = vec![0.; self.local.dim];
        let mut prepared = vec![];
        for (center, group) in [(a, left), (b, right)] {
            let mut new = self.local.alloc_node()?;
            new.placement = target.placement;
            let centroid = center.to_vec();
            prepared.push(StoredNode {
                routing: RoutingNode {
                    navigation: Default::default(),
                    target: new,
                    parent: old.parent,
                    centroid_code: Code::<4>::quantize(&centroid, &zero).as_ref().to_vec(),
                    centroid,
                    children: group
                        .into_iter()
                        .map(|(slot, _, _)| old.children[slot as usize].clone())
                        .collect(),
                    leaf: false,
                    revision: 0,
                },
                postings: None,
            });
        }
        let refs = prepared
            .iter()
            .map(|n| n.routing.target)
            .collect::<Vec<_>>();
        if self.install_split(&old, prepared)? {
            for node in refs {
                self.schedule(node, current_depth())?;
            }
        } else {
            self.enqueue(BalanceJob {
                envelope: self
                    .local
                    .envelope("balance-job", vec![(target.id, target.placement)])?,
                node: target,
                depth: current_depth(),
                internal: true,
            })?;
        }
        Ok(())
    }
    fn merge_leaf(&self, source: NodeRef) -> Result<(), String> {
        self.merge_leaf_for_job(source, None)
    }
    fn merge_leaf_for_job(
        &self,
        source: NodeRef,
        producer: Option<AttemptId>,
    ) -> Result<(), String> {
        let old = match self.local.node(source) {
            Ok(n) => n,
            Err(_) => return Ok(()),
        };
        if old.parent.is_none() {
            return Ok(());
        }
        let resident = match self.local.resident(source) {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };
        let ids = {
            let n = resident.read();
            if n.reserved.is_some() {
                return Ok(());
            }
            let p = n.postings.as_ref().ok_or("merge requires leaf")?;
            if p.ids.is_empty() {
                drop(n);
                return self.retire_empty_leaf(source);
            }
            if p.ids.len() >= self.local.config.merge_threshold {
                return Ok(());
            }
            p.ids.clone()
        };
        let candidates = self
            .navigate(
                &[old.centroid.clone()],
                &self.write_policy(),
                Some(source.id),
            )?
            .pop()
            .unwrap_or_default();
        let Some(&(first, _)) = candidates.first() else {
            return Ok(());
        };
        let destination = match self.node(first) {
            Ok(n) => n,
            Err(_) => {
                self.schedule(source, current_depth())?;
                return Ok(());
            }
        };
        let mut proposed = vec![];
        for id in &ids {
            let v = self.local.vector(*id)?;
            if self.local.metric.distance(&v, &destination.centroid)
                < self.local.metric.distance(&v, &old.centroid)
            {
                proposed.push(*id)
            }
        }
        if !proposed.is_empty() {
            match self.transfer_for_job(source, first, proposed, producer) {
                Ok(()) => self.schedule(first, follow_up_depth())?,
                Err(error)
                    if self.local.ensure_healthy().is_ok()
                        && retryable_transfer_dependency(&error) =>
                {
                    // A different worker can move the captured rows before our
                    // pair is protected. Re-evaluate current membership instead
                    // of treating that known rejection as a partial mutation.
                    return self.retire_empty_leaf(source);
                }
                Err(error) => return Err(error),
            }
        }
        // Forced evacuation belongs to this attempt's captured membership.
        // Reassignment filters rows that already moved; later arrivals stay
        // current until ordinary size eligibility chooses their next operation.
        if self.reassign_inner::<true>(source, ids, Some(source.id), producer)?
            == ReassignmentCompletion::Deferred
        {
            return Ok(());
        }
        // Another leaf's reassignment can add current members after the
        // evacuation snapshot. Preserve them and re-evaluate normal size work;
        // an empty source is retired only after protected current validation.
        self.retire_empty_leaf(source)
    }
    fn retire_empty_leaf(&self, source: NodeRef) -> Result<(), String> {
        let resident = match self.local.resident(source) {
            Ok(record) => record,
            Err(_) => return Ok(()),
        };
        let snapshot = {
            let record = resident.read();
            let Some(postings) = record.postings.as_ref() else {
                return Ok(());
            };
            if !postings.ids.is_empty() {
                drop(record);
                return self.schedule(source, current_depth());
            }
            (*record.routing).clone()
        };
        if snapshot.parent.is_none() {
            return Ok(());
        }
        if !self.install_split(&snapshot, vec![])? {
            // Ordinary size scheduling excludes empty leaves. Keep retirement
            // as explicit, coalesced work when its protected snapshot changes.
            self.enqueue(BalanceJob {
                envelope: self
                    .local
                    .envelope("balance-job", vec![(source.id, source.placement)])?,
                node: source,
                depth: current_depth(),
                internal: false,
            })?;
        }
        Ok(())
    }
}

impl OwnerService {
    pub(super) fn boundary_save_quiescent(&self) -> Result<(), String> {
        if self
            .boundaries
            .lock()
            .values()
            .any(|p| !p.finished || p.guard.is_some())
        {
            return Err("unfinished structural publication prevents save".into());
        }
        Ok(())
    }
}

impl Cluster {
    pub(super) fn schedule_shared_eligible(&self) -> Result<(), String> {
        if self.local.owner != self.coordinator {
            return Ok(());
        }
        let view = self.current_view();
        for node in view.value.shared.values() {
            if node.children.len() > self.local.config.branching_factor
                || node.children.is_empty()
                || (node.target == view.value.root.target && node.children.len() == 1)
            {
                self.enqueue(BalanceJob {
                    envelope: self
                        .local
                        .envelope("balance-job", vec![(node.target.id, node.target.placement)])?,
                    node: node.target,
                    depth: 0,
                    internal: true,
                })?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "construction_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "source_leaf_claim_tests.rs"]
mod source_leaf_claim_tests;

#[cfg(test)]
#[path = "reassignment_coalescing_tests.rs"]
mod reassignment_coalescing_tests;

//! Cumulative work and inclusive operation times. Stack-local counters merge once
//! per operation; these nested times must not be summed as phase latency.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[repr(usize)] #[derive(Clone, Copy)] pub enum Counter {$($name,)* Count}
        const COUNTER_NAMES:&[&str]=&[$(stringify!($name),)*];
    }
}
counters!(
    BoundaryCentroidAttempts,
    BoundaryCentroidPrepared,
    BoundaryCentroidStalePrepareRetries,
    BoundaryCentroidChangedRetries,
    BoundaryCentroidNoop,
    BoundaryCentroidPublished,
    ReservationRequests,
    ReservationPumpCalls,
    ReservationCandidates,
    ReservationConflictVisits,
    ReservationAncestrySteps,
    ReservationQueuedWaitNanos,
    ReservationMutexWaitNanos,
    ReservationMutexHoldNanos,
    LocalTransferBatches,
    OwnerTransferBatches,
    OwnerTransferTargetGroups,
    LocalTransferTargetGroups,
    LocalTransferMembershipProbes,
    TransferGroups,
    TransferProposedRows,
    TransferProtocolCalls,
    TransferRetryCalls,
    TransferCompletedGroups,
    TransferRejectedGroups,
    TransferMembershipProbes,
    TransferMembershipComparisonUpperBound,
    TransferSourceEntriesScanned,
    TransferCompactions,
    TransferCopiedIdBytes,
    TransferCopiedCodeBytes,
    TransferPayloadIdBytesCloned,
    TransferPayloadCodeBytesCloned,
    ReassignmentInputRows,
    NeighborCandidateRows,
    NeighborDistanceCalculations,
    NeighborProposedRows,
    InternalSplitInputChildren,
    ReassignmentRounds,
    ReassignmentRejectedGroups,
    ReassignmentQueryBytesCopied,
    NavigationCalls,
    NavigationAttempts,
    // Pending ID chunks accepted for retry behind other active structural jobs.
    NavigationDeferredJobs,
    NavigationQueryRows,
    NavigationQueryBytesCopied,
    NavigationReplyBytesCloned,
    NavigationRepliesValidated,
    NavigationChildRowsValidated,
    NavigationCandidateRows,
    LocalEngineCalls,
    RemoteEngineCalls,
    ExpansionRemoteCalls,
    ExpansionInitialGroups,
    ExpansionMultiGroupCalls,
    ExpansionSubcalls,
    ExpansionSizeSplits,
    ExpansionGroups1,
    ExpansionGroups2,
    ExpansionGroups3To4,
    ExpansionGroups5To8,
    ExpansionGroups9Plus,
    OwnedRoutingDistances,
    SharedRoutingDistances,
    RootLeafDistances,
    SplitAttempts,
    SplitInputRows,
    SplitPrepared,
    SplitPreparedRows,
    SplitCommitted,
    SplitCommittedRows,
    SplitDiscarded,
    SplitDiscardedRows,
    BalanceScheduledDepth0,
    BalanceScheduledDepth1,
    BalanceScheduledDepth2,
    BalanceScheduledDepth3,
    BalanceScheduledDepth4,
    BalanceDepthDeferred
);
macro_rules! scopes {
    ($($name:ident),* $(,)?) => {
        #[repr(usize)] #[derive(Clone, Copy)] pub enum Scope {$($name,)* Count}
        const SCOPE_NAMES:&[&str]=&[$(stringify!($name),)*];
    }
}
scopes!(
    BoundaryCentroidRefresh,
    BoundaryCentroidRebuild,
    BoundaryCentroidTurnWait,
    BoundaryCentroidTurnHeld,
    BoundaryCentroidGateWait,
    BoundaryCentroidBroadcast,
    InsertionRoutePhase,
    InsertionAppendPhase,
    InsertionRoute,
    InsertionGroup,
    InsertionAppend,
    SchedulerDispatch,
    SchedulerExecution,
    NeighborSelection,
    InternalSplit,
    ReassignmentMembershipScan,
    ReassignmentQueryBuild,
    BalanceJob,
    ScheduledBalance,
    ScheduledNeighbor,
    ScheduledReassign,
    ReservationRequest,
    TransferRetry,
    ReservationMutex,
    ReservationPump,
    ReservationQueueWait,
    LocalTransfer,
    LocalTransferAdmission,
    LocalTransferCleanup,
    LocalTransferTerminal,
    LocalTransferPreparation,
    LocalTransferCommit,
    TransferDriver,
    TransferPrepare,
    TransferStep,
    TransferCompaction,
    TransferQuantize,
    EngineRequest,
    Navigation,
    NavigationBatch,
    NavigationQueryCopy,
    NavigationReplyValidation,
    NavigationCandidates,
    NavigationCanonicalOrder,
    NavigationSort,
    NavigationGather,
    NavigationAllocationCopy,
    TransportSerialize,
    TransportDeserialize,
    TransportConnect,
    TransportClientElapsed,
    TransportServerExecution,
    TransportThreadStartWait,
    TransportThreadSpawn,
    OwnedRoutingScoring,
    ExpansionBackend,
    ExpansionSequentialGroups,
    SharedRoutingScoring,
    SplitPreparation,
    SplitInstallation,
    BalanceScheduling
);
#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize)]
pub struct TimeTotal {
    pub calls: u64,
    /// Log2 microsecond upper-bound buckets; final bucket includes overflow.
    pub wall_histogram: [u64; 32],
    pub wall_nanos: u64,
    pub max_wall_nanos: u64,
    pub max_thread_cpu_nanos: u64,
    pub thread_cpu_nanos: u64,
    pub thread_cpu_supported_calls: u64,
}
/// The scheduled operation that initiates work. Attribution is independent of
/// the nested operation name, so reassignment navigation remains distinguishable.
#[derive(Clone, Copy, Debug)]
pub enum JobKind {
    Balance,
    Neighbor,
    Reassign,
}
impl JobKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Balance => "balance",
            Self::Neighbor => "neighbor",
            Self::Reassign => "reassign",
        }
    }
}
/// Direct request durations attributed to the initiating job. Server handler time
/// includes any nested calls; concurrent requests overlap and must not be summed
/// as a job critical path. Keys are fixed transport request kinds.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct RemoteRequestTiming {
    pub calls: u64,
    pub errors: u64,
    pub timed_replies: u64,
    pub server_queue_timed_replies: u64,
    pub client_elapsed_nanos: u64,
    pub client_decode_nanos: u64,
    pub server_decode_queue_nanos: u64,
    pub server_decode_nanos: u64,
    pub server_execution_queue_nanos: u64,
    pub server_handler_nanos: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub max_client_elapsed_nanos: u64,
}
impl RemoteRequestTiming {
    pub fn merge(&mut self, other: &Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.errors = self.errors.saturating_add(other.errors);
        self.timed_replies = self.timed_replies.saturating_add(other.timed_replies);
        self.server_queue_timed_replies = self
            .server_queue_timed_replies
            .saturating_add(other.server_queue_timed_replies);
        self.client_elapsed_nanos = self
            .client_elapsed_nanos
            .saturating_add(other.client_elapsed_nanos);
        self.client_decode_nanos = self
            .client_decode_nanos
            .saturating_add(other.client_decode_nanos);
        self.server_decode_queue_nanos = self
            .server_decode_queue_nanos
            .saturating_add(other.server_decode_queue_nanos);
        self.server_decode_nanos = self
            .server_decode_nanos
            .saturating_add(other.server_decode_nanos);
        self.server_execution_queue_nanos = self
            .server_execution_queue_nanos
            .saturating_add(other.server_execution_queue_nanos);
        self.server_handler_nanos = self
            .server_handler_nanos
            .saturating_add(other.server_handler_nanos);
        self.bytes_sent = self.bytes_sent.saturating_add(other.bytes_sent);
        self.bytes_received = self.bytes_received.saturating_add(other.bytes_received);
        self.max_client_elapsed_nanos = self
            .max_client_elapsed_nanos
            .max(other.max_client_elapsed_nanos);
    }
}
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Attribution {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_requests: BTreeMap<String, RemoteRequestTiming>,
    pub counters: BTreeMap<String, u64>,
    pub inclusive_scopes: BTreeMap<String, TimeTotal>,
}
impl Attribution {
    fn add(
        &mut self,
        scope: Scope,
        elapsed: u64,
        cpu: Option<u64>,
        counts: &[u64; Counter::Count as usize],
        mut touched: u128,
    ) {
        while touched != 0 {
            let i = touched.trailing_zeros() as usize;
            let count = self.counters.entry(COUNTER_NAMES[i].into()).or_default();
            *count = count.saturating_add(counts[i]);
            touched &= touched - 1;
        }
        let time = self
            .inclusive_scopes
            .entry(SCOPE_NAMES[scope as usize].into())
            .or_default();
        time.calls += 1;
        time.wall_nanos = time.wall_nanos.saturating_add(elapsed);
        time.max_wall_nanos = time.max_wall_nanos.max(elapsed);
        let micros = elapsed.saturating_add(999) / 1000;
        time.wall_histogram[(64 - micros.saturating_sub(1).leading_zeros() as usize).min(31)] += 1;
        if let Some(cpu) = cpu {
            time.thread_cpu_nanos = time.thread_cpu_nanos.saturating_add(cpu);
            time.max_thread_cpu_nanos = time.max_thread_cpu_nanos.max(cpu);
            time.thread_cpu_supported_calls += 1;
        }
    }
}
#[derive(Clone)]
pub struct JobContext {
    kind: JobKind,
    work: Arc<Mutex<Attribution>>,
}
thread_local! { static CONTEXT: RefCell<Option<JobContext>> = const { RefCell::new(None) }; }
/// Capture before spawning a child; install inside it. Never infer attribution
/// from another thread's current job or from a process-wide counter difference.
pub fn current_context() -> Option<JobContext> {
    CONTEXT.with(|c| c.borrow().clone())
}
pub fn install_context(context: Option<JobContext>) -> ContextGuard {
    ContextGuard {
        previous: CONTEXT.with(|c| c.replace(context)),
        _same_thread: std::marker::PhantomData,
    }
}
pub struct ContextGuard {
    previous: Option<JobContext>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.with(|c| {
            c.replace(self.previous.take());
        });
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobRecord {
    pub kind: String,
    pub owner: u64,
    pub node_id: String,
    pub attempt_authority: u64,
    pub attempt_sequence: u64,
    pub started_unix_nanos: u64,
    pub wall_nanos: u64,
    pub thread_cpu_nanos: Option<u64>,
    pub work: Attribution,
}
pub struct JobGuard<'a> {
    profile: &'a WorkProfile,
    context_guard: Option<ContextGuard>,
    context: Option<JobContext>,
    record: Option<JobRecord>,
    started: Instant,
    cpu: Option<u64>,
}
impl Drop for JobGuard<'_> {
    fn drop(&mut self) {
        self.context_guard.take();
        if let Some(mut record) = self.record.take() {
            record.wall_nanos = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            record.thread_cpu_nanos = self
                .cpu
                .zip(chroma_index::spann::utils::clustering_thread_cpu_nanos())
                .map(|(a, b)| b.saturating_sub(a));
            record.work = self.context.as_ref().unwrap().work.lock().clone();
            let mut inner = self.profile.inner.lock();
            inner.last_finished_jobs.push(record.clone());
            inner.last_finished_jobs.sort_by_key(|r| {
                std::cmp::Reverse(r.started_unix_nanos.saturating_add(r.wall_nanos))
            });
            inner.last_finished_jobs.truncate(32);
            inner.longest_jobs.push(record);
            inner
                .longest_jobs
                .sort_by_key(|r| std::cmp::Reverse(r.wall_nanos));
            inner.longest_jobs.truncate(32);
        }
    }
}
struct Inner {
    remote_requests: BTreeMap<String, RemoteRequestTiming>,
    counts: [u64; Counter::Count as usize],
    times: [TimeTotal; Scope::Count as usize],
    by_origin: BTreeMap<String, Attribution>,
    longest_jobs: Vec<JobRecord>,
    last_finished_jobs: Vec<JobRecord>,
}
impl Default for Inner {
    fn default() -> Self {
        Self {
            remote_requests: BTreeMap::new(),
            counts: [0; Counter::Count as usize],
            times: [TimeTotal::default(); Scope::Count as usize],
            by_origin: BTreeMap::new(),
            longest_jobs: Vec::new(),
            last_finished_jobs: Vec::new(),
        }
    }
}
#[derive(Default)]
pub struct WorkProfile {
    inner: Mutex<Inner>,
}
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct WorkSnapshot {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_requests: BTreeMap<String, RemoteRequestTiming>,
    /// Inclusive same-thread work plus explicitly propagated expansion workers.
    /// Remote handlers have no wire context and are explicitly unassigned.
    #[serde(default)]
    pub by_origin: BTreeMap<String, Attribution>,
    #[serde(default)]
    pub longest_jobs: Vec<JobRecord>,
    /// Latest completion estimates, independently of duration. Completion uses
    /// the wall-clock start plus monotonic elapsed time, avoiding clock jumps
    /// during a job. Filter by phase when comparing with benchmark events.
    #[serde(default)]
    pub last_finished_jobs: Vec<JobRecord>,
    pub counters: BTreeMap<String, u64>,
    pub inclusive_scopes: BTreeMap<String, TimeTotal>,
}
impl WorkProfile {
    pub fn record_remote(&self, kind: &'static str, timing: RemoteRequestTiming) {
        self.inner
            .lock()
            .remote_requests
            .entry(kind.into())
            .or_default()
            .merge(&timing);
        if let Some(context) = current_context() {
            context
                .work
                .lock()
                .remote_requests
                .entry(kind.into())
                .or_default()
                .merge(&timing);
        }
    }

    pub fn job(
        &self,
        kind: JobKind,
        owner: u64,
        node_id: u128,
        attempt: super::protocol::AttemptId,
    ) -> JobGuard<'_> {
        self.job_enabled(kind, owner, node_id, attempt, detailed())
    }
    fn job_enabled(
        &self,
        kind: JobKind,
        owner: u64,
        node_id: u128,
        attempt: super::protocol::AttemptId,
        enabled: bool,
    ) -> JobGuard<'_> {
        let context = enabled.then(|| JobContext {
            kind,
            work: Arc::new(Mutex::new(Attribution::default())),
        });
        let context_guard = enabled.then(|| install_context(context.clone()));
        let record = enabled.then(|| JobRecord {
            kind: kind.as_str().into(),
            owner,
            node_id: node_id.to_string(),
            attempt_authority: attempt.authority,
            attempt_sequence: attempt.sequence,
            started_unix_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            wall_nanos: 0,
            thread_cpu_nanos: None,
            work: Attribution::default(),
        });
        JobGuard {
            profile: self,
            context_guard,
            context,
            record,
            started: Instant::now(),
            cpu: enabled
                .then(chroma_index::spann::utils::clustering_thread_cpu_nanos)
                .flatten(),
        }
    }
    pub fn operation(&self, scope: Scope) -> WorkOperation<'_> {
        self.operation_enabled(scope, detailed())
    }
    fn operation_enabled(&self, scope: Scope, attributed: bool) -> WorkOperation<'_> {
        WorkOperation {
            profile: self,
            scope,
            started: Instant::now(),
            cpu: chroma_index::spann::utils::clustering_thread_cpu_nanos(),
            counts: [0; Counter::Count as usize],
            touched: 0,
            context: attributed.then(current_context).flatten(),
            attributed,
        }
    }
    pub fn record_wall(&self, scope: Scope, elapsed: u64) {
        let mut inner = self.inner.lock();
        if detailed() {
            let context = current_context();
            let origin = context
                .as_ref()
                .map(|c| c.kind.as_str())
                .unwrap_or("unassigned");
            let counts = [0; Counter::Count as usize];
            inner
                .by_origin
                .entry(origin.into())
                .or_default()
                .add(scope, elapsed, None, &counts, 0);
            if let Some(context) = context {
                context.work.lock().add(scope, elapsed, None, &counts, 0);
            }
        }
        let time = &mut inner.times[scope as usize];
        time.calls += 1;
        time.wall_nanos = time.wall_nanos.saturating_add(elapsed);
        time.max_wall_nanos = time.max_wall_nanos.max(elapsed);
        let micros = elapsed.saturating_add(999) / 1000;
        time.wall_histogram[(64 - micros.saturating_sub(1).leading_zeros() as usize).min(31)] += 1;
    }
    /// Sparse totals for short-lived transport profiles. Attribution and job
    /// records are intentionally absent; callers merge operation and remote totals.
    pub fn snapshot_nonzero(&self) -> WorkSnapshot {
        let inner = self.inner.lock();
        WorkSnapshot {
            remote_requests: inner.remote_requests.clone(),
            counters: COUNTER_NAMES
                .iter()
                .enumerate()
                .filter(|(i, _)| inner.counts[*i] != 0)
                .map(|(i, n)| (n.to_string(), inner.counts[i]))
                .collect(),
            inclusive_scopes: SCOPE_NAMES
                .iter()
                .enumerate()
                .filter(|(i, _)| inner.times[*i].calls != 0)
                .map(|(i, n)| (n.to_string(), inner.times[i]))
                .collect(),
            ..WorkSnapshot::default()
        }
    }
    pub fn snapshot(&self) -> WorkSnapshot {
        let inner = self.inner.lock();
        WorkSnapshot {
            remote_requests: inner.remote_requests.clone(),
            by_origin: inner.by_origin.clone(),
            longest_jobs: inner.longest_jobs.clone(),
            last_finished_jobs: inner.last_finished_jobs.clone(),
            counters: COUNTER_NAMES
                .iter()
                .enumerate()
                .map(|(i, n)| (n.to_string(), inner.counts[i]))
                .collect(),
            inclusive_scopes: SCOPE_NAMES
                .iter()
                .enumerate()
                .map(|(i, n)| (n.to_string(), inner.times[i]))
                .collect(),
        }
    }
}
pub struct WorkOperation<'a> {
    profile: &'a WorkProfile,
    scope: Scope,
    started: Instant,
    cpu: Option<u64>,
    counts: [u64; Counter::Count as usize],
    touched: u128,
    context: Option<JobContext>,
    attributed: bool,
}
impl WorkOperation<'_> {
    pub fn add(&mut self, key: Counter, value: u64) {
        let index = key as usize;
        self.counts[index] = self.counts[index].saturating_add(value);
        self.touched |= 1u128 << index;
    }
}
impl Drop for WorkOperation<'_> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let cpu = self
            .cpu
            .zip(chroma_index::spann::utils::clustering_thread_cpu_nanos())
            .map(|(a, b)| b.saturating_sub(a));
        let mut inner = self.profile.inner.lock();
        if self.attributed {
            let origin = self
                .context
                .as_ref()
                .map(|c| c.kind.as_str())
                .unwrap_or("unassigned");
            inner.by_origin.entry(origin.into()).or_default().add(
                self.scope,
                elapsed,
                cpu,
                &self.counts,
                self.touched,
            );
            if let Some(context) = &self.context {
                context
                    .work
                    .lock()
                    .add(self.scope, elapsed, cpu, &self.counts, self.touched);
            }
        }
        while self.touched != 0 {
            let index = self.touched.trailing_zeros() as usize;
            inner.counts[index] = inner.counts[index].saturating_add(self.counts[index]);
            self.touched &= self.touched - 1;
        }
        let time = &mut inner.times[self.scope as usize];
        time.calls += 1;
        let micros = elapsed.saturating_add(999) / 1000;
        let bucket = (64 - micros.saturating_sub(1).leading_zeros() as usize).min(31);
        time.wall_histogram[bucket] += 1;
        time.max_wall_nanos = time.max_wall_nanos.max(elapsed);
        time.wall_nanos = time.wall_nanos.saturating_add(elapsed);
        if let Some(cpu) = cpu {
            time.thread_cpu_nanos = time.thread_cpu_nanos.saturating_add(cpu);
            time.max_thread_cpu_nanos = time.max_thread_cpu_nanos.max(cpu);
            time.thread_cpu_supported_calls += 1;
        }
    }
}
/// Preserve the optimized slice scan. These are logical probes and a conservative
/// comparison bound, not measured machine-level comparisons.
pub fn counted_contains(ids: &[u64], needle: &u64, probes: &mut u64, bound: &mut u64) -> bool {
    *probes += 1;
    *bound = bound.saturating_add(ids.len() as u64);
    ids.contains(needle)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn context_attributes_children_restores_parent_and_leaves_remote_unassigned() {
        let p = WorkProfile::default();
        let attempt = super::super::protocol::AttemptId {
            authority: 2,
            sequence: 7,
        };
        {
            let _job = p.job_enabled(JobKind::Balance, 2, u128::MAX, attempt, true);
            {
                let mut op = p.operation_enabled(Scope::Navigation, true);
                op.add(Counter::NavigationQueryRows, 3);
            }
            std::thread::scope(|scope| {
                let context = current_context();
                let p = &p;
                scope
                    .spawn(move || {
                        let _context = install_context(context);
                        let mut op = p.operation_enabled(Scope::OwnedRoutingScoring, true);
                        op.add(Counter::OwnedRoutingDistances, 5);
                        p.record_remote(
                            "Expand",
                            RemoteRequestTiming {
                                calls: 1,
                                client_elapsed_nanos: 50,
                                server_handler_nanos: 20,
                                ..RemoteRequestTiming::default()
                            },
                        );
                    })
                    .join()
                    .unwrap();
                scope
                    .spawn(move || {
                        // Independent server threads do not inherit initiating jobs.
                        let mut op = p.operation_enabled(Scope::OwnedRoutingScoring, true);
                        op.add(Counter::OwnedRoutingDistances, 11);
                        p.record_remote(
                            "Expand",
                            RemoteRequestTiming {
                                calls: 1,
                                client_elapsed_nanos: 100,
                                ..RemoteRequestTiming::default()
                            },
                        );
                    })
                    .join()
                    .unwrap();
            });
            {
                let _nested = p.job_enabled(JobKind::Reassign, 2, 8, attempt, true);
                let mut op = p.operation_enabled(Scope::Navigation, true);
                op.add(Counter::NavigationQueryRows, 7);
            }
            let mut op = p.operation_enabled(Scope::Navigation, true);
            op.add(Counter::NavigationQueryRows, 13);
        }
        assert!(current_context().is_none());
        let s = p.snapshot();
        assert_eq!(s.counters["NavigationQueryRows"], 23);
        assert_eq!(s.by_origin["balance"].counters["NavigationQueryRows"], 16);
        assert_eq!(s.by_origin["reassign"].counters["NavigationQueryRows"], 7);
        assert_eq!(s.by_origin["balance"].counters["OwnedRoutingDistances"], 5);
        assert_eq!(
            s.by_origin["unassigned"].counters["OwnedRoutingDistances"],
            11
        );
        let job = s.longest_jobs.iter().find(|j| j.kind == "balance").unwrap();
        assert_eq!(job.node_id, u128::MAX.to_string());
        assert_eq!(job.work.counters["NavigationQueryRows"], 16);
        assert_eq!(job.work.counters["OwnedRoutingDistances"], 5);
        assert_eq!(job.work.remote_requests["Expand"].calls, 1);
        assert_eq!(job.work.remote_requests["Expand"].client_elapsed_nanos, 50);
        assert_eq!(s.remote_requests["Expand"].calls, 2);
        assert_eq!(s.remote_requests["Expand"].client_elapsed_nanos, 150);
        serde_json::to_vec(&s).unwrap();
    }
    #[test]
    fn job_records_are_bounded_and_disabled_context_is_empty() {
        let p = WorkProfile::default();
        let attempt = super::super::protocol::AttemptId {
            authority: 1,
            sequence: 0,
        };
        {
            let _disabled = p.job_enabled(JobKind::Neighbor, 1, 0, attempt, false);
            assert!(current_context().is_none());
        }
        assert!(p.snapshot().longest_jobs.is_empty());
        assert!(p.snapshot().last_finished_jobs.is_empty());
        {
            let _long_early_job = p.job_enabled(JobKind::Neighbor, 1, 999, attempt, true);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        for id in 0..40 {
            let _job = p.job_enabled(JobKind::Neighbor, 1, id, attempt, true);
        }
        let s = p.snapshot();
        assert_eq!(s.longest_jobs.len(), 32);
        assert_eq!(s.last_finished_jobs.len(), 32);
        assert_eq!(s.last_finished_jobs[0].node_id, "39");
        assert_eq!(s.last_finished_jobs[31].node_id, "8");
        assert!(s.longest_jobs.iter().any(|j| j.node_id == "999"));
        assert!(!s.last_finished_jobs.iter().any(|j| j.node_id == "999"));
        assert!(s
            .longest_jobs
            .windows(2)
            .all(|w| w[0].wall_nanos >= w[1].wall_nanos));
        assert!(current_context().is_none());
    }
    #[test]
    fn local_counters_merge_on_error_or_scope_exit() {
        let p = WorkProfile::default();
        {
            let mut op = p.operation(Scope::TransferPrepare);
            op.add(Counter::TransferMembershipProbes, 3);
            op.add(Counter::TransferMembershipProbes, 4);
        }
        let s = p.snapshot();
        assert_eq!(s.counters["TransferMembershipProbes"], 7);
        assert_eq!(s.inclusive_scopes["TransferPrepare"].calls, 1);
        let (mut n, mut bound) = (0, 0);
        assert!(counted_contains(&[2, 4, 6], &4, &mut n, &mut bound));
        assert_eq!((n, bound), (1, 3));
        assert!(
            s.inclusive_scopes["TransferPrepare"].max_wall_nanos
                <= s.inclusive_scopes["TransferPrepare"].wall_nanos
        );
    }
}

/// Additional fine scopes can be disabled to measure their overhead separately.
pub fn detailed() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("SPANN_DETAILED_DIAGNOSTICS").as_deref() == Ok("1"))
}

//! Cumulative work and inclusive operation times. Stack-local counters merge once
//! per operation; these nested times must not be summed as phase latency.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Instant};
macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[repr(usize)] #[derive(Clone, Copy)] pub enum Counter {$($name,)* Count}
        const COUNTER_NAMES:&[&str]=&[$(stringify!($name),)*];
    }
}
counters!(
    BoundaryCentroidAttempts,
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
    SplitDiscardedRows
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
    BoundaryCentroidBroadcast,
    InsertionRoutePhase,
    InsertionAppendPhase,
    InsertionRoute,
    InsertionGroup,
    InsertionAppend,
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
    SharedRoutingScoring,
    SplitPreparation,
    SplitInstallation
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
struct Inner {
    counts: [u64; Counter::Count as usize],
    times: [TimeTotal; Scope::Count as usize],
}
impl Default for Inner {
    fn default() -> Self {
        Self {
            counts: [0; Counter::Count as usize],
            times: [TimeTotal::default(); Scope::Count as usize],
        }
    }
}
#[derive(Default)]
pub struct WorkProfile {
    inner: Mutex<Inner>,
}
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct WorkSnapshot {
    pub counters: BTreeMap<String, u64>,
    pub inclusive_scopes: BTreeMap<String, TimeTotal>,
}
impl WorkProfile {
    pub fn operation(&self, scope: Scope) -> WorkOperation<'_> {
        WorkOperation {
            profile: self,
            scope,
            started: Instant::now(),
            cpu: chroma_index::spann::utils::clustering_thread_cpu_nanos(),
            counts: [0; Counter::Count as usize],
            touched: 0,
        }
    }
    pub fn record_wall(&self, scope: Scope, elapsed: u64) {
        let mut inner = self.inner.lock();
        let time = &mut inner.times[scope as usize];
        time.calls += 1;
        time.wall_nanos = time.wall_nanos.saturating_add(elapsed);
        time.max_wall_nanos = time.max_wall_nanos.max(elapsed);
        let micros = elapsed.saturating_add(999) / 1000;
        time.wall_histogram[(64 - micros.saturating_sub(1).leading_zeros() as usize).min(31)] += 1;
    }
    pub fn snapshot(&self) -> WorkSnapshot {
        let inner = self.inner.lock();
        WorkSnapshot {
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

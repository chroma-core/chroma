use super::*;
use crate::hierarchical_index::distributed::observability::{
    Counter, Scope, WorkOperation, WorkProfile,
};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Instant,
};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionNode {
    pub parent: Option<NodeId>,
    pub placement: u64,
    pub revision: u64,
    pub live: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionRequest {
    pub roots: Vec<NodeId>,
    pub topology_sequence: u64,
    pub placements: Vec<(NodeId, u64)>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationStatus {
    Queued,
    Granted,
    Cancelled,
    Released,
    Replan,
}
struct Record {
    request: Option<RegionRequest>,
    roots: Vec<NodeId>,
    ancestry: BTreeMap<NodeId, (Option<NodeId>, u64)>,
    status: ReservationStatus,
    rejection: Option<String>,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
struct PumpWork {
    calls: u64,
    candidates: u64,
    record_visits: u64,
    granted_overlap_checks: u64,
}
struct Inner {
    nodes: BTreeMap<NodeId, RegionNode>,
    sequence: u64,
    records: BTreeMap<AttemptId, Record>,
    // Retain terminal outcomes for replay without visiting them for conflicts.
    granted: BTreeSet<AttemptId>,
    queue: VecDeque<AttemptId>,
    limit: usize,
    profile: Arc<WorkProfile>,
    ancestry_steps: Cell<u64>,
    #[cfg(test)]
    work: PumpWork,
}
/// Bookkeeping mutex only: methods never do I/O, wait for node locks, or call peers.
pub struct RegionManager {
    inner: Mutex<Inner>,
    profile: Arc<WorkProfile>,
}
struct MeasuredGuard<'a> {
    guard: Option<parking_lot::MutexGuard<'a, Inner>>,
    operation: WorkOperation<'a>,
    held: Instant,
    ancestry_before: u64,
}
impl Deref for MeasuredGuard<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        self.guard.as_ref().unwrap()
    }
}
impl DerefMut for MeasuredGuard<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        self.guard.as_mut().unwrap()
    }
}
impl Drop for MeasuredGuard<'_> {
    fn drop(&mut self) {
        self.operation.add(
            Counter::ReservationMutexHoldNanos,
            self.held.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        self.operation.add(
            Counter::ReservationAncestrySteps,
            self.guard
                .as_ref()
                .unwrap()
                .ancestry_steps
                .get()
                .saturating_sub(self.ancestry_before),
        );
        drop(self.guard.take());
    }
}

impl Inner {
    fn ancestor(&self, a: NodeId, mut b: NodeId) -> Result<bool> {
        let mut seen = BTreeSet::new();
        loop {
            self.ancestry_steps.set(self.ancestry_steps.get() + 1);
            if a == b {
                return Ok(true);
            }
            if !seen.insert(b) {
                return Err(ProtocolError::InvalidTopology);
            }
            let n = self.nodes.get(&b).ok_or(ProtocolError::InvalidTopology)?;
            match n.parent {
                Some(p) => b = p,
                None => return Ok(false),
            }
        }
    }
    fn overlap(&self, a: &[NodeId], b: &[NodeId]) -> Result<bool> {
        for x in a {
            for y in b {
                if self.ancestor(*x, *y)? || self.ancestor(*y, *x)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
    fn normalize(&self, req: &RegionRequest) -> Result<Vec<NodeId>> {
        if req.topology_sequence != self.sequence {
            return Err(ProtocolError::StalePlacement);
        }
        self.normalize_current(req)
    }
    fn normalize_current(&self, req: &RegionRequest) -> Result<Vec<NodeId>> {
        let mut roots = req.roots.clone();
        roots.sort_unstable();
        roots.dedup();
        if roots.is_empty() {
            return Err(ProtocolError::InvalidTopology);
        }
        for r in &roots {
            let n = self
                .nodes
                .get(r)
                .filter(|n| n.live)
                .ok_or(ProtocolError::StalePlacement)?;
            if !req.placements.contains(&(*r, n.placement)) {
                return Err(ProtocolError::StalePlacement);
            }
            self.ancestor(*r, *r)?;
        }
        let mut out = vec![];
        for r in &roots {
            let mut covered = false;
            for other in &roots {
                if other != r && self.ancestor(*other, *r)? {
                    covered = true;
                    break;
                }
            }
            if !covered {
                out.push(*r)
            }
        }
        Ok(out)
    }
    // A queued request depends on these logical paths and placements, not on
    // structural edits elsewhere in the owner's forest. Revisions can advance
    // when a sibling changes while this region's meaning remains unchanged.
    fn ancestry_dependencies(
        &self,
        roots: &[NodeId],
    ) -> Result<BTreeMap<NodeId, (Option<NodeId>, u64)>> {
        let mut dependencies = BTreeMap::new();
        for root in roots {
            let mut cursor = Some(*root);
            let mut seen = BTreeSet::new();
            while let Some(id) = cursor {
                self.ancestry_steps.set(self.ancestry_steps.get() + 1);
                if !seen.insert(id) {
                    return Err(ProtocolError::InvalidTopology);
                }
                let node = self.nodes.get(&id).ok_or(ProtocolError::InvalidTopology)?;
                if !node.live {
                    return Err(ProtocolError::StalePlacement);
                }
                dependencies.insert(id, (node.parent, node.placement));
                cursor = node.parent;
            }
        }
        Ok(dependencies)
    }
    fn pump(&mut self) -> Result<()> {
        let profile = self.profile.clone();
        let mut observation = profile.operation(Scope::ReservationPump);
        observation.add(Counter::ReservationPumpCalls, 1);
        let mut candidates = 0;
        let mut visits = 0;
        #[cfg(test)]
        {
            self.work.calls += 1;
        }
        let mut blocked: Vec<Vec<NodeId>> = vec![];
        let pending: Vec<_> = self.queue.drain(..).collect();
        for id in pending {
            let rec = &self.records[&id];
            if rec.status != ReservationStatus::Queued {
                continue;
            }
            #[cfg(test)]
            {
                self.work.candidates += 1;
            }
            candidates += 1;
            let req = rec.request.as_ref().unwrap();
            let normalized = self.normalize_current(req);
            let ancestry = self.ancestry_dependencies(&req.roots);
            let rejection = if normalized.as_ref().ok() != Some(&rec.roots) {
                Some(format!("queued normalization changed: {normalized:?}"))
            } else if ancestry.as_ref().ok() != Some(&rec.ancestry) {
                let difference = ancestry.as_ref().ok().and_then(|current| {
                    rec.ancestry
                        .iter()
                        .find(|(node, dependency)| current.get(node) != Some(dependency))
                });
                Some(format!("queued ancestry changed: first_difference={difference:?}, current={ancestry:?}"))
            } else {
                None
            };
            if let Some(reason) = rejection {
                let record = self.records.get_mut(&id).unwrap();
                record.status = ReservationStatus::Replan;
                record.rejection = Some(reason);
                continue;
            }
            let roots = rec.roots.clone();
            let mut conflict = false;
            for other in &self.granted {
                visits += 1;
                let r = self.records.get(other).ok_or(ProtocolError::Unknown)?;
                debug_assert_eq!(r.status, ReservationStatus::Granted);
                #[cfg(test)]
                {
                    self.work.record_visits += 1;
                    self.work.granted_overlap_checks += 1;
                }
                if self.overlap(&roots, &r.roots)? {
                    conflict = true;
                    break;
                }
            }
            for earlier in &blocked {
                if self.overlap(&roots, earlier)? {
                    conflict = true;
                    break;
                }
            }
            if conflict {
                blocked.push(roots);
                self.queue.push_back(id)
            } else {
                self.records.get_mut(&id).unwrap().status = ReservationStatus::Granted;
                self.granted.insert(id);
            }
        }
        observation.add(Counter::ReservationCandidates, candidates);
        observation.add(Counter::ReservationConflictVisits, visits);
        Ok(())
    }
}
impl RegionManager {
    pub fn new(nodes: BTreeMap<NodeId, RegionNode>, limit: usize) -> Result<Self> {
        Self::new_profiled(nodes, limit, Arc::new(WorkProfile::default()))
    }
    pub fn new_profiled(
        nodes: BTreeMap<NodeId, RegionNode>,
        limit: usize,
        profile: Arc<WorkProfile>,
    ) -> Result<Self> {
        let inner = Inner {
            nodes,
            sequence: 0,
            records: BTreeMap::new(),
            granted: BTreeSet::new(),
            queue: VecDeque::new(),
            limit,
            profile: profile.clone(),
            ancestry_steps: Cell::new(0),
            #[cfg(test)]
            work: PumpWork::default(),
        };
        for (id, n) in &inner.nodes {
            if n.live {
                let mut cursor = Some(*id);
                let mut seen = BTreeSet::new();
                while let Some(x) = cursor {
                    if !seen.insert(x) {
                        return Err(ProtocolError::InvalidTopology);
                    }
                    cursor = inner
                        .nodes
                        .get(&x)
                        .filter(|n| n.live)
                        .ok_or(ProtocolError::InvalidTopology)?
                        .parent;
                }
            }
        }
        Ok(Self {
            inner: Mutex::new(inner),
            profile,
        })
    }
    fn measured_lock(&self) -> MeasuredGuard<'_> {
        let mut operation = self.profile.operation(Scope::ReservationMutex);
        let wait = Instant::now();
        let guard = self.inner.lock();
        operation.add(
            Counter::ReservationMutexWaitNanos,
            wait.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let ancestry_before = guard.ancestry_steps.get();
        MeasuredGuard {
            guard: Some(guard),
            operation,
            held: Instant::now(),
            ancestry_before,
        }
    }
    pub fn state_snapshot(&self) -> BTreeMap<String, u64> {
        let s = self.measured_lock();
        BTreeMap::from([
            ("retained_records".into(), s.records.len() as u64),
            ("active_grants".into(), s.granted.len() as u64),
            ("queued_requests".into(), s.queue.len() as u64),
            ("topology_sequence".into(), s.sequence),
        ])
    }
    pub fn sequence(&self) -> u64 {
        self.measured_lock().sequence
    }
    pub fn snapshot(&self, node: NodeId) -> Result<(RegionNode, u64)> {
        let s = self.measured_lock();
        let record = s
            .nodes
            .get(&node)
            .filter(|n| n.live)
            .ok_or(ProtocolError::InvalidTopology)?;
        Ok((record.clone(), s.sequence))
    }
    pub fn request(&self, id: AttemptId, req: RegionRequest) -> Result<ReservationStatus> {
        self.request_with(id, req, true)
    }
    /// Posting transfers identify current endpoints rather than publishing an
    /// edit against a captured topology. Bind their current paths atomically;
    /// the caller still validates endpoint content under the granted bundle.
    pub fn request_current(&self, id: AttemptId, req: RegionRequest) -> Result<ReservationStatus> {
        self.request_with(id, req, false)
    }
    fn request_with(
        &self,
        id: AttemptId,
        req: RegionRequest,
        snapshot_fenced: bool,
    ) -> Result<ReservationStatus> {
        let mut request_work = self.profile.operation(Scope::ReservationRequest);
        request_work.add(Counter::ReservationRequests, 1);
        let mut s = self.measured_lock();
        if let Some(r) = s.records.get(&id) {
            if r.request.as_ref().is_some_and(|old| old != &req) {
                return Err(ProtocolError::Conflict);
            }
            return Ok(r.status);
        }
        if s.records.len() >= s.limit {
            return Err(ProtocolError::Capacity);
        }
        let normalized = if snapshot_fenced {
            s.normalize(&req)
        } else {
            s.normalize_current(&req)
        };
        let roots = match normalized {
            Ok(roots) => roots,
            Err(ProtocolError::StalePlacement) => {
                let reason = if snapshot_fenced && req.topology_sequence != s.sequence {
                    format!(
                        "initial topology sequence changed: captured={} current={}",
                        req.topology_sequence, s.sequence
                    )
                } else {
                    "initial requested root is retired, missing or has changed placement".into()
                };
                s.records.insert(
                    id,
                    Record {
                        request: Some(req),
                        roots: vec![],
                        ancestry: BTreeMap::new(),
                        status: ReservationStatus::Replan,
                        rejection: Some(reason),
                    },
                );
                return Ok(ReservationStatus::Replan);
            }
            Err(error) => return Err(error),
        };
        let ancestry = s.ancestry_dependencies(&req.roots)?;
        s.records.insert(
            id,
            Record {
                request: Some(req),
                roots,
                ancestry,
                status: ReservationStatus::Queued,
                rejection: None,
            },
        );
        s.queue.push_back(id);
        s.pump()?;
        Ok(s.records[&id].status)
    }
    pub fn diagnostic(&self, id: AttemptId) -> String {
        let s = self.measured_lock();
        match s.records.get(&id) {
            Some(record) => format!(
                "status={:?}; {}; captured_sequence={:?}; current_sequence={}",
                record.status,
                record
                    .rejection
                    .as_deref()
                    .unwrap_or("no recorded rejection"),
                record.request.as_ref().map(|r| r.topology_sequence),
                s.sequence
            ),
            None => "unknown reservation".into(),
        }
    }
    pub fn status(&self, id: AttemptId) -> Result<ReservationStatus> {
        self.measured_lock()
            .records
            .get(&id)
            .map(|r| r.status)
            .ok_or(ProtocolError::Unknown)
    }
    pub fn cancel(&self, id: AttemptId) -> Result<ReservationStatus> {
        let mut s = self.measured_lock();
        if let Some(r) = s.records.get_mut(&id) {
            match r.status {
                ReservationStatus::Queued => r.status = ReservationStatus::Cancelled,
                ReservationStatus::Granted => return Err(ProtocolError::Prerequisite),
                _ => {}
            }
        } else {
            if s.records.len() >= s.limit {
                return Err(ProtocolError::Capacity);
            }
            s.records.insert(
                id,
                Record {
                    request: None,
                    roots: vec![],
                    ancestry: BTreeMap::new(),
                    status: ReservationStatus::Cancelled,
                    rejection: None,
                },
            );
        }
        s.pump()?;
        Ok(s.records[&id].status)
    }
    pub fn release(&self, id: AttemptId) -> Result<()> {
        let mut s = self.measured_lock();
        let r = s.records.get_mut(&id).ok_or(ProtocolError::Unknown)?;
        if r.status == ReservationStatus::Released {
            return Ok(());
        }
        if r.status != ReservationStatus::Granted {
            return Err(ProtocolError::Prerequisite);
        }
        r.status = ReservationStatus::Released;
        s.granted.remove(&id);
        s.pump()
    }
    /// Caller holds node protection and the local routing publication gate. All
    /// affected old/new parent regions must be covered by this granted bundle.
    pub fn update(&self, id: AttemptId, node: NodeId, replacement: RegionNode) -> Result<u64> {
        self.publish_updates(id, vec![(node, replacement)])
    }
    /// Install a complete structural edit atomically with reservation bookkeeping.
    /// Retired controls keep their ancestry until protection is released. Newly
    /// created records must attach below an existing protected region.
    pub fn publish_updates(
        &self,
        id: AttemptId,
        changes: Vec<(NodeId, RegionNode)>,
    ) -> Result<u64> {
        self.publish_root_replacements(id, changes, vec![])
    }
    /// Replace owned forest roots under the coordinator publication turn. The
    /// explicit relation transfers this operation's exclusion to replacement roots.
    pub fn publish_root_replacements(
        &self,
        id: AttemptId,
        changes: Vec<(NodeId, RegionNode)>,
        replacements: Vec<(NodeId, Vec<NodeId>)>,
    ) -> Result<u64> {
        let mut s = self.measured_lock();
        let r = s.records.get(&id).ok_or(ProtocolError::Unknown)?;
        if r.status != ReservationStatus::Granted || changes.is_empty() {
            return Err(ProtocolError::Prerequisite);
        }
        let mut roots = r.roots.clone();
        let mut proposed = s.nodes.clone();
        let mut seen = BTreeSet::new();
        for (node, replacement) in &changes {
            if !seen.insert(*node) {
                return Err(ProtocolError::Conflict);
            }
            if let Some(old) = s.nodes.get(node) {
                if replacement.revision
                    != old
                        .revision
                        .checked_add(1)
                        .ok_or(ProtocolError::Exhausted)?
                {
                    return Err(ProtocolError::Conflict);
                }
            }
            proposed.insert(*node, replacement.clone());
        }
        for (old, new_roots) in &replacements {
            if !roots.contains(old)
                || s.nodes
                    .get(old)
                    .is_none_or(|n| n.parent.is_some() || !n.live)
                || proposed.get(old).is_none_or(|n| n.live)
            {
                return Err(ProtocolError::Prerequisite);
            }
            for new_root in new_roots {
                if (s.nodes.contains_key(new_root) && !s.ancestor(*old, *new_root)?)
                    || proposed
                        .get(new_root)
                        .is_none_or(|n| !n.live || n.parent.is_some())
                {
                    return Err(ProtocolError::Conflict);
                }
                if !roots.contains(new_root) {
                    roots.push(*new_root);
                }
            }
        }
        for (old, new_roots) in &replacements {
            for (id, previous) in &s.nodes {
                if !previous.live
                    || !s.ancestor(*old, *id)?
                    || proposed.get(id).is_none_or(|n| !n.live)
                {
                    continue;
                }
                let mut cursor = Some(*id);
                let mut visited = BTreeSet::new();
                let mut remains_covered = false;
                while let Some(node) = cursor {
                    if new_roots.contains(&node) {
                        remains_covered = true;
                        break;
                    }
                    if !visited.insert(node) {
                        return Err(ProtocolError::InvalidTopology);
                    }
                    cursor = proposed
                        .get(&node)
                        .ok_or(ProtocolError::InvalidTopology)?
                        .parent;
                }
                if !remains_covered {
                    return Err(ProtocolError::Prerequisite);
                }
            }
        }
        let covered = |nodes: &BTreeMap<NodeId, RegionNode>, mut node: NodeId| -> Result<bool> {
            let mut visited = BTreeSet::new();
            loop {
                if roots.contains(&node) {
                    return Ok(true);
                }
                if !visited.insert(node) {
                    return Err(ProtocolError::InvalidTopology);
                }
                match nodes
                    .get(&node)
                    .ok_or(ProtocolError::InvalidTopology)?
                    .parent
                {
                    Some(p) => node = p,
                    None => return Ok(false),
                }
            }
        };
        for (node, replacement) in &changes {
            let old = s.nodes.get(node);
            let parent_changes = old
                .is_none_or(|old| old.parent != replacement.parent || old.live != replacement.live);
            if let Some(old) = old {
                if !covered(&s.nodes, *node)? {
                    return Err(ProtocolError::Prerequisite);
                }
                if parent_changes {
                    if let Some(parent) = old.parent {
                        if !covered(&s.nodes, parent)? {
                            return Err(ProtocolError::Prerequisite);
                        }
                    }
                }
            }
            if !covered(&proposed, *node)? {
                return Err(ProtocolError::Prerequisite);
            }
            if parent_changes {
                if let Some(parent) = replacement.parent {
                    if !covered(&proposed, parent)? {
                        return Err(ProtocolError::Prerequisite);
                    }
                }
            }
        }
        for (node, record) in &proposed {
            if !record.live {
                continue;
            }
            let mut cursor = Some(*node);
            let mut visited = BTreeSet::new();
            while let Some(current) = cursor {
                if !visited.insert(current) {
                    return Err(ProtocolError::InvalidTopology);
                }
                cursor = proposed
                    .get(&current)
                    .filter(|n| n.live)
                    .ok_or(ProtocolError::InvalidTopology)?
                    .parent;
            }
        }
        let next = s.sequence.checked_add(1).ok_or(ProtocolError::Exhausted)?;
        s.nodes = proposed;
        // Old controls remain for stale-handle identity, but a granted region
        // follows live replacement roots. Retired ancestry is not an admission
        // dependency and can reference removed structural ancestors.
        roots.retain(|root| s.nodes.get(root).is_some_and(|node| node.live));
        s.records.get_mut(&id).unwrap().roots = roots;
        s.sequence = next;
        s.pump()?;
        Ok(next)
    }
    /// Only call after the enclosing epoch is sealed and handlers are drained.
    pub fn reclaim_sealed(&self) -> Result<()> {
        let mut s = self.measured_lock();
        if !s.granted.is_empty()
            || s.records.values().any(|r| {
                matches!(
                    r.status,
                    ReservationStatus::Queued | ReservationStatus::Granted
                )
            })
        {
            return Err(ProtocolError::Unresolved);
        }
        s.records.clear();
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u64) -> AttemptId {
        AttemptId {
            authority: 1,
            sequence: n,
        }
    }
    #[test]
    fn profile_counts_actual_conflict_work_and_preserves_history_gauges() {
        let m = manager();
        m.request(id(1), req(vec![2])).unwrap();
        m.request(id(2), req(vec![4])).unwrap();
        m.release(id(1)).unwrap();
        m.release(id(2)).unwrap();
        let profile = m.profile.snapshot();
        let actual = m.inner.lock().work;
        assert_eq!(profile.counters["ReservationPumpCalls"], actual.calls);
        assert_eq!(profile.counters["ReservationCandidates"], actual.candidates);
        assert_eq!(
            profile.counters["ReservationConflictVisits"],
            actual.record_visits
        );
        assert!(profile.counters["ReservationAncestrySteps"] > 0);
        assert_eq!(m.state_snapshot()["retained_records"], 2);
        assert_eq!(m.state_snapshot()["active_grants"], 0);
        assert!(profile.counters["ReservationMutexHoldNanos"] > 0);
    }
    fn manager() -> RegionManager {
        RegionManager::new(
            [(1, None), (2, Some(1)), (3, Some(1)), (4, Some(2))]
                .into_iter()
                .map(|(id, parent)| {
                    (
                        id,
                        RegionNode {
                            parent,
                            placement: 0,
                            revision: 0,
                            live: true,
                        },
                    )
                })
                .collect(),
            20,
        )
        .unwrap()
    }
    fn req(roots: Vec<NodeId>) -> RegionRequest {
        RegionRequest {
            placements: roots.iter().map(|r| (*r, 0)).collect(),
            roots,
            topology_sequence: 0,
        }
    }
    #[test]
    fn fifo_disjoint_and_cancel() {
        let m = manager();
        assert_eq!(
            m.request(id(1), req(vec![4])).unwrap(),
            ReservationStatus::Granted
        );
        assert_eq!(
            m.request(id(2), req(vec![2])).unwrap(),
            ReservationStatus::Queued
        );
        assert_eq!(
            m.request(id(3), req(vec![4])).unwrap(),
            ReservationStatus::Queued
        );
        assert_eq!(
            m.request(id(4), req(vec![3])).unwrap(),
            ReservationStatus::Granted
        );
        m.release(id(1)).unwrap();
        assert_eq!(m.status(id(2)).unwrap(), ReservationStatus::Granted);
        assert_eq!(m.status(id(3)).unwrap(), ReservationStatus::Queued);
        assert_eq!(m.cancel(id(2)), Err(ProtocolError::Prerequisite));
        m.cancel(id(3)).unwrap();
        assert_eq!(
            m.request(id(3), req(vec![4])).unwrap(),
            ReservationStatus::Cancelled
        );
        m.cancel(id(9)).unwrap();
        assert_eq!(
            m.request(id(9), req(vec![1])).unwrap(),
            ReservationStatus::Cancelled
        );
    }
    #[test]
    fn all_or_none_and_revision() {
        let m = manager();
        m.request(id(1), req(vec![2])).unwrap();
        assert_eq!(
            m.request(id(2), req(vec![3, 4])).unwrap(),
            ReservationStatus::Queued
        );
        assert_eq!(
            m.request(id(3), req(vec![3])).unwrap(),
            ReservationStatus::Queued
        );
        m.release(id(1)).unwrap();
        assert_eq!(m.status(id(2)).unwrap(), ReservationStatus::Granted);
        assert_eq!(m.request(id(2), req(vec![1])), Err(ProtocolError::Conflict));
    }
    #[test]
    fn normalize_and_topology_fence() {
        let m = manager();
        m.request(id(1), req(vec![1, 2, 4])).unwrap();
        let changed = RegionNode {
            parent: Some(3),
            placement: 0,
            revision: 1,
            live: true,
        };
        assert_eq!(m.update(id(1), 4, changed), Ok(1));
        assert_eq!(
            m.request(id(2), req(vec![3])),
            Ok(ReservationStatus::Replan)
        );
    }
    #[test]
    fn atomic_split_retirement_and_cycle_rejection() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        let mut retired = m.snapshot(4).unwrap().0;
        retired.live = false;
        retired.revision = 1;
        let new = RegionNode {
            parent: Some(2),
            placement: 0,
            revision: 0,
            live: true,
        };
        m.publish_updates(id(1), vec![(4, retired), (5, new)])
            .unwrap();
        assert!(m.snapshot(4).is_err());
        assert_eq!(m.snapshot(5).unwrap().0.parent, Some(2));
        let mut cycle = m.snapshot(2).unwrap().0;
        cycle.parent = Some(5);
        cycle.revision = 1;
        assert_eq!(
            m.update(id(1), 2, cycle),
            Err(ProtocolError::InvalidTopology)
        );
        assert_eq!(m.snapshot(2).unwrap().0.parent, Some(1));
        m.release(id(1)).unwrap();
    }
    #[test]
    fn grant_cancel_race_has_one_authoritative_outcome() {
        for _ in 0..40 {
            let m = std::sync::Arc::new(manager());
            let other = m.clone();
            let worker = std::thread::spawn(move || other.request(id(1), req(vec![2])));
            let cancellation = m.cancel(id(1));
            let granted = worker.join().unwrap().unwrap();
            match granted {
                ReservationStatus::Cancelled => {
                    assert_eq!(cancellation, Ok(ReservationStatus::Cancelled))
                }
                ReservationStatus::Granted => {
                    assert_eq!(cancellation, Err(ProtocolError::Prerequisite));
                    m.release(id(1)).unwrap();
                }
                _ => panic!("unexpected uncontended status"),
            }
        }
    }
    #[test]
    fn replace_owned_root_preserves_exclusion() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        let n = |parent, revision, live| RegionNode {
            parent,
            placement: 0,
            revision,
            live,
        };
        m.publish_root_replacements(
            id(1),
            vec![
                (1, n(None, 1, false)),
                (2, n(Some(5), 1, true)),
                (3, n(Some(6), 1, true)),
                (5, n(None, 0, true)),
                (6, n(None, 0, true)),
            ],
            vec![(1, vec![5, 6])],
        )
        .unwrap();
        let request = RegionRequest {
            roots: vec![5],
            topology_sequence: 1,
            placements: vec![(5, 0)],
        };
        assert_eq!(
            m.request(id(2), request).unwrap(),
            ReservationStatus::Queued
        );
        m.release(id(1)).unwrap();
        assert_eq!(m.status(id(2)).unwrap(), ReservationStatus::Granted);
    }
    #[test]
    fn collapse_to_existing_descendant_and_retire_empty_owned_root() {
        let m = RegionManager::new(
            [
                (
                    1,
                    RegionNode {
                        parent: None,
                        placement: 0,
                        revision: 0,
                        live: true,
                    },
                ),
                (
                    2,
                    RegionNode {
                        parent: Some(1),
                        placement: 0,
                        revision: 0,
                        live: true,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            20,
        )
        .unwrap();
        m.request(id(1), req(vec![1])).unwrap();
        m.publish_root_replacements(
            id(1),
            vec![
                (
                    1,
                    RegionNode {
                        parent: None,
                        placement: 0,
                        revision: 1,
                        live: false,
                    },
                ),
                (
                    2,
                    RegionNode {
                        parent: None,
                        placement: 0,
                        revision: 1,
                        live: true,
                    },
                ),
            ],
            vec![(1, vec![2])],
        )
        .unwrap();
        m.release(id(1)).unwrap();
        let request = RegionRequest {
            roots: vec![2],
            topology_sequence: 1,
            placements: vec![(2, 0)],
        };
        m.request(id(2), request).unwrap();
        m.publish_root_replacements(
            id(2),
            vec![(
                2,
                RegionNode {
                    parent: None,
                    placement: 0,
                    revision: 2,
                    live: false,
                },
            )],
            vec![(2, vec![])],
        )
        .unwrap();
        m.release(id(2)).unwrap();
    }
    #[test]
    fn empty_root_replacement_cannot_orphan_live_descendants() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        assert_eq!(
            m.publish_root_replacements(
                id(1),
                vec![(
                    1,
                    RegionNode {
                        parent: None,
                        placement: 0,
                        revision: 1,
                        live: false
                    }
                )],
                vec![(1, vec![])]
            ),
            Err(ProtocolError::Prerequisite)
        );
        assert!(m.snapshot(1).unwrap().0.live);
    }
    #[test]
    fn editing_region_root_without_reparenting_does_not_lock_its_parent() {
        let m = manager();
        m.request(id(1), req(vec![2])).unwrap();
        let mut edited = m.snapshot(2).unwrap().0;
        edited.revision = 1;
        m.update(id(1), 2, edited).unwrap();
        let mut retired = m.snapshot(2).unwrap().0;
        retired.revision = 2;
        retired.live = false;
        assert_eq!(
            m.update(id(1), 2, retired),
            Err(ProtocolError::Prerequisite)
        );
    }
    #[test]
    fn held_replacement_grant_excludes_every_live_new_descendant() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        let n = |parent, revision, live| RegionNode {
            parent,
            placement: 0,
            revision,
            live,
        };
        m.publish_root_replacements(
            id(1),
            vec![
                (1, n(None, 1, false)),
                (2, n(Some(5), 1, true)),
                (3, n(Some(6), 1, true)),
                (5, n(None, 0, true)),
                (6, n(None, 0, true)),
                (7, n(Some(6), 0, true)),
            ],
            vec![(1, vec![5, 6])],
        )
        .unwrap();
        // The authoritative held bundle follows the complete live frontier.
        assert_eq!(m.inner.lock().records[&id(1)].roots, vec![5, 6]);
        for (attempt, node) in [(2, 4), (3, 7)] {
            let request = RegionRequest {
                roots: vec![node],
                topology_sequence: 1,
                placements: vec![(node, 0)],
            };
            assert_eq!(
                m.request(id(attempt), request).unwrap(),
                ReservationStatus::Queued
            );
        }
        m.release(id(1)).unwrap();
        assert_eq!(m.status(id(2)).unwrap(), ReservationStatus::Granted);
        assert_eq!(m.status(id(3)).unwrap(), ReservationStatus::Granted);
    }
    #[test]
    fn retired_or_unknown_requested_roots_return_recorded_replan() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        m.publish_updates(
            id(1),
            vec![(
                4,
                RegionNode {
                    parent: Some(2),
                    placement: 0,
                    revision: 1,
                    live: false,
                },
            )],
        )
        .unwrap();
        m.release(id(1)).unwrap();
        for (attempt, node) in [(2, 4), (3, 999)] {
            let request = RegionRequest {
                roots: vec![node],
                topology_sequence: 1,
                placements: vec![(node, 0)],
            };
            assert_eq!(
                m.request(id(attempt), request.clone()),
                Ok(ReservationStatus::Replan)
            );
            assert_eq!(
                m.request(id(attempt), request),
                Ok(ReservationStatus::Replan)
            );
        }
    }
    #[test]
    fn queued_bundle_keeps_disjoint_edits_but_replans_changed_requested_ancestry() {
        for change in 0..5 {
            let m = manager();
            assert_eq!(
                m.request(id(1), req(vec![1])).unwrap(),
                ReservationStatus::Granted
            );
            // Node4 is normalized away, but its requested path remains a dependency.
            assert_eq!(
                m.request(id(2), req(vec![2, 3, 4])).unwrap(),
                ReservationStatus::Queued
            );
            let (node, mut image) = if change == 0 {
                (3, m.snapshot(3).unwrap().0)
            } else if change == 4 {
                (2, m.snapshot(2).unwrap().0)
            } else {
                (4, m.snapshot(4).unwrap().0)
            };
            image.revision += 1;
            match change {
                0 => {} // A sibling's structural bookkeeping alone changes no path.
                1 => image.parent = Some(3),
                2 => image.live = false,
                3 => image.placement += 1,
                4 => image.parent = Some(3),
                _ => unreachable!(),
            }
            m.publish_updates(id(1), vec![(node, image)]).unwrap();
            let expected = if change == 0 {
                ReservationStatus::Queued
            } else {
                ReservationStatus::Replan
            };
            assert_eq!(m.status(id(2)).unwrap(), expected, "change {change}");
            m.release(id(1)).unwrap();
            assert_eq!(
                m.status(id(2)).unwrap(),
                if change == 0 {
                    ReservationStatus::Granted
                } else {
                    ReservationStatus::Replan
                }
            );
        }
    }
    #[test]
    fn current_endpoint_admission_preserves_placement_liveness_and_duplicate_fences() {
        for change in 0..3 {
            let m = manager();
            m.request(id(1), req(vec![1])).unwrap();
            let node = if change == 0 { 3 } else { 4 };
            let mut image = m.snapshot(node).unwrap().0;
            image.revision += 1;
            if change == 1 {
                image.placement += 1;
            } else if change == 2 {
                image.live = false;
            }
            m.publish_updates(id(1), vec![(node, image)]).unwrap();
            m.release(id(1)).unwrap();
            let captured = req(vec![4]);
            assert_eq!(
                m.request(id(2), captured.clone()).unwrap(),
                ReservationStatus::Replan,
                "snapshot callers retain the initial sequence fence"
            );
            let expected = if change == 0 {
                ReservationStatus::Granted
            } else {
                ReservationStatus::Replan
            };
            assert_eq!(
                m.request_current(id(3), captured.clone()).unwrap(),
                expected
            );
            assert_eq!(m.request_current(id(3), captured).unwrap(), expected);
            assert_eq!(
                m.request_current(id(3), req(vec![3])),
                Err(ProtocolError::Conflict)
            );
            if expected == ReservationStatus::Granted {
                m.release(id(3)).unwrap();
            }
        }
    }
    #[test]
    fn released_history_does_not_increase_uncontended_conflict_discovery_work() {
        use std::time::Instant;
        #[derive(Debug, Serialize)]
        struct Observation {
            released_history: usize,
            topology_nodes: usize,
            topology_sequence: u64,
            active_before: usize,
            queued_before: usize,
            retained_records_before: usize,
            probe_work: PumpWork,
            request_nanos: u128,
        }
        fn observe(history: usize) -> Observation {
            let m = RegionManager::new(
                BTreeMap::from([(
                    1,
                    RegionNode {
                        parent: None,
                        placement: 0,
                        revision: 0,
                        live: true,
                    },
                )]),
                128,
            )
            .unwrap();
            let request = RegionRequest {
                roots: vec![1],
                topology_sequence: 0,
                placements: vec![(1, 0)],
            };
            for sequence in 1..=history as u64 {
                assert_eq!(
                    m.request_current(id(sequence), request.clone()).unwrap(),
                    ReservationStatus::Granted
                );
                m.release(id(sequence)).unwrap();
            }
            let (
                before,
                topology_nodes,
                topology_sequence,
                active_before,
                queued_before,
                retained_records_before,
            ) = {
                let s = m.inner.lock();
                assert_eq!(
                    s.nodes,
                    BTreeMap::from([(
                        1,
                        RegionNode {
                            parent: None,
                            placement: 0,
                            revision: 0,
                            live: true
                        }
                    )])
                );
                assert!(s
                    .records
                    .values()
                    .all(|r| r.status == ReservationStatus::Released));
                (
                    s.work,
                    s.nodes.len(),
                    s.sequence,
                    s.records
                        .values()
                        .filter(|r| r.status == ReservationStatus::Granted)
                        .count(),
                    s.queue.len(),
                    s.records.len(),
                )
            };
            let started = Instant::now();
            assert_eq!(
                m.request_current(id(100), request.clone()).unwrap(),
                ReservationStatus::Granted
            );
            let request_nanos = started.elapsed().as_nanos();
            let after = m.inner.lock().work;
            let probe_work = PumpWork {
                calls: after.calls - before.calls,
                candidates: after.candidates - before.candidates,
                record_visits: after.record_visits - before.record_visits,
                granted_overlap_checks: after.granted_overlap_checks
                    - before.granted_overlap_checks,
            };
            m.release(id(100)).unwrap();
            if history > 0 {
                assert_eq!(
                    m.request_current(id(1), request).unwrap(),
                    ReservationStatus::Released,
                    "completed duplicate outcomes remain retained"
                );
                m.release(id(1)).unwrap();
            }
            Observation {
                released_history: history,
                topology_nodes,
                topology_sequence,
                active_before,
                queued_before,
                retained_records_before,
                probe_work,
                request_nanos,
            }
        }
        let empty = observe(0);
        let completed = observe(64);
        println!(
            "RESERVATION_HISTORY_DIAGNOSTIC {}",
            serde_json::to_string(&[&empty, &completed]).unwrap()
        );
        assert_eq!(empty.probe_work.calls, 1);
        assert_eq!(completed.probe_work.calls, 1);
        assert_eq!(empty.probe_work.candidates, 1);
        assert_eq!(completed.probe_work.candidates, 1);
        assert_eq!(empty.probe_work.granted_overlap_checks, 0);
        assert_eq!(completed.probe_work.granted_overlap_checks, 0);
        assert_eq!(completed.probe_work.record_visits,empty.probe_work.record_visits,"Released history must not add conflict-discovery work when topology and active reservations are identical");
    }
    fn assert_granted_index(m: &RegionManager) {
        let s = m.inner.lock();
        let expected = s
            .records
            .iter()
            .filter_map(|(id, r)| (r.status == ReservationStatus::Granted).then_some(*id))
            .collect::<BTreeSet<_>>();
        assert_eq!(s.granted, expected);
    }
    #[test]
    fn active_conflict_index_tracks_fifo_cancellation_replay_and_reclamation() {
        let m = manager();
        assert_eq!(
            m.request(id(1), req(vec![2])).unwrap(),
            ReservationStatus::Granted
        );
        assert_granted_index(&m);
        assert_eq!(
            m.request(id(2), req(vec![4, 3])).unwrap(),
            ReservationStatus::Queued
        );
        assert_granted_index(&m);
        assert_eq!(
            m.request(id(3), req(vec![3])).unwrap(),
            ReservationStatus::Queued
        );
        assert_granted_index(&m);
        assert_eq!(m.cancel(id(1)), Err(ProtocolError::Prerequisite));
        assert_granted_index(&m);
        assert_eq!(m.cancel(id(2)).unwrap(), ReservationStatus::Cancelled);
        assert_granted_index(&m);
        assert_eq!(m.status(id(3)).unwrap(), ReservationStatus::Granted);
        assert_eq!(m.cancel(id(99)).unwrap(), ReservationStatus::Cancelled);
        assert_granted_index(&m);
        m.release(id(1)).unwrap();
        assert_granted_index(&m);
        assert_eq!(
            m.request(id(1), req(vec![2])).unwrap(),
            ReservationStatus::Released
        );
        assert_granted_index(&m);
        assert_eq!(m.request(id(1), req(vec![3])), Err(ProtocolError::Conflict));
        assert_granted_index(&m);
        m.release(id(1)).unwrap();
        assert_granted_index(&m);
        assert_eq!(m.reclaim_sealed(), Err(ProtocolError::Unresolved));
        assert_granted_index(&m);
        m.release(id(3)).unwrap();
        assert_granted_index(&m);
        m.reclaim_sealed().unwrap();
        assert_granted_index(&m);
        assert!(m.inner.lock().records.is_empty());
    }
    #[test]
    fn active_conflict_index_preserves_replans_and_replacement_root_exclusion() {
        let m = manager();
        m.request(id(1), req(vec![1])).unwrap();
        assert_granted_index(&m);
        m.request(id(2), req(vec![4])).unwrap();
        assert_granted_index(&m);
        let mut changed = m.snapshot(4).unwrap().0;
        changed.placement = 1;
        changed.revision += 1;
        m.publish_updates(id(1), vec![(4, changed)]).unwrap();
        assert_granted_index(&m);
        assert_eq!(m.status(id(2)).unwrap(), ReservationStatus::Replan);
        let n = |parent, revision, live| RegionNode {
            parent,
            placement: 0,
            revision,
            live,
        };
        m.publish_root_replacements(
            id(1),
            vec![
                (1, n(None, 1, false)),
                (2, n(Some(5), 1, true)),
                (3, n(Some(5), 1, true)),
                (5, n(None, 0, true)),
            ],
            vec![(1, vec![5])],
        )
        .unwrap();
        assert_granted_index(&m);
        let request = RegionRequest {
            roots: vec![4],
            topology_sequence: 2,
            placements: vec![(4, 1)],
        };
        assert_eq!(
            m.request(id(3), request.clone()).unwrap(),
            ReservationStatus::Queued
        );
        assert_granted_index(&m);
        m.release(id(1)).unwrap();
        assert_granted_index(&m);
        assert_eq!(m.status(id(3)).unwrap(), ReservationStatus::Granted);
        m.release(id(3)).unwrap();
        assert_granted_index(&m);
        assert_eq!(
            m.request(id(3), request).unwrap(),
            ReservationStatus::Released
        );
        assert_granted_index(&m);
    }
}

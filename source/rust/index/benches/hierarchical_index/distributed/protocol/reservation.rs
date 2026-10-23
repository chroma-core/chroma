use super::*;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
}
struct Inner {
    nodes: BTreeMap<NodeId, RegionNode>,
    sequence: u64,
    records: BTreeMap<AttemptId, Record>,
    queue: VecDeque<AttemptId>,
    limit: usize,
}
/// Bookkeeping mutex only: methods never do I/O, wait for node locks, or call peers.
pub struct RegionManager {
    inner: Mutex<Inner>,
}
impl Inner {
    fn ancestor(&self, a: NodeId, mut b: NodeId) -> Result<bool> {
        let mut seen = BTreeSet::new();
        loop {
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
        let mut blocked: Vec<Vec<NodeId>> = vec![];
        let pending: Vec<_> = self.queue.drain(..).collect();
        for id in pending {
            let rec = &self.records[&id];
            if rec.status != ReservationStatus::Queued {
                continue;
            }
            let req = rec.request.as_ref().unwrap();
            if self.normalize_current(req).ok().as_ref() != Some(&rec.roots)
                || self.ancestry_dependencies(&req.roots).ok().as_ref() != Some(&rec.ancestry)
            {
                self.records.get_mut(&id).unwrap().status = ReservationStatus::Replan;
                continue;
            }
            let roots = rec.roots.clone();
            let mut conflict = false;
            for (other, r) in &self.records {
                if *other != id
                    && r.status == ReservationStatus::Granted
                    && self.overlap(&roots, &r.roots)?
                {
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
                self.records.get_mut(&id).unwrap().status = ReservationStatus::Granted
            }
        }
        Ok(())
    }
}
impl RegionManager {
    pub fn new(nodes: BTreeMap<NodeId, RegionNode>, limit: usize) -> Result<Self> {
        let inner = Inner {
            nodes,
            sequence: 0,
            records: BTreeMap::new(),
            queue: VecDeque::new(),
            limit,
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
        })
    }
    pub fn sequence(&self) -> u64 {
        self.inner.lock().sequence
    }
    pub fn snapshot(&self, node: NodeId) -> Result<(RegionNode, u64)> {
        let s = self.inner.lock();
        let record = s
            .nodes
            .get(&node)
            .filter(|n| n.live)
            .ok_or(ProtocolError::InvalidTopology)?;
        Ok((record.clone(), s.sequence))
    }
    pub fn request(&self, id: AttemptId, req: RegionRequest) -> Result<ReservationStatus> {
        let mut s = self.inner.lock();
        if let Some(r) = s.records.get(&id) {
            if r.request.as_ref().is_some_and(|old| old != &req) {
                return Err(ProtocolError::Conflict);
            }
            return Ok(r.status);
        }
        if s.records.len() >= s.limit {
            return Err(ProtocolError::Capacity);
        }
        let roots = match s.normalize(&req) {
            Ok(roots) => roots,
            Err(ProtocolError::StalePlacement) => {
                s.records.insert(
                    id,
                    Record {
                        request: Some(req),
                        roots: vec![],
                        ancestry: BTreeMap::new(),
                        status: ReservationStatus::Replan,
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
            },
        );
        s.queue.push_back(id);
        s.pump()?;
        Ok(s.records[&id].status)
    }
    pub fn status(&self, id: AttemptId) -> Result<ReservationStatus> {
        self.inner
            .lock()
            .records
            .get(&id)
            .map(|r| r.status)
            .ok_or(ProtocolError::Unknown)
    }
    pub fn cancel(&self, id: AttemptId) -> Result<ReservationStatus> {
        let mut s = self.inner.lock();
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
                },
            );
        }
        s.pump()?;
        Ok(s.records[&id].status)
    }
    pub fn release(&self, id: AttemptId) -> Result<()> {
        let mut s = self.inner.lock();
        let r = s.records.get_mut(&id).ok_or(ProtocolError::Unknown)?;
        if r.status == ReservationStatus::Released {
            return Ok(());
        }
        if r.status != ReservationStatus::Granted {
            return Err(ProtocolError::Prerequisite);
        }
        r.status = ReservationStatus::Released;
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
        let mut s = self.inner.lock();
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
        let mut s = self.inner.lock();
        if s.records.values().any(|r| {
            matches!(
                r.status,
                ReservationStatus::Queued | ReservationStatus::Granted
            )
        }) {
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
}

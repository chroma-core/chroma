use super::*;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedView<V> {
    pub sequence: u64,
    pub attempt: Option<AttemptId>,
    pub value: V,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicationStatus {
    NotPublished,
    Aborted,
    Published { sequence: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationProposal<V> {
    pub expected_sequence: u64,
    pub required_owners: BTreeSet<AuthorityId>,
    pub value: V,
}
struct Record<V> {
    proposal: Option<PublicationProposal<V>>,
    ready: BTreeSet<AuthorityId>,
    outcome: PublicationStatus,
}
struct Inner<V> {
    view: Arc<PublishedView<V>>,
    records: BTreeMap<AttemptId, Record<V>>,
    turn: Option<AttemptId>,
    limit: usize,
}
/// A view pointer and its terminal operation outcome share one critical section.
/// Values must be prepared immutable routing/directory roots; no callbacks, I/O,
/// node-lock acquisition, or network calls happen while this mutex is held.
pub struct Publication<V> {
    inner: Mutex<Inner<V>>,
}
impl<V: Clone + PartialEq> Publication<V> {
    pub fn new(value: V, limit: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                view: Arc::new(PublishedView {
                    sequence: 0,
                    attempt: None,
                    value,
                }),
                records: BTreeMap::new(),
                turn: None,
                limit,
            }),
        }
    }
    /// Restore only into a fresh runtime; old requests must use the sealed epoch.
    pub fn from_checkpoint(view: PublishedView<V>, limit: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                view: Arc::new(view),
                records: BTreeMap::new(),
                turn: None,
                limit,
            }),
        }
    }
    pub fn view(&self) -> Arc<PublishedView<V>> {
        self.inner.lock().view.clone()
    }
    pub fn prepare(
        &self,
        id: AttemptId,
        proposal: PublicationProposal<V>,
    ) -> Result<PublicationStatus> {
        let mut s = self.inner.lock();
        if let Some(r) = s.records.get(&id) {
            if r.outcome == PublicationStatus::NotPublished && r.proposal.is_none() {
                if s.turn != Some(id) {
                    return Err(ProtocolError::Prerequisite);
                }
                if proposal.expected_sequence != s.view.sequence {
                    return Err(ProtocolError::StalePlacement);
                }
                s.records.get_mut(&id).unwrap().proposal = Some(proposal);
                return Ok(PublicationStatus::NotPublished);
            }
            if r.proposal.as_ref().is_some_and(|p| p != &proposal) {
                return Err(ProtocolError::Conflict);
            }
            return Ok(r.outcome);
        }
        if s.records.len() >= s.limit {
            return Err(ProtocolError::Capacity);
        }
        if proposal.expected_sequence != s.view.sequence {
            return Err(ProtocolError::StalePlacement);
        }
        s.records.insert(
            id,
            Record {
                proposal: Some(proposal),
                ready: BTreeSet::new(),
                outcome: PublicationStatus::NotPublished,
            },
        );
        Ok(PublicationStatus::NotPublished)
    }
    /// Reserve publication permission before constructing a proposal. A busy
    /// response creates no record. The caller must abort and release on failure,
    /// and must hold no owner reservation or node lock while waiting here.
    pub fn acquire_unprepared_turn(&self, id: AttemptId) -> Result<bool> {
        let mut s = self.inner.lock();
        if let Some(record) = s.records.get(&id) {
            if record.outcome != PublicationStatus::NotPublished || record.proposal.is_some() {
                return Err(ProtocolError::Prerequisite);
            }
            return if s.turn == Some(id) {
                Ok(true)
            } else {
                Err(ProtocolError::Prerequisite)
            };
        }
        if s.turn.is_some() {
            return Ok(false);
        }
        if s.records.len() >= s.limit {
            return Err(ProtocolError::Capacity);
        }
        s.records.insert(
            id,
            Record {
                proposal: None,
                ready: BTreeSet::new(),
                outcome: PublicationStatus::NotPublished,
            },
        );
        s.turn = Some(id);
        Ok(true)
    }
    /// Acquire before any owner reservations or node edit locks. A busy response is
    /// retried by the caller, never under data locks. This permission is not FIFO.
    pub fn acquire_turn(&self, id: AttemptId) -> Result<bool> {
        let mut s = self.inner.lock();
        let r = s.records.get(&id).ok_or(ProtocolError::Unknown)?;
        if r.outcome != PublicationStatus::NotPublished {
            return Err(ProtocolError::Prerequisite);
        }
        match s.turn {
            Some(old) => Ok(old == id),
            None => {
                s.turn = Some(id);
                Ok(true)
            }
        }
    }
    pub fn ready(&self, id: AttemptId, owner: AuthorityId) -> Result<()> {
        let mut s = self.inner.lock();
        let r = s.records.get(&id).ok_or(ProtocolError::Unknown)?;
        if r.ready.contains(&owner) {
            return Ok(());
        }
        if s.turn != Some(id) {
            return Err(ProtocolError::Prerequisite);
        }
        let r = s.records.get_mut(&id).unwrap();
        if r.outcome != PublicationStatus::NotPublished {
            return Err(ProtocolError::Prerequisite);
        }
        if !r
            .proposal
            .as_ref()
            .ok_or(ProtocolError::Prerequisite)?
            .required_owners
            .contains(&owner)
        {
            return Err(ProtocolError::Conflict);
        }
        r.ready.insert(owner);
        Ok(())
    }
    pub fn publish(&self, id: AttemptId) -> Result<PublicationStatus> {
        let mut s = self.inner.lock();
        let r = s.records.get(&id).ok_or(ProtocolError::Unknown)?;
        if r.outcome != PublicationStatus::NotPublished {
            return Ok(r.outcome);
        }
        if s.turn != Some(id) {
            return Err(ProtocolError::Prerequisite);
        }
        let proposal = r.proposal.as_ref().ok_or(ProtocolError::Prerequisite)?;
        if r.ready != proposal.required_owners {
            return Err(ProtocolError::Prerequisite);
        }
        if proposal.expected_sequence != s.view.sequence {
            return Err(ProtocolError::StalePlacement);
        }
        let sequence = s
            .view
            .sequence
            .checked_add(1)
            .ok_or(ProtocolError::Exhausted)?;
        let value = proposal.value.clone();
        s.view = Arc::new(PublishedView {
            sequence,
            attempt: Some(id),
            value,
        });
        let outcome = PublicationStatus::Published { sequence };
        s.records.get_mut(&id).unwrap().outcome = outcome;
        Ok(outcome)
    }
    pub fn abort(&self, id: AttemptId) -> Result<PublicationStatus> {
        let mut s = self.inner.lock();
        if let Some(r) = s.records.get_mut(&id) {
            if r.outcome == PublicationStatus::NotPublished {
                r.outcome = PublicationStatus::Aborted
            }
            return Ok(r.outcome);
        }
        if s.records.len() >= s.limit {
            return Err(ProtocolError::Capacity);
        }
        s.records.insert(
            id,
            Record {
                proposal: None,
                ready: BTreeSet::new(),
                outcome: PublicationStatus::Aborted,
            },
        );
        Ok(PublicationStatus::Aborted)
    }
    pub fn status(&self, id: AttemptId) -> Result<PublicationStatus> {
        self.inner
            .lock()
            .records
            .get(&id)
            .map(|r| r.outcome)
            .ok_or(ProtocolError::Unknown)
    }
    /// Caller has obtained every owner completion/retirement acknowledgement and
    /// released node/region protection. Publication turn is released last.
    pub fn release_turn(&self, id: AttemptId) -> Result<()> {
        let mut s = self.inner.lock();
        let outcome = s.records.get(&id).ok_or(ProtocolError::Unknown)?.outcome;
        if outcome == PublicationStatus::NotPublished {
            return Err(ProtocolError::Unresolved);
        }
        // A lost release acknowledgement may be replayed after another attempt
        // has acquired the turn. It must not release that later attempt's turn.
        if s.turn == Some(id) {
            s.turn = None;
        }
        Ok(())
    }
    pub fn reclaim_sealed(&self) -> Result<()> {
        let mut s = self.inner.lock();
        if s.turn.is_some()
            || s.records
                .values()
                .any(|r| r.outcome == PublicationStatus::NotPublished)
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
    fn proposal() -> PublicationProposal<(u64, u64)> {
        PublicationProposal {
            expected_sequence: 0,
            required_owners: [1, 2].into_iter().collect(),
            value: (11, 22),
        }
    }
    #[test]
    fn unprepared_turn_preserves_admission_and_sequence_checks() {
        let p = Publication::new((0, 0), 2);
        assert!(p.acquire_unprepared_turn(id(1)).unwrap());
        assert!(p.acquire_unprepared_turn(id(1)).unwrap());
        assert!(!p.acquire_unprepared_turn(id(2)).unwrap());
        assert_eq!(p.status(id(2)), Err(ProtocolError::Unknown));
        assert_eq!(p.publish(id(1)), Err(ProtocolError::Prerequisite));
        assert_eq!(p.ready(id(1), 1), Err(ProtocolError::Prerequisite));
        assert_eq!(p.release_turn(id(1)), Err(ProtocolError::Unresolved));
        let mut stale = proposal();
        stale.expected_sequence = 1;
        assert_eq!(p.prepare(id(1), stale), Err(ProtocolError::StalePlacement));
        p.prepare(id(1), proposal()).unwrap();
        p.ready(id(1), 1).unwrap();
        p.ready(id(1), 2).unwrap();
        assert_eq!(
            p.publish(id(1)),
            Ok(PublicationStatus::Published { sequence: 1 })
        );
        assert!(!p.acquire_unprepared_turn(id(2)).unwrap());
        p.release_turn(id(1)).unwrap();
        assert!(p.acquire_unprepared_turn(id(2)).unwrap());
        p.abort(id(2)).unwrap();
        p.release_turn(id(2)).unwrap();
        assert_eq!(
            p.acquire_unprepared_turn(id(3)),
            Err(ProtocolError::Capacity)
        );
        assert_eq!(
            p.acquire_unprepared_turn(id(2)),
            Err(ProtocolError::Prerequisite)
        );
    }
    #[test]
    fn publish_abort_status_atomic() {
        let p = Publication::new((0, 0), 10);
        p.prepare(id(1), proposal()).unwrap();
        p.acquire_turn(id(1)).unwrap();
        p.ready(id(1), 1).unwrap();
        assert_eq!(p.publish(id(1)), Err(ProtocolError::Prerequisite));
        p.ready(id(1), 2).unwrap();
        let old = p.view();
        let published = PublicationStatus::Published { sequence: 1 };
        assert_eq!(p.publish(id(1)), Ok(published));
        assert_eq!(p.abort(id(1)), Ok(published));
        assert_eq!(p.status(id(1)), Ok(published));
        assert_eq!(p.view().value, (11, 22));
        assert_eq!(old.value, (0, 0));
        assert_eq!(p.publish(id(1)), Ok(published));
        p.release_turn(id(1)).unwrap();
        p.release_turn(id(1)).unwrap();
        p.ready(id(1), 1).unwrap();
    }
    #[test]
    fn cancellation_before_delayed_prepare() {
        let p = Publication::new((0, 0), 10);
        p.abort(id(2)).unwrap();
        assert_eq!(p.prepare(id(2), proposal()), Ok(PublicationStatus::Aborted));
        assert_eq!(p.publish(id(2)), Ok(PublicationStatus::Aborted));
        assert_eq!(p.view().sequence, 0);
    }
    #[test]
    fn race_publish_abort() {
        for _ in 0..40 {
            let p = Arc::new(Publication::new((0, 0), 10));
            p.prepare(id(1), proposal()).unwrap();
            p.acquire_turn(id(1)).unwrap();
            p.ready(id(1), 1).unwrap();
            p.ready(id(1), 2).unwrap();
            let q = p.clone();
            let worker = std::thread::spawn(move || q.publish(id(1)).unwrap());
            let aborted = p.abort(id(1)).unwrap();
            let published = worker.join().unwrap();
            assert_eq!(aborted, published);
            assert_eq!(p.status(id(1)).unwrap(), published);
            match published {
                PublicationStatus::Published { sequence } => {
                    let v = p.view();
                    assert_eq!(v.sequence, sequence);
                    assert_eq!(v.attempt, Some(id(1)));
                }
                PublicationStatus::Aborted => assert_eq!(p.view().sequence, 0),
                _ => panic!(),
            }
        }
    }
}

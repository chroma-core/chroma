use super::publication::PublicationStatus;
use super::*;
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffPayload {
    pub root: NodeId,
    pub source: AuthorityId,
    pub destination: AuthorityId,
    pub old_placement: u64,
    pub new_placement: u64,
    pub vectors: u64,
    pub bytes: u64,
    pub manifest_digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HandoffState {
    Reserved,
    DestinationReady,
    SourceFenced,
    Published { sequence: u64 },
    DestinationActive,
    SourceCleaned,
    Complete,
    Aborted,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandoffRecord {
    pub payload: HandoffPayload,
    state: HandoffState,
    published_sequence: Option<u64>,
}
impl HandoffRecord {
    pub fn new(payload: HandoffPayload) -> Result<Self> {
        if payload.source == payload.destination
            || payload.vectors == 0
            || payload.old_placement.checked_add(1) != Some(payload.new_placement)
        {
            return Err(ProtocolError::Conflict);
        }
        Ok(Self {
            payload,
            state: HandoffState::Reserved,
            published_sequence: None,
        })
    }
    pub fn state(&self) -> HandoffState {
        self.state
    }
    pub fn ready(&mut self, digest: [u8; 32], vectors: u64, bytes: u64) -> Result<()> {
        if digest != self.payload.manifest_digest
            || vectors != self.payload.vectors
            || bytes != self.payload.bytes
        {
            return Err(ProtocolError::Conflict);
        }
        if self.state == HandoffState::Aborted {
            return Err(ProtocolError::Prerequisite);
        }
        if self.state == HandoffState::Reserved {
            self.state = HandoffState::DestinationReady
        }
        Ok(())
    }
    /// The source closes subtree admission and drains accepted requests before ack.
    pub fn fenced(&mut self, accepted_requests_drained: bool) -> Result<()> {
        if !accepted_requests_drained {
            return Err(ProtocolError::Unresolved);
        }
        match self.state {
            HandoffState::Reserved | HandoffState::Aborted => Err(ProtocolError::Prerequisite),
            HandoffState::DestinationReady => {
                self.state = HandoffState::SourceFenced;
                Ok(())
            }
            _ => Ok(()),
        }
    }
    pub fn published(&mut self, status: PublicationStatus) -> Result<()> {
        let sequence = match status {
            PublicationStatus::Published { sequence } => sequence,
            _ => return Err(ProtocolError::Unresolved),
        };
        if let Some(old) = self.published_sequence {
            return if old == sequence {
                Ok(())
            } else {
                Err(ProtocolError::Conflict)
            };
        }
        if self.state != HandoffState::SourceFenced {
            return Err(ProtocolError::Prerequisite);
        }
        self.published_sequence = Some(sequence);
        self.state = HandoffState::Published { sequence };
        Ok(())
    }
    pub fn active(&mut self, owner: AuthorityId, placement: u64, sequence: u64) -> Result<()> {
        if owner != self.payload.destination
            || placement != self.payload.new_placement
            || self.published_sequence != Some(sequence)
        {
            return Err(ProtocolError::StalePlacement);
        }
        match self.state {
            HandoffState::Published { .. } => {
                self.state = HandoffState::DestinationActive;
                Ok(())
            }
            HandoffState::DestinationActive
            | HandoffState::SourceCleaned
            | HandoffState::Complete => Ok(()),
            _ => Err(ProtocolError::Prerequisite),
        }
    }
    pub fn cleaned(&mut self, source_handles_drained: bool) -> Result<()> {
        if !source_handles_drained {
            return Err(ProtocolError::Unresolved);
        }
        match self.state {
            HandoffState::DestinationActive => {
                self.state = HandoffState::SourceCleaned;
                Ok(())
            }
            HandoffState::SourceCleaned | HandoffState::Complete => Ok(()),
            _ => Err(ProtocolError::Prerequisite),
        }
    }
    pub fn complete(&mut self) -> Result<()> {
        match self.state {
            HandoffState::SourceCleaned | HandoffState::Complete => {
                self.state = HandoffState::Complete;
                Ok(())
            }
            _ => Err(ProtocolError::Prerequisite),
        }
    }
    /// Only a coordinator's recorded Abort proves an unpublished handoff. Unknown
    /// and NotPublished status are insufficient because Publish may still race.
    pub fn abort(&mut self, status: PublicationStatus) -> Result<()> {
        if status != PublicationStatus::Aborted {
            return Err(ProtocolError::Unresolved);
        }
        if self.published_sequence.is_some() {
            return Err(ProtocolError::Conflict);
        }
        self.state = HandoffState::Aborted;
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capacity {
    pub vectors: u64,
    pub bytes: u64,
}
impl Capacity {
    fn checked_add(self, b: Self) -> Result<Self> {
        Ok(Self {
            vectors: self
                .vectors
                .checked_add(b.vectors)
                .ok_or(ProtocolError::Exhausted)?,
            bytes: self
                .bytes
                .checked_add(b.bytes)
                .ok_or(ProtocolError::Exhausted)?,
        })
    }
    fn checked_sub(self, b: Self) -> Result<Self> {
        Ok(Self {
            vectors: self
                .vectors
                .checked_sub(b.vectors)
                .ok_or(ProtocolError::Conflict)?,
            bytes: self
                .bytes
                .checked_sub(b.bytes)
                .ok_or(ProtocolError::Conflict)?,
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocationState {
    Pending,
    Assigned,
    Departed,
    Aborted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutgoingState {
    Reserved,
    Cleaned,
    Aborted,
}
struct Incoming {
    root: NodeId,
    amount: Capacity,
    state: AllocationState,
}
/// Handler-owned accounting; protect it with the owner control mutex. Pending
/// receives consume real capacity before Ready, and duplicate steps do not charge twice.
pub struct CapacityLedger {
    limit: Capacity,
    assigned: Capacity,
    pending: Capacity,
    incoming: BTreeMap<AttemptId, Incoming>,
    outgoing: BTreeMap<AttemptId, (NodeId, Capacity, OutgoingState)>,
}
impl CapacityLedger {
    pub fn new(limit: Capacity, assigned: Capacity) -> Result<Self> {
        if assigned.vectors > limit.vectors || assigned.bytes > limit.bytes {
            return Err(ProtocolError::Capacity);
        }
        Ok(Self {
            limit,
            assigned,
            pending: Capacity {
                vectors: 0,
                bytes: 0,
            },
            incoming: BTreeMap::new(),
            outgoing: BTreeMap::new(),
        })
    }
    pub fn assigned(&self) -> Capacity {
        self.assigned
    }
    pub fn pending(&self) -> Capacity {
        self.pending
    }
    pub fn reserve_incoming(
        &mut self,
        id: AttemptId,
        root: NodeId,
        amount: Capacity,
    ) -> Result<()> {
        if let Some(old) = self.incoming.get(&id) {
            if old.root != root || old.amount != amount {
                return Err(ProtocolError::Conflict);
            }
            return if old.state == AllocationState::Aborted {
                Err(ProtocolError::Prerequisite)
            } else {
                Ok(())
            };
        }
        let pending = self.pending.checked_add(amount)?;
        let total = self.assigned.checked_add(pending)?;
        if total.vectors > self.limit.vectors || total.bytes > self.limit.bytes {
            return Err(ProtocolError::Capacity);
        }
        if self.incoming.values().any(|r| {
            r.root == root
                && matches!(
                    r.state,
                    AllocationState::Pending | AllocationState::Assigned
                )
        }) {
            return Err(ProtocolError::Conflict);
        }
        self.pending = pending;
        self.incoming.insert(
            id,
            Incoming {
                root,
                amount,
                state: AllocationState::Pending,
            },
        );
        Ok(())
    }
    pub fn activate_incoming(&mut self, id: AttemptId) -> Result<()> {
        let old = self.incoming.get(&id).ok_or(ProtocolError::Unknown)?;
        if matches!(
            old.state,
            AllocationState::Assigned | AllocationState::Departed
        ) {
            return Ok(());
        }
        if old.state != AllocationState::Pending {
            return Err(ProtocolError::Prerequisite);
        }
        let assigned = self.assigned.checked_add(old.amount)?;
        let pending = self.pending.checked_sub(old.amount)?;
        self.assigned = assigned;
        self.pending = pending;
        self.incoming.get_mut(&id).unwrap().state = AllocationState::Assigned;
        Ok(())
    }
    pub fn abort_incoming(&mut self, id: AttemptId) -> Result<()> {
        let old = self.incoming.get(&id).ok_or(ProtocolError::Unknown)?;
        match old.state {
            AllocationState::Aborted => return Ok(()),
            AllocationState::Assigned | AllocationState::Departed => {
                return Err(ProtocolError::Prerequisite)
            }
            AllocationState::Pending => {}
        }
        self.pending = self.pending.checked_sub(old.amount)?;
        self.incoming.get_mut(&id).unwrap().state = AllocationState::Aborted;
        Ok(())
    }
    pub fn reserve_outgoing(
        &mut self,
        id: AttemptId,
        root: NodeId,
        amount: Capacity,
    ) -> Result<()> {
        if let Some(old) = self.outgoing.get(&id) {
            return if old.0 == root && old.1 == amount {
                if old.2 == OutgoingState::Aborted {
                    Err(ProtocolError::Prerequisite)
                } else {
                    Ok(())
                }
            } else {
                Err(ProtocolError::Conflict)
            };
        }
        if self
            .outgoing
            .values()
            .any(|r| r.0 == root && r.2 == OutgoingState::Reserved)
        {
            return Err(ProtocolError::Conflict);
        }
        let reserved = self
            .outgoing
            .values()
            .filter(|r| r.2 == OutgoingState::Reserved)
            .try_fold(amount, |total, r| total.checked_add(r.1))?;
        self.assigned.checked_sub(reserved)?;
        self.outgoing
            .insert(id, (root, amount, OutgoingState::Reserved));
        Ok(())
    }
    /// Invoke only after the handoff's recorded prepublication Abort. Retain a
    /// tombstone so a delayed reserve cannot resurrect the aborted attempt.
    pub fn abort_outgoing(&mut self, id: AttemptId) -> Result<()> {
        let old = self.outgoing.get_mut(&id).ok_or(ProtocolError::Unknown)?;
        match old.2 {
            OutgoingState::Cleaned => Err(ProtocolError::Prerequisite),
            _ => {
                old.2 = OutgoingState::Aborted;
                Ok(())
            }
        }
    }
    /// Keep outgoing bytes charged until source cleanup acknowledgement. This
    /// conservatively includes fenced retained state in memory capacity.
    pub fn cleanup_outgoing(&mut self, id: AttemptId) -> Result<()> {
        let old = self.outgoing.get(&id).ok_or(ProtocolError::Unknown)?;
        if old.2 == OutgoingState::Cleaned {
            return Ok(());
        }
        if old.2 == OutgoingState::Aborted {
            return Err(ProtocolError::Prerequisite);
        }
        let assigned = self.assigned.checked_sub(old.1)?;
        self.assigned = assigned;
        let root = old.0;
        self.outgoing.get_mut(&id).unwrap().2 = OutgoingState::Cleaned;
        for incoming in self.incoming.values_mut() {
            if incoming.root == root && incoming.state == AllocationState::Assigned {
                incoming.state = AllocationState::Departed;
            }
        }
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
    fn payload() -> HandoffPayload {
        HandoffPayload {
            root: 8,
            source: 1,
            destination: 2,
            old_placement: 9,
            new_placement: 10,
            vectors: 30,
            bytes: 300,
            manifest_digest: [2; 32],
        }
    }
    #[test]
    fn ordered_handoff() {
        let mut h = HandoffRecord::new(payload()).unwrap();
        assert_eq!(h.fenced(true), Err(ProtocolError::Prerequisite));
        h.ready([2; 32], 30, 300).unwrap();
        assert_eq!(h.fenced(false), Err(ProtocolError::Unresolved));
        h.fenced(true).unwrap();
        assert_eq!(
            h.published(PublicationStatus::NotPublished),
            Err(ProtocolError::Unresolved)
        );
        h.published(PublicationStatus::Published { sequence: 4 })
            .unwrap();
        assert_eq!(
            h.abort(PublicationStatus::Aborted),
            Err(ProtocolError::Conflict)
        );
        assert_eq!(h.active(1, 9, 4), Err(ProtocolError::StalePlacement));
        h.active(2, 10, 4).unwrap();
        assert_eq!(h.complete(), Err(ProtocolError::Prerequisite));
        h.cleaned(true).unwrap();
        h.complete().unwrap();
        h.ready([2; 32], 30, 300).unwrap();
        assert_eq!(h.state(), HandoffState::Complete);
    }
    #[test]
    fn capacity_exactly_once() {
        let amount = Capacity {
            vectors: 30,
            bytes: 300,
        };
        let mut l = CapacityLedger::new(
            Capacity {
                vectors: 70,
                bytes: 700,
            },
            Capacity {
                vectors: 20,
                bytes: 200,
            },
        )
        .unwrap();
        l.reserve_incoming(id(1), 8, amount).unwrap();
        l.reserve_incoming(id(1), 8, amount).unwrap();
        assert_eq!(l.pending(), amount);
        assert_eq!(
            l.reserve_incoming(id(2), 9, amount),
            Err(ProtocolError::Capacity)
        );
        l.activate_incoming(id(1)).unwrap();
        l.activate_incoming(id(1)).unwrap();
        assert_eq!(l.assigned().vectors, 50);
        l.reserve_outgoing(id(3), 8, amount).unwrap();
        assert_eq!(
            l.reserve_outgoing(id(4), 8, amount),
            Err(ProtocolError::Conflict)
        );
        l.cleanup_outgoing(id(3)).unwrap();
        l.cleanup_outgoing(id(3)).unwrap();
        assert_eq!(l.assigned().vectors, 20);
    }
    #[test]
    fn abort_outgoing_and_roundtrip_capacity() {
        let c = Capacity {
            vectors: 30,
            bytes: 300,
        };
        let mut l = CapacityLedger::new(
            Capacity {
                vectors: 100,
                bytes: 1000,
            },
            Capacity {
                vectors: 0,
                bytes: 0,
            },
        )
        .unwrap();
        l.reserve_incoming(id(1), 8, c).unwrap();
        l.activate_incoming(id(1)).unwrap();
        l.reserve_outgoing(id(2), 8, c).unwrap();
        l.abort_outgoing(id(2)).unwrap();
        assert_eq!(
            l.reserve_outgoing(id(2), 8, c),
            Err(ProtocolError::Prerequisite)
        );
        l.reserve_outgoing(id(3), 8, c).unwrap();
        l.cleanup_outgoing(id(3)).unwrap();
        l.activate_incoming(id(1)).unwrap();
        assert_eq!(l.assigned().vectors, 0);
        l.reserve_incoming(id(4), 8, c).unwrap();
        l.activate_incoming(id(4)).unwrap();
        assert_eq!(l.assigned().vectors, 30);
    }
}

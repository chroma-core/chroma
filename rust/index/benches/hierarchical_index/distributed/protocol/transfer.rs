use super::*;
use std::collections::BTreeSet;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Source,
    Destination,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    Prepared,
    Staged,
    Completing,
    Applied,
    Completed,
    Released,
    Aborted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TransferStep {
    Stage,
    BeginCompletion,
    RemoveSource,
    ActivateDestination,
    Complete,
    Release,
    Abort,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferPayload {
    pub source_owner: AuthorityId,
    pub destination_owner: AuthorityId,
    pub source_leaf: NodeId,
    pub destination_leaf: NodeId,
    pub source_placement: u64,
    /// Revisions describe captured routing state. Posting-only changes can
    /// advance them without invalidating centroid-dependent transfer codes.
    pub source_revision: u64,
    pub destination_revision: u64,
    pub source_centroid_digest: [u8; 32],
    pub destination_centroid_digest: [u8; 32],
    pub destination_placement: u64,
    pub vectors: Vec<u64>,
    pub content_digest: [u8; 32],
}
/// Endpoint transition record. The enclosing phase binds attempt and payload;
/// callers hold both leaf protections until the driver authorizes release.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferRecord {
    pub role: Role,
    pub payload: TransferPayload,
    state: TransferState,
    steps: BTreeSet<TransferStep>,
}
impl TransferRecord {
    pub fn new(role: Role, payload: TransferPayload) -> Result<Self> {
        if payload.vectors.is_empty()
            || payload.vectors.iter().collect::<BTreeSet<_>>().len() != payload.vectors.len()
        {
            return Err(ProtocolError::Conflict);
        }
        Ok(Self {
            role,
            payload,
            state: TransferState::Prepared,
            steps: BTreeSet::new(),
        })
    }
    pub fn state(&self) -> TransferState {
        self.state
    }
    pub fn apply<F>(
        &mut self,
        step: TransferStep,
        proof: &TransferProof,
        edit: F,
    ) -> Result<TransferState>
    where
        F: FnOnce() -> Result<()>,
    {
        apply_endpoint_transition(
            self.role,
            &mut self.state,
            &mut self.steps,
            step,
            proof,
            edit,
        )
    }
}

/// The same two-role transition rules serve a single leaf or an owner bundle.
#[derive(Clone, Debug)]
pub struct EndpointTransitions {
    role: Role,
    state: TransferState,
    steps: BTreeSet<TransferStep>,
}
impl EndpointTransitions {
    pub fn new(role: Role) -> Self {
        Self {
            role,
            state: TransferState::Prepared,
            steps: BTreeSet::new(),
        }
    }
    pub fn state(&self) -> TransferState {
        self.state
    }
    pub fn apply<F>(
        &mut self,
        step: TransferStep,
        proof: &TransferProof,
        edit: F,
    ) -> Result<TransferState>
    where
        F: FnOnce() -> Result<()>,
    {
        apply_endpoint_transition(
            self.role,
            &mut self.state,
            &mut self.steps,
            step,
            proof,
            edit,
        )
    }
}
fn apply_endpoint_transition<F>(
    role: Role,
    state: &mut TransferState,
    steps: &mut BTreeSet<TransferStep>,
    step: TransferStep,
    proof: &TransferProof,
    edit: F,
) -> Result<TransferState>
where
    F: FnOnce() -> Result<()>,
{
    if steps.contains(&step) {
        return Ok(*state);
    }
    let next = match step {
        TransferStep::Stage if role == Role::Destination && *state == TransferState::Prepared => {
            TransferState::Staged
        }
        TransferStep::BeginCompletion
            if (role == Role::Source && *state == TransferState::Prepared)
                || (role == Role::Destination && *state == TransferState::Staged) =>
        {
            TransferState::Completing
        }
        TransferStep::RemoveSource
            if role == Role::Source && *state == TransferState::Completing && proof.both_begin =>
        {
            TransferState::Applied
        }
        TransferStep::ActivateDestination
            if role == Role::Destination
                && *state == TransferState::Completing
                && proof.both_begin
                && proof.source_applied =>
        {
            TransferState::Applied
        }
        TransferStep::Complete if *state == TransferState::Applied && proof.both_applied => {
            TransferState::Completed
        }
        TransferStep::Release if *state == TransferState::Completed && proof.both_complete => {
            TransferState::Released
        }
        TransferStep::Abort
            if matches!(*state, TransferState::Prepared | TransferState::Staged)
                && proof.abort_decided =>
        {
            TransferState::Aborted
        }
        _ => return Err(ProtocolError::Prerequisite),
    };
    edit()?;
    *state = next;
    steps.insert(step);
    Ok(next)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferProof {
    pub both_begin: bool,
    pub source_applied: bool,
    pub both_applied: bool,
    pub both_complete: bool,
    pub abort_decided: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriverDecision {
    Undecided,
    Complete,
    Abort,
}
/// One serialized driver owns this record. Proofs carry acknowledgements already
/// observed for this attempt, not assumptions inferred from a timeout.
pub struct TransferDriver {
    decision: DriverDecision,
    staged: bool,
    begin: [bool; 2],
    applied: [bool; 2],
    complete: [bool; 2],
    released: [bool; 2],
}
impl Default for TransferDriver {
    fn default() -> Self {
        Self {
            decision: DriverDecision::Undecided,
            staged: false,
            begin: [false; 2],
            applied: [false; 2],
            complete: [false; 2],
            released: [false; 2],
        }
    }
}
impl TransferDriver {
    pub fn decide(&mut self, d: DriverDecision) -> Result<()> {
        if d == DriverDecision::Undecided {
            return Err(ProtocolError::Prerequisite);
        }
        if self.decision == d {
            return Ok(());
        }
        if self.decision != DriverDecision::Undecided {
            return Err(ProtocolError::Conflict);
        }
        if d == DriverDecision::Complete && !self.staged {
            return Err(ProtocolError::Prerequisite);
        }
        self.decision = d;
        Ok(())
    }
    pub fn acknowledge(&mut self, role: Role, step: TransferStep) -> Result<()> {
        let i = if role == Role::Source { 0 } else { 1 };
        match step {
            TransferStep::Stage if role == Role::Destination => self.staged = true,
            TransferStep::BeginCompletion
                if self.decision == DriverDecision::Complete
                    && (role == Role::Source || self.staged) =>
            {
                self.begin[i] = true
            }
            TransferStep::RemoveSource if role == Role::Source && self.begin.iter().all(|b| *b) => {
                self.applied[0] = true
            }
            TransferStep::ActivateDestination
                if role == Role::Destination
                    && self.begin.iter().all(|b| *b)
                    && self.applied[0] =>
            {
                self.applied[1] = true
            }
            TransferStep::Complete if self.applied.iter().all(|b| *b) => self.complete[i] = true,
            TransferStep::Release if self.complete.iter().all(|b| *b) => self.released[i] = true,
            _ => return Err(ProtocolError::Prerequisite),
        }
        Ok(())
    }
    pub fn proof(&self) -> TransferProof {
        TransferProof {
            both_begin: self.begin.iter().all(|b| *b),
            source_applied: self.applied[0],
            both_applied: self.applied.iter().all(|b| *b),
            both_complete: self.complete.iter().all(|b| *b),
            abort_decided: self.decision == DriverDecision::Abort,
        }
    }
    pub fn success(&self) -> bool {
        self.decision == DriverDecision::Complete && self.released.iter().all(|b| *b)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn payload() -> TransferPayload {
        TransferPayload {
            source_owner: 1,
            destination_owner: 2,
            source_leaf: 10,
            destination_leaf: 20,
            source_placement: 0,
            source_revision: 1,
            destination_revision: 1,
            source_centroid_digest: [3; 32],
            destination_centroid_digest: [4; 32],
            destination_placement: 0,
            vectors: vec![1, 2],
            content_digest: [7; 32],
        }
    }
    #[test]
    fn transfer_order_duplicates_and_decision() {
        let mut d = TransferDriver::default();
        let mut src = TransferRecord::new(Role::Source, payload()).unwrap();
        let mut dst = TransferRecord::new(Role::Destination, payload()).unwrap();
        assert_eq!(
            src.apply(TransferStep::RemoveSource, &d.proof(), || Ok(())),
            Err(ProtocolError::Prerequisite)
        );
        dst.apply(TransferStep::Stage, &d.proof(), || Ok(()))
            .unwrap();
        d.acknowledge(Role::Destination, TransferStep::Stage)
            .unwrap();
        d.decide(DriverDecision::Complete).unwrap();
        assert_eq!(
            d.decide(DriverDecision::Abort),
            Err(ProtocolError::Conflict)
        );
        for (role, record) in [(Role::Source, &mut src), (Role::Destination, &mut dst)] {
            record
                .apply(TransferStep::BeginCompletion, &d.proof(), || Ok(()))
                .unwrap();
            d.acknowledge(role, TransferStep::BeginCompletion).unwrap();
        }
        assert_eq!(
            dst.apply(TransferStep::ActivateDestination, &d.proof(), || Ok(())),
            Err(ProtocolError::Prerequisite)
        );
        src.apply(TransferStep::RemoveSource, &d.proof(), || Ok(()))
            .unwrap();
        d.acknowledge(Role::Source, TransferStep::RemoveSource)
            .unwrap();
        src.apply(TransferStep::RemoveSource, &d.proof(), || {
            panic!("duplicate edit")
        })
        .unwrap();
        dst.apply(TransferStep::ActivateDestination, &d.proof(), || Ok(()))
            .unwrap();
        d.acknowledge(Role::Destination, TransferStep::ActivateDestination)
            .unwrap();
        for (role, record) in [(Role::Source, &mut src), (Role::Destination, &mut dst)] {
            record
                .apply(TransferStep::Complete, &d.proof(), || Ok(()))
                .unwrap();
            d.acknowledge(role, TransferStep::Complete).unwrap();
        }
        assert!(!d.success());
        for (role, record) in [(Role::Destination, &mut dst), (Role::Source, &mut src)] {
            record
                .apply(TransferStep::Release, &d.proof(), || Ok(()))
                .unwrap();
            d.acknowledge(role, TransferStep::Release).unwrap();
        }
        assert!(d.success());
    }
    #[test]
    fn failed_edit_preserves_state_and_abort_terminal() {
        let mut r = TransferRecord::new(Role::Destination, payload()).unwrap();
        assert_eq!(
            r.apply(TransferStep::Stage, &TransferProof::default(), || Err(
                ProtocolError::Capacity
            )),
            Err(ProtocolError::Capacity)
        );
        assert_eq!(r.state(), TransferState::Prepared);
        let mut d = TransferDriver::default();
        d.decide(DriverDecision::Abort).unwrap();
        r.apply(TransferStep::Abort, &d.proof(), || Ok(())).unwrap();
        assert_eq!(
            r.apply(TransferStep::Stage, &d.proof(), || Ok(())),
            Err(ProtocolError::Prerequisite)
        );
    }
}

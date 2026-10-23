use super::*;
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub run: u128,
    pub epoch: u64,
    pub attempt: AttemptId,
    pub kind: String,
    pub placements: Vec<(NodeId, u64)>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhaseState {
    Open,
    Closing,
    Sealed,
}
/// Serial access is required by the handler. Terminal records count against the
/// bound until sealing; admission failure never prevents existing steps draining.
pub struct Phase<P> {
    run: u128,
    epoch: u64,
    state: PhaseState,
    limit: usize,
    records: BTreeMap<AttemptId, (Envelope, P, bool)>,
}
impl<P: PartialEq> Phase<P> {
    pub fn new(run: u128, epoch: u64, limit: usize) -> Self {
        Self {
            run,
            epoch,
            state: PhaseState::Open,
            limit,
            records: BTreeMap::new(),
        }
    }
    pub fn check(&self, e: &Envelope) -> Result<()> {
        if e.run != self.run {
            return Err(ProtocolError::StaleRun);
        }
        if e.epoch != self.epoch {
            return Err(ProtocolError::WrongEpoch);
        }
        if self.state == PhaseState::Sealed {
            return Err(ProtocolError::Sealed);
        }
        Ok(())
    }
    pub fn admit(&mut self, e: Envelope, payload: P) -> Result<bool> {
        self.check(&e)?;
        if let Some((old, p, _)) = self.records.get(&e.attempt) {
            return if old == &e && p == &payload {
                Ok(false)
            } else {
                Err(ProtocolError::Conflict)
            };
        }
        if self.state != PhaseState::Open {
            return Err(ProtocolError::Closing);
        }
        if self.records.len() >= self.limit {
            return Err(ProtocolError::Capacity);
        }
        self.records.insert(e.attempt, (e, payload, false));
        Ok(true)
    }
    pub fn lookup(&self, e: &Envelope) -> Result<&P> {
        self.check(e)?;
        let (old, p, _) = self.records.get(&e.attempt).ok_or(ProtocolError::Unknown)?;
        if old != e {
            return Err(ProtocolError::Conflict);
        }
        Ok(p)
    }
    pub fn terminal(&mut self, e: &Envelope) -> Result<()> {
        self.check(e)?;
        let (old, _, done) = self
            .records
            .get_mut(&e.attempt)
            .ok_or(ProtocolError::Unknown)?;
        if old != e {
            return Err(ProtocolError::Conflict);
        }
        *done = true;
        Ok(())
    }
    pub fn close(&mut self) {
        if self.state == PhaseState::Open {
            self.state = PhaseState::Closing
        }
    }
    /// The caller must additionally drain drivers/handlers and release acknowledgements.
    pub fn seal(&mut self, handlers_drained: bool) -> Result<()> {
        if self.state != PhaseState::Closing
            || !handlers_drained
            || self.records.values().any(|r| !r.2)
        {
            return Err(ProtocolError::Unresolved);
        }
        self.state = PhaseState::Sealed;
        self.records.clear();
        Ok(())
    }
    pub fn state(&self) -> PhaseState {
        self.state
    }
}
#[derive(Debug)]
pub struct CheckedIds {
    authority: AuthorityId,
    sequence: u64,
    node: u128,
}
impl CheckedIds {
    pub fn new(authority: AuthorityId) -> Self {
        Self {
            authority,
            sequence: 0,
            node: 0,
        }
    }
    pub fn attempt(&mut self) -> Result<AttemptId> {
        advance(&mut self.sequence)?;
        Ok(AttemptId {
            authority: self.authority,
            sequence: self.sequence,
        })
    }
    pub fn node(&mut self) -> Result<NodeId> {
        let next = self
            .node
            .checked_add(1)
            .filter(|n| *n <= u64::MAX as u128)
            .ok_or(ProtocolError::Exhausted)?;
        self.node = next;
        Ok(((self.authority as u128) << 64) | next)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn env() -> Envelope {
        Envelope {
            run: 1,
            epoch: 2,
            attempt: AttemptId {
                authority: 3,
                sequence: 1,
            },
            kind: "move".into(),
            placements: vec![],
        }
    }
    #[test]
    fn fence_and_bounds() {
        let mut p = Phase::new(1, 2, 1);
        let e = env();
        assert_eq!(p.admit(e.clone(), 7), Ok(true));
        assert_eq!(p.admit(e.clone(), 7), Ok(false));
        assert_eq!(p.admit(e.clone(), 8), Err(ProtocolError::Conflict));
        let mut other = e.clone();
        other.attempt.sequence = 2;
        assert_eq!(p.admit(other, 7), Err(ProtocolError::Capacity));
        p.close();
        assert_eq!(p.lookup(&e), Ok(&7));
        assert_eq!(p.seal(true), Err(ProtocolError::Unresolved));
        p.terminal(&e).unwrap();
        p.seal(true).unwrap();
        assert_eq!(p.admit(e, 7), Err(ProtocolError::Sealed));
    }
    #[test]
    fn checked_overflow() {
        let mut n = u64::MAX;
        assert_eq!(advance(&mut n), Err(ProtocolError::Exhausted));
        let mut ids = CheckedIds::new(4);
        assert_eq!(ids.node().unwrap(), (4u128 << 64) | 1);
        ids.sequence = u64::MAX;
        assert_eq!(ids.attempt(), Err(ProtocolError::Exhausted));
    }
}

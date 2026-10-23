//! Control-plane contracts shared by local calls and remote construction handlers.
//! These records do not provide crash recovery. Callers retain data protection until
//! acknowledged release and stop construction on an unresolved remote outcome.
pub mod envelope;
pub mod handoff;
pub mod publication;
pub mod reservation;
pub mod transfer;

use serde::{Deserialize, Serialize};
pub type NodeId = u128;
pub type AuthorityId = u64;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AttemptId {
    pub authority: AuthorityId,
    pub sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolError {
    StaleRun,
    WrongEpoch,
    Sealed,
    Closing,
    Exhausted,
    Capacity,
    Unknown,
    Conflict,
    Prerequisite,
    InvalidTopology,
    StalePlacement,
    Unresolved,
}
pub type Result<T> = std::result::Result<T, ProtocolError>;
pub fn advance(value: &mut u64) -> Result<u64> {
    *value = value.checked_add(1).ok_or(ProtocolError::Exhausted)?;
    Ok(*value)
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "distributed protocol: {self:?}")
    }
}
impl std::error::Error for ProtocolError {}

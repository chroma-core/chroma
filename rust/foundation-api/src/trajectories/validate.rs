#![allow(missing_docs)]

use super::error::TrajectoryError;
use super::ids::encode_index;
use super::limits::{CALL_INDEX_WIDTH, ENTRY_INDEX_WIDTH};
use super::model::{ReasoningEntry, ReasoningTrajectoryFile};

/// Validate a pruned reasoning trajectory before it is split into records.
pub(crate) fn validate_file(file: &ReasoningTrajectoryFile) -> Result<(), TrajectoryError> {
    encode_index(file.trajectory.entries.len(), ENTRY_INDEX_WIDTH)?;
    for entry in &file.trajectory.entries {
        validate_entry(entry)?;
    }
    Ok(())
}

/// Return the canonical stored form of one pruned reasoning entry.
pub(crate) fn normalize_entry(entry: &ReasoningEntry) -> Result<ReasoningEntry, TrajectoryError> {
    let Some(entry) = entry.normalized() else {
        return Err(TrajectoryError::InvalidValue(
            "reasoning entry must have reasoning or writes".to_string(),
        ));
    };
    encode_index(entry.writes.len(), CALL_INDEX_WIDTH)?;
    Ok(entry)
}

/// Validate one pruned reasoning entry.
pub(crate) fn validate_entry(entry: &ReasoningEntry) -> Result<(), TrajectoryError> {
    normalize_entry(entry).map(drop)
}

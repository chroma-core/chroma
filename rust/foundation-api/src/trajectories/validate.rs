#![allow(missing_docs)]

use serde_json::to_vec;

use super::error::TrajectoryError;
use super::ids::encode_index;
use super::limits::{CALL_INDEX_WIDTH, ROOT_METADATA_MAX_BYTES};
use super::model::{Action, GenerateTrajectoryFile, Observation, TrajectoryEntry};
use super::record_format::root_metadata_value;

/// Validate a complete trajectory file before it is split into Chroma records.
pub(crate) fn validate_file(file: &GenerateTrajectoryFile) -> Result<(), TrajectoryError> {
    for entry in &file.trajectory.actions_and_observations {
        validate_entry(entry)?;
    }
    let root = root_metadata_value(file)?;
    let root_bytes = to_vec(&root)?;
    if root_bytes.len() > ROOT_METADATA_MAX_BYTES {
        return Err(TrajectoryError::SizeLimit(format!(
            "root metadata for {} is {} bytes, max is {ROOT_METADATA_MAX_BYTES}",
            file.trajectory.id,
            root_bytes.len()
        )));
    }
    Ok(())
}

/// Validate one action or observation entry according to its variant.
pub(crate) fn validate_entry(entry: &TrajectoryEntry) -> Result<(), TrajectoryError> {
    match entry {
        TrajectoryEntry::Action(action) => validate_action(action),
        TrajectoryEntry::Observation(observation) => validate_observation(observation),
    }
}

/// Validate the parallel tool, parameter, and source vectors in an action.
pub(crate) fn validate_action(action: &Action) -> Result<(), TrajectoryError> {
    if action.tools.len() != action.params.len() || action.tools.len() != action.sources.len() {
        return Err(TrajectoryError::InvalidValue(format!(
            "action parallel vector mismatch: tools={}, params={}, sources={}",
            action.tools.len(),
            action.params.len(),
            action.sources.len()
        )));
    }
    encode_index(action.tools.len(), CALL_INDEX_WIDTH)?;
    Ok(())
}

/// Validate the parallel observation, source, and metadata vectors in an observation.
pub(crate) fn validate_observation(observation: &Observation) -> Result<(), TrajectoryError> {
    if observation.observations.len() != observation.sources.len()
        || observation.observations.len() != observation.tool_metadata.len()
    {
        return Err(TrajectoryError::InvalidValue(format!(
            "observation parallel vector mismatch: observations={}, sources={}, tool_metadata={}",
            observation.observations.len(),
            observation.sources.len(),
            observation.tool_metadata.len()
        )));
    }
    encode_index(observation.observations.len(), CALL_INDEX_WIDTH)?;
    Ok(())
}

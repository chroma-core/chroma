#![allow(missing_docs)]

use std::collections::BTreeMap;

use chroma::types::Metadata;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::chunkset::{push_chunkset, push_json_record, read_chunkset, read_json};
use super::citations::{read_citations, write_citations};
use super::error::TrajectoryError;
use super::ids::{encode_index, uuid_to_tid};
use super::limits::{ENTRY_INDEX_WIDTH, VALUE_MAX_BYTES};
use super::model::{ReasoningEntry, ReasoningTrajectory, ReasoningTrajectoryFile, WriteState};
use super::validate::validate_entry;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChromaRecord {
    pub(crate) id: String,
    pub(crate) document: String,
    pub(crate) metadata: Metadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TrajectoryHeader {
    pub(crate) v: u8,
    #[serde(rename = "type")]
    pub(crate) record_type: String,
    pub(crate) tid: String,
    pub(crate) entries: usize,
    pub(crate) write_state: WriteState,
    pub(crate) has_citations: bool,
}

/// Collect every record needed to store a reasoning trajectory file.
pub(crate) fn collect_file_records(
    records: &mut Vec<ChromaRecord>,
    file: &ReasoningTrajectoryFile,
    write_state: WriteState,
) -> Result<(), TrajectoryError> {
    let uuid = file.trajectory.id;
    let tid = uuid_to_tid(uuid)?;

    let has_citations = if let Some(citations) = &file.citations {
        write_citations(records, &tid, citations)?;
        true
    } else {
        false
    };

    for (index, entry) in file.trajectory.entries.iter().enumerate() {
        collect_entry_records(records, &tid, index, entry)?;
    }

    let header = TrajectoryHeader {
        v: 1,
        record_type: "ReasoningTrajectoryFile".to_string(),
        tid,
        entries: file.trajectory.entries.len(),
        write_state,
        has_citations,
    };
    let header_key = trajectory_header_key(uuid)?;
    push_json_record(records, &header_key, &header, VALUE_MAX_BYTES)?;
    Ok(())
}

/// Collect the records that replace an open trajectory with finalized content.
pub(crate) fn collect_finalization_records(
    records: &mut Vec<ChromaRecord>,
    header: &mut TrajectoryHeader,
    file: &ReasoningTrajectoryFile,
    tid: &str,
) -> Result<(), TrajectoryError> {
    for (index, entry) in file.trajectory.entries.iter().enumerate() {
        collect_entry_records(records, tid, index, entry)?;
    }
    header.entries = file.trajectory.entries.len();
    header.has_citations = if let Some(citations) = &file.citations {
        write_citations(records, tid, citations)?;
        true
    } else {
        false
    };
    header.write_state = WriteState::Finalized;
    push_json_record(
        records,
        &format!("gt/{tid}/header"),
        header,
        VALUE_MAX_BYTES,
    )
}

/// Rehydrate one reasoning trajectory file from documents carrying its id.
pub(crate) fn load_one_from_documents(
    documents: &BTreeMap<String, String>,
    tid_uuid: Uuid,
    require_finalized: bool,
) -> Result<ReasoningTrajectoryFile, TrajectoryError> {
    let tid = uuid_to_tid(tid_uuid)?;
    let header = read_trajectory_header(documents, tid_uuid)?;
    validate_trajectory_header(&header, &tid)?;
    if require_finalized && header.write_state != WriteState::Finalized {
        return Err(TrajectoryError::FinalizedRequired { tid: tid_uuid });
    }

    let citations = if header.has_citations {
        Some(read_citations(documents, &tid)?)
    } else {
        None
    };

    let mut entries = Vec::with_capacity(header.entries);
    for index in 0..header.entries {
        entries.push(read_entry(documents, &tid, index)?);
    }

    Ok(ReasoningTrajectoryFile {
        citations,
        trajectory: ReasoningTrajectory {
            id: tid_uuid,
            entries,
        },
    })
}

/// Read the root trajectory header document for a UUID.
pub(crate) fn read_trajectory_header(
    documents: &BTreeMap<String, String>,
    tid_uuid: Uuid,
) -> Result<TrajectoryHeader, TrajectoryError> {
    let key = trajectory_header_key(tid_uuid)?;
    read_json(documents, &key)
}

/// Check that a decoded trajectory header matches the supported schema and key.
pub(crate) fn validate_trajectory_header(
    header: &TrajectoryHeader,
    tid: &str,
) -> Result<(), TrajectoryError> {
    if header.v != 1 {
        return Err(TrajectoryError::InvalidValue(format!(
            "unsupported trajectory header version {}",
            header.v
        )));
    }
    if header.record_type != "ReasoningTrajectoryFile" {
        return Err(TrajectoryError::InvalidValue(format!(
            "unexpected trajectory header type {}",
            header.record_type
        )));
    }
    if header.tid != tid {
        return Err(TrajectoryError::InvalidValue(format!(
            "header tid {} does not match key tid {tid}",
            header.tid
        )));
    }
    Ok(())
}

/// Build the Chroma key that stores the trajectory header for a UUID.
pub(crate) fn trajectory_header_key(uuid: Uuid) -> Result<String, TrajectoryError> {
    Ok(format!("gt/{}/header", uuid_to_tid(uuid)?))
}

/// Collect all records that represent one pruned reasoning entry.
pub(crate) fn collect_entry_records(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    index: usize,
    entry: &ReasoningEntry,
) -> Result<(), TrajectoryError> {
    validate_entry(entry)?;
    let entry = entry.normalized().ok_or_else(|| {
        TrajectoryError::InvalidValue("reasoning entry must have reasoning or writes".to_string())
    })?;
    let entry_component = encode_index(index, ENTRY_INDEX_WIDTH)?;
    push_chunkset(
        records,
        &format!("gt/{tid}/entries/{entry_component}/entry"),
        &entry,
    )
}

/// Rehydrate one pruned reasoning entry.
fn read_entry(
    documents: &BTreeMap<String, String>,
    tid: &str,
    index: usize,
) -> Result<ReasoningEntry, TrajectoryError> {
    let entry_component = encode_index(index, ENTRY_INDEX_WIDTH)?;
    let value = read_chunkset(
        documents,
        &format!("gt/{tid}/entries/{entry_component}/entry"),
    )?;
    Ok(serde_json::from_value(value)?)
}

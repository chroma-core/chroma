#![allow(missing_docs)]

use std::collections::BTreeMap;

use chroma::types::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::chunkset::{push_chunkset, push_json_record, read_chunkset, read_json};
use super::citations::{read_citations, write_citations};
use super::error::TrajectoryError;
use super::ids::{encode_index, uuid_to_tid};
use super::limits::{
    CALL_INDEX_WIDTH, ENTRY_INDEX_WIDTH, ROOT_METADATA_MAX_BYTES, VALUE_MAX_BYTES,
};
use super::model::{GenerateTrajectoryFile, TrajectoryEntry, WriteState};
use super::validate::{validate_action, validate_observation};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EntryKind {
    Action,
    Observation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EntryHeader {
    i: String,
    kind: EntryKind,
    calls: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    has_reasoning: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    has_reasoning_signature: Option<bool>,
}

/// Collect every record needed to store a trajectory file in one write state.
pub(crate) fn collect_file_records(
    records: &mut Vec<ChromaRecord>,
    file: &GenerateTrajectoryFile,
    write_state: WriteState,
) -> Result<(), TrajectoryError> {
    let uuid = file.trajectory.id;
    let tid = uuid_to_tid(uuid)?;

    write_root_metadata(records, file)?;

    let has_citations = if let Some(citations) = &file.citations {
        write_citations(records, &tid, citations)?;
        true
    } else {
        false
    };

    for (index, entry) in file.trajectory.actions_and_observations.iter().enumerate() {
        collect_entry_records(records, &tid, index, entry)?;
    }

    let header = TrajectoryHeader {
        v: 1,
        record_type: "GenerateTrajectoryFile".to_string(),
        tid,
        entries: file.trajectory.actions_and_observations.len(),
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
    file: &GenerateTrajectoryFile,
    tid: &str,
) -> Result<(), TrajectoryError> {
    write_root_metadata(records, file)?;
    for (index, entry) in file.trajectory.actions_and_observations.iter().enumerate() {
        collect_entry_records(records, tid, index, entry)?;
    }
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

/// Rehydrate one trajectory file from all documents carrying its trajectory id.
pub(crate) fn load_one_from_documents(
    documents: &BTreeMap<String, String>,
    tid_uuid: Uuid,
    require_finalized: bool,
) -> Result<GenerateTrajectoryFile, TrajectoryError> {
    let tid = uuid_to_tid(tid_uuid)?;
    let header = read_trajectory_header(documents, tid_uuid)?;
    validate_trajectory_header(&header, &tid)?;
    if require_finalized && header.write_state != WriteState::Finalized {
        return Err(TrajectoryError::FinalizedRequired { tid: tid_uuid });
    }

    let mut root: Value = read_json(documents, &format!("gt/{tid}/root_metadata"))?;
    let Some(root_object) = root.as_object_mut() else {
        return Err(TrajectoryError::InvalidValue(format!(
            "gt/{tid}/root_metadata must be an object"
        )));
    };

    if header.has_citations {
        let citations = read_citations(documents, &tid)?;
        root_object.insert("citations".to_string(), citations);
    }

    let mut entries = Vec::with_capacity(header.entries);
    for index in 0..header.entries {
        entries.push(read_entry(documents, &tid, index)?);
    }

    let trajectory = serde_json::json!({
        "actions_and_observations": entries,
        "id": tid_uuid,
    });
    root_object.insert("trajectory".to_string(), trajectory);

    Ok(serde_json::from_value(root)?)
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
    if header.record_type != "GenerateTrajectoryFile" {
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

/// Store the top-level trajectory metadata without entries or citations.
fn write_root_metadata(
    records: &mut Vec<ChromaRecord>,
    file: &GenerateTrajectoryFile,
) -> Result<(), TrajectoryError> {
    let value = root_metadata_value(file)?;
    let key = format!("gt/{}/root_metadata", uuid_to_tid(file.trajectory.id)?);
    push_json_record(records, &key, &value, ROOT_METADATA_MAX_BYTES)
}

/// Serialize the top-level file fields that belong in root metadata.
pub(crate) fn root_metadata_value(file: &GenerateTrajectoryFile) -> Result<Value, TrajectoryError> {
    let value = serde_json::to_value(file)?;
    let Value::Object(mut object) = value else {
        return Err(TrajectoryError::InvalidValue(
            "GenerateTrajectoryFile must serialize as an object".to_string(),
        ));
    };
    object.remove("trajectory");
    object.remove("citations");
    Ok(Value::Object(object))
}

/// Collect all records that represent one trajectory stream entry.
pub(crate) fn collect_entry_records(
    records: &mut Vec<ChromaRecord>,
    tid: &str,
    index: usize,
    entry: &TrajectoryEntry,
) -> Result<(), TrajectoryError> {
    let entry_component = encode_index(index, ENTRY_INDEX_WIDTH)?;
    match entry {
        TrajectoryEntry::Action(action) => {
            validate_action(action)?;
            let calls = action.tools.len();
            for call in 0..calls {
                let call_component = encode_index(call, CALL_INDEX_WIDTH)?;
                push_chunkset(
                    records,
                    &format!("gt/{tid}/entries/{entry_component}/action/tools/{call_component}"),
                    &action.tools[call],
                )?;
                push_chunkset(
                    records,
                    &format!("gt/{tid}/entries/{entry_component}/action/params/{call_component}"),
                    &action.params[call],
                )?;
                push_chunkset(
                    records,
                    &format!("gt/{tid}/entries/{entry_component}/action/sources/{call_component}"),
                    &action.sources[call],
                )?;
            }

            if let Some(reasoning) = &action.reasoning {
                push_chunkset(
                    records,
                    &format!("gt/{tid}/entries/{entry_component}/action/reasoning"),
                    reasoning,
                )?;
            }
            if let Some(signature) = &action.reasoning_signature {
                push_chunkset(
                    records,
                    &format!("gt/{tid}/entries/{entry_component}/action/reasoning_signature"),
                    signature,
                )?;
            }

            let header = EntryHeader {
                i: entry_component.clone(),
                kind: EntryKind::Action,
                calls,
                has_reasoning: Some(action.reasoning.is_some()),
                has_reasoning_signature: Some(action.reasoning_signature.is_some()),
            };
            push_json_record(
                records,
                &format!("gt/{tid}/entries/{entry_component}/header"),
                &header,
                VALUE_MAX_BYTES,
            )?;
        }
        TrajectoryEntry::Observation(observation) => {
            validate_observation(observation)?;
            let calls = observation.observations.len();
            for call in 0..calls {
                let call_component = encode_index(call, CALL_INDEX_WIDTH)?;
                push_chunkset(
                    records,
                    &format!(
                        "gt/{tid}/entries/{entry_component}/observation/observations/{call_component}"
                    ),
                    &observation.observations[call],
                )?;
                push_chunkset(
                    records,
                    &format!(
                        "gt/{tid}/entries/{entry_component}/observation/sources/{call_component}"
                    ),
                    &observation.sources[call],
                )?;
                push_chunkset(
                    records,
                    &format!(
                        "gt/{tid}/entries/{entry_component}/observation/tool_metadata/{call_component}"
                    ),
                    &observation.tool_metadata[call],
                )?;
            }

            let header = EntryHeader {
                i: entry_component.clone(),
                kind: EntryKind::Observation,
                calls,
                has_reasoning: None,
                has_reasoning_signature: None,
            };
            push_json_record(
                records,
                &format!("gt/{tid}/entries/{entry_component}/header"),
                &header,
                VALUE_MAX_BYTES,
            )?;
        }
    }
    Ok(())
}

/// Rehydrate one entry from its header and chunked field records.
fn read_entry(
    documents: &BTreeMap<String, String>,
    tid: &str,
    index: usize,
) -> Result<Value, TrajectoryError> {
    let entry_component = encode_index(index, ENTRY_INDEX_WIDTH)?;
    let header_key = format!("gt/{tid}/entries/{entry_component}/header");
    let header: EntryHeader = read_json(documents, &header_key)?;
    if header.i != entry_component {
        return Err(TrajectoryError::InvalidValue(format!(
            "entry header index {} does not match key index {entry_component}",
            header.i
        )));
    }

    let entry_base = format!("gt/{tid}/entries/{entry_component}");
    let mut object = Map::new();
    match header.kind {
        EntryKind::Action => {
            let mut tools = Vec::with_capacity(header.calls);
            let mut params = Vec::with_capacity(header.calls);
            let mut sources = Vec::with_capacity(header.calls);
            for call in 0..header.calls {
                let call_component = encode_index(call, CALL_INDEX_WIDTH)?;
                tools.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/action/tools/{call_component}"),
                )?);
                params.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/action/params/{call_component}"),
                )?);
                sources.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/action/sources/{call_component}"),
                )?);
            }
            object.insert("tools".to_string(), Value::Array(tools));
            object.insert("params".to_string(), Value::Array(params));
            object.insert("sources".to_string(), Value::Array(sources));

            if header.has_reasoning.unwrap_or(false) {
                object.insert(
                    "reasoning".to_string(),
                    read_chunkset(documents, &format!("{entry_base}/action/reasoning"))?,
                );
            }
            if header.has_reasoning_signature.unwrap_or(false) {
                object.insert(
                    "reasoning_signature".to_string(),
                    read_chunkset(
                        documents,
                        &format!("{entry_base}/action/reasoning_signature"),
                    )?,
                );
            }
        }
        EntryKind::Observation => {
            let mut observations = Vec::with_capacity(header.calls);
            let mut sources = Vec::with_capacity(header.calls);
            let mut tool_metadata = Vec::with_capacity(header.calls);
            for call in 0..header.calls {
                let call_component = encode_index(call, CALL_INDEX_WIDTH)?;
                observations.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/observation/observations/{call_component}"),
                )?);
                sources.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/observation/sources/{call_component}"),
                )?);
                tool_metadata.push(read_chunkset(
                    documents,
                    &format!("{entry_base}/observation/tool_metadata/{call_component}"),
                )?);
            }
            object.insert("observations".to_string(), Value::Array(observations));
            object.insert("sources".to_string(), Value::Array(sources));
            object.insert("tool_metadata".to_string(), Value::Array(tool_metadata));
        }
    }

    Ok(Value::Object(object))
}

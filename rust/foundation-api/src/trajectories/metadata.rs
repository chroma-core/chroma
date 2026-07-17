#![allow(missing_docs)]

use chroma::types::{Metadata, MetadataValue, UpdateMetadata, UpdateMetadataValue};

use super::error::TrajectoryError;
use super::ids::{decode_fixed_base36_to_be_bytes, decode_index, hex_lower, tid_to_uuid};
use super::limits::{CHUNK_INDEX_WIDTH, ENTRY_INDEX_WIDTH, ITEM_ID_WIDTH};

/// Convert Chroma metadata into the update form accepted by Chroma mutations.
pub(crate) fn update_metadata_from_metadata(metadata: Metadata) -> UpdateMetadata {
    metadata
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                MetadataValue::Bool(value) => UpdateMetadataValue::Bool(value),
                MetadataValue::Int(value) => UpdateMetadataValue::Int(value),
                MetadataValue::Float(value) => UpdateMetadataValue::Float(value),
                MetadataValue::Str(value) => UpdateMetadataValue::Str(value),
                MetadataValue::SparseVector(value) => UpdateMetadataValue::SparseVector(value),
                MetadataValue::BoolArray(value) => UpdateMetadataValue::BoolArray(value),
                MetadataValue::IntArray(value) => UpdateMetadataValue::IntArray(value),
                MetadataValue::FloatArray(value) => UpdateMetadataValue::FloatArray(value),
                MetadataValue::StringArray(value) => UpdateMetadataValue::StringArray(value),
            };
            (key, value)
        })
        .collect()
}

/// Derive queryable metadata from a generated trajectory record key.
pub(crate) fn metadata_for_key(key: &str) -> Result<Metadata, TrajectoryError> {
    let parts = key.split('/').collect::<Vec<_>>();
    if parts.len() < 3 || parts[0] != "gt" {
        return Err(TrajectoryError::InvalidKey(key.to_string()));
    }

    let tid = parts[1];
    let tid_uuid = tid_to_uuid(tid)?;
    let mut metadata = Metadata::new();
    metadata_insert_str(&mut metadata, "schema", "reasoning_trajectory");
    metadata_insert_str(&mut metadata, "tid", &tid_uuid.to_string());
    metadata_insert_str(&mut metadata, "tid_key", tid);

    match parts.as_slice() {
        ["gt", _, "header"] => {
            metadata_insert_str(&mut metadata, "part", "header");
            metadata_insert_str(&mut metadata, "record_kind", "trajectory_header");
        }
        ["gt", _, "citations", "header"] => {
            metadata_insert_str(&mut metadata, "subtree", "citations");
            metadata_insert_str(&mut metadata, "part", "header");
            metadata_insert_str(&mut metadata, "record_kind", "citations_header");
        }
        ["gt", _, "citations", collection, item_id, "metadata"] => {
            metadata_insert_str(&mut metadata, "subtree", "citations");
            metadata_insert_str(&mut metadata, "collection", collection);
            metadata_insert_item_id(&mut metadata, item_id)?;
            metadata_insert_str(&mut metadata, "part", "metadata");
        }
        ["gt", _, "citations", collection, item_id, "chunks", chunk] => {
            metadata_insert_str(&mut metadata, "subtree", "citations");
            metadata_insert_str(&mut metadata, "collection", collection);
            metadata_insert_item_id(&mut metadata, item_id)?;
            metadata_insert_str(&mut metadata, "part", "chunk");
            metadata_insert_int(
                &mut metadata,
                "chunk",
                decode_index(chunk, CHUNK_INDEX_WIDTH)?,
            )?;
        }
        ["gt", _, "entries", entry, "entry", "metadata"] => {
            metadata_insert_entry(&mut metadata, entry)?;
            metadata_insert_str(&mut metadata, "field", "entry");
            metadata_insert_str(&mut metadata, "part", "metadata");
        }
        ["gt", _, "entries", entry, "entry", "chunks", chunk] => {
            metadata_insert_entry(&mut metadata, entry)?;
            metadata_insert_str(&mut metadata, "field", "entry");
            metadata_insert_str(&mut metadata, "part", "chunk");
            metadata_insert_int(
                &mut metadata,
                "chunk",
                decode_index(chunk, CHUNK_INDEX_WIDTH)?,
            )?;
        }
        _ => {
            return Err(TrajectoryError::InvalidKey(key.to_string()));
        }
    }

    Ok(metadata)
}

/// Insert metadata fields shared by entry payload records.
fn metadata_insert_entry(metadata: &mut Metadata, entry: &str) -> Result<(), TrajectoryError> {
    metadata_insert_str(metadata, "subtree", "entries");
    metadata_insert_int(metadata, "entry", decode_index(entry, ENTRY_INDEX_WIDTH)?)?;
    Ok(())
}

/// Insert hash metadata for a citation item id.
fn metadata_insert_item_id(metadata: &mut Metadata, item_id: &str) -> Result<(), TrajectoryError> {
    if item_id.len() != ITEM_ID_WIDTH {
        return Err(TrajectoryError::InvalidKey(format!(
            "citation item id must be {ITEM_ID_WIDTH} bytes, got {}: {item_id}",
            item_id.len()
        )));
    }
    let bytes = decode_fixed_base36_to_be_bytes(item_id, 32)?;
    metadata_insert_str(metadata, "item_key", item_id);
    metadata_insert_str(metadata, "item_id", &hex_lower(&bytes));
    Ok(())
}

/// Insert a string metadata value.
fn metadata_insert_str(metadata: &mut Metadata, key: &str, value: &str) {
    metadata.insert(key.to_string(), MetadataValue::Str(value.to_string()));
}

/// Insert an integer metadata value after a lossless signed conversion.
fn metadata_insert_int(
    metadata: &mut Metadata,
    key: &str,
    value: usize,
) -> Result<(), TrajectoryError> {
    metadata.insert(key.to_string(), MetadataValue::Int(i64::try_from(value)?));
    Ok(())
}

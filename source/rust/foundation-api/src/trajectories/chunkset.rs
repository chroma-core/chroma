#![allow(missing_docs)]

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::TrajectoryError;
use super::ids::{encode_index, sha256_base36};
use super::limits::{CHUNKSET_BASE_MAX_BYTES, CHUNK_INDEX_WIDTH, KEY_MAX_BYTES, VALUE_MAX_BYTES};
use super::metadata::metadata_for_key;
use super::record_format::ChromaRecord;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ChunkMetadata {
    n: usize,
    len: usize,
    sha256: String,
}

/// Find chunkset base keys that are direct children of a prefix.
pub(crate) fn direct_chunkset_bases(
    documents: &BTreeMap<String, String>,
    prefix: &str,
) -> Vec<String> {
    let mut bases = Vec::new();
    for key in documents.keys() {
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        let Some(base_rest) = rest.strip_suffix("/metadata") else {
            continue;
        };
        if base_rest.contains('/') {
            continue;
        }
        bases.push(format!("{prefix}{base_rest}"));
    }
    bases
}

/// Store a serializable value as a metadata record plus bounded UTF-8 chunks.
pub(crate) fn push_chunkset<T: Serialize>(
    records: &mut Vec<ChromaRecord>,
    base_key: &str,
    value: &T,
) -> Result<(), TrajectoryError> {
    if base_key.len() > CHUNKSET_BASE_MAX_BYTES {
        return Err(TrajectoryError::SizeLimit(format!(
            "chunkset base key is {} bytes, max is {CHUNKSET_BASE_MAX_BYTES}: {base_key}",
            base_key.len()
        )));
    }

    let bytes = serde_json::to_vec(value)?;
    let digest = sha256_base36(&bytes)?;
    let chunks = utf8_chunks(&bytes, VALUE_MAX_BYTES)?;

    let metadata = ChunkMetadata {
        n: chunks.len(),
        len: bytes.len(),
        sha256: digest,
    };
    push_json_record(
        records,
        &format!("{base_key}/metadata"),
        &metadata,
        VALUE_MAX_BYTES,
    )?;

    for (index, chunk) in chunks.into_iter().enumerate() {
        let chunk_component = encode_index(index, CHUNK_INDEX_WIDTH)?;
        push_document_record(
            records,
            &format!("{base_key}/chunks/{chunk_component}"),
            std::str::from_utf8(chunk)?,
            VALUE_MAX_BYTES,
        )?;
    }
    Ok(())
}

/// Reassemble and verify a chunkset into its original JSON value.
pub(crate) fn read_chunkset(
    documents: &BTreeMap<String, String>,
    base_key: &str,
) -> Result<Value, TrajectoryError> {
    let metadata_key = format!("{base_key}/metadata");
    let metadata: ChunkMetadata = read_json(documents, &metadata_key)?;

    let mut bytes = Vec::with_capacity(metadata.len);
    for index in 0..metadata.n {
        let chunk_component = encode_index(index, CHUNK_INDEX_WIDTH)?;
        let key = format!("{base_key}/chunks/{chunk_component}");
        let chunk = documents
            .get(&key)
            .ok_or_else(|| TrajectoryError::MissingKey(key.clone()))?;
        bytes.extend_from_slice(chunk.as_bytes());
    }

    if bytes.len() != metadata.len {
        return Err(TrajectoryError::InvalidValue(format!(
            "chunkset {base_key} length mismatch: expected {}, got {}",
            metadata.len,
            bytes.len()
        )));
    }

    let actual = sha256_base36(&bytes)?;
    if actual != metadata.sha256 {
        return Err(TrajectoryError::HashMismatch {
            base_key: base_key.to_string(),
            expected: metadata.sha256,
            actual,
        });
    }

    Ok(serde_json::from_slice(&bytes)?)
}

/// Serialize a JSON record and enforce its value byte limit.
pub(crate) fn push_json_record<T: Serialize>(
    records: &mut Vec<ChromaRecord>,
    key: &str,
    value: &T,
    max_value_bytes: usize,
) -> Result<(), TrajectoryError> {
    let bytes = serde_json::to_vec(value)?;
    push_document_record(records, key, std::str::from_utf8(&bytes)?, max_value_bytes)
}

/// Add one Chroma document record after validating key and value sizes.
pub(crate) fn push_document_record(
    records: &mut Vec<ChromaRecord>,
    key: &str,
    document: &str,
    max_value_bytes: usize,
) -> Result<(), TrajectoryError> {
    if key.len() > KEY_MAX_BYTES {
        return Err(TrajectoryError::SizeLimit(format!(
            "key is {} bytes, max is {KEY_MAX_BYTES}: {key}",
            key.len()
        )));
    }
    if document.len() > max_value_bytes {
        return Err(TrajectoryError::SizeLimit(format!(
            "value for {key} is {} bytes, max is {max_value_bytes}",
            document.len()
        )));
    }
    records.push(ChromaRecord {
        id: key.to_string(),
        document: document.to_string(),
        metadata: metadata_for_key(key)?,
    });
    Ok(())
}

/// Deserialize a required JSON document from a record map.
pub(crate) fn read_json<T: DeserializeOwned>(
    documents: &BTreeMap<String, String>,
    key: &str,
) -> Result<T, TrajectoryError> {
    let document = documents
        .get(key)
        .ok_or_else(|| TrajectoryError::MissingKey(key.to_string()))?;
    Ok(serde_json::from_str(document)?)
}

/// Split UTF-8 bytes into nonempty chunks without breaking scalar boundaries.
pub(crate) fn utf8_chunks(bytes: &[u8], max_bytes: usize) -> Result<Vec<&[u8]>, TrajectoryError> {
    let text = std::str::from_utf8(bytes)?;
    if bytes.is_empty() {
        return Ok(vec![bytes]);
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < bytes.len() {
        let mut end = start.saturating_add(max_bytes).min(bytes.len());
        while end > start && !text.is_char_boundary(end) {
            end = end.checked_sub(1).ok_or_else(|| {
                TrajectoryError::SizeLimit(format!(
                    "UTF-8 chunk boundary underflow at byte {start}"
                ))
            })?;
        }
        if end == start {
            return Err(TrajectoryError::SizeLimit(format!(
                "UTF-8 scalar at byte {start} exceeds max chunk size {max_bytes}"
            )));
        }
        chunks.push(&bytes[start..end]);
        start = end;
    }
    Ok(chunks)
}

//! Opt-in conversion of actual materializer output into the add-only task input.
//! The caller supplies an exact previously validated parent ownership binding.
//! This function does not establish that binding, authorize a task, or write data.
use chroma_segment::types::MaterializeLogsResult;
use chroma_types::{Chunk, Collection, CollectionUuid, LogRecord, MaterializedLogOperation, Operation, ScalarEncoding};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct ParentOwnershipBinding {
    pub deployment_sha256: String,
    pub ownership_generation: u64,
    /// The existing prototype requires the next actual record offset to equal
    /// its dense global row ID. Production holes/other namespaces are rejected.
    pub next_global_id: u64,
}

#[derive(Debug, Serialize)]
pub struct SourceAddNew {
    pub log_offset: i64,
    pub global_id: u64,
    pub record_offset_id: u32,
    pub user_id: String,
    pub embedding_bits: Vec<u32>,
}

#[derive(Debug, Serialize)]
pub struct CapturedMaterializedInput {
    pub collection_id: CollectionUuid,
    pub collection_version: i32,
    pub parent_log_position: i64,
    pub pulled_log_offset: i64,
    pub parent_deployment_sha256: String,
    pub ownership_generation: u64,
    pub rows: Vec<SourceAddNew>,
}

/// Make an unpartitioned private allocator from the actual retained reader.
/// This is not a replacement for the shared allocator of a partitioned job.
/// The conservative window check leaves the allocator state representable after
/// every possible raw-row allocation; a rejected attempt must be discarded.
pub fn private_add_allocator(
    reader: Option<&chroma_segment::blockfile_record::RecordSegmentReader<'_>>,
    raw_count: usize,
) -> Result<std::sync::Arc<std::sync::atomic::AtomicU32>, String> {
    let first = checked_record_window_start(reader.map(|r| r.get_max_offset_id()), raw_count)?;
    Ok(std::sync::Arc::new(std::sync::atomic::AtomicU32::new(first)))
}

fn checked_record_window_start(max_offset: Option<u32>, raw_count: usize) -> Result<u32, String> {
    if raw_count == 0 || raw_count > 100_000 { return Err("bounded nonempty raw window required".into()); }
    let first = match max_offset {
        Some(max) => max.checked_add(1).ok_or("record offset namespace exhausted")?,
        None => 1,
    };
    first.checked_add(u32::try_from(raw_count).map_err(|_| "record window overflow")?)
        .ok_or("record offset window would wrap allocator")?;
    Ok(first)
}

#[cfg(test)]
mod allocator_tests {
    use super::checked_record_window_start;
    #[test]
    fn exhaustion_rejects_before_allocator_creation() {
        assert_eq!(checked_record_window_start(Some(16), 2).unwrap(),17);
        assert_eq!(checked_record_window_start(None, 1).unwrap(),1);
        assert!(checked_record_window_start(Some(u32::MAX),1).is_err());
        assert!(checked_record_window_start(Some(u32::MAX-1),1).is_err());
        assert!(checked_record_window_start(Some(u32::MAX-2),2).is_err());
    }
}

/// Validate the complete raw window and exact materialized correspondence.
/// Rejection occurs before writer calls, but materialization may already have
/// advanced its private ID allocator. Discard that attempt; do not roll it back
/// or reuse its allocator as if no IDs had been consumed.
pub async fn capture_add_new(
    collection: &Collection,
    pulled_log_offset: i64,
    parent: &ParentOwnershipBinding,
    logs: &Chunk<LogRecord>,
    materialized: &MaterializeLogsResult,
) -> Result<CapturedMaterializedInput, String> {
    let dimension = collection.dimension.filter(|d| *d > 0).ok_or("positive collection dimension required")? as usize;
    let count = pulled_log_offset.checked_sub(collection.log_position).ok_or("source window overflow")?;
    if collection.version < 0 || collection.log_position < -1 || count <= 0
        || count as u64 > 100_000 || count as usize != logs.total_len()
        || logs.len() != logs.total_len() || materialized.len() != logs.len()
        || materialized.has_backfill()
    { return Err("complete nonempty bounded source window required".into()); }
    let payload_bytes = (count as usize).checked_mul(dimension).and_then(|n| n.checked_mul(4))
        .ok_or("embedding byte budget overflow")?;
    if payload_bytes > 512 * 1024 * 1024 {
        return Err("embedding byte budget exceeded".into());
    }
    let owned_source = materialized.source_logs();
    if owned_source.total_len() != logs.total_len() || owned_source.len() != logs.len() {
        return Err("materializer source chunk differs".into());
    }
    if parent.deployment_sha256.len() != 64 || !parent.deployment_sha256.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || parent.ownership_generation == 0
    { return Err("exact parent deployment binding required".into()); }
    let end = parent.next_global_id.checked_add(count as u64).ok_or("record ID range overflow")?;
    if end > 100_000 || end > u32::MAX as u64 {
        return Err("dense prototype record ID range unsupported".into());
    }
    let mut source = HashMap::new();
    for (index, (row, _)) in logs.iter().enumerate() {
        let actual_source = owned_source.get(index).ok_or("missing materializer source row")?;
        let exact_embedding = match (&row.record.embedding, &actual_source.record.embedding) {
            (Some(a), Some(b)) => a.len() == b.len() && a.iter().zip(b).all(|(a,b)| a.to_bits() == b.to_bits()),
            _ => false,
        };
        if actual_source.log_offset != row.log_offset || actual_source.record.operation != row.record.operation
            || actual_source.record.id != row.record.id || actual_source.record.encoding != row.record.encoding
            || actual_source.record.metadata.is_some() || actual_source.record.document.is_some()
            || !exact_embedding
        { return Err("materializer source row differs from captured raw row".into()); }
        if row.log_offset != collection.log_position + index as i64 + 1
            || row.record.operation != Operation::Add || row.record.id.is_empty()
            || row.record.metadata.is_some() || row.record.document.is_some()
            || row.record.encoding.as_ref().is_some_and(|v| *v != ScalarEncoding::FLOAT32)
        { return Err("unsupported raw operation, payload, or source offset".into()); }
        let embedding = row.record.embedding.as_ref().ok_or("source embedding required")?;
        if embedding.len() != dimension || embedding.iter().any(|v| !v.is_finite()) {
            return Err("finite dimension-matched source embedding required".into());
        }
        if source.insert(row.record.id.as_str(), row).is_some() {
            return Err("duplicate raw user ID would fold source rows".into());
        }
    }
    let mut seen_users = HashSet::new();
    let mut seen_offsets = HashSet::new();
    let mut rows = Vec::with_capacity(logs.len());
    for record in materialized {
        if record.get_operation() != MaterializedLogOperation::AddNew {
            return Err("only materialized AddNew is supported".into());
        }
        let user = record.get_user_id(None).await.map_err(|e| e.to_string())?;
        let raw = source.get(user.as_str()).ok_or("materialized user absent from source")?;
        if !seen_users.insert(user.clone()) || !seen_offsets.insert(record.get_offset_id()) {
            return Err("duplicate materialized user or record offset".into());
        }
        let actual = record.embeddings_ref_from_log().ok_or("materialized source embedding required")?;
        let expected = raw.record.embedding.as_ref().unwrap();
        if actual.len() != expected.len() || actual.iter().zip(expected).any(|(a,b)| a.to_bits() != b.to_bits()) {
            return Err("materialized embedding bits differ from source".into());
        }
        rows.push(SourceAddNew { log_offset: raw.log_offset, global_id: u64::from(record.get_offset_id()),
            record_offset_id: record.get_offset_id(), user_id: user,
            embedding_bits: actual.iter().map(|v| v.to_bits()).collect() });
    }
    rows.sort_by_key(|r| r.log_offset);
    if rows.len() != source.len() || rows.iter().enumerate().any(|(i,r)| r.global_id != parent.next_global_id + i as u64) {
        return Err("actual materializer IDs differ from dense parent namespace".into());
    }
    Ok(CapturedMaterializedInput { collection_id: collection.collection_id, collection_version: collection.version,
        parent_log_position: collection.log_position, pulled_log_offset,
        parent_deployment_sha256: parent.deployment_sha256.clone(), ownership_generation: parent.ownership_generation, rows })
}

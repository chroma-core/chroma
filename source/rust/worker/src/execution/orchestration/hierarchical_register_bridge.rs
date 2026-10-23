//! Opt-in projection into the existing collection registration input.
//! Semantic saved-index validation belongs to the compactor producer, not this bridge.
use chroma_types::{Collection, CollectionUuid, Segment, SegmentFlushInfo, SegmentScope, SegmentType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const HIERARCHY_BUNDLE: &str = "hierarchical_spann_bundle_v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedContext {
    pub collection_id: CollectionUuid,
    pub tenant: String,
    pub database: String,
    pub collection_version: i32,
    pub parent_log_position: i64,
    pub pulled_log_offset: i64,
    pub target_total_records: u64,
    pub target_logical_size_bytes: u64,
    pub vector_segment_id: Uuid,
    pub parent_vector_files: HashMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorReference {
    path: String,
    sha256: String,
    bytes: u64,
}

/// Preserve the actual compaction context and every non-hierarchy file path.
/// This does not authenticate a caller, inspect lower blocks, or publish anything.
pub fn attach_descriptor(
    collection: &Collection,
    pulled_log_offset: i64,
    logical_size_bytes: u64,
    vector_segment: &Segment,
    parent_segments: &[SegmentFlushInfo],
    mut outputs: Vec<SegmentFlushInfo>,
    expected: &CapturedContext,
    reference: String,
) -> Result<Vec<SegmentFlushInfo>, String> {
    if collection.collection_id != expected.collection_id
        || collection.tenant != expected.tenant
        || collection.database != expected.database
        || collection.version != expected.collection_version
        || collection.log_position != expected.parent_log_position
        || pulled_log_offset != expected.pulled_log_offset
        || collection.total_records_post_compaction != expected.target_total_records
        || logical_size_bytes != expected.target_logical_size_bytes
        || collection.version < 0
        || collection.log_position < -1
        || pulled_log_offset < collection.log_position
        || collection.version.checked_add(1).is_none()
    { return Err("hierarchy registration differs from captured collection context".into()); }
    if vector_segment.collection != collection.collection_id
        || vector_segment.id.0 != expected.vector_segment_id
        || vector_segment.scope != SegmentScope::VECTOR
        || vector_segment.r#type != SegmentType::Spann
        || vector_segment.file_path != expected.parent_vector_files
    { return Err("hierarchy registration differs from captured vector segment".into()); }
    let parent_ids: HashSet<_> = parent_segments.iter().map(|s|s.segment_id).collect();
    let output_ids: HashSet<_> = outputs.iter().map(|s|s.segment_id).collect();
    if parent_ids.len()!=parent_segments.len() || output_ids.len()!=outputs.len() || parent_ids!=output_ids {
        return Err("complete unique segment output set required".into());
    }
    let parent=parent_segments.iter().find(|s|s.segment_id==vector_segment.id).ok_or("missing parent vector segment")?;
    if parent.file_paths!=expected.parent_vector_files || parent.file_paths.get(HIERARCHY_BUNDLE).is_none_or(|refs|refs.len()!=1) {
        return Err("registered parent hierarchy descriptor required; legacy bootstrap is unsupported".into());
    }
    let descriptor:DescriptorReference=serde_json::from_str(&reference).map_err(|e|e.to_string())?;
    if descriptor.path.is_empty() || descriptor.bytes==0 || descriptor.sha256.len()!=64 || !descriptor.sha256.bytes().all(|b|b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
        return Err("invalid complete hierarchy descriptor reference".into());
    }
    let selected=outputs.iter_mut().find(|s|s.segment_id==vector_segment.id).ok_or("missing vector output")?;
    if selected.file_paths!=expected.parent_vector_files {
        return Err("hierarchy bridge cannot replace other staged vector changes".into());
    }
    selected.file_paths.insert(HIERARCHY_BUNDLE.into(),vec![reference]);
    Ok(outputs)
}

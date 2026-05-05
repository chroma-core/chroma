//! Typed views over the slices of Notion's recordMap / search response that
//! we actually use. Everything else stays as `serde_json::Value`.

use serde::{Deserialize, Serialize};

/// Subset of a `block` recordMap entry we care about for discovery + diffing.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSummary {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub parent_table: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub last_edited_time: Option<i64>,
}

/// One row of `<output>/sidebar.jsonl` (a top-level container in the user's
/// workspace). Same field names as the Python schema.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SidebarEntry {
    pub id: String,
    #[serde(default)]
    pub title: String,
    /// `space_page` or `teamspace_page`.
    #[serde(default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub teamspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub teamspace_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_edited_time: Option<i64>,
    /// Only present in the on-disk sidebar.jsonl, not in the in-memory list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
}

/// One row of `<output>/discovery.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PageRecord {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_edited_time: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
}

/// Raw recordMap shape we navigate. The full schema is huge; we just walk the
/// `block` and `team` tables.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct RecordMapEnvelope {
    #[serde(default)]
    pub block: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub team: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub space: serde_json::Map<String, serde_json::Value>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct SearchResponse {
    #[serde(default)]
    pub results: Vec<serde_json::Value>,
    #[serde(rename = "recordMap", default)]
    pub record_map: Option<serde_json::Value>,
}

/// Structured form of a top-level container as the export loop sees it.
/// (Same shape as `SidebarEntry` minus the on-disk-only `space_id`.)
pub type Container = SidebarEntry;

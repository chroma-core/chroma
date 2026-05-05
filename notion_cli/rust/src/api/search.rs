//! Pagination + recordMap walking on top of `client.search`.

use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;

use super::client::NotionInternal;
use super::types::{BlockSummary, SidebarEntry};

/// Mirror of Python `_search_all`: paginate `/api/v3/search` until exhausted
/// or `max_pages` hit. Returns `(blocks_by_id, teams_by_id)`. Each value is
/// the inner record dict (already unwrapped from its versioned wrapper, like
/// `{"value": {...}}`). De-duplicates across pages.
pub async fn search_all(
    client: &NotionInternal,
    space_id: &str,
    page_size: u32,
    max_pages: u32,
    progress: bool,
) -> Result<(BTreeMap<String, Value>, BTreeMap<String, Value>)> {
    let mut blocks: BTreeMap<String, Value> = BTreeMap::new();
    let mut teams: BTreeMap<String, Value> = BTreeMap::new();
    let mut seen_ids: std::collections::HashSet<String> = Default::default();
    let mut total_seen: u32 = 0;
    let mut last_batch_new: u32 = page_size;
    let mut batch: u32 = 0;
    while last_batch_new > 0 && total_seen < max_pages {
        batch += 1;
        let resp = client.search(space_id, "", page_size, "minimal").await?;
        let results = resp
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if let Some(rmap) = resp.get("recordMap") {
            walk_record_map(rmap, "block", &mut blocks);
            walk_record_map(rmap, "team", &mut teams);
        }
        let mut new_count: u32 = 0;
        for r in &results {
            if let Some(id) = r.get("id").and_then(Value::as_str) {
                if seen_ids.insert(id.to_string()) {
                    new_count += 1;
                }
            }
        }
        last_batch_new = new_count;
        total_seen = total_seen.saturating_add(new_count);
        if progress {
            println!(
                "  [search batch {batch}] returned={} new={new_count} \
                 unique_pages={total_seen} blocks_in_map={} teams_in_map={}",
                results.len(),
                blocks.len(),
                teams.len()
            );
        }
        if (results.len() as u32) < page_size {
            break;
        }
    }
    Ok((blocks, teams))
}

/// Notion stores recordMap entries either as `{ "value": {...} }` or as
/// `{ "value": { "value": {...}, "role": ... } }` depending on endpoint and
/// version. Unwrap both shapes.
pub fn walk_record_map(
    record_map: &Value,
    table: &str,
    out: &mut BTreeMap<String, Value>,
) {
    let Some(table_map) = record_map.get(table).and_then(Value::as_object) else {
        return;
    };
    for (id, raw) in table_map {
        let mut v = raw.clone();
        for _ in 0..2 {
            if let Some(inner) = v.get("value").cloned() {
                v = inner;
            } else {
                break;
            }
        }
        out.insert(id.clone(), v);
    }
}

/// Pull the rendered title of a block from its raw recordMap entry. Mirrors
/// Python `_block_title`: handles `properties.title` (an array of arrays of
/// `[text, ...]`) and falls back to a few other paths.
pub fn block_title(block: &Value) -> String {
    if let Some(props) = block.get("properties") {
        if let Some(title) = props.get("title").and_then(Value::as_array) {
            let mut buf = String::new();
            for chunk in title {
                if let Some(arr) = chunk.as_array() {
                    if let Some(s) = arr.first().and_then(Value::as_str) {
                        buf.push_str(s);
                    }
                }
            }
            if !buf.is_empty() {
                return buf;
            }
        }
    }
    if let Some(name) = block.get("name").and_then(Value::as_str) {
        return name.to_string();
    }
    String::new()
}

/// Convert one raw block record into a `BlockSummary`.
#[allow(dead_code)]
pub fn block_summary(id: &str, b: &Value) -> BlockSummary {
    BlockSummary {
        id: id.to_string(),
        title: block_title(b),
        r#type: b.get("type").and_then(Value::as_str).map(str::to_string),
        parent_table: b
            .get("parent_table")
            .and_then(Value::as_str)
            .map(str::to_string),
        parent_id: b
            .get("parent_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        last_edited_time: b
            .get("last_edited_time")
            .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))),
    }
}

/// Derive the in-memory sidebar (top-level containers) from a fully-walked
/// blocks + teams recordMap. Same filtering rules as Python
/// `_walk_sidebar_top_level` and `cmd_discover`'s inline derivation.
pub fn derive_sidebar(
    blocks: &BTreeMap<String, Value>,
    teams: &BTreeMap<String, Value>,
    space_id: &str,
) -> Vec<SidebarEntry> {
    let mut out: Vec<SidebarEntry> = Vec::new();
    for (bid, b) in blocks {
        let parent_table = b
            .get("parent_table")
            .and_then(Value::as_str)
            .unwrap_or("");
        let parent_id = b.get("parent_id").and_then(Value::as_str).unwrap_or("");
        let last_edited = b
            .get("last_edited_time")
            .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)));
        if parent_table == "space" && parent_id == space_id {
            out.push(SidebarEntry {
                id: bid.clone(),
                title: block_title(b),
                kind: "space_page".into(),
                r#type: b.get("type").and_then(Value::as_str).map(str::to_string),
                teamspace_id: None,
                teamspace_name: None,
                last_edited_time: last_edited,
                space_id: None,
            });
        } else if parent_table == "team" {
            let team = teams.get(parent_id);
            let team_space_id = team
                .and_then(|t| t.get("space_id"))
                .and_then(Value::as_str);
            if let Some(t_space) = team_space_id {
                if t_space != space_id {
                    continue;
                }
            }
            let team_name = team
                .and_then(|t| t.get("name"))
                .and_then(Value::as_str)
                .map(str::to_string);
            out.push(SidebarEntry {
                id: bid.clone(),
                title: block_title(b),
                kind: "teamspace_page".into(),
                r#type: b.get("type").and_then(Value::as_str).map(str::to_string),
                teamspace_id: Some(parent_id.to_string()),
                teamspace_name: team_name,
                last_edited_time: last_edited,
                space_id: None,
            });
        }
    }
    out.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.teamspace_name.cmp(&b.teamspace_name))
            .then_with(|| a.title.cmp(&b.title))
    });
    out
}

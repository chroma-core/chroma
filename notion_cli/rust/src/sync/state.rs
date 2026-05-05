//! Loader for the previous-run state used to compute the dirty-container plan.
//!
//! Reads `discovery.jsonl` and `dump.summary.jsonl` (both produced by prior
//! runs of this same binary, or by the legacy Python script — schema is
//! intentionally compatible). Also enumerates `exports/` so we can detect
//! containers whose on-disk export is missing even though we have history
//! for them.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::api::PageRecord;

/// Rolled-up history of one container's prior exports.
#[derive(Debug, Clone, Default)]
pub struct ContainerHistory {
    /// Latest successful export's epoch ms (`dumped_at_ms`). May be `None` if
    /// the legacy Python summary lines didn't carry this field.
    pub last_dumped_at_ms: Option<i64>,
    /// `true` if at least one prior summary line claims `ok: true` and the
    /// matching `exports/<dir>/` is still on disk.
    pub on_disk: bool,
    /// Path to the unzipped dir (if present), used by Merkle baseline scans
    /// and changelog emission.
    pub unzipped_dir: Option<PathBuf>,
    /// Slug used in the directory name from the prior run, so we can detect
    /// title renames and avoid leaving orphan dirs around.
    pub dir_slug: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PrevState {
    /// `id -> PageRecord` from the previous `discovery.jsonl`.
    pub pages: BTreeMap<String, PageRecord>,
    /// `container_id -> history`, derived from `dump.summary.jsonl` + walking
    /// `exports/`.
    pub containers: BTreeMap<String, ContainerHistory>,
    /// The space id baked into the previous discovery.jsonl, if any. Detect
    /// space-id mismatch and warn (don't auto-truncate).
    pub last_space_id: Option<String>,
}

impl PrevState {
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty() && self.containers.is_empty()
    }
}

#[derive(Debug, Deserialize)]
struct LegacyContainerField {
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DumpSummaryLine {
    container: LegacyContainerField,
    #[serde(default)]
    ok: bool,
    /// Set by the new Rust dumper. Older Python lines don't have it.
    #[serde(default)]
    dumped_at_ms: Option<i64>,
    #[serde(default, alias = "zip")]
    zip_path: Option<String>,
}

pub fn load_prev_state(output_dir: &Path) -> Result<PrevState> {
    let mut out = PrevState::default();

    // 1. discovery.jsonl
    let disc = output_dir.join("discovery.jsonl");
    if disc.exists() {
        let content =
            std::fs::read_to_string(&disc).with_context(|| format!("reading {}", disc.display()))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<PageRecord>(line) {
                Ok(p) => {
                    if out.last_space_id.is_none() {
                        out.last_space_id = p.space_id.clone();
                    }
                    out.pages.insert(p.id.clone(), p);
                }
                Err(_) => continue,
            }
        }
    }

    // 2. dump.summary.jsonl - rollup per container
    let sum = output_dir.join("dump.summary.jsonl");
    if sum.exists() {
        let content =
            std::fs::read_to_string(&sum).with_context(|| format!("reading {}", sum.display()))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parsed: DumpSummaryLine = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if !parsed.ok {
                continue;
            }
            let h = out
                .containers
                .entry(parsed.container.id.clone())
                .or_default();
            // Keep the latest dumped_at_ms; `None` is treated as oldest.
            if parsed.dumped_at_ms > h.last_dumped_at_ms {
                h.last_dumped_at_ms = parsed.dumped_at_ms;
            }
            // Try to recover the dir slug from the zip path so we can detect
            // legacy dirs that lived under a now-stale slug.
            if h.dir_slug.is_none() {
                if let Some(zp) = parsed.zip_path.as_deref() {
                    if let Some(parent) = std::path::Path::new(zp).parent() {
                        if let Some(name) = parent.file_name().and_then(|s| s.to_str()) {
                            h.dir_slug = Some(name.to_string());
                        }
                    }
                }
            }
        }
    }

    // 3. Cross-check `exports/<slug>__<id>/` actually exists on disk. This is
    //    cheap enough (one readdir + parse the trailing UUID) and lets us
    //    treat half-completed previous runs as needing re-export.
    let exports_dir = output_dir.join("exports");
    if exports_dir.is_dir() {
        if let Ok(read) = std::fs::read_dir(&exports_dir) {
            for entry in read.flatten() {
                let p = entry.path();
                if !p.is_dir() {
                    continue;
                }
                let name = match p.file_name().and_then(|s| s.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                let Some(id) = parse_id_from_dir_name(&name) else {
                    continue;
                };
                let h = out.containers.entry(id).or_default();
                h.on_disk = true;
                let unzipped = p.join("unzipped");
                if unzipped.is_dir() {
                    h.unzipped_dir = Some(unzipped);
                }
                if h.dir_slug.is_none() {
                    h.dir_slug = Some(name);
                }
            }
        }
    }

    Ok(out)
}

/// Container dir names are `"{slug}__{uuid}"`. Pull out the trailing UUID.
fn parse_id_from_dir_name(name: &str) -> Option<String> {
    let idx = name.rfind("__")?;
    let candidate = &name[idx + 2..];
    if candidate.len() == 36 && candidate.chars().filter(|c| *c == '-').count() == 4 {
        Some(candidate.to_string())
    } else {
        None
    }
}

/// Convert the raw `BTreeMap<String, Value>` produced by `_search_all` into
/// the `PageRecord` shape used by the diff. `space_id` is stamped into every
/// record so the on-disk discovery.jsonl is self-describing.
pub fn pages_from_search(
    blocks: &BTreeMap<String, Value>,
    space_id: &str,
) -> BTreeMap<String, PageRecord> {
    use crate::api::search::block_title;
    let mut out: BTreeMap<String, PageRecord> = BTreeMap::new();
    for (id, b) in blocks {
        let last_edited_time = b
            .get("last_edited_time")
            .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)));
        out.insert(
            id.clone(),
            PageRecord {
                id: id.clone(),
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
                last_edited_time,
                space_id: Some(space_id.to_string()),
            },
        );
    }
    out
}

/// Persist the latest sync-run metadata next to the changelog. Idempotent.
#[derive(Debug, Serialize, Deserialize)]
pub struct LastRunMeta {
    pub sync_run_id: String,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    pub dirty_count: usize,
    pub removed_count: usize,
    pub changelog_entries: usize,
    pub workspace_merkle_root_hex: Option<String>,
}

pub fn write_last_run(output_dir: &Path, meta: &LastRunMeta) -> Result<()> {
    let dir = output_dir.join("_state");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("last_run.json");
    let bytes = serde_json::to_vec_pretty(meta)?;
    crate::util::write_atomic(path, &bytes)?;
    Ok(())
}

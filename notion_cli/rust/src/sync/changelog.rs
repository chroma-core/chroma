//! Append-only `dump.changelog.jsonl` writer.
//!
//! One line per (container x file x change-type) tuple. Downstream consumers
//! (the Chroma upserter that's coming) tail this file and:
//!
//!   - `added` / `modified`  -> upsert the file's contents into Chroma keyed
//!     on `{container_id}/{file_path}`.
//!   - `removed`             -> delete by the same key.
//!
//! The schema is intentionally flat / one-object-per-line so consumers can
//! stream-parse it without loading the whole file. Future fields are
//! additive; consumers should ignore unknown keys.
//!
//! Each line:
//! ```json
//! {
//!   "sync_run_id": "2026-05-04T18:30:00.123Z",
//!   "container_id": "192db242-39af-...",
//!   "container_title": "Archive",
//!   "container_kind": "space_page",
//!   "change_type": "added" | "modified" | "removed",
//!   "file_path": "Private & Shared/Archive/Support.md",
//!   "absolute_path": "/abs/.../Archive__192d.../unzipped/Private & Shared/Archive/Support.md",
//!   "size_bytes": 1234,                  // null for removed
//!   "content_hash": "sha256:0x...",      // null for removed
//!   "ts_ms": 1714851000123
//! }
//! ```

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::merkle::{ContainerHashMap, FileLeaf, HashDiff};
use crate::api::Container;

/// In-memory representation of one changelog line. Matches the on-disk schema
/// 1:1 (we intentionally keep this as the single source of truth).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangelogEntry {
    pub sync_run_id: String,
    pub container_id: String,
    pub container_title: String,
    pub container_kind: String,
    pub change_type: ChangeType,
    pub file_path: String,
    pub absolute_path: String,
    pub size_bytes: Option<u64>,
    pub content_hash: Option<String>,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChangeType {
    Added,
    Modified,
    Removed,
}

/// Build the flat list of changelog entries for one container's diff.
///
/// `unzipped_dir` is the container's unzipped/ root after the new export
/// landed (used to compute `absolute_path` for added/modified entries).
/// `prev_leaves` is the leaf map from the previous run (used to produce
/// `content_hash` and `size_bytes` for `removed` entries).
pub fn entries_for_container_diff(
    sync_run_id: &str,
    container: &Container,
    unzipped_dir: &Path,
    diff: &HashDiff,
    new_leaves: &ContainerHashMap,
    prev_leaves: &ContainerHashMap,
    ts_ms: i64,
) -> Vec<ChangelogEntry> {
    let mut out = Vec::with_capacity(diff.added.len() + diff.modified.len() + diff.removed.len());
    for path in &diff.added {
        if let Some(leaf) = new_leaves.get(path) {
            out.push(present_entry(
                sync_run_id,
                container,
                unzipped_dir,
                ChangeType::Added,
                path,
                leaf,
                ts_ms,
            ));
        }
    }
    for path in &diff.modified {
        if let Some(leaf) = new_leaves.get(path) {
            out.push(present_entry(
                sync_run_id,
                container,
                unzipped_dir,
                ChangeType::Modified,
                path,
                leaf,
                ts_ms,
            ));
        }
    }
    for path in &diff.removed {
        let prev = prev_leaves.get(path);
        out.push(ChangelogEntry {
            sync_run_id: sync_run_id.to_string(),
            container_id: container.id.clone(),
            container_title: container.title.clone(),
            container_kind: container.kind.clone(),
            change_type: ChangeType::Removed,
            file_path: path.clone(),
            absolute_path: absolute_in(unzipped_dir, path).display().to_string(),
            size_bytes: prev.map(|l| l.size_bytes),
            content_hash: prev.map(|l| format!("sha256:0x{}", l.sha256)),
            ts_ms,
        });
    }
    out
}

/// Build the changelog entries for a tombstoned container -- every leaf the
/// container previously had becomes a `removed` entry.
pub fn entries_for_tombstone(
    sync_run_id: &str,
    container_id: &str,
    container_title: &str,
    container_kind: &str,
    prev_unzipped_dir: Option<&Path>,
    prev_leaves: &ContainerHashMap,
    ts_ms: i64,
) -> Vec<ChangelogEntry> {
    let mut out = Vec::with_capacity(prev_leaves.len());
    for (path, leaf) in prev_leaves {
        let abs = match prev_unzipped_dir {
            Some(d) => absolute_in(d, path).display().to_string(),
            None => path.clone(),
        };
        out.push(ChangelogEntry {
            sync_run_id: sync_run_id.to_string(),
            container_id: container_id.to_string(),
            container_title: container_title.to_string(),
            container_kind: container_kind.to_string(),
            change_type: ChangeType::Removed,
            file_path: path.clone(),
            absolute_path: abs,
            size_bytes: Some(leaf.size_bytes),
            content_hash: Some(format!("sha256:0x{}", leaf.sha256)),
            ts_ms,
        });
    }
    out
}

fn present_entry(
    sync_run_id: &str,
    container: &Container,
    unzipped_dir: &Path,
    change: ChangeType,
    rel_path: &str,
    leaf: &FileLeaf,
    ts_ms: i64,
) -> ChangelogEntry {
    ChangelogEntry {
        sync_run_id: sync_run_id.to_string(),
        container_id: container.id.clone(),
        container_title: container.title.clone(),
        container_kind: container.kind.clone(),
        change_type: change,
        file_path: rel_path.to_string(),
        absolute_path: absolute_in(unzipped_dir, rel_path).display().to_string(),
        size_bytes: Some(leaf.size_bytes),
        content_hash: Some(format!("sha256:0x{}", leaf.sha256)),
        ts_ms,
    }
}

fn absolute_in(unzipped_dir: &Path, rel: &str) -> PathBuf {
    let mut p = unzipped_dir.to_path_buf();
    for part in rel.split('/').filter(|s| !s.is_empty()) {
        p.push(part);
    }
    p
}

/// Append a batch of entries to `<output>/dump.changelog.jsonl`.
/// Locks via O_APPEND so concurrent writers are safe (we don't actually do
/// concurrent writes today, but cheap insurance).
pub fn append_changelog_entries(output_dir: &Path, entries: &[ChangelogEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join("dump.changelog.jsonl");
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    for e in entries {
        let line = serde_json::to_string(e).context("serializing changelog entry")?;
        writeln!(f, "{line}").with_context(|| format!("writing to {}", path.display()))?;
    }
    f.flush().ok();
    Ok(())
}

//! Subtree-stitching: replace a sub-portion of a container's `unzipped/`
//! tree with a freshly-exported sub-block ZIP.
//!
//! Notion's filename convention (verified against real exports):
//!   - Every page becomes a file `<title> <bare_uuid>.md` where `bare_uuid`
//!     is the block id with hyphens stripped.
//!   - Pages with children additionally get a sibling directory named just
//!     `<title>/` (no UUID, same title with identical sanitisation).
//!
//! So given a block id, we can find its on-disk anchors anywhere in a
//! container's `unzipped/` tree by recursive walk + suffix match. After a
//! sub-block export we know the LCA's block id, so we replace those two
//! anchors atomically: `rm` the old `.md`, `rm -rf` the old children dir,
//! `mv` the new ones (which may have a different title if the page was
//! renamed) into the same parent dir.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct SubtreeAnchors {
    /// The `<title> <bare_uuid>.md` file.
    pub md_file: PathBuf,
    /// The sibling `<title>/` directory, present iff the page has children.
    pub children_dir: Option<PathBuf>,
    /// The directory containing both anchors. Stable across renames; this
    /// is where we move the freshly-exported anchors to.
    pub parent_dir: PathBuf,
    /// Title we recovered from the .md filename. Surfaced for debug/log
    /// output even when we don't directly format it on the happy path.
    #[allow(dead_code)]
    pub title: String,
}

/// Recursively walk `root`, find the `.md` file ending in ` {bare_uuid}.md`,
/// and return the anchor pair. Returns `Ok(None)` if no such file exists in
/// the tree (e.g. the page was created on Notion since the last full export
/// and isn't yet on disk; caller should fall back to a full container
/// re-export so the new page lands).
pub fn find_anchors(root: &Path, block_id: &str) -> Result<Option<SubtreeAnchors>> {
    let bare = block_id.replace('-', "");
    let suffix = format!(" {bare}.md");
    for entry in walkdir::WalkDir::new(root).follow_links(false) {
        let entry = entry.with_context(|| format!("walking {}", root.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let name = match entry.file_name().to_str() {
            Some(s) => s,
            None => continue,
        };
        if !name.ends_with(&suffix) {
            continue;
        }
        let title = name
            .strip_suffix(&suffix)
            .ok_or_else(|| anyhow!("strip_suffix failed for {name}"))?
            .to_string();
        let parent = entry
            .path()
            .parent()
            .ok_or_else(|| anyhow!("no parent for {}", entry.path().display()))?
            .to_path_buf();
        let candidate_dir = parent.join(&title);
        let children_dir = if candidate_dir.is_dir() {
            Some(candidate_dir)
        } else {
            None
        };
        return Ok(Some(SubtreeAnchors {
            md_file: entry.path().to_path_buf(),
            children_dir,
            parent_dir: parent,
            title,
        }));
    }
    Ok(None)
}

#[derive(Debug, Clone)]
pub struct StitchOutcome {
    /// Paths under the container's unzipped/ root that we removed. These
    /// will surface in the changelog as `removed` if no replacement is
    /// written -- but the normal case is that we then write replacement
    /// files at `placed_paths`. Not consumed today; kept around so we can
    /// log a per-stitch breakdown if subtree mode misbehaves in the wild.
    #[allow(dead_code)]
    pub removed_anchors: Vec<PathBuf>,
    /// Final on-disk paths (under the container's unzipped/ root) of the
    /// freshly-stitched anchors. Same rationale as `removed_anchors`.
    #[allow(dead_code)]
    pub placed_paths: Vec<PathBuf>,
}

/// Replace the existing on-disk anchors for `block_id` with the freshly
/// exported anchors found inside `fresh_root`. Both directories live on the
/// same filesystem (we put `.subtree.tmp/` inside the container's export
/// dir), so `rename` is atomic.
pub fn stitch_subtree(
    container_unzipped_root: &Path,
    fresh_root: &Path,
    block_id: &str,
) -> Result<StitchOutcome> {
    let prev = find_anchors(container_unzipped_root, block_id)?
        .ok_or_else(|| anyhow!(
            "subtree-export: block {block_id} has no .md anchor in {} -- \
             this happens when the page was created since the last full \
             export. Caller should fall back to a full container export.",
            container_unzipped_root.display()
        ))?;
    let new = find_anchors(fresh_root, block_id)?
        .ok_or_else(|| anyhow!(
            "subtree-export: block {block_id} has no .md anchor in the \
             freshly-extracted ZIP at {} -- the export response didn't \
             include the block we asked for, which is a Notion API bug. \
             Falling back to full container export is the safe move.",
            fresh_root.display()
        ))?;

    let mut removed_anchors: Vec<PathBuf> = Vec::new();
    let mut placed_paths: Vec<PathBuf> = Vec::new();

    // Remove old.
    if prev.md_file.exists() {
        std::fs::remove_file(&prev.md_file)
            .with_context(|| format!("removing {}", prev.md_file.display()))?;
        removed_anchors.push(prev.md_file.clone());
    }
    if let Some(d) = &prev.children_dir {
        if d.exists() {
            std::fs::remove_dir_all(d)
                .with_context(|| format!("removing {}", d.display()))?;
            removed_anchors.push(d.clone());
        }
    }

    // Place new under prev.parent_dir, using NEW filenames (which differ if
    // the page was renamed in Notion). The new files keep the bare_uuid
    // suffix, so future stitches still find them.
    let new_md_name = new
        .md_file
        .file_name()
        .ok_or_else(|| anyhow!("new md file has no name"))?
        .to_owned();
    let dest_md = prev.parent_dir.join(&new_md_name);
    std::fs::rename(&new.md_file, &dest_md)
        .with_context(|| format!("rename {} -> {}", new.md_file.display(), dest_md.display()))?;
    placed_paths.push(dest_md);

    if let Some(new_dir) = &new.children_dir {
        let new_dir_name = new_dir
            .file_name()
            .ok_or_else(|| anyhow!("new children dir has no name"))?
            .to_owned();
        let dest_dir = prev.parent_dir.join(&new_dir_name);
        std::fs::rename(new_dir, &dest_dir).with_context(|| {
            format!("rename {} -> {}", new_dir.display(), dest_dir.display())
        })?;
        placed_paths.push(dest_dir);
    }

    Ok(StitchOutcome {
        removed_anchors,
        placed_paths,
    })
}

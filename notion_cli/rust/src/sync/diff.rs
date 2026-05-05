//! `compute_dirty_containers`: turn current pages + sidebar into a re-export
//! plan, given previous state.
//!
//! The unit of re-export is a *top-level container* (a block whose
//! `parent_table` is `space` or `team`). Each leaf page in `discovery.jsonl`
//! belongs to exactly one container -- the one you reach by walking
//! `parent_id` up to a `parent_table in {space, team}` block.
//!
//! A container is dirty if any of:
//!   - it's brand new (in current sidebar, no prior history, no on-disk dir)
//!   - any descendant page's `last_edited_time` advanced vs prev state
//!   - any descendant page's `parent_id` changed (move within the workspace)
//!   - the container has prior pages in `discovery.jsonl` but no on-disk
//!     `exports/<...>/unzipped/` (interrupted previous run)
//!   - a descendant page disappeared (deletion within the container)

use std::collections::{BTreeMap, BTreeSet};

use crate::api::{Container, PageRecord};

use super::state::PrevState;

#[derive(Debug, Clone, Default)]
pub struct DirtyPlan {
    /// Container ids to re-export.
    pub dirty: BTreeSet<String>,
    /// Per-dirty-container, the set of *page* ids inside it that triggered
    /// the dirty bit. Used by `--subtree-export` mode to compute the
    /// lowest-common-ancestor block to re-export instead of the full
    /// container. Empty set for a given container means "no per-page hint --
    /// fall back to full export" (e.g. the container had no on-disk export
    /// to begin with).
    pub dirty_pages_by_container: BTreeMap<String, BTreeSet<String>>,
    /// Container ids that existed before but are gone now. Tombstone or prune.
    pub removed: BTreeSet<String>,
    /// Per-container human-readable reasons (multiple reasons fold into one
    /// list per container). Useful for `--dry-run` output.
    pub reasons: BTreeMap<String, Vec<String>>,
    /// `true` if the previous discovery + summary state was empty. Callers
    /// usually treat this as "do a full dump" rather than enumerate every
    /// container as dirty.
    pub is_first_run: bool,
    /// `true` if the current sidebar has containers that have *prior* pages
    /// in discovery.jsonl but no on-disk export. Common after upgrading from
    /// the legacy Python script that didn't always finish.
    pub has_partial_prior_exports: bool,
}

pub fn compute_dirty_containers(
    prev: &PrevState,
    curr_pages: &BTreeMap<String, PageRecord>,
    curr_sidebar: &[Container],
) -> DirtyPlan {
    let mut plan = DirtyPlan {
        is_first_run: prev.is_empty(),
        ..Default::default()
    };

    let curr_ids: BTreeSet<&str> = curr_sidebar.iter().map(|c| c.id.as_str()).collect();
    let prev_container_ids: BTreeSet<&str> =
        prev.containers.keys().map(|s| s.as_str()).collect();

    // 1. Build prev parent->container map: for each previous page, walk to
    //    its container (using prev page records) and remember the answer.
    //    We do the same for curr pages and compare.
    let prev_container_of: BTreeMap<&str, String> = build_container_index(&prev.pages);
    let curr_container_of: BTreeMap<&str, String> = build_container_index(curr_pages);

    // 2. Walk current pages, mark their container dirty if anything changed
    //    vs prev.
    for (pid, curr) in curr_pages {
        let curr_cid = match curr_container_of.get(pid.as_str()) {
            Some(c) => c.clone(),
            None => continue, // orphan page (rare, parent walked off the graph)
        };
        if !curr_ids.contains(curr_cid.as_str()) {
            // page belongs to something not in the current sidebar (e.g. a
            // collection that's its own thing). Skip.
            continue;
        }
        let prev_page = prev.pages.get(pid);
        match prev_page {
            None => {
                add_reason(
                    &mut plan,
                    &curr_cid,
                    format!("new page {}", short(pid)),
                    Some(pid),
                );
            }
            Some(p) => {
                if p.last_edited_time.unwrap_or(0) < curr.last_edited_time.unwrap_or(0) {
                    add_reason(
                        &mut plan,
                        &curr_cid,
                        format!("page edited {}", short(pid)),
                        Some(pid),
                    );
                }
                if p.parent_id != curr.parent_id {
                    add_reason(
                        &mut plan,
                        &curr_cid,
                        format!("page moved-in {}", short(pid)),
                        Some(pid),
                    );
                    if let Some(old_cid) = prev_container_of.get(pid.as_str()) {
                        if old_cid.as_str() != curr_cid.as_str()
                            && curr_ids.contains(old_cid.as_str())
                        {
                            add_reason(
                                &mut plan,
                                old_cid,
                                format!("page moved-out {}", short(pid)),
                                Some(pid),
                            );
                        }
                    }
                }
            }
        }
    }

    // 3. Walk previous pages: if a page is gone now, the container that owned
    //    it (if it still exists) is dirty.
    for (pid, prev_p) in &prev.pages {
        if curr_pages.contains_key(pid) {
            continue;
        }
        let Some(prev_cid) = prev_container_of.get(pid.as_str()) else {
            continue;
        };
        if curr_ids.contains(prev_cid.as_str()) {
            add_reason(
                &mut plan,
                prev_cid,
                format!(
                    "page deleted {} (was '{}')",
                    short(pid),
                    truncate_for_reason(&prev_p.title)
                ),
                Some(pid),
            );
        }
    }

    // 4. Containers that exist now but never had a successful export.
    //    Note: we deliberately do NOT add a per-page hint here -- the
    //    subtree-export path keys off `dirty_pages_by_container`, and an
    //    empty set means "no useful LCA, just do full export".
    for c in curr_sidebar {
        let h = prev.containers.get(&c.id);
        let on_disk = h.map(|h| h.on_disk).unwrap_or(false);
        if !on_disk {
            add_reason(&mut plan, &c.id, "no on-disk export".into(), None);
            if h.is_some() {
                plan.has_partial_prior_exports = true;
            }
        }
    }

    // 5. Containers that existed before but are gone from the current sidebar.
    for cid in &prev_container_ids {
        if !curr_ids.contains(cid) {
            plan.removed.insert((*cid).to_string());
        }
    }

    plan
}

/// Build an index `page_id -> container_id` by walking `parent_id` up to a
/// `parent_table in {space, team}` block. Pages whose chain can't be resolved
/// (because of a broken parent link or because the chain hits a record we
/// don't have in `pages`) are omitted from the index.
fn build_container_index(pages: &BTreeMap<String, PageRecord>) -> BTreeMap<&str, String> {
    let mut out: BTreeMap<&str, String> = BTreeMap::new();
    let mut cache: BTreeMap<&str, Option<String>> = BTreeMap::new();
    for id in pages.keys() {
        let cid = walk_to_container_root(id.as_str(), pages, &mut cache);
        if let Some(c) = cid {
            out.insert(id.as_str(), c);
        }
    }
    out
}

/// Recursive parent walk with memoisation. Returns `Some(container_id)` if the
/// chain terminates in a `space` or `team` parent_table; `None` otherwise.
pub fn walk_to_container_root<'a>(
    page_id: &'a str,
    pages: &'a BTreeMap<String, PageRecord>,
    cache: &mut BTreeMap<&'a str, Option<String>>,
) -> Option<String> {
    if let Some(hit) = cache.get(page_id) {
        return hit.clone();
    }
    // Cycle/depth guard.
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut cur = page_id;
    let mut steps = 0usize;
    loop {
        if steps > 256 || !visited.insert(cur) {
            cache.insert(page_id, None);
            return None;
        }
        steps += 1;
        let Some(rec) = pages.get(cur) else {
            cache.insert(page_id, None);
            return None;
        };
        match rec.parent_table.as_deref() {
            Some("space") | Some("team") => {
                let answer = Some(rec.id.clone());
                cache.insert(page_id, answer.clone());
                return answer;
            }
            _ => {}
        }
        let Some(parent) = rec.parent_id.as_deref() else {
            cache.insert(page_id, None);
            return None;
        };
        cur = parent;
    }
}

fn add_reason(
    plan: &mut DirtyPlan,
    container_id: &str,
    reason: String,
    page_id: Option<&str>,
) {
    plan.dirty.insert(container_id.to_string());
    plan.reasons
        .entry(container_id.to_string())
        .or_default()
        .push(reason);
    if let Some(pid) = page_id {
        plan.dirty_pages_by_container
            .entry(container_id.to_string())
            .or_default()
            .insert(pid.to_string());
    }
}

fn short(uuid: &str) -> String {
    if uuid.len() >= 8 {
        format!("{}…", &uuid[..8])
    } else {
        uuid.to_string()
    }
}

fn truncate_for_reason(s: &str) -> String {
    s.chars().take(40).collect()
}

// ---------------------------------------------------------------------------
// LCA / subtree-export helpers
// ---------------------------------------------------------------------------

/// Lowest common ancestor of a set of dirty pages within a single container.
///
/// Returns the block id at which to call `enqueueExportBlock` such that the
/// resulting ZIP covers every dirty page. When the LCA is the container
/// itself (or can't be resolved), returns `None` -- caller should then do
/// the existing full-container export.
///
/// We accept both `curr_pages` and `prev_pages` because some "dirty" pages
/// were deleted (so they're not in curr) and we still need to walk *their*
/// previous parent chain to know which subtree-on-disk to refresh. For
/// each page we try curr first, fall back to prev.
pub fn compute_lca(
    container_id: &str,
    dirty_page_ids: &BTreeSet<String>,
    curr_pages: &BTreeMap<String, crate::api::PageRecord>,
    prev_pages: &BTreeMap<String, crate::api::PageRecord>,
) -> Option<String> {
    if dirty_page_ids.is_empty() {
        return None;
    }
    let mut chains: Vec<Vec<String>> = Vec::with_capacity(dirty_page_ids.len());
    for pid in dirty_page_ids {
        let chain = walk_chain_to(container_id, pid, curr_pages)
            .or_else(|| walk_chain_to(container_id, pid, prev_pages));
        match chain {
            Some(c) if !c.is_empty() => chains.push(c),
            _ => return None, // unresolvable -> bail to full
        }
    }
    Some(longest_common_prefix_last(&chains))
}

/// Returns root-first chain: `[container_id, ..., page_id]`.
/// Returns `None` if the parent chain doesn't terminate at `container_id`
/// within the depth bound.
fn walk_chain_to(
    container_id: &str,
    page_id: &str,
    pages: &BTreeMap<String, crate::api::PageRecord>,
) -> Option<Vec<String>> {
    let mut out: Vec<String> = vec![page_id.to_string()];
    let mut cur = page_id.to_string();
    let mut steps = 0usize;
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        if cur == container_id {
            out.reverse();
            return Some(out);
        }
        steps += 1;
        if steps > 256 || !visited.insert(cur.clone()) {
            return None;
        }
        let rec = pages.get(&cur)?;
        let parent = rec.parent_id.clone()?;
        out.push(parent.clone());
        cur = parent;
    }
}

/// Longest common prefix of root-first chains, returning the last id of the
/// shared prefix. Caller pre-validates that all chains share `chains[i][0]`
/// (the container_id).
fn longest_common_prefix_last(chains: &[Vec<String>]) -> String {
    let first = &chains[0];
    let mut deepest_common = first[0].clone();
    for i in 0..first.len() {
        let probe = &first[i];
        for c in &chains[1..] {
            if c.get(i) != Some(probe) {
                return deepest_common;
            }
        }
        deepest_common = probe.clone();
    }
    deepest_common
}

/// Count the number of pages in the subtree rooted at `block_id` (inclusive),
/// using `pages` for the parent index. Used by the subtree-vs-full heuristic.
pub fn count_subtree_pages(
    block_id: &str,
    pages: &BTreeMap<String, crate::api::PageRecord>,
) -> usize {
    let mut children_of: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for p in pages.values() {
        if let Some(parent) = p.parent_id.as_deref() {
            children_of.entry(parent).or_default().push(p.id.as_str());
        }
    }
    let mut count = 0usize;
    let mut stack = vec![block_id];
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    while let Some(cur) = stack.pop() {
        if !visited.insert(cur) {
            continue;
        }
        count += 1;
        if let Some(kids) = children_of.get(cur) {
            stack.extend_from_slice(kids);
        }
    }
    count
}

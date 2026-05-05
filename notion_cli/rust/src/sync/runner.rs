//! `run_incremental_dump`: the orchestrator that ties discovery → diff →
//! filtered exports → per-file Merkle hashing → changelog → atomic
//! discovery.jsonl swap into one call.
//!
//! Used by both `cmd dump` (the user-facing default-incremental command)
//! and `cmd sync` (the quiet daemon wrapper). The shape of the inputs is
//! the union of both surfaces; defaults applied by the caller.

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::api::{
    export::{export_one, export_subtree, ExportResult},
    search::{derive_sidebar, search_all},
    Container, NotionInternal, PageRecord, TaskPool, TokenBucket,
};
use crate::util::{container_dir_name, now_ms, now_iso8601, truncate_str, write_atomic};

use super::changelog::{
    append_changelog_entries, entries_for_container_diff, entries_for_tombstone, ChangelogEntry,
};
use super::diff::{compute_dirty_containers, compute_lca, count_subtree_pages, DirtyPlan};
use super::merkle::{
    container_merkle_root_hex, delete_hash_map, diff_hash_maps, hash_directory, load_hash_map,
    previous_leaves_or_empty, save_hash_map, workspace_merkle_root_hex, ContainerHashMap,
};
use super::state::{load_prev_state, pages_from_search, write_last_run, LastRunMeta};
use super::stitch::stitch_subtree;

#[derive(Debug, Clone)]
pub struct IncrementalParams {
    pub output: PathBuf,
    pub space_id: String,
    pub space_name: String,
    pub parallel: usize,
    pub poll_interval_s: f64,
    pub poll_bucket: TokenBucket,
    pub task_timeout_s: f64,
    pub include_files: String,
    pub unzip: bool,
    pub page_size: u32,
    pub max_pages: u32,
    /// `Some(set)` means: only consider these container ids (or titles) as
    /// candidates. Applied *after* discovery.
    pub only: Option<Vec<String>>,
    pub skip: Option<Vec<String>>,
    pub full_resync: bool,
    pub dry_run: bool,
    /// `true` = actually delete tombstoned container dirs. `false` = move them
    /// to `<output>/.tombstones/<id>_<ts>/` so the next run can recover.
    pub prune_removed: bool,
    /// Quiet mode: skip per-batch search progress and only print the final
    /// summary. Used by `sync`.
    pub quiet: bool,
    /// Opt-in: when a dirty container has a non-trivial LCA (i.e. all dirty
    /// pages share an ancestor strictly below the container root) AND the
    /// LCA's subtree is at most `subtree_max_fraction` of the container's
    /// total page count, re-export only the LCA and stitch it into the
    /// existing on-disk tree instead of re-exporting the full container.
    /// Recommended for users with very large top-level containers (1000+
    /// pages). Pair with a documented periodic `--full` to absorb
    /// cross-subtree link / database refreshes. Default: off.
    pub subtree_export: bool,
    /// Upper bound on subtree-vs-container page ratio to actually use the
    /// subtree path. e.g. `0.5` (the default when --subtree-export is on)
    /// means: only do subtree if it covers at most 50% of the container.
    /// Above that, fall back to a full container export -- the savings
    /// don't justify the extra link-drift risk.
    pub subtree_max_fraction: f64,
}

#[derive(Debug, Clone)]
enum ExportMode {
    Full,
    Subtree {
        block_id: String,
        block_label: String,
        subtree_pages: usize,
        /// Total page count of the parent container -- carried for future
        /// per-line reporting ("Engineering (subtree, 30/992 pages)") even
        /// though the current report formatter only uses subtree_pages.
        #[allow(dead_code)]
        container_pages: usize,
    },
}

#[derive(Debug, Clone, Default)]
pub struct IncrementalRunSummary {
    pub sync_run_id: String,
    pub containers_seen: usize,
    pub dirty_count: usize,
    pub removed_count: usize,
    pub exported_ok: usize,
    pub exported_fail: usize,
    pub changelog_entries_added: usize,
    pub changelog_entries_modified: usize,
    pub changelog_entries_removed: usize,
    pub workspace_merkle_root_hex: Option<String>,
    pub elapsed_s: f64,
}

pub async fn run_incremental_dump(
    client: NotionInternal,
    params: IncrementalParams,
) -> Result<IncrementalRunSummary> {
    let started = Instant::now();
    let sync_run_id = now_iso8601();
    let started_ms = now_ms();

    if !params.quiet {
        println!(
            "space:  '{}' ({})  sync_run_id={}",
            params.space_name, params.space_id, sync_run_id
        );
    }

    let output = params.output.clone();
    tokio::fs::create_dir_all(&output)
        .await
        .with_context(|| format!("creating {}", output.display()))?;
    let state_dir = output.join("_state");
    tokio::fs::create_dir_all(&state_dir).await.ok();

    // 1. Load previous on-disk state.
    let prev_state = load_prev_state(&output)?;
    if !params.quiet {
        if prev_state.is_empty() {
            println!("prev state: empty (first run, will export everything)");
        } else {
            println!(
                "prev state: {} pages, {} containers (last_dumped baseline)",
                prev_state.pages.len(),
                prev_state.containers.len()
            );
        }
        if let Some(prev_space) = &prev_state.last_space_id {
            if prev_space != &params.space_id {
                println!(
                    "WARNING: previous discovery.jsonl was for space {} but we're \
                     now dumping {}. Consider --full or a fresh --output.",
                    prev_space, params.space_id
                );
            }
        }
    }

    // 2. Walk /api/v3/search to get the current state.
    if !params.quiet {
        println!(
            "--- /api/v3/search (page_size {}, max_pages {}) ---",
            params.page_size, params.max_pages
        );
    }
    let (blocks, teams) = search_all(
        &client,
        &params.space_id,
        params.page_size,
        params.max_pages,
        !params.quiet,
    )
    .await?;
    let curr_pages = pages_from_search(&blocks, &params.space_id);
    let curr_sidebar = derive_sidebar(&blocks, &teams, &params.space_id);

    if !params.quiet {
        println!(
            "current sidebar: {} containers, {} discovered pages",
            curr_sidebar.len(),
            curr_pages.len()
        );
    }

    // 3. Compute dirty plan against the FULL current world. We deliberately
    //    don't apply --only/--skip here -- those flags filter the *export
    //    targets* below, not the diff. Filtering before the diff would mark
    //    every excluded container as "removed" and produce a bogus changelog.
    let plan = compute_dirty_containers(&prev_state, &curr_pages, &curr_sidebar);

    // Build the set of container ids the user wants to consider for export.
    let user_filter = build_user_filter(
        &curr_sidebar,
        params.only.as_deref(),
        params.skip.as_deref(),
    );

    let (dirty_set, plan_summary): (std::collections::BTreeSet<String>, String) =
        if params.full_resync {
            let all: std::collections::BTreeSet<String> =
                curr_sidebar.iter().map(|c| c.id.clone()).collect();
            let intersected: std::collections::BTreeSet<String> = match &user_filter {
                Some(f) => all.intersection(f).cloned().collect(),
                None => all.clone(),
            };
            (
                intersected.clone(),
                format!(
                    "full resync requested ({} containers, {} after only/skip)",
                    all.len(),
                    intersected.len(),
                ),
            )
        } else if plan.is_first_run {
            let all: std::collections::BTreeSet<String> =
                curr_sidebar.iter().map(|c| c.id.clone()).collect();
            let intersected: std::collections::BTreeSet<String> = match &user_filter {
                Some(f) => all.intersection(f).cloned().collect(),
                None => all.clone(),
            };
            (
                intersected.clone(),
                format!(
                    "first run -> exporting {} container(s)",
                    intersected.len()
                ),
            )
        } else {
            let intersected: std::collections::BTreeSet<String> = match &user_filter {
                Some(f) => plan.dirty.intersection(f).cloned().collect(),
                None => plan.dirty.clone(),
            };
            (
                intersected.clone(),
                format!(
                    "incremental: {} dirty (of {} total), {} removed, {} unchanged",
                    intersected.len(),
                    plan.dirty.len(),
                    plan.removed.len(),
                    curr_sidebar.len().saturating_sub(plan.dirty.len()),
                ),
            )
        };
    let removed_visible: std::collections::BTreeSet<String> = plan
        .removed
        .iter()
        .filter(|cid| match &user_filter {
            Some(f) => f.contains(cid.as_str()),
            None => true,
        })
        .cloned()
        .collect();

    if !params.quiet {
        println!("plan: {plan_summary}");
        if !plan.is_first_run && !params.full_resync {
            for cid in dirty_set.iter().take(50) {
                let title = curr_sidebar
                    .iter()
                    .find(|c| &c.id == cid)
                    .map(|c| c.title.as_str())
                    .unwrap_or("");
                let reasons = plan
                    .reasons
                    .get(cid)
                    .map(|v| v.join("; "))
                    .unwrap_or_default();
                println!(
                    "  dirty {} {}  -- {}",
                    cid,
                    truncate_str(title, 40),
                    reasons
                );
            }
            if dirty_set.len() > 50 {
                println!("  ... and {} more", dirty_set.len() - 50);
            }
            for cid in removed_visible.iter().take(50) {
                println!("  removed {cid}");
            }
        }
    }

    if params.dry_run {
        println!("--dry-run: not exporting / mutating anything.");
        return Ok(IncrementalRunSummary {
            sync_run_id,
            containers_seen: curr_sidebar.len(),
            dirty_count: dirty_set.len(),
            removed_count: removed_visible.len(),
            elapsed_s: started.elapsed().as_secs_f64(),
            ..Default::default()
        });
    }

    // 4. First-run-after-upgrade baseline: for any non-dirty container with
    //    an on-disk export but no _state/file-hashes/<id>.json, snapshot
    //    silently so the *next* incremental has a baseline. We do NOT emit
    //    changelog entries for these -- they represent state we already had.
    backfill_baseline_hashes(&output, &state_dir, &prev_state, &dirty_set, &plan, params.quiet)?;

    // 5. Filter the export targets to dirty, and pick a per-container
    //    strategy (full vs subtree).
    let to_export: Vec<Container> = curr_sidebar
        .iter()
        .filter(|c| dirty_set.contains(&c.id))
        .cloned()
        .collect();

    let strategies: Vec<(Container, ExportMode)> = to_export
        .into_iter()
        .map(|c| {
            let mode = decide_export_mode(
                &c,
                &plan,
                &curr_pages,
                &prev_state.pages,
                params.subtree_export,
                params.subtree_max_fraction,
            );
            (c, mode)
        })
        .collect();

    if !params.quiet && params.subtree_export {
        let n_subtree = strategies
            .iter()
            .filter(|(_, m)| matches!(m, ExportMode::Subtree { .. }))
            .count();
        println!(
            "subtree-export: {} of {} dirty container(s) eligible (LCA below root + subtree fraction <= {:.0}%)",
            n_subtree,
            strategies.len(),
            params.subtree_max_fraction * 100.0,
        );
    }

    let mut summary = IncrementalRunSummary {
        sync_run_id: sync_run_id.clone(),
        containers_seen: curr_sidebar.len(),
        dirty_count: dirty_set.len(),
        removed_count: removed_visible.len(),
        ..Default::default()
    };

    // 6. Fan-out the exports.
    let task_pool = TaskPool::new(client.clone(), params.poll_interval_s, params.poll_bucket.clone());
    task_pool.start();
    let summary_path = output.join("dump.summary.jsonl");
    let mut all_changelog: Vec<ChangelogEntry> = Vec::new();
    let total = strategies.len();
    let parallel = params.parallel.max(1);

    // Use a small JoinSet-like fan-out via FuturesUnordered with a semaphore.
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(parallel));
    let mut futs = FuturesUnordered::new();
    for (c, mode) in strategies {
        let sem = sem.clone();
        let client_c = client.clone();
        let task_pool_c = task_pool.clone();
        let output_c = output.clone();
        let space_id_c = params.space_id.clone();
        let include_files_c = params.include_files.clone();
        let task_timeout = params.task_timeout_s;
        let unzip = params.unzip;
        futs.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore");
            run_one_export(
                &client_c,
                c,
                mode,
                &space_id_c,
                &output_c,
                &task_pool_c,
                task_timeout,
                &include_files_c,
                unzip,
            )
            .await
        }));
    }

    let mut done = 0usize;
    let counter_w = total.to_string().len();
    let mut exported_ok_set: std::collections::BTreeSet<String> = Default::default();
    while let Some(joined) = futs.next().await {
        let res = match joined {
            Ok(r) => r,
            Err(e) => {
                eprintln!("export task panicked: {e}");
                continue;
            }
        };
        done += 1;
        if res.ok {
            exported_ok_set.insert(res.container.id.clone());
        }
        // Write the dump.summary.jsonl line.
        let summary_line = build_summary_line(&res, &sync_run_id, started_ms);
        append_jsonl(&summary_path, &summary_line)?;

        // Report.
        report_export(&res, done, total, counter_w);

        if !res.ok {
            summary.exported_fail += 1;
            continue;
        }
        summary.exported_ok += 1;

        // Hash the new export and emit changelog entries.
        if let Some(unzipped_dir) = res.unzipped_dir.clone() {
            let new_hashes = hash_directory(&unzipped_dir).context("hashing new export")?;
            let prev_persisted = load_hash_map(&state_dir, &res.container.id)?;
            let prev_leaves: ContainerHashMap = prev_persisted
                .as_ref()
                .map(|p| p.leaves.clone())
                .unwrap_or_default();
            let new_root = container_merkle_root_hex(&new_hashes);
            let prev_root = prev_persisted.as_ref().map(|p| p.merkle_root_hex.clone());
            if Some(&new_root) == prev_root.as_ref() {
                // Root unchanged -> no actual file content changed even though
                // discovery suggested otherwise. Still persist the new map (it's
                // identical) and emit nothing.
                save_hash_map(&state_dir, &res.container.id, &new_hashes)?;
                continue;
            }
            let diff = diff_hash_maps(&prev_leaves, &new_hashes);
            if !diff.is_empty() {
                let entries = entries_for_container_diff(
                    &sync_run_id,
                    &res.container,
                    &unzipped_dir,
                    &diff,
                    &new_hashes,
                    &prev_leaves,
                    started_ms,
                );
                summary.changelog_entries_added += diff.added.len();
                summary.changelog_entries_modified += diff.modified.len();
                summary.changelog_entries_removed += diff.removed.len();
                all_changelog.extend(entries);
            }
            save_hash_map(&state_dir, &res.container.id, &new_hashes)?;
        }
    }
    task_pool.stop().await;

    // 7. Tombstone removed containers + emit `removed` changelog lines.
    //    Respect the user filter -- if --only was used, we only tombstone
    //    containers that the user *also* explicitly asked us to consider.
    let removed_to_process: Vec<&String> = plan
        .removed
        .iter()
        .filter(|cid| match &user_filter {
            Some(f) => f.contains(cid.as_str()),
            None => true,
        })
        .collect();
    for cid in removed_to_process {
        let history = prev_state.containers.get(cid);
        let prev_unzipped = history.and_then(|h| h.unzipped_dir.clone());
        let prev_leaves = previous_leaves_or_empty(&state_dir, cid);
        // Title from prev page records (find a page whose container is this one).
        let title = best_effort_container_title(&prev_state, cid);
        let kind = best_effort_container_kind(&prev_state, cid);
        let entries = entries_for_tombstone(
            &sync_run_id,
            cid,
            &title,
            &kind,
            prev_unzipped.as_deref(),
            &prev_leaves,
            started_ms,
        );
        summary.changelog_entries_removed += entries.len();
        all_changelog.extend(entries);
        // Move the dir to .tombstones/ unless --prune.
        if let Some(slug) = history.and_then(|h| h.dir_slug.clone()) {
            let src = output.join("exports").join(&slug);
            if src.is_dir() {
                if params.prune_removed {
                    let _ = std::fs::remove_dir_all(&src);
                } else {
                    let dest = output
                        .join(".tombstones")
                        .join(format!("{cid}_{}", started_ms));
                    if let Err(e) = std::fs::create_dir_all(dest.parent().unwrap()) {
                        eprintln!("tombstone mkdir failed: {e}");
                    }
                    if let Err(e) = std::fs::rename(&src, &dest) {
                        eprintln!("tombstone rename failed: {e}");
                    }
                }
            }
        }
        delete_hash_map(&state_dir, cid).ok();
    }

    // 8. Append all changelog lines in a single write.
    append_changelog_entries(&output, &all_changelog)?;

    // 9. Atomically replace discovery.jsonl + sidebar.jsonl with the new
    //    state.
    //
    //    discovery.jsonl gets a *merged* view: for every page, we either keep
    //    the previous record (if its container wasn't actually updated this
    //    run) or take the current record (if its container is unchanged or
    //    successfully re-exported). This keeps containers that were dirty
    //    but skipped (e.g. via --only, or that failed) marked dirty next run
    //    instead of silently swallowing the pending edits.
    let merged_discovery = merge_discovery_baseline(
        &prev_state,
        &curr_pages,
        &curr_sidebar,
        &exported_ok_set,
        &plan.dirty,
        &plan.removed,
    );
    write_jsonl(
        &output.join("discovery.jsonl"),
        merged_discovery
            .values()
            .map(|p| serde_json::to_value(p).unwrap()),
    )?;
    // sidebar.jsonl is purely a snapshot of the current top-level world, no
    // merge needed.
    write_jsonl(
        &output.join("sidebar.jsonl"),
        curr_sidebar.iter().map(|s| {
            let mut v = serde_json::to_value(s).unwrap();
            if let Some(map) = v.as_object_mut() {
                map.insert(
                    "space_id".into(),
                    Value::String(params.space_id.clone()),
                );
            }
            v
        }),
    )?;

    // 10. Compute the workspace-level Merkle root over the *current* state
    //     (every container that has a hash file on disk).
    let workspace_root = compute_workspace_root(&state_dir, &curr_sidebar)?;
    summary.workspace_merkle_root_hex = Some(workspace_root.clone());

    let ended_ms = now_ms();
    write_last_run(
        &output,
        &LastRunMeta {
            sync_run_id: sync_run_id.clone(),
            started_at_ms: started_ms,
            ended_at_ms: ended_ms,
            dirty_count: summary.dirty_count,
            removed_count: summary.removed_count,
            changelog_entries: all_changelog.len(),
            workspace_merkle_root_hex: Some(workspace_root),
        },
    )?;

    summary.elapsed_s = started.elapsed().as_secs_f64();

    if !params.quiet {
        println!();
        println!("--- sync summary ---");
        println!(
            "containers:  exported_ok={}  exported_fail={}  unchanged={}  removed={}",
            summary.exported_ok,
            summary.exported_fail,
            curr_sidebar
                .len()
                .saturating_sub(summary.dirty_count + summary.removed_count),
            summary.removed_count,
        );
        println!(
            "changelog:   +{} added  ~{} modified  -{} removed",
            summary.changelog_entries_added,
            summary.changelog_entries_modified,
            summary.changelog_entries_removed,
        );
        if let Some(r) = &summary.workspace_merkle_root_hex {
            println!("manifest:    workspace_merkle_root={}", short_hex(r));
        }
        println!("wall time:   {:.1}s", summary.elapsed_s);
        let avg_batch = if task_pool.poll_count() > 0 {
            task_pool.batched_count() as f64 / task_pool.poll_count() as f64
        } else {
            0.0
        };
        println!(
            "poller:      {} batched call(s), avg {:.1} task ids/call",
            task_pool.poll_count(),
            avg_batch
        );
        println!("changelog:   {}", output.join("dump.changelog.jsonl").display());
        println!("summary:     {}", output.join("dump.summary.jsonl").display());
        println!("discovery:   {}", output.join("discovery.jsonl").display());
    }

    Ok(summary)
}

/// Translate the user's `--only` / `--skip` flags (which may name containers
/// by id or by title) into a concrete set of container ids that the user is
/// willing to consider for export. Returns `None` when the user supplied
/// neither flag (i.e. "consider all").
fn build_user_filter(
    sidebar: &[Container],
    only: Option<&[String]>,
    skip: Option<&[String]>,
) -> Option<std::collections::BTreeSet<String>> {
    if only.map(|v| v.is_empty()).unwrap_or(true) && skip.map(|v| v.is_empty()).unwrap_or(true) {
        return None;
    }
    let mut allowed: std::collections::BTreeSet<String> = sidebar.iter().map(|c| c.id.clone()).collect();
    if let Some(only) = only {
        if !only.is_empty() {
            let want: std::collections::HashSet<&str> =
                only.iter().map(|s| s.as_str()).collect();
            allowed.retain(|id| {
                let title = sidebar
                    .iter()
                    .find(|c| &c.id == id)
                    .map(|c| c.title.as_str())
                    .unwrap_or("");
                want.contains(id.as_str()) || want.contains(title)
            });
        }
    }
    if let Some(skip) = skip {
        if !skip.is_empty() {
            let drop: std::collections::HashSet<&str> =
                skip.iter().map(|s| s.as_str()).collect();
            allowed.retain(|id| {
                let title = sidebar
                    .iter()
                    .find(|c| &c.id == id)
                    .map(|c| c.title.as_str())
                    .unwrap_or("");
                !drop.contains(id.as_str()) && !drop.contains(title)
            });
        }
    }
    Some(allowed)
}

fn build_summary_line(res: &ExportResult, sync_run_id: &str, dumped_at_ms: i64) -> Value {
    let mut v = serde_json::Map::new();
    v.insert("container".into(), serde_json::to_value(&res.container).unwrap());
    v.insert("ok".into(), Value::Bool(res.ok));
    if let Some(t) = &res.task_id {
        v.insert("task_id".into(), Value::String(t.clone()));
    }
    if let Some(z) = &res.zip_path {
        v.insert("zip".into(), Value::String(z.display().to_string()));
    }
    if let Some(u) = &res.unzipped_dir {
        v.insert("unzipped_dir".into(), Value::String(u.display().to_string()));
    }
    v.insert("bytes".into(), Value::from(res.bytes));
    v.insert("pages_exported".into(), Value::from(res.pages_exported));
    v.insert(
        "elapsed_s".into(),
        Value::from((res.elapsed_s * 1000.0).round() / 1000.0),
    );
    if let Some(p) = &res.phase {
        v.insert("phase".into(), Value::String(p.clone()));
    }
    if let Some(e) = &res.error {
        v.insert("error".into(), Value::String(e.clone()));
    }
    if let Some(b) = &res.subtree_block_id {
        v.insert("subtree_block_id".into(), Value::String(b.clone()));
    }
    if let Some(n) = res.subtree_page_count {
        v.insert("subtree_page_count".into(), Value::from(n as u64));
    }
    v.insert("sync_run_id".into(), Value::String(sync_run_id.into()));
    v.insert("dumped_at_ms".into(), Value::from(dumped_at_ms));
    Value::Object(v)
}

fn report_export(res: &ExportResult, done: usize, total: usize, w: usize) {
    let mut label = truncate_str(
        if res.container.title.is_empty() {
            &res.container.id
        } else {
            &res.container.title
        },
        40,
    );
    if res.subtree_block_id.is_some() {
        // Append a "(subtree)" suffix so users can see at a glance which
        // containers took the fast path. Keep total label width <=40 by
        // truncating the title further if needed.
        let suffix = if let Some(n) = res.subtree_page_count {
            format!(" (subtree, {n} pages)")
        } else {
            " (subtree)".to_string()
        };
        let max_title = 40_usize.saturating_sub(suffix.len());
        let trimmed = truncate_str(
            if res.container.title.is_empty() {
                &res.container.id
            } else {
                &res.container.title
            },
            max_title,
        );
        label = format!("{trimmed}{suffix}");
    }
    let tag = if res.ok { "OK  " } else { "FAIL" };
    if res.ok {
        let n = res.pages_exported;
        let pages_str = if n == 1 { "1 page " } else { "" };
        let pages_fmt = if n == 1 {
            pages_str.to_string()
        } else {
            format!("{n} pages")
        };
        println!(
            "[{:>w$}/{}] {}  {:<40}  {:>10}  {:>5.1} MB  {:>5.1}s",
            done,
            total,
            tag,
            label,
            pages_fmt,
            res.bytes as f64 / 1e6,
            res.elapsed_s,
            w = w,
        );
    } else {
        let err = res
            .error
            .as_deref()
            .map(|s| s.chars().take(100).collect::<String>())
            .unwrap_or_default();
        println!(
            "[{:>w$}/{}] {}  {:<40}  phase={} err={}",
            done,
            total,
            tag,
            label,
            res.phase.as_deref().unwrap_or("?"),
            err,
            w = w,
        );
    }
}

/// Pick `Subtree { .. }` iff every condition is met, else `Full`.
///
/// Conditions for subtree:
///   1. caller passed `--subtree-export`
///   2. the dirty plan has at least one per-page hint for this container
///   3. the LCA of those hints resolves to something *strictly below* the
///      container root
///   4. the LCA's subtree page count is at most `max_fraction` of the
///      container's total page count (otherwise the savings don't pay for
///      the cross-subtree link/database refresh risk)
fn decide_export_mode(
    container: &Container,
    plan: &DirtyPlan,
    curr_pages: &BTreeMap<String, PageRecord>,
    prev_pages: &BTreeMap<String, PageRecord>,
    enabled: bool,
    max_fraction: f64,
) -> ExportMode {
    if !enabled {
        return ExportMode::Full;
    }
    let Some(dirty_pages) = plan.dirty_pages_by_container.get(&container.id) else {
        return ExportMode::Full;
    };
    if dirty_pages.is_empty() {
        return ExportMode::Full;
    }
    let lca = match compute_lca(&container.id, dirty_pages, curr_pages, prev_pages) {
        Some(id) if id != container.id => id,
        _ => return ExportMode::Full,
    };
    let subtree_pages = count_subtree_pages(&lca, curr_pages);
    let container_pages = count_subtree_pages(&container.id, curr_pages).max(1);
    let fraction = subtree_pages as f64 / container_pages as f64;
    if fraction > max_fraction {
        return ExportMode::Full;
    }
    let label = curr_pages
        .get(&lca)
        .map(|p| p.title.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| lca.clone());
    ExportMode::Subtree {
        block_id: lca,
        block_label: label,
        subtree_pages,
        container_pages,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_one_export(
    client: &NotionInternal,
    container: Container,
    mode: ExportMode,
    space_id: &str,
    output: &Path,
    task_pool: &TaskPool,
    task_timeout_s: f64,
    include_files: &str,
    unzip: bool,
) -> ExportResult {
    match mode {
        ExportMode::Full => {
            export_one(
                client,
                container,
                space_id,
                output,
                task_pool,
                task_timeout_s,
                include_files,
                unzip,
            )
            .await
        }
        ExportMode::Subtree {
            block_id,
            block_label,
            subtree_pages,
            container_pages: _,
        } => {
            let res = export_subtree(
                client,
                container.clone(),
                block_id.clone(),
                block_label,
                space_id,
                output,
                task_pool,
                task_timeout_s,
                include_files,
                subtree_pages,
            )
            .await;
            if !res.ok || res.unzipped_dir.is_none() {
                // Subtree failed at enqueue / download / unzip. Bubble up so
                // the runner records a failed export and the container stays
                // dirty (next sync will retry, possibly without --subtree-
                // export if the user disables the flag).
                return res;
            }
            // Stitch the freshly-extracted subtree into the container's
            // existing on-disk tree.
            let label = truncate_str(
                if container.title.is_empty() {
                    &container.id
                } else {
                    &container.title
                },
                40,
            );
            let container_dir = output
                .join("exports")
                .join(container_dir_name(&label, &container.id));
            let container_unzipped = container_dir.join("unzipped");
            let fresh_unzipped = res.unzipped_dir.clone().unwrap();
            let stitch_res = tokio::task::spawn_blocking({
                let container_unzipped = container_unzipped.clone();
                let fresh_unzipped = fresh_unzipped.clone();
                let block_id = block_id.clone();
                move || stitch_subtree(&container_unzipped, &fresh_unzipped, &block_id)
            })
            .await;
            match stitch_res {
                Ok(Ok(_outcome)) => {
                    // Best-effort temp cleanup. Doesn't change correctness if
                    // we leak the .subtree.tmp dir.
                    let temp_dir = container_dir.join(".subtree.tmp");
                    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
                    let mut res = res;
                    // Hashing pipeline below operates on the *full* container
                    // tree; redirect.
                    res.unzipped_dir = Some(container_unzipped);
                    res
                }
                Ok(Err(e)) => {
                    // Stitch failed because (e.g.) the LCA wasn't found in
                    // the existing tree (page created since last full export).
                    // Fall back to a full container export so the new page
                    // lands and the on-disk tree converges.
                    eprintln!(
                        "subtree-stitch fell back for {} ({}): {}; running full \
                         container export instead",
                        container.id,
                        truncate_str(&container.title, 40),
                        e
                    );
                    export_one(
                        client,
                        container,
                        space_id,
                        output,
                        task_pool,
                        task_timeout_s,
                        include_files,
                        unzip,
                    )
                    .await
                }
                Err(e) => {
                    eprintln!("subtree-stitch task panicked for {}: {e}", container.id);
                    export_one(
                        client,
                        container,
                        space_id,
                        output,
                        task_pool,
                        task_timeout_s,
                        include_files,
                        unzip,
                    )
                    .await
                }
            }
        }
    }
}

fn append_jsonl(path: &Path, value: &Value) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let line = serde_json::to_string(value)?;
    writeln!(f, "{line}")?;
    Ok(())
}

fn write_jsonl<I: Iterator<Item = Value>>(path: &Path, iter: I) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    for v in iter {
        let line = serde_json::to_string(&v)?;
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    write_atomic(path, &buf)?;
    Ok(())
}

fn backfill_baseline_hashes(
    output: &Path,
    state_dir: &Path,
    prev: &super::state::PrevState,
    dirty_set: &std::collections::BTreeSet<String>,
    _plan: &DirtyPlan,
    quiet: bool,
) -> Result<()> {
    let mut backfilled = 0usize;
    for (cid, hist) in &prev.containers {
        if dirty_set.contains(cid) {
            continue;
        }
        let Some(unzipped) = &hist.unzipped_dir else {
            continue;
        };
        if state_dir
            .join("file-hashes")
            .join(format!("{cid}.json"))
            .exists()
        {
            continue;
        }
        let map = hash_directory(unzipped)?;
        save_hash_map(state_dir, cid, &map)?;
        backfilled += 1;
    }
    if backfilled > 0 && !quiet {
        println!(
            "baseline: snapshotted {} pre-existing container(s) (no changelog written for these)",
            backfilled
        );
    }
    let _ = output;
    Ok(())
}

/// Merge previous and current page records into the next discovery.jsonl
/// baseline.
///
/// Rules per page (identified by id):
/// - If the page's *current* container was tombstoned this run -> drop.
/// - If the page's *current* container was successfully re-exported this run,
///   OR is unchanged (i.e. the diff said it isn't dirty) -> take the
///   current record. The current record is the source of truth for that
///   container's state.
/// - If the page's current container was dirty but we did NOT re-export it
///   (because of --only, --skip, a failed export, or an interrupted run) ->
///   keep the previous record so the container shows up as dirty again on
///   the next sync. If there is no previous record (i.e. the page is brand
///   new), keep the current record but the container is still in
///   `plan.dirty` so it'll be picked up next run.
/// - Pages with no current container (no parent_table walk reaches space/team)
///   are kept-as-is from prev, dropped if not in prev. Rare orphan case.
fn merge_discovery_baseline(
    prev: &super::state::PrevState,
    curr_pages: &BTreeMap<String, crate::api::PageRecord>,
    curr_sidebar: &[Container],
    exported_ok_set: &std::collections::BTreeSet<String>,
    plan_dirty: &std::collections::BTreeSet<String>,
    plan_removed: &std::collections::BTreeSet<String>,
) -> BTreeMap<String, crate::api::PageRecord> {
    use super::diff::walk_to_container_root;

    let curr_sidebar_ids: std::collections::BTreeSet<String> =
        curr_sidebar.iter().map(|c| c.id.clone()).collect();

    let mut out: BTreeMap<String, crate::api::PageRecord> = BTreeMap::new();

    let mut cache: BTreeMap<&str, Option<String>> = BTreeMap::new();
    for (pid, curr) in curr_pages {
        let cid_opt =
            walk_to_container_root(pid.as_str(), curr_pages, &mut cache);
        let take_curr = match cid_opt.as_deref() {
            Some(cid) if plan_removed.contains(cid) => continue, // dropped
            Some(cid) if exported_ok_set.contains(cid) => true,
            Some(cid) if !plan_dirty.contains(cid) && curr_sidebar_ids.contains(cid) => true,
            Some(_) => false,
            None => true, // orphan: not part of any container, keep curr.
        };
        if take_curr {
            out.insert(pid.clone(), curr.clone());
        } else {
            // Container was dirty + not re-exported. Preserve prev record
            // when we have one so the container stays dirty next run.
            match prev.pages.get(pid) {
                Some(p) => {
                    out.insert(pid.clone(), p.clone());
                }
                None => {
                    // Brand new page in a not-yet-exported container. Keep curr.
                    out.insert(pid.clone(), curr.clone());
                }
            }
        }
    }

    // Pages that were in prev but vanished from curr: only keep them if their
    // container was dirty + not re-exported (so we don't lose evidence that a
    // page was deleted; the next run will still see it dirty).
    let mut prev_cache: BTreeMap<&str, Option<String>> = BTreeMap::new();
    for (pid, prev_p) in &prev.pages {
        if curr_pages.contains_key(pid) {
            continue;
        }
        let cid_opt = walk_to_container_root(pid.as_str(), &prev.pages, &mut prev_cache);
        match cid_opt.as_deref() {
            Some(cid)
                if plan_dirty.contains(cid)
                    && !exported_ok_set.contains(cid)
                    && !plan_removed.contains(cid)
                    && curr_sidebar_ids.contains(cid) =>
            {
                out.insert(pid.clone(), prev_p.clone());
            }
            _ => {}
        }
    }

    out
}

fn compute_workspace_root(
    state_dir: &Path,
    curr_sidebar: &[Container],
) -> Result<String> {
    let mut roots: BTreeMap<String, String> = BTreeMap::new();
    for c in curr_sidebar {
        if let Some(p) = load_hash_map(state_dir, &c.id)? {
            roots.insert(c.id.clone(), p.merkle_root_hex);
        }
    }
    Ok(workspace_merkle_root_hex(&roots))
}

fn best_effort_container_title(prev: &super::state::PrevState, cid: &str) -> String {
    prev.pages
        .get(cid)
        .map(|p| p.title.clone())
        .unwrap_or_default()
}

fn best_effort_container_kind(prev: &super::state::PrevState, cid: &str) -> String {
    match prev.pages.get(cid).and_then(|p| p.parent_table.clone()).as_deref() {
        Some("space") => "space_page".into(),
        Some("team") => "teamspace_page".into(),
        _ => "".into(),
    }
}

fn short_hex(h: &str) -> String {
    if h.len() <= 14 {
        h.to_string()
    } else {
        format!("{}…{}", &h[..10], &h[h.len() - 4..])
    }
}


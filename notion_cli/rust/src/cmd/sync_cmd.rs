//! `sync`: quiet incremental run for use by launchd / systemd / cron.
//!
//! Same engine as `dump` but:
//!   - quiet by default (one-line summary if there's nothing to do)
//!   - never prompts for a workspace pin (assumes NOTION_INTERNAL_SPACE_ID
//!     is set, which the python `login` command writes to .env)
//!   - exit codes are stable for schedulers:
//!       0  = success (including "no-op")
//!       2  = config / credentials problem (token missing, space missing)
//!       4  = some exports failed but credentials & state are fine
//!   - `--every <s>` runs in a loop, useful during development; the
//!     scheduled-execution path uses the OS timer instead.

use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::time::Duration;

use crate::api::{NotionInternal, RateLimitGate, TokenBucket};
use crate::sync::runner::{run_incremental_dump, IncrementalParams, IncrementalRunSummary};
use crate::token;
use crate::{
    DEFAULT_MAX_PAGES, DEFAULT_OUTPUT, DEFAULT_PAGE_SIZE, DEFAULT_PARALLEL,
    DEFAULT_POLL_INTERVAL_S, DEFAULT_POLL_RPS, DEFAULT_RPS, DEFAULT_TASK_TIMEOUT_S,
};

#[derive(clap::Args, Debug, Clone)]
pub struct Args {
    #[arg(long)]
    pub token_v2: Option<String>,
    #[arg(long)]
    pub space_id: Option<String>,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: PathBuf,
    #[arg(long, default_value_t = DEFAULT_RPS)]
    pub rps: f64,
    #[arg(long, default_value_t = DEFAULT_POLL_RPS)]
    pub poll_rps: f64,
    #[arg(long, default_value_t = DEFAULT_PARALLEL)]
    pub parallel: usize,
    #[arg(long, default_value_t = DEFAULT_POLL_INTERVAL_S)]
    pub poll_interval: f64,
    #[arg(long, default_value_t = DEFAULT_TASK_TIMEOUT_S)]
    pub task_timeout: f64,
    #[arg(long, default_value = "everything", value_parser = ["everything", "no_files"])]
    pub include_files: String,
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub page_size: u32,
    #[arg(long, default_value_t = DEFAULT_MAX_PAGES)]
    pub max_pages: u32,
    /// Compute the dirty plan, print it, do nothing else.
    #[arg(long)]
    pub dry_run: bool,
    /// Loop mode: sleep this many seconds between syncs and start over. Use
    /// the `sync-install` subcommand for long-lived scheduled execution
    /// instead of relying on this.
    #[arg(long)]
    pub every: Option<u64>,
    /// Print the full per-batch progress instead of just the final summary.
    #[arg(long)]
    pub verbose: bool,
    /// Re-export every container, ignoring incremental diff.
    #[arg(long)]
    pub full: bool,
    /// `rm -rf` removed containers instead of moving them under `.tombstones/`.
    #[arg(long)]
    pub prune: bool,
    /// Opt-in: re-export only the lowest-common-ancestor of dirty pages
    /// inside large containers, then stitch the result into the existing
    /// on-disk tree. Mirrors `dump --subtree-export`. Pair with a periodic
    /// `--full` (e.g. `sync --full --every 604800`) to absorb cross-subtree
    /// link drift.
    #[arg(long)]
    pub subtree_export: bool,
    /// Upper bound on subtree-vs-container page ratio for `--subtree-export`
    /// to actually take effect. Default 0.5.
    #[arg(long, default_value_t = 0.5)]
    pub subtree_max_fraction: f64,
}

pub async fn run(args: Args) -> Result<()> {
    let tokens = token::load(args.token_v2.as_deref())?;
    let space_id = token::load_space_id(args.space_id.as_deref())?
        .ok_or_else(|| anyhow!("sync: no NOTION_INTERNAL_SPACE_ID; run python `login` first"))?;

    if let Some(every) = args.every {
        loop {
            let _ = do_one(&args, tokens.clone(), space_id.clone()).await?;
            tracing::info!("sleeping {every}s before next sync");
            tokio::time::sleep(Duration::from_secs(every)).await;
        }
    } else {
        let summary = do_one(&args, tokens.clone(), space_id.clone()).await?;
        if summary.exported_fail > 0 {
            std::process::exit(4);
        }
        Ok(())
    }
}

async fn do_one(
    args: &Args,
    tokens: crate::token::Tokens,
    space_id: String,
) -> Result<IncrementalRunSummary> {
    let bucket = TokenBucket::new(args.rps);
    let poll_bucket = TokenBucket::new(args.poll_rps);
    let gate = RateLimitGate::new();
    let client = NotionInternal::new(tokens, bucket, gate)?;
    let params = IncrementalParams {
        output: args.output.clone(),
        space_id: space_id.clone(),
        space_name: space_id.clone(),
        parallel: args.parallel,
        poll_interval_s: args.poll_interval,
        poll_bucket,
        task_timeout_s: args.task_timeout,
        include_files: args.include_files.clone(),
        unzip: true,
        page_size: args.page_size,
        max_pages: args.max_pages,
        only: None,
        skip: None,
        full_resync: args.full,
        dry_run: args.dry_run,
        prune_removed: args.prune,
        quiet: !args.verbose,
        subtree_export: args.subtree_export,
        subtree_max_fraction: args.subtree_max_fraction,
    };
    let summary = run_incremental_dump(client, params).await?;
    if !args.verbose {
        // Quiet one-liner for syslog/launchd consumption.
        println!(
            "sync: run_id={} ok={} fail={} unchanged={} removed={} +{}/~{}/-{} in {:.1}s",
            summary.sync_run_id,
            summary.exported_ok,
            summary.exported_fail,
            summary
                .containers_seen
                .saturating_sub(summary.dirty_count + summary.removed_count),
            summary.removed_count,
            summary.changelog_entries_added,
            summary.changelog_entries_modified,
            summary.changelog_entries_removed,
            summary.elapsed_s,
        );
    }
    Ok(summary)
}

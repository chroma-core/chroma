//! `dump`: incremental-by-default exportBlock per dirty top-level container.
//!
//! Thin wrapper around `sync::run_incremental_dump`. The non-trivial work
//! lives in the runner module so `cmd_sync` can share it verbatim.

use anyhow::{anyhow, Result};
use std::path::PathBuf;

use crate::api::{NotionInternal, RateLimitGate, TokenBucket};
use crate::sync::runner::{run_incremental_dump, IncrementalParams};
use crate::token;
use crate::{
    DEFAULT_MAX_PAGES, DEFAULT_OUTPUT, DEFAULT_PAGE_SIZE, DEFAULT_PARALLEL,
    DEFAULT_POLL_INTERVAL_S, DEFAULT_POLL_RPS, DEFAULT_RPS, DEFAULT_TASK_TIMEOUT_S,
};

#[derive(clap::Args, Debug, Clone)]
pub struct DumpFlags {
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
    #[arg(long)]
    pub no_unzip: bool,
    /// Comma-separated container ids or titles. Only export these.
    #[arg(long)]
    pub only: Option<String>,
    /// Comma-separated container ids or titles. Skip these.
    #[arg(long)]
    pub skip: Option<String>,
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub page_size: u32,
    #[arg(long, default_value_t = DEFAULT_MAX_PAGES)]
    pub max_pages: u32,
    /// Re-export every container, ignoring incremental diff.
    #[arg(long)]
    pub full: bool,
    /// Compute the dirty plan, print it, do nothing else. No exports, no
    /// changelog, no state writes.
    #[arg(long)]
    pub dry_run: bool,
    /// `rm -rf` removed containers instead of moving them under `.tombstones/`.
    #[arg(long)]
    pub prune: bool,
    /// Opt-in: for very large containers (1000+ pages) where only a small
    /// subtree is dirty, re-export only the lowest-common-ancestor of the
    /// dirty pages and stitch it into the existing on-disk tree, instead of
    /// re-exporting the full container. Trades a small amount of cross-
    /// subtree link drift for ~10-20x faster syncs on big containers.
    /// Pair with a periodic `--full` resync (e.g. weekly) to absorb that
    /// drift.
    #[arg(long, default_value_t = true)]
    pub subtree_export: bool,
    /// Upper bound on subtree-vs-container page ratio for `--subtree-export`
    /// to actually take effect. e.g. `0.5` means subtree mode kicks in only
    /// when the LCA's subtree is at most 50% of the container.
    #[arg(long, default_value_t = 0.5)]
    pub subtree_max_fraction: f64,
}

#[derive(clap::Args, Debug, Clone)]
pub struct Args {
    #[arg(long)]
    pub token_v2: Option<String>,
    #[arg(long)]
    pub space_id: Option<String>,
    #[command(flatten)]
    pub flags: DumpFlags,
}

impl Args {
    pub fn from_grab(g: crate::GrabArgs) -> Self {
        Self {
            token_v2: g.discover.token_v2,
            space_id: g.discover.space_id,
            flags: g.dump,
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    let tokens = token::load(args.token_v2.as_deref())?;
    println!("auth: token_v2 source = {}", tokens.source);
    if tokens.file_token.is_none() {
        eprintln!(
            "WARNING: no file_token cookie on disk -- enqueueing exports \
             will succeed but downloading from file.notion.so returns 403. \
             Run `notion-internal-dump login` to capture both cookies."
        );
    }
    let space_id = token::load_space_id(args.space_id.as_deref())?
        .ok_or_else(|| anyhow!("no space_id (pass --space-id or set NOTION_INTERNAL_SPACE_ID)"))?;
    let bucket = TokenBucket::new(args.flags.rps);
    let poll_bucket = TokenBucket::new(args.flags.poll_rps);
    let gate = RateLimitGate::new();
    let client = NotionInternal::new(tokens, bucket, gate)?;

    let space_name = resolve_space_name(&client, &space_id).await.unwrap_or_else(|_| space_id.clone());

    let params = IncrementalParams {
        output: args.flags.output,
        space_id,
        space_name,
        parallel: args.flags.parallel,
        poll_interval_s: args.flags.poll_interval,
        poll_bucket,
        task_timeout_s: args.flags.task_timeout,
        include_files: args.flags.include_files,
        unzip: !args.flags.no_unzip,
        page_size: args.flags.page_size,
        max_pages: args.flags.max_pages,
        only: args
            .flags
            .only
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()),
        skip: args
            .flags
            .skip
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()),
        full_resync: args.flags.full,
        dry_run: args.flags.dry_run,
        prune_removed: args.flags.prune,
        quiet: false,
        subtree_export: args.flags.subtree_export,
        subtree_max_fraction: args.flags.subtree_max_fraction,
    };

    let summary = run_incremental_dump(client, params).await?;
    if summary.exported_fail > 0 {
        std::process::exit(4);
    }
    Ok(())
}

async fn resolve_space_name(client: &NotionInternal, space_id: &str) -> Result<String> {
    let v = client
        .sync_record_values(vec![serde_json::json!({
            "pointer": { "table": "space", "id": space_id },
            "version": -1
        })])
        .await?;
    let name = v
        .get("recordMap")
        .and_then(|m| m.get("space"))
        .and_then(|s| s.get(space_id))
        .and_then(|wrap| wrap.get("value"))
        .and_then(|inner| inner.get("value").or(Some(inner)))
        .and_then(|val| val.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        Ok(space_id.to_string())
    } else {
        Ok(name)
    }
}

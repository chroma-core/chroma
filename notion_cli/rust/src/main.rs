//! notion-internal-dump (Rust)
//!
//! Daemon-shaped half of the notion-internal-dump CLI.
//!
//! Login still lives in Python (`./notion_internal_dump.sh login` and friends);
//! this binary picks up the saved tokens and handles the discover / dump /
//! sync / sync-install path:
//!
//!   - `discover` walks `/api/v3/search` and writes
//!     `<output>/sidebar.jsonl` + `<output>/discovery.jsonl`
//!   - `dump` runs `enqueueExportBlock` for every dirty top-level container
//!     and downloads the resulting zips. Incremental by default: only
//!     re-exports containers whose descendant pages' `last_edited_time`
//!     advanced (see `compute_dirty_containers`)
//!   - `grab` = discover + dump
//!   - `sync` = quiet wrapper around `grab` for use by launchd / systemd /
//!     Task Scheduler
//!   - `sync-install` / `sync-uninstall` set up the per-OS scheduler entry
//!
//! For every successful container export the binary also computes per-file
//! SHA256 leaves and a per-container Merkle root (via `rs_merkle`), diffs
//! against the previous run's hash map under `<output>/_state/file-hashes/`,
//! and appends one JSON line per added/modified/removed file to
//! `<output>/dump.changelog.jsonl`. Downstream consumers (Chroma upserter)
//! tail that file to know exactly which files to upsert/delete.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod api;
mod cmd;
mod sync;
mod token;
mod util;

use cmd::{discover, dump, sync_cmd, sync_install};

const DEFAULT_OUTPUT: &str = "./notion-internal-dump";
const DEFAULT_RPS: f64 = 1.0;
const DEFAULT_POLL_RPS: f64 = 0.5;
const DEFAULT_PARALLEL: usize = 4;
const DEFAULT_POLL_INTERVAL_S: f64 = 5.0;
const DEFAULT_TASK_TIMEOUT_S: f64 = 1800.0;
const DEFAULT_PAGE_SIZE: u32 = 300;
const DEFAULT_MAX_PAGES: u32 = 100_000;
const DEFAULT_SYNC_INTERVAL_S: u64 = 900;

#[derive(Parser, Debug)]
#[command(
    name = "notion-internal-dump",
    about = "Notion internal-API dump (Rust port: discover/dump/sync/sync-install)",
    long_about = None,
    propagate_version = true,
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// walk sidebar containers + /api/v3/search, write sidebar.jsonl + discovery.jsonl
    Discover(discover::Args),
    /// exportBlock per top-level container, download zips. incremental by default.
    Dump(dump::Args),
    /// discover + dump (the all-in-one)
    Grab(GrabArgs),
    /// quiet incremental sync, intended for launchd / systemd / cron
    Sync(sync_cmd::Args),
    /// install the per-OS scheduler entry that runs `sync` periodically
    #[command(name = "sync-install")]
    SyncInstall(sync_install::InstallArgs),
    /// remove the per-OS scheduler entry installed by `sync-install`
    #[command(name = "sync-uninstall")]
    SyncUninstall(sync_install::UninstallArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct GrabArgs {
    #[command(flatten)]
    pub discover: discover::Args,
    #[command(flatten)]
    pub dump: dump::DumpFlags,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Discover(a) => discover::run(a).await,
        Cmd::Dump(a) => dump::run(a).await,
        Cmd::Grab(a) => {
            discover::run(a.discover.clone()).await?;
            // grab reuses the discover args' --output / --token / --space-id and
            // the dump-specific knobs from a.dump.
            let dump_args = dump::Args::from_grab(a);
            dump::run(dump_args).await
        }
        Cmd::Sync(a) => sync_cmd::run(a).await,
        Cmd::SyncInstall(a) => sync_install::run_install(a).await,
        Cmd::SyncUninstall(a) => sync_install::run_uninstall(a).await,
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_env("NOTION_LOG").unwrap_or_else(|_| {
        EnvFilter::new("notion_internal_dump=info,reqwest=warn,hyper=warn")
    });
    let _ = fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .compact()
        .try_init();
}

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
#[path = "sharding/storage_audit.rs"]
mod storage_audit;
#[derive(Parser)]
#[command(
    about = "Read-only live-versus-physical bytes for retained hierarchical snapshots; performs no cleanup"
)]
struct Args {
    #[arg(long)]
    checkpoint_dir: PathBuf,
    #[arg(long)]
    retained_manifest: Vec<PathBuf>,
    #[arg(long)]
    expected_current_sha256: Option<String>,
    #[arg(long, default_value_t = 1_000_000)]
    max_inventory_entries: usize,
    #[arg(long, default_value_t = 10_000)]
    max_manifests: usize,
    #[arg(long, default_value_t=16*1024*1024)]
    max_root_bytes: u64,
    #[arg(long, default_value_t = 1_000_000)]
    max_root_references: usize,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let a = Args::parse();
    let mut options = storage_audit::Options::new(a.checkpoint_dir);
    options.retained_manifests = a.retained_manifest;
    options.expected_current_sha256 = a.expected_current_sha256;
    options.max_inventory_entries = a.max_inventory_entries;
    options.max_manifests = a.max_manifests;
    options.max_root_bytes = a.max_root_bytes;
    options.max_root_references = a.max_root_references;
    serde_json::to_writer_pretty(
        std::io::stdout().lock(),
        &storage_audit::audit(&options).await?,
    )?;
    println!();
    Ok(())
}

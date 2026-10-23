use chroma_cli::hnsw_integrity_check::{hnsw_integrity_check_exit_code, HnswIntegrityCheckArgs};
use clap::Parser;
use std::process::ExitCode;

fn main() -> ExitCode {
    hnsw_integrity_check_exit_code(HnswIntegrityCheckArgs::parse())
}

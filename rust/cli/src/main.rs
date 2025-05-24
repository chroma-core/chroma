#![windows_subsystem = "console"]

use chroma_cli::chroma_cli;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let _ = chroma_cli(args);
}

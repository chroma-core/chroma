#![windows_subsystem = "console"]

mod utils;

use chroma_cli::chroma_cli;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    chroma_cli(args);
}

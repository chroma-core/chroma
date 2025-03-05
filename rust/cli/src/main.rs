#![windows_subsystem = "console"]

use std::env;
use cli::chroma_cli;

fn main() {
    let args: Vec<String> = env::args().collect();
    chroma_cli(args);
}

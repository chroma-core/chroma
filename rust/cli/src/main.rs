use chroma_cli::{run_command_from_args, Cli};
use clap::Parser;

fn main() {
    let cli = Cli::parse();
    run_command_from_args(cli);
}

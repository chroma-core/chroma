pub mod completion;
pub mod version;

use clap::{Parser, Subcommand};
use clap_complete::Shell;

#[derive(Parser)]
#[command(name = "foundation")]
#[command(about = "Chroma Foundation — team knowledge at your fingertips")]
#[command(long_about = None)]
#[command(disable_version_flag = true)] // We handle `version` as an explicit subcommand
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Show version information
    Version,
    /// Generate shell completion scripts
    Completion {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
}

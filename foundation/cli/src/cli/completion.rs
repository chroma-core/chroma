use clap::CommandFactory;
use clap_complete::{generate, Shell};

use crate::cli::Cli;

/// Execute the `foundation completion <shell>` command.
///
/// Writes the completion script for the given shell to stdout. Returns an exit code.
pub fn execute(shell: Shell) -> i32 {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, "foundation", &mut std::io::stdout());
    0
}

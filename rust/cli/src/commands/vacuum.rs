use std::path::Path;
use clap::Parser;
use colored::Colorize;
use dialoguer::Confirm;
use chroma_frontend::frontend::Frontend;
use chroma_config::registry::Registry;
use crate::utils::{get_frontend_config, LocalFrontendCommandArgs, DEFAULT_PERSISTENT_PATH, SQLITE_FILENAME};

#[derive(Parser, Debug)]
pub struct VacuumArgs {
    #[clap(flatten)]
    frontend_args: LocalFrontendCommandArgs,
    #[arg(long)]
    force: bool,
}

pub fn vacuum(args: VacuumArgs) {
    // Vacuum the database. This may result in a small increase in performance.
    // If you recently upgraded Chroma from a version below 0.5.6 to 0.5.6 or above, you should run this command once to greatly reduce the size of your database and enable continuous database pruning. In most other cases, vacuuming will save very little disk space.
    // The execution time of this command scales with the size of your database. It blocks both reads and writes to the database while it is running.
    let mut config = match get_frontend_config(
        args.frontend_args.config_path,
        args.frontend_args.persistent_path,
        None
    ) {
        Ok(config) => config,
        Err(e) => {
            println!("{}", e.red());
            return;
        }
    };
    
    let persistent_path = config.persist_path.unwrap_or(DEFAULT_PERSISTENT_PATH.into());
    
    if (!Path::new(&persistent_path).exists()) {
        println!("{}", format!("Path does not exist: {}", &persistent_path).red());
        return;
    }

    if (!Path::new(format!("{}/{}", &persistent_path, SQLITE_FILENAME).as_str()).exists()) {
        println!("{}", format!("Not a Chroma path: {}", &persistent_path).red());
        return;
    }

    let proceed = Confirm::new()
        .with_prompt("Are you sure you want to vacuum the database? This will block both reads and writes to the database and may take a while. We recommend shutting down the server before running this command. Continue?")
        .default(false)
        .interact()
        .unwrap_or_else(|e| {
            eprintln!("Failed to get confirmation: {}", e);
            false
        });

    if (!proceed) {
        println!("{}", "Vacuum cancelled".red());
        return;
    }

    let registry = Registry::new();
    

    println!();
}
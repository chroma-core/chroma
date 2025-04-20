use clap::Parser;
use thiserror::Error;
use crate::commands::install::InstallError;
use crate::utils::CliError;

#[derive(Parser, Debug)]
pub struct BrowseArgs {
    #[clap(index = 1, help = "The name of the collection to browse")]
    collection_name: String,
    #[clap(long = "db")]
    db_name: Option<String>,
    #[clap(long, conflicts_with_all = ["host", "config_path"])]
    path: Option<String>,
    #[clap(long, conflicts_with_all = ["path", "config_path"])]
    host: Option<String>,
    #[clap(long = "config", conflicts_with_all = ["host", "path"])]
    config_path: Option<String>,
    #[clap(long)]
    local: bool,
}

#[derive(Debug, Error)]
pub enum BrowseError {
    
}

pub fn browse(args: BrowseArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|_| InstallError::RuntimeError)?;
    runtime.block_on(async {  })?;
    Ok(())
}
use clap::Parser;
use thiserror::Error;
use crate::utils::CliError;

// Skip installing
#[derive(Debug, Error)]
pub enum InstallError {
    #[error("Failed to install sample app {0}")]
    InstallFailed(String),
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[clap(
        index = 1,
        help = "The name of the sample app to install",
    )]
    name: String,
}

pub fn install(args: InstallArgs) -> Result<(), CliError> {
    // Get all apps manifest to verify app name
    
    // Get app manifest and build config
    
    // Verify CLI version compat
    
    // Download files
    
    // Download Chroma from S3
    
    // Ask for installer
    
    // Run installer
    
    // Output run instructions
    
    // Add app to CLI config with version 
    Ok(())
}
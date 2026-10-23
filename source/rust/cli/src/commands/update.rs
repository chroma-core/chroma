use crate::utils::CliError;
use colored::Colorize;
use regex::Regex;
use semver::Version;
use serde::Deserialize;
use std::error::Error;
use thiserror::Error;

const GITHUB_RELEASES_URL: &str = "https://api.github.com/repos/chroma-core/chroma/releases";
const UNIX_CURL: &str = "curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash";
const WINDOWS_CURL: &str = "iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1'))";

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("Failed to fetch the latest Chroma CLI version")]
    FailedVersionFetch,
    #[error("Failed to get the current Chroma CLI version")]
    CurrentVersionUndetected,
    #[error("Failed to update the Chroma CLI")]
    UpdateFailed,
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
}

async fn version_check(current_version: Version) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let releases = client
        .get(GITHUB_RELEASES_URL)
        .header("User-Agent", "reqwest")
        .send()
        .await?
        .json::<Vec<Release>>()
        .await?;

    let cli_version_pattern = Regex::new(r"^cli-(\d+\.\d+\.\d+)$")?;
    let mut cli_versions = Vec::new();

    for release in releases {
        if let Some(caps) = cli_version_pattern.captures(&release.tag_name) {
            if let Some(ver_match) = caps.get(1) {
                let ver_str = ver_match.as_str();
                if let Ok(ver) = Version::parse(ver_str) {
                    cli_versions.push(ver);
                }
            }
        }
    }

    if cli_versions.is_empty() {
        return Err(UpdateError::FailedVersionFetch.into());
    }

    let latest = cli_versions
        .into_iter()
        .max()
        .unwrap_or(current_version.clone());

    if latest == current_version {
        println!("{}", "Your Chroma CLI version is up-to-date!".green());
    } else {
        println!(
            "A new version of the Chroma CLI is available! To upgrade to version {} run",
            latest
        );
        if cfg!(target_os = "windows") {
            println!("{}", WINDOWS_CURL.green());
        } else {
            println!("{}", UNIX_CURL.green());
        }
    }

    Ok(())
}

pub fn update() -> Result<(), CliError> {
    let current_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|_| UpdateError::CurrentVersionUndetected)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|_| UpdateError::UpdateFailed)?;
    Ok(runtime
        .block_on(version_check(current_version))
        .map_err(|_| UpdateError::FailedVersionFetch)?)
}

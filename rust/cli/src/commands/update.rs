use colored::Colorize;
use regex::Regex;
use semver::Version;

const GITHUB_RELEASES_URL: &str = "https://api.github.com/repos/chroma-core/chroma/releases";
const UNIX_CURL: &str = "curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash";
const WINDOWS_CURL: &str = "iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1'))";

pub async fn version_check(current_version: Version) {
    let client = reqwest::Client::new();
    let response = match client
        .get(GITHUB_RELEASES_URL)
        .header("User-Agent", "reqwest")
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(_) => {
            eprintln!("Couldn't fetch the latest Chroma CLI version");
            return;
        }
    };

    if !response.status().is_success() {
        eprintln!("Couldn't fetch the latest Chroma CLI version");
        return;
    }

    let releases: Vec<String> = match response.json().await {
        Ok(data) => data,
        Err(_) => {
            eprintln!("Couldn't fetch the latest Chroma CLI version");
            return;
        }
    };

    let cli_version_pattern = Regex::new(r"^cli-(\d+\.\d+\.\d+)$").unwrap();
    let mut cli_versions = Vec::new();

    for release in releases {
        if let Some(caps) = cli_version_pattern.captures(&release) {
            if let Some(ver_match) = caps.get(1) {
                let ver_str = ver_match.as_str();
                if let Ok(ver) = Version::parse(ver_str) {
                    cli_versions.push(ver);
                }
            }
        }
    }

    if cli_versions.is_empty() {
        eprintln!("Couldn't fetch the latest Chroma CLI version");
        return;
    }

    let latest = cli_versions.into_iter().max().unwrap();

    if latest == current_version {
        println!("{}", "\nYour Chroma CLI version is up-to-date!\n".green());
    } else {
        println!(
            "\nA new version of the Chroma CLI is available! To upgrade to version {} run\n",
            latest
        );
        if cfg!(target_os = "windows") {
            eprintln!("{}", WINDOWS_CURL.green());
        } else {
            eprintln!("{}", UNIX_CURL.green());
        }
    }
}

pub fn update() {
    let current_version =
        Version::parse(env!("CARGO_PKG_VERSION")).expect("Couldn't parse current CLI version");
    let runtime = tokio::runtime::Runtime::new().expect("Failed to update Chroma");
    runtime.block_on(version_check(current_version));
}

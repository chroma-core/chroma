use chroma_frontend::config::FrontendServerConfig;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::{fs, io};

pub const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

pub const DEFAULT_PERSISTENT_PATH: &str = "./chroma";
pub const SQLITE_FILENAME: &str = "chroma.sqlite3";

#[derive(Parser, Debug)]
pub struct LocalFrontendCommandArgs {
    #[arg(long = "config")]
    pub config_path: Option<String>,
    #[arg(long = "path")]
    pub persistent_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub api_key: String,
    pub tenant_id: String,
    pub team: String,
}

#[derive(Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
}

pub fn get_frontend_config(
    config_path: Option<String>,
    persistent_path: Option<String>,
    port: Option<u16>,
) -> Result<FrontendServerConfig, String> {
    if config_path.is_some() && (persistent_path.is_some() || port.is_some()) {
        return Err("Cannot specify a config file with persistent path or port.".into());
    }

    let mut config = match config_path {
        Some(config_path) => FrontendServerConfig::load_from_path(&config_path),
        None => FrontendServerConfig::single_node_default(),
    };

    config.persist_path = persistent_path;

    if let Some(ref mut sqlite_config) = config.frontend.sqlitedb {
        sqlite_config.url = Some(format!(
            "{}/{}",
            config
                .persist_path
                .as_ref()
                .unwrap_or(&DEFAULT_PERSISTENT_PATH.to_string()),
            SQLITE_FILENAME
        ));
    }

    config.port = port.unwrap_or(config.port);

    Ok(config)
}

pub fn get_or_create_config_file() -> PathBuf {
    let home_dir = dirs::home_dir().expect("\nCould not find home directory\n");

    let mut chroma_dir = PathBuf::from(&home_dir);
    chroma_dir.push(".chroma");

    let mut config_file = chroma_dir.clone();
    config_file.push("config.json");

    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir).expect("\nCould not create Chroma directory\n");
    }

    if !config_file.exists() {
        File::create(&config_file).expect("\nCould not create config file\n");
    }

    config_file
}

pub fn get_or_create_credentials_file() -> PathBuf {
    let home_dir = dirs::home_dir().expect("\nCould not find home directory\n");

    let mut chroma_dir = PathBuf::from(&home_dir);
    chroma_dir.push(".chroma");

    let mut credentials_file = chroma_dir.clone();
    credentials_file.push("credentials");

    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir).expect("\nCould not create Chroma directory\n");
    }

    if !credentials_file.exists() {
        File::create(&credentials_file).expect("\nCould not create credentials file\n");
    }

    credentials_file
}

pub fn write_credentials_file(
    credentials: &HashMap<String, Profile>,
    file_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let toml_string = toml::to_string(credentials)?;
    fs::write(file_path, toml_string)?;
    Ok(())
}

pub fn read_credentials_file(
    credentials_file: &PathBuf,
) -> Result<HashMap<String, Profile>, Box<dyn std::error::Error>> {
    let toml_string = fs::read_to_string(credentials_file)?;
    let profiles: HashMap<String, Profile> = toml::from_str(&toml_string)?;
    Ok(profiles)
}

pub fn write_cli_config(current_profile: String) {
    let path = get_or_create_config_file();
    let config = CliConfig { current_profile };
    let file = File::create(path).expect("\nCould not write config file\n");
    serde_json::to_writer_pretty(file, &config).expect("\nCould not write config file\n");
}

pub fn read_cli_config() -> CliConfig {
    let path = get_or_create_config_file();
    let file = File::open(path).expect("\nCould not read config file\n");
    serde_json::from_reader(file).expect("\nCould not parse config file\n")
}

pub fn get_profile(profile_name: String) -> Profile {
    let credentials_file = get_or_create_credentials_file();
    let profiles =
        read_credentials_file(&credentials_file).expect("\nCould not read credentials file\n");
    profiles
        .get(&profile_name)
        .cloned()
        .expect(format!("\nCould not find {} profile\n", &profile_name).as_str())
}

pub fn get_current_profile() -> Profile {
    let config = read_cli_config();
    let current_profile = config.current_profile;
    get_profile(current_profile)
}

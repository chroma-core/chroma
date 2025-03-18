use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub name: String,
    pub api_key: String,
    pub team_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
}

pub type Profiles = HashMap<String, Profile>;

fn get_credentials_file_path() -> Result<PathBuf, Box<dyn Error>> {
    let home_dir = std::env::var("HOME")?;
    let chroma_dir = PathBuf::from(home_dir).join(".chroma");
    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir)?;
    }

    let credentials_path = chroma_dir.join("credentials");
    if !credentials_path.exists() {
        fs::write(&credentials_path, "")?;
    }

    Ok(credentials_path)
}

pub fn get_profiles() -> Result<Profiles, Box<dyn Error>> {
    let credentials_path = get_credentials_file_path()?;
    let contents = fs::read_to_string(credentials_path)?;
    let profiles: Profiles = toml::from_str(&contents)?;
    Ok(profiles)
}

pub fn write_profiles(profiles: &Profiles) -> Result<(), Box<dyn Error>> {
    let credentials_path = get_credentials_file_path()?;
    let toml_str = toml::to_string(profiles)?;
    fs::write(credentials_path, toml_str)?;
    Ok(())
}

fn get_config_file_path() -> Result<PathBuf, Box<dyn Error>> {
    let home_dir = std::env::var("HOME")?;
    let chroma_dir = PathBuf::from(home_dir).join(".chroma");
    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir)?;
    }

    let config_path = chroma_dir.join("config.json");
    if !config_path.exists() {
        let default_config = CliConfig {
            current_profile: String::new(),
        };
        let json_str = serde_json::to_string_pretty(&default_config)?;
        fs::write(&config_path, json_str)?;
    }

    Ok(config_path)
}

pub fn read_config() -> Result<CliConfig, Box<dyn Error>> {
    let config_path = get_config_file_path()?;
    let contents = fs::read_to_string(&config_path)?;
    let config: CliConfig = serde_json::from_str(&contents)?;
    Ok(config)
}

pub fn write_config(config: &CliConfig) -> Result<(), Box<dyn Error>> {
    let config_path = get_config_file_path()?;
    let json_str = serde_json::to_string_pretty(config)?;
    fs::write(config_path, json_str)?;
    Ok(())
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use colored::Colorize;

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

const CHROMA_DIR: &str = ".chroma";
const CREDENTIALS_FILE: &str = "credentials";
const CONFIG_FILE: &str = "config.json";

#[derive(Debug)]
pub struct ChromaCliError {
    #[allow(dead_code)]
    pub message: String,
}

impl ChromaCliError {
    fn new(msg: &str) -> ChromaCliError {
        ChromaCliError { message: msg.to_string() }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub api_key: String,
    pub team_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CliConfig {
    pub current_profile: String,
}

pub type Profiles = HashMap<String, Profile>;

fn get_chroma_dir() -> Result<PathBuf, Box<dyn Error>> {
    let home_dir = dirs::home_dir().ok_or("Could not determine home directory")?;
    let chroma_dir = home_dir.join(CHROMA_DIR);
    if !chroma_dir.exists() {
        fs::create_dir_all(&chroma_dir)?;
    };
    Ok(chroma_dir)
}

fn get_credentials_file_path() -> Result<PathBuf, Box<dyn Error>> {
    let chroma_dir = get_chroma_dir()?;
    let credentials_path = chroma_dir.join(CREDENTIALS_FILE);
    if !credentials_path.exists() {
        fs::write(&credentials_path, "")?;
    }
    Ok(credentials_path)
}

pub fn read_profiles() -> Result<Profiles, Box<dyn Error>> {
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
    let chroma_dir = get_chroma_dir()?;
    let config_path = chroma_dir.join(CONFIG_FILE);
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

pub fn get_config() -> Option<CliConfig> {
    match read_config() {
        Ok(config) => Some(config),
        Err(_) => {
            eprintln!("{}", "Could not load CLI config".red());
            None
        }
    }
}

pub fn get_profiles() -> Option<Profiles> {
    match read_profiles() {
        Ok(profiles) => Some(profiles),
        Err(_) => {
            eprintln!("{}", "Could not load profiles".red());
            None
        }
    }
}

pub fn save_config(config: &CliConfig) -> Result<(), ChromaCliError> {
    match write_config(config) {
        Ok(_) => Ok(()),
        Err(_e) => {
            let message = "Could not save CLI config".red();
            eprintln!("{}", message);
            Err(ChromaCliError::new(&message))
        }
    }
}

pub fn save_profiles(profiles: &Profiles) -> Result<(), ChromaCliError> {
    match write_profiles(profiles) {
        Ok(_) => Ok(()),
        Err(_e) => {
            let message = "Could not save credentials".red();
            eprintln!("{}", message);
            Err(ChromaCliError::new(&message))
        }
    }
}

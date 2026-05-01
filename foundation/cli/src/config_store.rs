use crate::error::FoundationError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

pub type Profiles = HashMap<String, Profile>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Profile {
    pub api_key: String,
    pub tenant_id: String,
}

impl Profile {
    pub fn new(api_key: String, tenant_id: String) -> Self {
        Self { api_key, tenant_id }
    }
}

/// Minimal config stored alongside credentials.
/// We use the same file as chroma-cli so credentials are shared.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FoundationConfig {
    #[serde(default)]
    pub current_profile: String,
}

pub trait ConfigStore {
    fn read_profiles(&self) -> Result<Profiles, FoundationError>;
    fn write_profiles(&self, profiles: &Profiles) -> Result<(), FoundationError>;
    fn read_config(&self) -> Result<FoundationConfig, FoundationError>;
    fn write_config(&self, config: &FoundationConfig) -> Result<(), FoundationError>;
    fn config_dir(&self) -> String;

    fn get_profile(&self, name: &str) -> Result<Profile, FoundationError> {
        let profiles = self.read_profiles()?;
        profiles
            .get(name)
            .cloned()
            .ok_or_else(|| FoundationError::ProfileNotFound(name.to_string()))
    }

    fn get_current_profile(&self) -> Result<(String, Profile), FoundationError> {
        let config = self.read_config()?;
        if config.current_profile.is_empty() {
            return Err(FoundationError::NoActiveProfile);
        }
        let name = config.current_profile.clone();
        let profile = self
            .get_profile(&name)
            .map_err(|_| FoundationError::NoActiveProfile)?;
        Ok((name, profile))
    }
}

pub struct FileConfigStore {
    chroma_dir: String,
    credentials_file: String,
    config_file: String,
}

impl FileConfigStore {
    fn get_chroma_dir(&self) -> Result<PathBuf, FoundationError> {
        let home_dir = home::home_dir().ok_or(FoundationError::HomeDirNotFound)?;
        let chroma_dir = home_dir.join(&self.chroma_dir);
        if chroma_dir.exists() && !chroma_dir.is_dir() {
            return Err(FoundationError::ConfigDirNotADirectory);
        }
        if !chroma_dir.exists() {
            fs::create_dir_all(&chroma_dir).map_err(|_| FoundationError::ConfigDirCreateFailed)?;
        }
        Ok(chroma_dir)
    }

    fn credentials_path(&self) -> Result<PathBuf, FoundationError> {
        Ok(self.get_chroma_dir()?.join(&self.credentials_file))
    }

    fn config_path(&self) -> Result<PathBuf, FoundationError> {
        Ok(self.get_chroma_dir()?.join(&self.config_file))
    }
}

impl Default for FileConfigStore {
    fn default() -> Self {
        Self {
            // Intentionally share ~/.chroma with chroma-cli so a user
            // logged into `chroma` is automatically auth'd with `foundation`.
            chroma_dir: ".chroma".to_string(),
            credentials_file: "credentials".to_string(),
            config_file: "config.json".to_string(),
        }
    }
}

impl ConfigStore for FileConfigStore {
    fn config_dir(&self) -> String {
        format!("~/{}", self.chroma_dir)
    }

    fn read_profiles(&self) -> Result<Profiles, FoundationError> {
        let path = self.credentials_path()?;
        if !path.exists() {
            return Ok(Profiles::new());
        }
        let contents =
            fs::read_to_string(path).map_err(|_| FoundationError::CredsFileReadFailed)?;
        let profiles: Profiles =
            toml::from_str(&contents).map_err(|_| FoundationError::CredsFileParseFailed)?;
        Ok(profiles)
    }

    fn write_profiles(&self, profiles: &Profiles) -> Result<(), FoundationError> {
        let path = self.credentials_path()?;
        let toml_str =
            toml::to_string(profiles).map_err(|_| FoundationError::CredsFileParseFailed)?;
        fs::write(path, toml_str).map_err(|_| FoundationError::CredsFileWriteFailed)?;
        Ok(())
    }

    fn read_config(&self) -> Result<FoundationConfig, FoundationError> {
        let path = self.config_path()?;
        if !path.exists() {
            return Ok(FoundationConfig::default());
        }
        let contents =
            fs::read_to_string(&path).map_err(|_| FoundationError::ConfigFileReadFailed)?;
        // Parse as a generic JSON value so we can extract just current_profile
        // without failing if chroma-cli has extra fields (sample_apps, theme, etc.)
        let value: serde_json::Value =
            serde_json::from_str(&contents).map_err(|_| FoundationError::ConfigFileParseFailed)?;
        let current_profile = value
            .get("current_profile")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(FoundationConfig { current_profile })
    }

    fn write_config(&self, config: &FoundationConfig) -> Result<(), FoundationError> {
        let path = self.config_path()?;
        // Read existing JSON to preserve chroma-cli fields (sample_apps, theme, etc.)
        let mut value: serde_json::Value = if path.exists() {
            let contents =
                fs::read_to_string(&path).map_err(|_| FoundationError::ConfigFileReadFailed)?;
            serde_json::from_str(&contents).unwrap_or(serde_json::Value::Object(Default::default()))
        } else {
            serde_json::Value::Object(Default::default())
        };
        if let serde_json::Value::Object(ref mut map) = value {
            map.insert(
                "current_profile".to_string(),
                serde_json::Value::String(config.current_profile.clone()),
            );
        }
        let json_str = serde_json::to_string_pretty(&value)
            .map_err(|_| FoundationError::ConfigFileParseFailed)?;
        fs::write(path, json_str).map_err(|_| FoundationError::ConfigFileWriteFailed)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_config_store {
    use super::{ConfigStore, FoundationConfig, Profiles};
    use crate::error::FoundationError;
    use std::cell::RefCell;

    pub struct InMemoryConfigStore {
        profiles: RefCell<Profiles>,
        config: RefCell<FoundationConfig>,
    }

    impl InMemoryConfigStore {
        pub fn new(profiles: Profiles, config: FoundationConfig) -> Self {
            Self {
                profiles: RefCell::new(profiles),
                config: RefCell::new(config),
            }
        }
    }

    impl ConfigStore for InMemoryConfigStore {
        fn config_dir(&self) -> String {
            "memory".to_string()
        }

        fn read_profiles(&self) -> Result<Profiles, FoundationError> {
            Ok(self.profiles.borrow().clone())
        }

        fn write_profiles(&self, profiles: &Profiles) -> Result<(), FoundationError> {
            *self.profiles.borrow_mut() = profiles.clone();
            Ok(())
        }

        fn read_config(&self) -> Result<FoundationConfig, FoundationError> {
            Ok(self.config.borrow().clone())
        }

        fn write_config(&self, config: &FoundationConfig) -> Result<(), FoundationError> {
            *self.config.borrow_mut() = config.clone();
            Ok(())
        }
    }
}

use crate::commands::profile::ProfileError;
use crate::utils::{CliConfig, CliError, Profile, Profiles, UtilsError};
use std::fs;
use std::path::PathBuf;

pub trait ConfigStore {
    fn read_profiles(&self) -> Result<Profiles, CliError>;
    fn write_profiles(&self, profiles: &Profiles) -> Result<(), CliError>;
    fn read_config(&self) -> Result<CliConfig, CliError>;
    fn write_config(&self, config: &CliConfig) -> Result<(), CliError>;
    fn config_dir(&self) -> String;

    fn get_profile(&self, name: String) -> Result<Profile, CliError> {
        let profiles = self.read_profiles()?;
        if !profiles.contains_key(&name) {
            Err(ProfileError::ProfileNotFound(name).into())
        } else {
            Ok(profiles[&name].clone())
        }
    }

    fn get_current_profile(&self) -> Result<(String, Profile), CliError> {
        let config = self.read_config()?;
        if config.current_profile.is_empty() {
            return Err(ProfileError::NoActiveProfile.into());
        }
        let profile_name = config.current_profile.clone();
        let profile = self
            .get_profile(config.current_profile)
            .map_err(|e| match e {
                CliError::Profile(ProfileError::ProfileNotFound(_)) => {
                    ProfileError::NoActiveProfile.into()
                }
                _ => e,
            })?;
        Ok((profile_name, profile))
    }
}

pub struct FileConfigStore {
    chroma_dir: String,
    credentials_file: String,
    config_file: String,
}

impl FileConfigStore {
    fn default_chroma_dir() -> String {
        ".chroma".to_string()
    }

    fn default_credentials_file() -> String {
        "credentials".to_string()
    }

    fn default_config_file() -> String {
        "config.json".to_string()
    }

    fn get_chroma_dir(&self) -> Result<PathBuf, CliError> {
        let home_dir = dirs::home_dir().ok_or(UtilsError::HomeDirNotFound)?;
        let chroma_dir = home_dir.join(&self.chroma_dir);
        if chroma_dir.exists() && !chroma_dir.is_dir() {
            return Err(UtilsError::ChromaDirNotADirectory.into());
        }
        if !chroma_dir.exists() {
            fs::create_dir_all(&chroma_dir).map_err(|_| UtilsError::ChromaDirCreateFailed)?;
        }
        Ok(chroma_dir)
    }

    fn get_credentials_file_path(&self) -> Result<PathBuf, CliError> {
        let chroma_dir = self.get_chroma_dir()?;
        Ok(chroma_dir.join(&self.credentials_file))
    }

    fn get_config_file_path(&self) -> Result<PathBuf, CliError> {
        let chroma_dir = self.get_chroma_dir()?;
        Ok(chroma_dir.join(&self.config_file))
    }
}

impl Default for FileConfigStore {
    fn default() -> Self {
        Self {
            chroma_dir: Self::default_chroma_dir(),
            credentials_file: Self::default_credentials_file(),
            config_file: Self::default_config_file(),
        }
    }
}

impl ConfigStore for FileConfigStore {
    fn config_dir(&self) -> String {
        format!("~/{}", self.chroma_dir)
    }

    fn read_profiles(&self) -> Result<Profiles, CliError> {
        let credentials_path = self.get_credentials_file_path()?;
        if !credentials_path.exists() {
            return Ok(Profiles::new());
        }
        let contents =
            fs::read_to_string(credentials_path).map_err(|_| UtilsError::CredsFileReadFailed)?;
        let profiles: Profiles =
            toml::from_str(&contents).map_err(|_| UtilsError::CredsFileParseFailed)?;
        Ok(profiles)
    }

    fn write_profiles(&self, profiles: &Profiles) -> Result<(), CliError> {
        let credentials_path = self.get_credentials_file_path()?;
        let toml_str = toml::to_string(profiles).map_err(|_| UtilsError::CredsFileParseFailed)?;
        fs::write(credentials_path, toml_str).map_err(|_| UtilsError::CredsFileWriteFailed)?;
        Ok(())
    }

    fn read_config(&self) -> Result<CliConfig, CliError> {
        let config_path = self.get_config_file_path()?;
        if !config_path.exists() {
            return Ok(CliConfig::default());
        }
        let contents =
            fs::read_to_string(&config_path).map_err(|_| UtilsError::ConfigFileReadFailed)?;
        let config: CliConfig =
            serde_json::from_str(&contents).map_err(|_| UtilsError::ConfigFileParseFailed)?;
        Ok(config)
    }

    fn write_config(&self, config: &CliConfig) -> Result<(), CliError> {
        let config_path = self.get_config_file_path()?;
        let json_str =
            serde_json::to_string_pretty(config).map_err(|_| UtilsError::ConfigFileParseFailed)?;
        fs::write(config_path, json_str).map_err(|_| UtilsError::ConfigFileWriteFailed)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_config_store {
    use super::ConfigStore;
    use crate::utils::{CliConfig, CliError, Profiles};
    use std::cell::RefCell;

    pub struct InMemoryConfigStore {
        profiles: RefCell<Profiles>,
        config: RefCell<CliConfig>,
    }

    impl InMemoryConfigStore {
        pub fn new(profiles: Profiles, config: CliConfig) -> Self {
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

        fn read_profiles(&self) -> Result<Profiles, CliError> {
            Ok(self.profiles.borrow().clone())
        }

        fn write_profiles(&self, profiles: &Profiles) -> Result<(), CliError> {
            *self.profiles.borrow_mut() = profiles.clone();
            Ok(())
        }

        fn read_config(&self) -> Result<CliConfig, CliError> {
            Ok(self.config.borrow().clone())
        }

        fn write_config(&self, config: &CliConfig) -> Result<(), CliError> {
            *self.config.borrow_mut() = config.clone();
            Ok(())
        }
    }
}

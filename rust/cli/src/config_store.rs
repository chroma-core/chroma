use crate::commands::profile::ProfileError;
use crate::utils::{self, CliConfig, CliError, Profile, Profiles};

pub fn get_current_profile(store: &dyn ConfigStore) -> Result<(String, Profile), CliError> {
    let config = store.read_config()?;
    let profiles = store.read_profiles()?;
    let profile_name = config.current_profile.clone();
    let profile = profiles
        .get(&profile_name)
        .cloned()
        .ok_or_else(|| -> CliError {
            match profiles.contains_key(&profile_name) {
                true => unreachable!(),
                false => ProfileError::NoActiveProfile.into(),
            }
        })?;
    Ok((profile_name, profile))
}

pub trait ConfigStore {
    fn read_profiles(&self) -> Result<Profiles, CliError>;
    fn write_profiles(&self, profiles: &Profiles) -> Result<(), CliError>;
    fn read_config(&self) -> Result<CliConfig, CliError>;
    fn write_config(&self, config: &CliConfig) -> Result<(), CliError>;
}

pub struct FileConfigStore;

impl ConfigStore for FileConfigStore {
    fn read_profiles(&self) -> Result<Profiles, CliError> {
        utils::read_profiles()
    }

    fn write_profiles(&self, profiles: &Profiles) -> Result<(), CliError> {
        utils::write_profiles(profiles)
    }

    fn read_config(&self) -> Result<CliConfig, CliError> {
        utils::read_config()
    }

    fn write_config(&self, config: &CliConfig) -> Result<(), CliError> {
        utils::write_config(config)
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

        pub fn profiles(&self) -> Profiles {
            self.profiles.borrow().clone()
        }

        pub fn config(&self) -> CliConfig {
            self.config.borrow().clone()
        }
    }

    impl ConfigStore for InMemoryConfigStore {
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

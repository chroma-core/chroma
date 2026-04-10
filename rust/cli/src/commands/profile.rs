use crate::config_store::{ConfigStore, FileConfigStore};
use crate::terminal::{SystemTerminal, Terminal};
use crate::utils::{CliConfig, CliError, Profiles};
use clap::{Args, Subcommand};
use colored::Colorize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("Profile {0} not found")]
    ProfileNotFound(String),
    #[error("No current profile found.\nTo set a new profile use: chroma login")]
    NoActiveProfile,
    #[error("Profile {0} already exists.\nTo delete it use: chroma profile delete {0}")]
    ProfileAlreadyExists(String),
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(index = 1, help = "The name of the profile to delete")]
    name: String,
    #[clap(
        long,
        default_value_t = false,
        help = "Skip delete confirmation for the active profile"
    )]
    force: bool,
}

#[derive(Args, Debug)]
pub struct RenameArgs {
    #[clap(index = 1, help = "The name of the profile to rename")]
    name: String,
    #[clap(index = 2, help = "The new name for the profile to rename")]
    new_name: String,
}

#[derive(Args, Debug)]
pub struct UseArgs {
    #[clap(help = "The name of the profile to use as the active profile")]
    name: String,
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    #[command(about = "Delete profiles")]
    Delete(DeleteArgs),
    #[clap(about = "List all available profiles")]
    List,
    #[clap(about = "Show the current active profile")]
    Show,
    #[clap(about = "Rename a profile")]
    Rename(RenameArgs),
    #[clap(about = "Set the profile to use as the active profile")]
    Use(UseArgs),
}

fn confirm_profile_delete_message(profile_name: &str) -> String {
    format!(
        "{}\n{}\n{} {}, {}\n\nDo you want to delete profile {}? (Y/n)",
        "Warning! You are deleting the currently active profile"
            .yellow()
            .bold(),
        "All Chroma Cloud CLI operations will fail without an active profile.",
        "If you wish to proceed, please use:",
        "chroma profile use <profile name>".yellow(),
        "to set a new profile",
        profile_name
    )
}

fn profile_delete_abort_message() -> String {
    format!("{}", "\nDelete cancelled!".green())
}

fn profile_delete_success_message(profile_name: &str) -> String {
    format!(
        "{} {} {}",
        "Profile".green(),
        profile_name.green(),
        "successfully removed".green()
    )
}

fn no_profiles_found_message() -> String {
    format!(
        "No profiles defined at the moment. To add a new profile use {}",
        "chroma login".yellow()
    )
}

fn current_profile_set_message(profile_name: &str) -> String {
    format!("Current profile set to {}", profile_name)
        .green()
        .to_string()
}

fn no_current_profile_message() -> String {
    format!(
        "No profile set currently. Please use {} to add a profile, or {} to set an existing profile",
        "chroma login".yellow(),
        "chroma profile use <profile name>".yellow()
    )
}

fn current_profile_message(profile_name: &str) -> String {
    format!("{}\n{}", "Current profile: ".blue().bold(), profile_name)
}

fn rename_success_message(old_name: &str, new_name: &str) -> String {
    format!("Successfully renamed profile {} to {}", old_name, new_name)
        .green()
        .to_string()
}

fn confirm_deletion(profile_name: &str, term: &mut dyn Terminal) -> Result<bool, CliError> {
    term.println(&confirm_profile_delete_message(profile_name));

    let confirm = term.prompt_input()?;

    let confirmed = confirm.to_lowercase() == "y" || confirm.to_lowercase() == "yes";
    if confirmed {
        term.println("");
    }

    Ok(confirmed)
}

fn delete_profile(
    args: DeleteArgs,
    profiles: &mut Profiles,
    config: &mut CliConfig,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let delete_profile_name = args.name;
    if !profiles.contains_key(&delete_profile_name) {
        return Err(ProfileError::ProfileNotFound(delete_profile_name).into());
    }

    if config.current_profile == delete_profile_name {
        let confirmed = args.force || confirm_deletion(&delete_profile_name, term)?;
        if confirmed {
            config.current_profile = "".to_string();
        } else {
            term.println(&profile_delete_abort_message());
            return Ok(());
        }
    };

    profiles.remove(&delete_profile_name);

    term.println(&profile_delete_success_message(&delete_profile_name));
    Ok(())
}

fn list_profiles(
    profiles: &Profiles,
    config: &CliConfig,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    if profiles.is_empty() {
        term.println(&no_profiles_found_message());
        return Ok(());
    }

    term.println(&format!("{}", "Available profiles:".blue().bold()));

    if !config.current_profile.is_empty() {
        let current_profile_label = format!("{} (current)", config.current_profile).bold();
        term.println(&format!("{} {}", ">".yellow(), current_profile_label));
    }

    for key in profiles.keys() {
        if *key != config.current_profile {
            term.println(&format!("{} {}", ">".yellow(), key));
        }
    }

    Ok(())
}

fn rename(
    args: RenameArgs,
    profiles: &mut Profiles,
    config: &mut CliConfig,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let rename_profile_name = args.name;
    let new_name = args.new_name;

    if !profiles.contains_key(&rename_profile_name) {
        return Err(ProfileError::ProfileNotFound(rename_profile_name).into());
    }

    if profiles.contains_key(&new_name) {
        return Err(ProfileError::ProfileAlreadyExists(new_name).into());
    }

    let is_current = rename_profile_name.eq(&config.current_profile);

    profiles.insert(
        new_name.clone(),
        profiles.get(&rename_profile_name).unwrap().clone(),
    );

    if is_current {
        config.current_profile = new_name.clone();
    }

    term.println(&rename_success_message(&rename_profile_name, &new_name));

    Ok(())
}

fn show(config: &CliConfig, term: &mut dyn Terminal) -> Result<(), CliError> {
    if config.current_profile.is_empty() {
        term.println(&no_current_profile_message());
        return Ok(());
    }

    term.println(&current_profile_message(&config.current_profile));
    Ok(())
}

fn use_profile(
    args: UseArgs,
    profiles: &Profiles,
    config: &mut CliConfig,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    if !profiles.contains_key(&args.name) {
        return Err(ProfileError::ProfileNotFound(args.name.clone()).into());
    }

    config.current_profile = args.name;
    term.println(&current_profile_set_message(&config.current_profile));
    Ok(())
}

pub fn run_profile_command(
    command: ProfileCommand,
    store: &dyn ConfigStore,
    term: &mut dyn Terminal,
) -> Result<(), CliError> {
    let mut profiles = store.read_profiles()?;
    let mut config = store.read_config()?;

    match command {
        ProfileCommand::Delete(args) => {
            delete_profile(args, &mut profiles, &mut config, term)?;
            store.write_profiles(&profiles)?;
            store.write_config(&config)?;
        }
        ProfileCommand::List => list_profiles(&profiles, &config, term)?,
        ProfileCommand::Rename(args) => {
            rename(args, &mut profiles, &mut config, term)?;
            store.write_profiles(&profiles)?;
            store.write_config(&config)?;
        }
        ProfileCommand::Show => show(&config, term)?,
        ProfileCommand::Use(args) => {
            use_profile(args, &profiles, &mut config, term)?;
            store.write_config(&config)?;
        }
    }

    Ok(())
}

pub fn profile_command(command: ProfileCommand) -> Result<(), CliError> {
    let store = FileConfigStore;
    let mut term = SystemTerminal;
    run_profile_command(command, &store, &mut term)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::test_config_store::InMemoryConfigStore;
    use crate::terminal::test_terminal::TestTerminal;
    use crate::utils::Profile;
    use std::collections::HashMap;

    fn make_config(current_profile: &str) -> CliConfig {
        CliConfig {
            current_profile: current_profile.to_string(),
            sample_apps: Default::default(),
            theme: Default::default(),
        }
    }

    fn make_profiles(names: &[&str]) -> Profiles {
        let mut profiles = HashMap::new();
        for name in names {
            profiles.insert(
                name.to_string(),
                Profile::new("test-api-key".to_string(), "test-tenant".to_string()),
            );
        }
        profiles
    }

    // ── inner function tests ──

    #[test]
    fn test_show_displays_current_profile() {
        let config = make_config("my-profile");
        let mut term = TestTerminal::new();

        show(&config, &mut term).unwrap();

        assert_eq!(term.output.len(), 1);
        assert!(term.output[0].contains("my-profile"));
        assert!(term.output[0].contains("Current profile:"));
    }

    #[test]
    fn test_show_no_current_profile() {
        let config = make_config("");
        let mut term = TestTerminal::new();

        show(&config, &mut term).unwrap();

        assert_eq!(term.output.len(), 1);
        assert!(term.output[0].contains("No profile set currently"));
    }

    #[test]
    fn test_list_empty_profiles() {
        let profiles = make_profiles(&[]);
        let config = make_config("");
        let mut term = TestTerminal::new();

        list_profiles(&profiles, &config, &mut term).unwrap();

        assert_eq!(term.output.len(), 1);
        assert!(term.output[0].contains("No profiles defined"));
    }

    #[test]
    fn test_list_profiles_shows_current() {
        let profiles = make_profiles(&["default", "staging"]);
        let config = make_config("default");
        let mut term = TestTerminal::new();

        list_profiles(&profiles, &config, &mut term).unwrap();

        assert!(term.output[0].contains("Available profiles:"));
        assert!(term.output[1].contains("default"));
        assert!(term.output[1].contains("(current)"));
    }

    #[test]
    fn test_use_profile_switches_current() {
        let profiles = make_profiles(&["a", "b"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        use_profile(
            UseArgs {
                name: "b".to_string(),
            },
            &profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert_eq!(config.current_profile, "b");
        assert!(term.output[0].contains("Current profile set to"));
    }

    #[test]
    fn test_use_profile_not_found() {
        let profiles = make_profiles(&["a"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        let result = use_profile(
            UseArgs {
                name: "nonexistent".to_string(),
            },
            &profiles,
            &mut config,
            &mut term,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_delete_non_current_profile() {
        let mut profiles = make_profiles(&["a", "b"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        delete_profile(
            DeleteArgs {
                name: "b".to_string(),
                force: false,
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert!(!profiles.contains_key("b"));
        assert!(profiles.contains_key("a"));
        assert!(term.output.last().unwrap().contains("successfully removed"));
    }

    #[test]
    fn test_delete_current_profile_with_force() {
        let mut profiles = make_profiles(&["a"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        delete_profile(
            DeleteArgs {
                name: "a".to_string(),
                force: true,
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert!(!profiles.contains_key("a"));
        assert_eq!(config.current_profile, "");
    }

    #[test]
    fn test_delete_current_profile_confirmed() {
        let mut profiles = make_profiles(&["a"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new().with_inputs(vec!["y"]);

        delete_profile(
            DeleteArgs {
                name: "a".to_string(),
                force: false,
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert!(!profiles.contains_key("a"));
        assert_eq!(config.current_profile, "");
    }

    #[test]
    fn test_delete_current_profile_denied() {
        let mut profiles = make_profiles(&["a"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new().with_inputs(vec!["n"]);

        delete_profile(
            DeleteArgs {
                name: "a".to_string(),
                force: false,
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert!(profiles.contains_key("a"));
        assert_eq!(config.current_profile, "a");
        assert!(term.output.last().unwrap().contains("Delete cancelled"));
    }

    #[test]
    fn test_delete_nonexistent_profile() {
        let mut profiles = make_profiles(&["a"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        let result = delete_profile(
            DeleteArgs {
                name: "nope".to_string(),
                force: false,
            },
            &mut profiles,
            &mut config,
            &mut term,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_rename_profile() {
        let mut profiles = make_profiles(&["old-name"]);
        let mut config = make_config("");
        let mut term = TestTerminal::new();

        rename(
            RenameArgs {
                name: "old-name".to_string(),
                new_name: "new-name".to_string(),
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert!(profiles.contains_key("new-name"));
        assert!(term.output[0].contains("Successfully renamed"));
    }

    #[test]
    fn test_rename_current_profile_updates_config() {
        let mut profiles = make_profiles(&["active"]);
        let mut config = make_config("active");
        let mut term = TestTerminal::new();

        rename(
            RenameArgs {
                name: "active".to_string(),
                new_name: "renamed".to_string(),
            },
            &mut profiles,
            &mut config,
            &mut term,
        )
        .unwrap();

        assert_eq!(config.current_profile, "renamed");
    }

    #[test]
    fn test_rename_to_existing_name_fails() {
        let mut profiles = make_profiles(&["a", "b"]);
        let mut config = make_config("a");
        let mut term = TestTerminal::new();

        let result = rename(
            RenameArgs {
                name: "a".to_string(),
                new_name: "b".to_string(),
            },
            &mut profiles,
            &mut config,
            &mut term,
        );

        assert!(result.is_err());
    }

    // ── end-to-end profile_command tests ──

    #[test]
    fn test_command_show_persists_nothing() {
        let store = InMemoryConfigStore::new(make_profiles(&["a"]), make_config("a"));
        let mut term = TestTerminal::new();

        run_profile_command(ProfileCommand::Show, &store, &mut term).unwrap();

        assert!(term.output[0].contains("a"));
        // Store unchanged
        assert_eq!(store.config().current_profile, "a");
        assert!(store.profiles().contains_key("a"));
    }

    #[test]
    fn test_command_use_persists_config() {
        let store = InMemoryConfigStore::new(make_profiles(&["a", "b"]), make_config("a"));
        let mut term = TestTerminal::new();

        run_profile_command(
            ProfileCommand::Use(UseArgs {
                name: "b".to_string(),
            }),
            &store,
            &mut term,
        )
        .unwrap();

        assert_eq!(store.config().current_profile, "b");
    }

    #[test]
    fn test_command_delete_persists_removal() {
        let store = InMemoryConfigStore::new(make_profiles(&["a", "b"]), make_config("a"));
        let mut term = TestTerminal::new();

        run_profile_command(
            ProfileCommand::Delete(DeleteArgs {
                name: "b".to_string(),
                force: false,
            }),
            &store,
            &mut term,
        )
        .unwrap();

        assert!(!store.profiles().contains_key("b"));
        assert!(store.profiles().contains_key("a"));
    }

    #[test]
    fn test_command_delete_current_with_force_persists() {
        let store = InMemoryConfigStore::new(make_profiles(&["a"]), make_config("a"));
        let mut term = TestTerminal::new();

        run_profile_command(
            ProfileCommand::Delete(DeleteArgs {
                name: "a".to_string(),
                force: true,
            }),
            &store,
            &mut term,
        )
        .unwrap();

        assert!(!store.profiles().contains_key("a"));
        assert_eq!(store.config().current_profile, "");
    }

    #[test]
    fn test_command_delete_denied_persists_nothing() {
        let store = InMemoryConfigStore::new(make_profiles(&["a"]), make_config("a"));
        let mut term = TestTerminal::new().with_inputs(vec!["n"]);

        run_profile_command(
            ProfileCommand::Delete(DeleteArgs {
                name: "a".to_string(),
                force: false,
            }),
            &store,
            &mut term,
        )
        .unwrap();

        // Store unchanged — deletion was cancelled
        assert!(store.profiles().contains_key("a"));
        assert_eq!(store.config().current_profile, "a");
    }

    #[test]
    fn test_command_rename_persists() {
        let store = InMemoryConfigStore::new(make_profiles(&["old"]), make_config("old"));
        let mut term = TestTerminal::new();

        run_profile_command(
            ProfileCommand::Rename(RenameArgs {
                name: "old".to_string(),
                new_name: "new".to_string(),
            }),
            &store,
            &mut term,
        )
        .unwrap();

        assert!(store.profiles().contains_key("new"));
        assert_eq!(store.config().current_profile, "new");
    }

    #[test]
    fn test_command_list_with_profiles() {
        let store = InMemoryConfigStore::new(
            make_profiles(&["prod", "staging"]),
            make_config("prod"),
        );
        let mut term = TestTerminal::new();

        run_profile_command(ProfileCommand::List, &store, &mut term).unwrap();

        assert!(term.output[0].contains("Available profiles:"));
        assert!(term.output[1].contains("prod"));
        assert!(term.output[1].contains("(current)"));
    }
}

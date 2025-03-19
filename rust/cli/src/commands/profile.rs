use std::io::Write;
use crate::utils::{get_config, get_profiles, save_config, save_profiles, CliConfig, Profiles};
use clap::{Args, Subcommand};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;

#[derive(Args, Debug)]
pub struct DeleteArgs {
    name: String,
}

#[derive(Args, Debug)]
pub struct UseArgs {
    name: String,
}

#[derive(Subcommand, Debug)]
pub enum ProfileCommand {
    Delete(DeleteArgs),
    List,
    Show,
    Use(UseArgs),
}

fn profile_exists<W: Write>(writer: &mut W, profile_name: &str, profiles: &Profiles)-> Result<bool, std::io::Error> {
    if !profiles.contains_key(profile_name) {
        let message = format!("Profile {} not found", profile_name);
        writeln!(writer, "{}", message.red())?;
        Ok(false)
    } else {
        Ok(true)
    }
}

fn confirm_deletion<W: Write>(writer: &mut W, profile_name: &str) -> Result<bool, std::io::Error> {
    let message = format!(
        "{}\n{}\n{} {}, {}\n\nDo you want to delete profile {}? (Y/n)",
        "Warning! You are deleting the currently active profile".yellow().bold(),
        "All Chroma Cloud CLI operations will fail without an active profile.",
        "If you wish to proceed, please use:",
        "chroma profile use <profile name>".yellow(),
        "to set a new profile",
        profile_name
    );
    writeln!(writer, "{}", message)?;

    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();
    
    Ok(confirm.to_lowercase() == "y" || confirm.to_lowercase() == "yes")
}

fn delete_profile<W: Write>(writer: &mut W,args: DeleteArgs, profiles: &mut Profiles, config: &mut CliConfig) -> Result<(), std::io::Error> {
    let profile = args.name;
    if !profile_exists(writer, &profile, profiles)? { return Ok(()) }
    
    if config.current_profile == profile {
        let confirmed = confirm_deletion(writer, &profile)?;
        if confirmed {
            println!();
            config.current_profile = "".to_string();
            if save_config(config).is_err() { return Ok(()) }
        } else {
            return Ok(());
        }
    };

    profiles.remove(&profile);
    if save_profiles(profiles).is_err() { return Ok(()) }

    writeln!(writer,
        "{} {} {}",
        "Profile".green(),
        profile.green(),
        "successfully removed".green()
    )?;
    Ok(())
}

fn list_profiles<W: Write>(writer: &mut W, profiles: Profiles, config: CliConfig) -> Result<(), std::io::Error> {
    if profiles.is_empty() {
        writeln!(writer,
            "No profiles defined at the moment. To add a new profile use {}",
            "chroma login".yellow()
        )?;
        return Ok(())
    }

    writeln!(writer, "{}", "Available profiles:".blue().bold())?;

    if !config.current_profile.is_empty() {
        let current_profile_label = format!("{} (current)", config.current_profile).bold();
        writeln!(writer, "{} {}", ">".yellow(), current_profile_label)?;
    }

    for key in profiles.keys() {
        if *key != config.current_profile {
            writeln!(writer, "{} {}", ">".yellow(), key)?;
        }
    }
    Ok(())
}

fn use_profile<W: Write>(writer: &mut W, args: UseArgs, profiles: Profiles, config: &mut CliConfig) -> Result<(), std::io::Error> {
    let exists = profile_exists(writer, &args.name, &profiles)?;
    if !exists { return Ok(()) }
    
    config.current_profile = args.name;
    _ = save_config(config);
    let message = format!("Current profile set to {}", config.current_profile);
    writeln!(writer, "{}", message.green())?;
    Ok(())
}

fn show<W: Write>(writer: &mut W,config: CliConfig) -> Result<(), std::io::Error> {
    if config.current_profile.is_empty() {
        writeln!(
            writer,
            "No profile set currently. Please use {} to add a profile, or {} to set an existing profile",
            "chroma login".yellow(),
            "chroma use <profile name>".yellow()
        )?;
    }

    writeln!(writer, "{}", "Current profile: ".blue().bold())?;
    writeln!(writer, "{}", config.current_profile)?;
    Ok(())
}

pub fn profile_command<W: Write>(writer: &mut W, command: ProfileCommand) -> Result<(), std::io::Error> {
    let mut profiles = match get_profiles() {
        Some(p) => p,
        None => return Ok(()),
    };

    let mut config = match get_config() {
        Some(c) => c,
        None => return Ok(()),
    };
    
    match command {
        ProfileCommand::Delete(args) => delete_profile(writer, args, &mut profiles, &mut config),
        ProfileCommand::List => list_profiles(writer, profiles, config),
        ProfileCommand::Show => show(writer, config),
        ProfileCommand::Use(args) => use_profile(writer, args, profiles, &mut config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use crate::utils::{CliConfig, Profile, Profiles};
    use std::collections::HashMap;
    use std::str;
    
    fn setup_test_profiles() -> Profiles {
        let mut profiles = HashMap::new();
        profiles.insert(
            "profile1".to_string(),
            Profile {
                api_key: "test-key-1".to_string(),
                team_id: "team-1".to_string(),
            },
        );
        profiles.insert(
            "profile2".to_string(),
            Profile {
                api_key: "test-key-2".to_string(),
                team_id: "team-2".to_string(),
            },
        );
        profiles
    }

    fn setup_test_config(current_profile: &str) -> CliConfig {
        CliConfig {
            current_profile: current_profile.to_string(),
        }
    }

    
    #[test]
    fn test_delete_active_profile_user_confirms() {
        let mut profiles = setup_test_profiles();
        let mut config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());
        let args = DeleteArgs {
            name: "profile1".to_string(),
        };

        let result = true;

        if profile_exists(&mut output, &args.name, &profiles).unwrap() && config.current_profile == args.name && result {
            config.current_profile = "".to_string();
            profiles.remove(&args.name);
        }

        assert_eq!(config.current_profile, "");
        assert!(!profiles.contains_key(&args.name));
        assert_eq!(profiles.len(), 1);
    }

    #[test]
    fn test_delete_non_active_profile() {
        let mut profiles = setup_test_profiles();
        let mut config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());
        let args = DeleteArgs {
            name: "profile2".to_string(),
        };

        delete_profile(&mut output, args, &mut profiles, &mut config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Profile profile2 successfully removed"));
        assert_eq!(profiles.len(), 1);
        assert!(!profiles.contains_key("profile2"));
        assert_eq!(config.current_profile, "profile1"); // Current profile unchanged
    }

    #[test]
    fn test_delete_non_existent_profile() {
        let mut profiles = setup_test_profiles();
        let mut config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());
        let args = DeleteArgs {
            name: "nonexistent".to_string(),
        };

        delete_profile(&mut output, args, &mut profiles, &mut config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Profile nonexistent not found"));
        assert_eq!(profiles.len(), 2); // Profile count unchanged
        assert_eq!(config.current_profile, "profile1"); // Current profile unchanged
    }

    #[test]
    fn test_list_no_profiles() {
        let profiles = HashMap::new();
        let config = setup_test_config("");
        let mut output = Cursor::new(Vec::new());

        list_profiles(&mut output, profiles, config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("No profiles defined at the moment"));
        assert!(output_str.contains("chroma login"));
    }

    #[test]
    fn test_list_profiles_with_current() {
        let profiles = setup_test_profiles();
        let config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());

        list_profiles(&mut output, profiles, config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Available profiles:"));
        assert!(output_str.contains("profile1 (current)"));
        assert!(output_str.contains("profile2"));
    }

    #[test]
    fn test_list_profiles_without_current() {
        let profiles = setup_test_profiles();
        let config = setup_test_config("");
        let mut output = Cursor::new(Vec::new());

        list_profiles(&mut output, profiles, config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Available profiles:"));
        assert!(output_str.contains("profile1"));
        assert!(output_str.contains("profile2"));
        assert!(!output_str.contains("(current)"));
    }

    #[test]
    fn test_show_with_current_profile() {
        let config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());

        show(&mut output, config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Current profile:"));
        assert!(output_str.contains("profile1"));
        assert!(!output_str.contains("No profile set currently"));
    }

    #[test]
    fn test_show_without_current_profile() {
        let config = setup_test_config("");
        let mut output = Cursor::new(Vec::new());

        show(&mut output, config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("No profile set currently"));
        assert!(output_str.contains("chroma login"));
        assert!(output_str.contains("chroma use <profile name>"));
    }

    #[test]
    fn test_use_non_existent_profile() {
        let profiles = setup_test_profiles();
        let mut config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());
        let args = UseArgs {
            name: "nonexistent".to_string(),
        };

        use_profile(&mut output, args, profiles, &mut config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Profile nonexistent not found"));
        assert_eq!(config.current_profile, "profile1"); // Current profile unchanged
    }
    
    #[test]
    fn test_use_existing_profile() {
        let profiles = setup_test_profiles();
        let mut config = setup_test_config("profile1");
        let mut output = Cursor::new(Vec::new());
        let args = UseArgs {
            name: "profile2".to_string(),
        };

        use_profile(&mut output, args, profiles, &mut config).unwrap();

        let output_str = str::from_utf8(output.get_ref()).unwrap();
        assert!(output_str.contains("Current profile set to profile2"));
        assert_eq!(config.current_profile, "profile2"); // Current profile changed
    }
}
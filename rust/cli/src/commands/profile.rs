use clap::{Args, Subcommand};
use colored::Colorize;
use dialoguer::Input;
use dialoguer::theme::ColorfulTheme;
use crate::utils::{get_profiles, read_config, write_config, write_profiles};

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

#[allow(dead_code)]
fn delete_profile(args: DeleteArgs) {
    let mut profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };
    let profile = args.name;

    if !profiles.contains_key(&profile) {
        let message = format!("Profile {} not found", profile);
        eprintln!("\n{}\n", message.red());
        return;
    }

    let mut config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };

    if config.current_profile == profile {
        println!("{}", "\nWarning! You are deleting the currently active profile".yellow().bold());
        println!("All Chroma Cloud CLI operations will fail without an active profile.");
        print!("If you wish to proceed, please use: `{}`, to set a new active profile", "chroma profile use <profile name>".yellow());
        
        println!("\nDo you want to delete profile {}? (Y/n)", profile);
        let confirm: String = Input::with_theme(&ColorfulTheme::default())
            .interact_text()
            .unwrap();
        
        if confirm.to_lowercase() != "y" && confirm.to_lowercase() != "yes" {
            println!();
            return;
        }

        config.current_profile = "".to_string();
        match write_config(&config) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("\n{}\n", "Failed to save CLI config".red());
                return;
            }
        };
    }

    profiles.remove(&profile);
    match write_profiles(&profiles) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("\n{}\n", "Failed to save credentials file".red());
            return; 
        }
    }

    println!(
        "{} {} {}",
        "\nProfile".green(),
        profile.green(),
        "successfully removed\n".green()
    );
}

#[allow(dead_code)]
fn list_profiles() {
    let profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };

    let config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };

    if profiles.is_empty() {
        println!("\nNo profiles defined at the moment. To set a new profile use {}\n", "chroma login".yellow());
        return;
    }

    println!("{}", "\nAvailable profiles:".blue().bold());

    if !config.current_profile.is_empty() {
        let current_profile_label = format!("{} (current)", config.current_profile).bold();
        println!("{} {}", ">".yellow(), current_profile_label);
    }

    for key in profiles.keys() {
        if *key != config.current_profile {
            println!("{} {}", ">".yellow(), key)
        }
    }
    println!();
}

#[allow(dead_code)]
fn use_profile(args: UseArgs) {
    let profiles = match get_profiles() {
        Ok(profiles) => profiles,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load profiles".red());
            return;
        }
    };
    
    if !profiles.contains_key(&args.name) {
        let message = format!("Profile {} not found", args.name);
        eprintln!("\n{}\n", message.red());
        return;
    }

    let mut config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };
    
    config.current_profile = args.name;
    match write_config(&config) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("\n{}\n", "Failed to save CLI config".red());
        }
    };
    
}

fn show() {
    let config = match read_config() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("\n{}\n", "Could not load CLI config".red());
            return;
        }
    };
    
    if config.current_profile.is_empty() {
       println!("\nNo profile set currently. Please use {} to add a profile\n", "chroma login".yellow());
        return;
    }

    println!("\n{}", "Current profile: ".blue().bold());
    println!("{}\n", config.current_profile);

}

#[allow(dead_code)]
pub fn profile_command(command: ProfileCommand) {
    match command {
        ProfileCommand::Delete(args) => delete_profile(args),
        ProfileCommand::List => list_profiles(),
        ProfileCommand::Show => show(),
        ProfileCommand::Use(args) => use_profile(args),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use colored::control;
    use tempfile::TempDir;
    use crate::commands::profile::{delete_profile, list_profiles, profile_command, show, use_profile, DeleteArgs, ProfileCommand, UseArgs};
    use crate::utils::{CliConfig, Profile, Profiles};

    // Helper function to create a temporary .chroma directory with initial files
    fn setup_test_environment(profiles: Option<Profiles>, config: Option<CliConfig>) -> TempDir {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let chroma_dir = temp_dir.path().join(".chroma");
        fs::create_dir_all(&chroma_dir).expect("Failed to create .chroma directory");

        let credentials_path = chroma_dir.join("credentials");
        if let Some(profiles_data) = profiles {
            let toml_str = toml::to_string(&profiles_data).expect("Failed to serialize profiles");
            fs::write(&credentials_path, toml_str).expect("Failed to write credentials file");
        } else {
            fs::write(&credentials_path, "").expect("Failed to write empty credentials file");
        }

        let config_path = chroma_dir.join("config.json");
        if let Some(config_data) = config {
            let json_str =
                serde_json::to_string_pretty(&config_data).expect("Failed to serialize config");
            fs::write(&config_path, json_str).expect("Failed to write config file");
        } else {
            let default_config = CliConfig {
                current_profile: String::new(),
            };
            let json_str =
                serde_json::to_string_pretty(&default_config).expect("Failed to serialize default config");
            fs::write(&config_path, json_str).expect("Failed to write default config file");
        }

        // Set the HOME environment variable to the temporary directory
        std::env::set_var("HOME", temp_dir.path());

        temp_dir
    }

    #[test]
    fn test_delete_profile_success() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "default".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let args = DeleteArgs { name: "test".to_string() };
        delete_profile(args);

        let credentials_path = temp_dir.path().join(".chroma").join("credentials");
        let contents = fs::read_to_string(credentials_path).expect("Failed to read credentials");
        let updated_profiles: Profiles = toml::from_str(&contents).expect("Failed to parse profiles");

        assert!(updated_profiles.contains_key("default"));
        assert!(!updated_profiles.contains_key("test"));
    }

    #[test]
    fn test_delete_profile_not_found() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "default".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let args = DeleteArgs { name: "nonexistent".to_string() };
        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stderr_guard = TestStderr::new(&mut output);
            delete_profile(args);
        }
        control::unset_override();

        let stderr_output = String::from_utf8(output).unwrap();
        assert!(stderr_output.contains("Profile nonexistent not found"));
    }

    #[test]
    fn test_delete_profile_current_profile() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "default".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let args = DeleteArgs { name: "default".to_string() };
        delete_profile(args);

        let config_path = temp_dir.path().join(".chroma").join("config.json");
        let contents = fs::read_to_string(config_path).expect("Failed to read config");
        let updated_config: CliConfig =
            serde_json::from_str(&contents).expect("Failed to parse config");

        assert_eq!(updated_config.current_profile, "");
    }

    #[test]
    fn test_list_profiles_success() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let temp_dir = setup_test_environment(profiles, None);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            list_profiles();
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Available profiles:"));
        assert!(stdout_output.contains("> default"));
        assert!(stdout_output.contains("> test"));
    }

    #[test]
    fn test_list_profiles_empty() {
        let temp_dir = setup_test_environment(None, None);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            list_profiles();
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Available profiles:"));
        assert!(!stdout_output.contains(">"));
    }

    #[test]
    fn test_use_profile_success() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let args = UseArgs { name: "test".to_string() };
        use_profile(args);

        let config_path = temp_dir.path().join(".chroma").join("config.json");
        let contents = fs::read_to_string(config_path).expect("Failed to read config");
        let updated_config: CliConfig =
            serde_json::from_str(&contents).expect("Failed to parse config");

        assert_eq!(updated_config.current_profile, "test");
    }

    #[test]
    fn test_use_profile_not_found() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let args = UseArgs { name: "nonexistent".to_string() };
        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stderr_guard = TestStderr::new(&mut output);
            use_profile(args);
        }
        control::unset_override();

        let stderr_output = String::from_utf8(output).unwrap();
        assert!(stderr_output.contains("Profile nonexistent not found"));
    }

    #[test]
    fn test_show_no_profile_set() {
        let config = Some(CliConfig { current_profile: "".to_string() });
        let temp_dir = setup_test_environment(None, config);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            show();
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Current profile:"));
        assert!(stdout_output.contains("No profile set currently"));
    }

    #[test]
    fn test_show_profile_set() {
        let config = Some(CliConfig { current_profile: "test".to_string() });
        let temp_dir = setup_test_environment(None, config);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            show();
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Current profile:"));
        assert!(stdout_output.contains("test"));
    }

    #[test]
    fn test_profile_command_delete() {
        let profiles = Some(Profiles::from([
            ("default".to_string(), Profile { name: "default".to_string(), api_key: "test_key".to_string(), team_id: "test_team".to_string() }),
        ]));
        let temp_dir = setup_test_environment(profiles, None);

        let command = ProfileCommand::Delete(DeleteArgs { name: "default".to_string() });
        profile_command(command);

        let credentials_path = temp_dir.path().join(".chroma").join("credentials");
        let contents = fs::read_to_string(credentials_path).expect("Failed to read credentials");
        let updated_profiles: Profiles = toml::from_str(&contents).expect("Failed to parse profiles");
        assert!(!updated_profiles.contains_key("default"));
    }

    #[test]
    fn test_profile_command_list() {
        let profiles = Some(Profiles::from([
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let temp_dir = setup_test_environment(profiles, None);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            profile_command(ProfileCommand::List);
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Available profiles:"));
        assert!(stdout_output.contains("> test"));
    }

    #[test]
    fn test_profile_command_show() {
        let config = Some(CliConfig { current_profile: "test".to_string() });
        let temp_dir = setup_test_environment(None, config);

        let mut output = Vec::new();
        control::set_override(true); // Enable colored output for testing
        {
            let _stdout_guard = TestStdout::new(&mut output);
            profile_command(ProfileCommand::Show);
        }
        control::unset_override();

        let stdout_output = String::from_utf8(output).unwrap();
        assert!(stdout_output.contains("Current profile:"));
        assert!(stdout_output.contains("test"));
    }

    #[test]
    fn test_profile_command_use() {
        let profiles = Some(Profiles::from([
            ("test".to_string(), Profile { name: "test".to_string(), api_key: "another_key".to_string(), team_id: "another_team".to_string() }),
        ]));
        let config = Some(CliConfig { current_profile: "".to_string() });
        let temp_dir = setup_test_environment(profiles, config);

        let command = ProfileCommand::Use(UseArgs { name: "test".to_string() });
        profile_command(command);

        let config_path = temp_dir.path().join(".chroma").join("config.json");
        let contents = fs::read_to_string(config_path).expect("Failed to read config");
        let updated_config: CliConfig =
            serde_json::from_str(&contents).expect("Failed to parse config");
        assert_eq!(updated_config.current_profile, "test");
    }

    // Helper struct to capture stdout
    struct TestStdout {
        output: *mut Vec<u8>,
    }

    impl TestStdout {
        fn new(output: &mut Vec<u8>) -> Self {
            TestStdout {
                output: output as *mut Vec<u8>,
            }
        }
    }

    impl Drop for TestStdout {
        fn drop(&mut self) {
            unsafe {
                let output = &mut *self.output;
                let _ = std::io::Write::flush(output);
                let _ = std::io::Write::write_all(output, b"\n"); // Ensure a newline at the end
            }
        }
    }

    impl std::io::Write for TestStdout {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            unsafe {
                (*self.output).extend_from_slice(buf);
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // Helper struct to capture stderr
    struct TestStderr {
        output: *mut Vec<u8>,
    }

    impl TestStderr {
        fn new(output: &mut Vec<u8>) -> Self {
            TestStderr {
                output: output as *mut Vec<u8>,
            }
        }
    }

    impl Drop for TestStderr {
        fn drop(&mut self) {
            unsafe {
                let output = &mut *self.output;
                let _ = std::io::Write::flush(output);
                let _ = std::io::Write::write_all(output, b"\n"); // Ensure a newline at the end
            }
        }
    }

    impl std::io::Write for TestStderr {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            unsafe {
                (*self.output).extend_from_slice(buf);
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}

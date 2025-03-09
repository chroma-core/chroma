use std::fmt;
use clap::{Parser, ValueEnum};
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;

struct SampleAppConfig {
    package_managers: Vec<PackageManager>,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DbType {
    Cloud,
    Local,
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbType::Cloud => write!(f, "Chroma Cloud"),
            DbType::Local => write!(f, "Local"),
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum LlmProvider {
    Anthropic,
    Gemini,
    Ollama,
    OpenAI
}

#[derive(Debug, Clone, ValueEnum)]
pub enum PackageManager {
    Npm,
    Pnpm,
    Yarn,
    Bun,
    Pip,
    Poetry
}

impl fmt::Display for PackageManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lowercase_name = format!("{:?}", self).to_lowercase();
        write!(f, "{}", lowercase_name)
    }
}


#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[arg(long)]
    name: String,
    #[arg(long)]
    path: Option<String>,
    #[arg(long = "package-manager")]
    package_manager: Option<PackageManager>,
    #[arg(long = "app-db")]
    app_db: Option<DbType>,
    #[arg(long = "db-name")]
    db_name: Option<String>,
    #[arg(long = "db-path")]
    db_path: Option<String>,
    #[arg(long)]
    llm: Option<LlmProvider>,
}

pub fn install(args: InstallArgs) {
    let app_config = SampleAppConfig {
        package_managers: vec![PackageManager::Npm, PackageManager::Pnpm, PackageManager::Yarn, PackageManager::Bun],
    };

    let path = args.path.unwrap_or_else(|| {
        println!("{}", "\nWhere do you want to save this project?".blue().bold());
        Input::with_theme(&ColorfulTheme::default())
            .default("current working directory".to_string())
            .interact_text()
            .unwrap()
    });
    
    if path == "current working directory" {
        let path = String::from("./");
    }
    
    let package_manager = args.package_manager.unwrap_or_else(|| {
        println!("{}", "\nWhich package manager do you want to use?".blue().bold());
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&app_config.package_managers)
            .default(0)
            .interact()
            .unwrap();
        app_config.package_managers[selection].clone()
    });
    println!("{}", package_manager.to_string().green());
    
    let app_db = args.app_db.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Chroma Cloud (~instant copying)"),
            format!("{} {}", ">".yellow(), "Local (8GB download, est 20 minutes)"),
        ];

        println!("{}", "\nThis project comes with a Chroma DB - where would you like it saved? ".blue().bold());
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&options)
            .default(0)
            .interact()
            .unwrap();
        
        if selection == 0 { DbType::Cloud } else { DbType::Local }
    });
    println!("{}", app_db.to_string().green());
    
    println!();
}

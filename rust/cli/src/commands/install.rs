use std::io;
use std::io::Write;
use clap::{Parser, ValueEnum};
use colored::Colorize;
use dialoguer::{Input, Select};
use dialoguer::theme::ColorfulTheme;

#[derive(Debug, Clone, ValueEnum)]
pub enum DbType {
    Cloud,
    Local,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum LlmProvider {
    Anthropic,
    Gemini,
    Ollama,
    OpenAI
}

#[derive(Parser, Debug)]
pub struct InstallArgs {
    #[arg(long)]
    name: String,
    #[arg(long)]
    path: Option<String>,
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
    let path = args.path.unwrap_or_else(|| {
        println!("{}", "\nWhere do you want to save this project?".blue().bold());
        Input::with_theme(&ColorfulTheme::default())
            .default("./ (current working directory)".to_string())
            .interact_text()
            .unwrap()
    });
    
    
    
    let app_db = args.app_db.unwrap_or_else(|| {
        let options = vec![
            format!("{} {}", ">".yellow(), "Chroma Cloud"),
            format!("{} {}", ">".yellow(), "Local"),
        ];

        println!("{}", "\nChoose a profile name".blue().bold());
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&options)
            .default(0)
            .interact()
            .unwrap();
    })
}

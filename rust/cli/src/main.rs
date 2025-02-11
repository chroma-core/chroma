use clap::{Parser, Subcommand};

#[derive(Subcommand, Debug)]
enum Command {
    Docs,
    Run,
    Support
}

#[derive(Parser, Debug)]
#[command(name = "chroma")]
#[command(version = "0.0.1")]
#[command(about = "A CLI for Chroma", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Docs => {
            let url = "https://docs.trychroma.com";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}.", url);
            }
        }
        Command::Run => {
            println!("run");
        }
        Command::Support => {
            let url = "https://discord.gg/MMeYNTmh3x";
            if webbrowser::open(url).is_err() {
                eprintln!("Error: Failed to open the browser. Visit {}.", url);
            }
        }
    }
}

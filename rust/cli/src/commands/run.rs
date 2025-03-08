use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::sync::Arc;
use crate::utils::{get_frontend_config, LocalFrontendCommandArgs, DEFAULT_PERSISTENT_PATH, LOGO};

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(flatten)]
    frontend_args: LocalFrontendCommandArgs,
    #[arg(long)]
    port: Option<u16>,
}

pub fn run(args: RunArgs) {
    println!("{}", LOGO);
    println!("\n{}", "Running Chroma".bold());

    if args.frontend_args.config_path.is_some() {
        println!(
            "Config path: {}",
            args.frontend_args.config_path.clone().unwrap().bold()
        );
    }

    let config = match get_frontend_config(
        args.frontend_args.config_path,
        args.frontend_args.persistent_path,
        args.port,
    ) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{}", e.red());
            return;
        }
    };

    let persistent_path = config
        .persist_path
        .as_deref()
        .unwrap_or(DEFAULT_PERSISTENT_PATH);

    println!("Saving data to: {}", persistent_path.bold());
    println!(
        "Connect to Chroma at: {}",
        format!("http://localhost:{}", config.port)
            .underline()
            .blue()
    );
    println!(
        "Getting started guide: {}",
        "https://docs.trychroma.com/docs/overview/getting-started\n"
            .underline()
            .blue()
    );

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        tokio::select! {
            _ = frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config) => {},
            _ = tokio::signal::ctrl_c() => {
                println!();
            }
        }
    });
}

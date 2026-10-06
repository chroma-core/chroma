use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use chroma_auth_service::{config::Config, install, router};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Single-tenant static authentication service")]
struct Args {
    #[arg(long, default_value = "/etc/chroma-auth/config.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve dashboard-compatible auth routes.
    Serve {
        #[arg(long, default_value = "0.0.0.0:8002")]
        listen: SocketAddr,
    },
    /// Create the configured tenant and initial database in the data plane.
    InstallTenant {
        #[arg(long)]
        frontend_url: String,
    },
    /// Validate the mounted configuration without printing credentials.
    Validate,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    match args.command {
        Command::Serve { listen } => {
            let listener = tokio::net::TcpListener::bind(listen).await?;
            eprintln!("Auth service listening on {}", listener.local_addr()?);
            axum::serve(listener, router(config))
                .with_graceful_shutdown(shutdown())
                .await?;
        }
        Command::InstallTenant { frontend_url } => {
            install::install(&config, &frontend_url).await?;
            println!(
                "Tenant {} and database {} are installed",
                config.tenant, config.database
            );
        }
        Command::Validate => println!("Configuration is valid"),
    }
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Cannot install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

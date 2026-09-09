use std::{io, path::PathBuf};

use mdac_service::Config;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var_os("CONFIG_PATH").map(PathBuf::from);
    let config = Config::load(path.as_deref())?;
    mdac_service::init_otel_tracing(&config);
    let buckets = config.buckets()?;
    let shutdown = shutdown_signal()?;
    let listener = TcpListener::bind(config.listen_address).await?;
    tracing::info!(
        address = %listener.local_addr()?,
        configured_buckets = config.buckets.len(),
        "Token bucket server listening"
    );
    mdac_service::serve(listener, buckets, shutdown).await?;
    Ok(())
}

// Install signal handlers before serving so registration failures are startup errors.
fn shutdown_signal() -> io::Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    let signals = {
        use tokio::signal::unix::{signal, SignalKind};
        (
            signal(SignalKind::terminate())?,
            signal(SignalKind::interrupt())?,
        )
    };
    Ok(async move {
        #[cfg(unix)]
        {
            let (mut terminate, mut interrupt) = signals;
            tokio::select! {
                _ = terminate.recv() => {},
                _ = interrupt.recv() => {},
            }
        }
        #[cfg(not(unix))]
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "Failed to wait for shutdown signal");
        }
        tracing::info!("Shutting down token bucket server");
    })
}

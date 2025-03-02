use crate::utils::{get_frontend_config, LocalFrontendCommandArgs, DEFAULT_PERSISTENT_PATH, LOGO};
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(flatten)]
    frontend_args: LocalFrontendCommandArgs,
    #[arg(long)]
    port: Option<u16>,
}

pub fn run(args: RunArgs) {
    println!("{}", LOGO);
    println!("\n{}\n", "Running Chroma".bold());

    let config = match get_frontend_config(
        args.frontend_args.config_path,
        args.frontend_args.persistent_path,
        args.port,
    ) {
        Ok(config) => config,
        Err(e) => {
            println!("{}", e.red());
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
        "https://docs.trychroma.com/docs/overview/getting-started"
            .underline()
            .blue()
    );

    let runtime = tokio::runtime::Runtime::new().expect("Failed to start Chroma");
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), (), config).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::panic;
    use std::thread;

    #[tokio::test]
    async fn test_run() {
        let args = RunArgs {
            frontend_args: LocalFrontendCommandArgs {
                config_path: None,
                persistent_path: None,
            },
            port: None,
        };

        let handle = thread::spawn(move || {
            let result = panic::catch_unwind(|| {
                run(args);
            });
            result.unwrap();
        });

        let url = "http://localhost:8000/api/v2/heartbeat";
        let heartbeat_key = "nanosecond heartbeat";

        let response = reqwest::get(url)
            .await
            .expect("Failed to send heartbeat request");

        assert!(response.status().is_success(), "Heartbeat request failed");

        let body = response.text().await.expect("Failed to read response body");

        let response_json =
            serde_json::from_str::<Value>(&body).expect("Failed to deserialize response");

        let heartbeat = response_json
            .get(heartbeat_key)
            .and_then(Value::as_u64)
            .expect("Heartbeat not found or not a u64");

        assert!(heartbeat > 0, "Heartbeat not found");

        std::mem::forget(handle);
    }
}

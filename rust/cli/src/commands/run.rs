use crate::UtilsError;
use std::io;
use std::io::{Stdout, Write};
use crate::utils::LOGO;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::net::TcpListener;
use std::sync::Arc;
use thiserror::Error;
use crate::{cli_writeln, CliError, Handler};

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(
        index = 1,
        conflicts_with_all = &["path", "host", "port"],
        help = "The path to the Chroma config file"
    )]
    config_path: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The persistence path to your Chroma DB"
    )]
    path: Option<String>,

    #[clap(
        long,
        default_value = "localhost",
        conflicts_with = "config_path",
        help = "The host to listen to. Default: localhost"
    )]
    host: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The port to run the server on"
    )]
    port: Option<u16>,
}


#[derive(Debug, Error)]
pub enum RunError {
    #[error("Config file {0} does not exists")]
    ConfigFileNotFound(String),
    #[error("Address {0}:{1} is not available")]
    AddressUnavailable(String, u16),
    #[error("Failed to start a Chroma server")]
    ServerStartFailed,
}

fn run_message(config: &FrontendServerConfig) -> String {
    let chroma_url = format!("http://localhost:{}", config.port).underline().blue();
    let docs_url = "https://docs.trychroma.com/docs/overview/getting-started\n".underline().blue();
    format!(
        "{}\nSaving data to: {}\nConnect to Chroma at: {}\nGetting started guide: {}\n",
        LOGO,
        config.persist_path.bold(),
        chroma_url,
        docs_url,
    )
}

fn validate_host(address: &String, port: u16) -> bool {
    let socket = format!("{}:{}", address, port);
    TcpListener::bind(&socket).is_ok()
}

pub struct RunCommandHandler<W: Write> {
    run_args: RunArgs,
    writer: W,
}

impl<W: Write> RunCommandHandler<W> {
    pub fn new(run_args: RunArgs, writer: W) -> Self {
        RunCommandHandler { run_args, writer }
    }
    
    fn override_default_config_with_args(&self) -> Result<FrontendServerConfig, RunError> {
        let mut config = FrontendServerConfig::single_node_default();

        if let Some(path) = &self.run_args.path {
            config.persist_path = path.clone();
        }

        if let Some(port) = &self.run_args.port {
            config.port = *port;
        }

        if let Some(host) = &self.run_args.host {
            config.listen_address = host.clone();
        }

        if !validate_host(&config.listen_address, config.port) {
            return Err(RunError::AddressUnavailable(config.listen_address, config.port));
        }

        Ok(config)
    }

    fn get_config_from_args(&self) -> Result<FrontendServerConfig, RunError> {
        match &self.run_args.config_path {
            Some(config_path) => {
                if !std::path::Path::new(config_path).exists() {
                    return Err(RunError::ConfigFileNotFound(config_path.to_string()));
                }
                Ok(FrontendServerConfig::load_from_path(config_path))
            }
            None => Ok(self.override_default_config_with_args()?),
        }
    }
}

impl RunCommandHandler<Stdout> {
    pub fn default(run_args: RunArgs) -> Self {
        let stdout = io::stdout();
        RunCommandHandler::new(run_args, stdout)
    }
}

#[async_trait::async_trait]
impl<W: Write + Send> Handler for RunCommandHandler<W> {
    async fn run(&mut self) -> Result<(), CliError> {
        let config = self.get_config_from_args()?;
        cli_writeln!(self.writer, "{}", run_message(&config))?;
        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &config).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::client::ChromaClient;
    use crate::commands::run::{run_message, RunArgs, RunCommandHandler};
    use crate::Handler;

    #[tokio::test]
    async fn test_run() {
        use tokio::time::{sleep, Duration};

        let port = 8001;
        let path = "test_data".to_string();
        let run_args = RunArgs {
            config_path: None,
            path: Some(path.clone()),
            port: Some(port),
            host: None,
        };

        let writer = Vec::new();
        let run_handler = Arc::new(Mutex::new(RunCommandHandler::new(run_args, writer)));

        let run_handler_for_server = run_handler.clone();
        let server_handle = tokio::spawn(async move {
            let mut handler = run_handler_for_server.lock().await;
            handler.run().await.unwrap();
        });
        
        sleep(Duration::from_millis(500)).await;
        
        let url = format!("http://localhost:{}", port);
        let chroma_client = ChromaClient::new(
            url,
            "default_tenant".to_string(),
            Some("default_database".to_string()),
            None,
        );
        let response = chroma_client.heartbeat().await.unwrap();
        
        assert!(response > 0);
        server_handle.abort();
        
        let handler = run_handler.lock().await;
        let message = run_message(&handler.get_config_from_args().unwrap());
        let output = String::from_utf8(handler.writer.clone()).unwrap();
        assert!(output.contains(message.as_str()));
        assert!(output.contains(port.to_string().as_str()));
        assert!(output.contains(path.as_str()));
    }
}
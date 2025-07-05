use crate::ui_utils::LOGO;
use crate::utils::CliError;
use crate::utils::UtilsError;
use crate::{cli_writeln, CommandHandler};
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use clap::Parser;
use colored::Colorize;
use std::io::{stdout, Stdout, Write};
use std::net::TcpListener;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Config file {0} does not exist")]
    ConfigFileNotFound(String),
    #[error("Address {0}:{1} is not available")]
    AddressUnavailable(String, u16),
    #[error("Failed to start a Chroma server")]
    ServerStartFailed,
}

#[derive(Parser, Debug, Clone)]
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

pub struct RunCommand<W: Write> {
    args: RunArgs,
    config: FrontendServerConfig,
    writer: W,
}

impl<W: Write> RunCommand<W> {
    pub fn new(args: RunArgs, writer: W) -> Self {
        Self {
            args,
            writer,
            config: FrontendServerConfig::single_node_default(),
        }
    }

    fn validate_host(address: &String, port: u16) -> bool {
        let socket = format!("{}:{}", address, port);
        TcpListener::bind(&socket).is_ok()
    }

    fn override_default_config_with_args(&mut self) -> Result<(), CliError> {
        if let Some(path) = &self.args.path {
            self.config.persist_path = path.to_owned();
        }

        if let Some(port) = &self.args.port {
            self.config.port = port.to_owned();
        }

        if let Some(host) = &self.args.host {
            self.config.listen_address = host.to_owned();
        }

        if !Self::validate_host(&self.config.listen_address, self.config.port) {
            return Err(RunError::AddressUnavailable(
                self.config.listen_address.to_owned(),
                self.config.port,
            )
            .into());
        }

        Ok(())
    }

    fn run_message(&self) -> String {
        let host = format!("http://localhost:{}", self.config.port)
            .underline()
            .blue();

        let docs = "https://docs.trychroma.com/docs/overview/getting-started\n"
            .underline()
            .blue();

        format!(
            "{}\nSaving data to: {}\nConnect to Chroma at: {}\nGetting started guide: {}",
            LOGO,
            self.config.persist_path.bold(),
            host,
            docs
        )
    }
}

impl RunCommand<Stdout> {
    pub fn default(run_args: RunArgs) -> Self {
        let stdout = stdout();
        RunCommand::new(run_args, stdout)
    }
}

#[async_trait::async_trait]
impl<W: Write + Send> CommandHandler for RunCommand<W> {
    async fn run(&mut self) -> Result<(), CliError> {
        match &self.args.config_path {
            Some(config_path) => {
                if !std::path::Path::new(config_path).exists() {
                    return Err(RunError::ConfigFileNotFound(config_path.to_string()).into());
                }
                self.config = FrontendServerConfig::load_from_path(config_path)
            }
            None => self.override_default_config_with_args()?,
        };

        cli_writeln!(self.writer, "{}", self.run_message())?;

        frontend_service_entrypoint_with_config(Arc::new(()), Arc::new(()), &self.config).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::client::chroma_client::ChromaClient;
    use crate::commands::run::{RunArgs, RunCommand};
    use crate::CommandHandler;
    use std::sync::Arc;
    use tokio::sync::Mutex;

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
        let run_command_arc = Arc::new(Mutex::new(RunCommand::new(run_args, writer)));

        let run_command_arc_clone = run_command_arc.clone();
        let server_handle = tokio::spawn(async move {
            let mut command = run_command_arc_clone.lock().await;
            command.run().await.unwrap();
        });

        sleep(Duration::from_millis(500)).await;

        let url = format!("http://localhost:{}", port);
        let chroma_client = ChromaClient::local_default();
        let response = chroma_client.healthcheck().await.unwrap();

        server_handle.abort();

        let run_command = run_command_arc.lock().await;
        let message = run_command.run_message();
        let output = String::from_utf8(run_command.writer.clone()).unwrap();
        assert!(output.contains(message.as_str()));
        assert!(output.contains(port.to_string().as_str()));
        assert!(output.contains(path.as_str()));
    }
}

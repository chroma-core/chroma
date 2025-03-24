use thiserror::Error;
use crate::{CliError, Handler};

pub const DOCS_URL: &str = "https://docs.trychroma.com";
pub const DISCORD_URL: &str = "https://discord.gg/MMeYNTmh3x";

#[derive(Debug, Error)]
pub enum BrowserError {
    #[error("Failed to open browser. Visit {0}")]
    BrowserFailed(String),
}

pub struct BrowserCommandHandler {
    url: String,
}

impl BrowserCommandHandler {
    pub fn new(url: &str) -> Self {
        BrowserCommandHandler { url: url.to_string() }
    }

    fn open_browser(&self) -> Result<(), BrowserError> {
        webbrowser::open(self.url.as_str()).map_err(|e| BrowserError::BrowserFailed(e.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler for BrowserCommandHandler {
    async fn run(&mut self) -> Result<(), CliError> {
        self.open_browser()?;
        Ok(())
    }
}

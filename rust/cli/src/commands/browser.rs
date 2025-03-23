use std::io::{self, Write};
use thiserror::Error;
use crate::types::{CliError, Handler};

const DOCS_URL: &str = "https://docs.trychroma.com";
const DISCORD_URL: &str = "https://discord.gg/MMeYNTmh3x";

#[derive(Debug, Error)]
pub enum BrowserError {
    #[error("Failed to open browser. Visit {0}")]
    BrowserFailed(String),
}

pub struct BrowserCommandHandler<W: Write> {
    url: String,
    writer: W
}

impl<W: Write> BrowserCommandHandler<W> {
    pub fn new(url: &str, writer: W) -> Self {
        BrowserCommandHandler { url: url.to_string(), writer }
    }
    
    fn open_browser(&self) -> Result<(), BrowserError> {
        webbrowser::open(self.url.as_str()).map_err(|e| BrowserError::BrowserFailed(e.to_string()))?;
        Ok(())
    }
}

impl BrowserCommandHandler<io::StdoutLock<'_>> {
    pub fn default(url: &str) -> Self {
        let stdout = io::stdout().lock();
        BrowserCommandHandler::new(url, stdout)
    }
}

impl<W: Write> Handler for BrowserCommandHandler<W> {
    fn run(&mut self) -> Result<(), CliError> {
        writeln!(self.writer, "{}", self.url)?;
        Ok(())
    }
}

use crate::utils::CliError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WebPageError {
    #[error("Failed to open browser. {0}")]
    BrowserOpenFailed(String),
}

pub enum WebPageCommand {
    Discord,
    Docs,
}

impl WebPageCommand {
    fn url(&self) -> &'static str {
        match self {
            WebPageCommand::Discord => "https://discord.gg/MMeYNTmh3x",
            WebPageCommand::Docs => "https://docs.trychroma.com",
        }
    }
}

pub fn open_browser(command: WebPageCommand) -> Result<(), CliError> {
    let url = command.url();
    let error_message = format!("Visit {}", url);
    webbrowser::open(url).map_err(|_| WebPageError::BrowserOpenFailed(error_message))?;
    Ok(())
}

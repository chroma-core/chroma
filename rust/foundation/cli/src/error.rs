use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)] // variants added for future use are intentional
pub enum FoundationError {
    // File system / config
    #[error("User home directory not found")]
    HomeDirNotFound,
    #[error("Failed to create ~/.chroma directory")]
    ConfigDirCreateFailed,
    #[error("~/.chroma exists but is not a directory")]
    ConfigDirNotADirectory,
    #[error("Failed to read credentials file")]
    CredsFileReadFailed,
    #[error("Failed to parse credentials file")]
    CredsFileParseFailed,
    #[error("Failed to write credentials file")]
    CredsFileWriteFailed,
    #[error("Failed to read config file")]
    ConfigFileReadFailed,
    #[error("Failed to parse config file")]
    ConfigFileParseFailed,
    #[error("Failed to write config file")]
    ConfigFileWriteFailed,

    // Auth / login
    #[error("Browser authentication failed or timed out")]
    BrowserAuthFailed,
    #[error("No teams found for this account")]
    NoTeamsFound,
    #[error("Team '{0}' not found")]
    TeamNotFound(String), // used in filter_team (tests) and future headless login
    #[error("Profile '{0}' already exists")]
    ProfileAlreadyExists(String), // reserved for future headless --api-key flow
    #[error("Profile '{0}' not found")]
    ProfileNotFound(String),
    #[error("No active profile — run `foundation login` first")]
    NoActiveProfile,

    // Input / validation
    #[error("Failed to get user input")]
    UserInputFailed,
    #[error("Profile name cannot be empty and must only contain alphanumerics, underscores, or hyphens")]
    InvalidProfileName,

    // Network
    #[error("Network error: {0}")]
    NetworkError(String),
}

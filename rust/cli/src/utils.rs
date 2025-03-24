use thiserror::Error;
use dialoguer::Input;
use dialoguer::theme::ColorfulTheme;

pub const LOGO: &str = "
                \x1b[38;5;069m(((((((((    \x1b[38;5;203m(((((\x1b[38;5;220m####
             \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((\x1b[38;5;220m#########
           \x1b[38;5;069m(((((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m###########
         \x1b[38;5;069m((((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
        \x1b[38;5;069m(((((((((((((\x1b[38;5;203m((((((((((((((\x1b[38;5;220m#############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m(((((((((((((\x1b[38;5;220m##############
         \x1b[38;5;069m((((((((((((\x1b[38;5;203m((((((((((((\x1b[38;5;220m##############
           \x1b[38;5;069m((((((((((\x1b[38;5;203m(((((((((((\x1b[38;5;220m#############
             \x1b[38;5;069m((((((((\x1b[38;5;203m((((((((\x1b[38;5;220m##############
                \x1b[38;5;069m(((((\x1b[38;5;203m((((    \x1b[38;5;220m#########\x1b[0m
";

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("Failed to write output to console")]
    ConsoleWriteFailed,
    #[error("Failed to get input from the user")]
    UserInputFailed
}


#[macro_export]
macro_rules! cli_writeln {
    ($writer:expr, $($arg:tt)*) => {
        writeln!($writer, $($arg)*)
            .map_err(|_| UtilsError::ConsoleWriteFailed)
    }
}

pub trait InputProvider {
    fn get_user_input(&self) -> Result<String, UtilsError>;
}

pub struct CliInput;

impl CliInput {
    pub fn new() -> Self {
        Self
    }
}

impl InputProvider for CliInput {
    fn get_user_input(&self) -> Result<String, UtilsError> {
        Input::with_theme(&ColorfulTheme::default())
            .interact_text()
            .map_err(|_| UtilsError::UserInputFailed)
    }
}
use chroma_frontend::config::FrontendServerConfig;
use clap::Parser;

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

pub const DEFAULT_PERSISTENT_PATH: &str = "./chroma";
pub const SQLITE_FILENAME: &str = "chroma.sqlite3";

#[derive(Parser, Debug)]
pub struct LocalFrontendCommandArgs {
    #[arg(long = "config")]
    pub config_path: Option<String>,
    #[arg(long = "path")]
    pub persistent_path: Option<String>,
}

pub fn get_frontend_config(
    config_path: Option<String>,
    persistent_path: Option<String>,
    port: Option<u16>,
) -> Result<FrontendServerConfig, String> {
    if config_path.is_some() && (persistent_path.is_some() || port.is_some()) {
        return Err("Cannot specify a config file with persistent path or port.".into());
    }

    let mut config = match config_path {
        Some(config_path) => FrontendServerConfig::load_from_path(&config_path),
        None => FrontendServerConfig::single_node_default(),
    };

    config.persist_path = persistent_path;

    if let Some(ref mut sqlite_config) = config.frontend.sqlitedb {
        sqlite_config.url = Some(format!(
            "{}/{}",
            config
                .persist_path
                .as_ref()
                .unwrap_or(&DEFAULT_PERSISTENT_PATH.to_string()),
            SQLITE_FILENAME
        ));
    }

    config.port = port.unwrap_or(config.port);

    Ok(config)
}

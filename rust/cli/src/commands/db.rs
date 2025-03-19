use crate::client::{ChromaClient, ChromaClientTrait};
use crate::utils::{get_current_profile, load_cli_env_config, Profile};
use arboard::Clipboard;
use chroma_types::Database;
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Input, Select};
use std::fmt;
use std::io::Write;

const LIST_DB_SELECTION_LIMIT: usize = 5;

#[derive(Debug, Clone, ValueEnum)]
pub enum Language {
    Python,
    JavaScript,
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Language::Python => write!(f, "python"),
            Language::JavaScript => write!(f, "javascript"),
        }
    }
}

#[derive(Args, Debug)]
pub struct DbArgs {
    #[clap(long, hide = true, help = "Flag to use during development")]
    dev: bool,
}

#[derive(Args, Debug)]
pub struct CreateArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct DeleteArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    name: Option<String>,
}

#[derive(Args, Debug)]
pub struct ConnectArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    name: Option<String>,
    language: Option<Language>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    #[clap(flatten)]
    db_args: DbArgs,
}

#[derive(Subcommand, Debug)]
pub enum DbCommand {
    Connect(ConnectArgs),
    Create(CreateArgs),
    Delete(DeleteArgs),
    List(ListArgs),
}

pub fn get_python_connection(
    url: String,
    tenant_id: String,
    db_name: String,
    api_key: String,
) -> String {
    format!(
        "
    import chromadb
    client = chromadb.HttpClient(
        ssl=True,
        host={},
        tenant='{}',
        database='{}',
        headers={{
            'x-chroma-token': '{}'
        }}
    )",
        url, tenant_id, db_name, api_key
    )
}

pub fn get_js_connection(
    url: String,
    tenant_id: String,
    db_name: String,
    api_key: String,
) -> String {
    format!(
        "
    import {{ ChromaClient }} from 'chromadb';
    const client = new ChromaClient({{
        path: '{}',
        auth: {{ provider: 'token', credentials: '{}', tokenHeaderType: 'X_CHROMA_TOKEN' }},
        tenant: '{}',
        database: '{}'
    }});
",
        url, api_key, tenant_id, db_name
    )
}

pub fn prompt_db_name<W: Write>(writer: &mut W, prompt: &str) -> Result<String, std::io::Error> {
    writeln!(writer, "{}", prompt.blue().bold())?;
    let input = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();
    Ok(input)
}

pub fn get_chroma_client<W: Write>(
    writer: &mut W,
    profile: &Profile,
    dev: bool,
) -> Result<Option<ChromaClient>, std::io::Error> {
    let cli_env_config = load_cli_env_config(dev);
    match ChromaClient::from_profile(cli_env_config.frontend_url, profile) {
        Ok(client) => Ok(Some(client)),
        Err(e) => {
            let message = format!("Failed to connect to Chroma Cloud {}", e);
            writeln!(writer, "{}", message.red())?;
            Ok(None)
        }
    }
}

fn fetch_dbs<W: Write>(
    writer: &mut W,
    chroma_client: &ChromaClient,
    profile_name: &str,
) -> Result<Option<Vec<Database>>, std::io::Error> {
    match chroma_client.list_databases() {
        Ok(dbs) => Ok(Some(dbs)),
        Err(_) => {
            let message = format!("Failed to fetch DBs for profile {}", profile_name);
            writeln!(writer, "{}", message.red())?;
            Ok(None)
        }
    }
}

pub fn get_chroma_client_and_dbs<W: Write>(
    writer: &mut W,
    profile: &Profile,
    profile_name: String,
    dev: bool,
) -> Result<Option<(ChromaClient, Vec<Database>)>, std::io::Error> {
    let chroma_client = match get_chroma_client(writer, profile, dev)? {
        Some(client) => client,
        None => return Ok(None),
    };

    let dbs = match fetch_dbs(writer, &chroma_client, &profile_name)? {
        Some(dbs) => dbs,
        None => return Ok(None),
    };
    Ok(Some((chroma_client, dbs)))
}

fn validate_db_name(db_name: &str) -> Result<String, String> {
    if db_name.is_empty() {
        return Err("Database name cannot be empty".to_string());
    }

    if !db_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(
            "Database name must contain only alphanumeric characters, hyphens, or underscores"
                .to_string(),
        );
    }

    Ok(db_name.to_string())
}

fn get_db_name_from_args<W: Write>(
    writer: &mut W,
    name: Option<String>,
    prompt: &str,
) -> Result<Option<String>, std::io::Error> {
    let name = match name {
        Some(name) => name,
        None => prompt_db_name(writer, prompt)?,
    };
    match validate_db_name(&name) {
        Ok(name) => Ok(Some(name)),
        Err(e) => {
            writeln!(writer, "{}", e.as_str().red())?;
            Ok(None)
        }
    }
}

pub fn connect<W: Write>(
    writer: &mut W,
    args: ConnectArgs,
    profile_name: String,
    current_profile: Profile,
) -> Result<(), std::io::Error> {
    let (chroma_client, dbs) = match get_chroma_client_and_dbs(
        writer,
        &current_profile,
        profile_name.clone(),
        args.db_args.dev,
    )? {
        Some((client, dbs)) => (client, dbs),
        None => return Ok(()),
    };

    let prompt = "Which DB would you like to connect to?";
    let name = if dbs.len() < LIST_DB_SELECTION_LIMIT {
        writeln!(writer, "{}", prompt.blue().bold())?;
        let db_names: Vec<String> = dbs.iter().map(|db| db.name.clone()).collect();
        let selection = Select::with_theme(&ColorfulTheme::default())
            .items(&db_names)
            .default(0)
            .interact()
            .unwrap();
        db_names[selection].clone()
    } else {
        match args.name {
            Some(name) => name,
            None => prompt_db_name(writer, prompt)?,
        }
    };

    if !dbs.iter().any(|db| db.name == name) {
        let message = format!("DB {} not found", name);
        writeln!(writer, "{}", message.red())?;
        return Ok(());
    }

    let language = match args.language {
        Some(language) => language,
        None => {
            let options = vec![
                format!("{} {}", ">".yellow(), "Python"),
                format!("{} {}", ">".yellow(), "JavaScript/Typescript"),
            ];

            writeln!(
                writer,
                "\n{}",
                "Which language would you like to use?".blue().bold()
            )?;
            let selection = Select::with_theme(&ColorfulTheme::default())
                .items(&options)
                .default(0)
                .interact()
                .unwrap();

            if selection == 0 {
                Language::Python
            } else {
                Language::JavaScript
            }
        }
    };

    writeln!(writer, "{}", language.to_string().green())?;

    let connection_string = match language {
        Language::Python => get_python_connection(
            chroma_client.api_url,
            current_profile.team_id,
            name,
            current_profile.api_key,
        ),
        Language::JavaScript => get_js_connection(
            chroma_client.api_url,
            current_profile.team_id,
            name,
            current_profile.api_key,
        ),
    };

    writeln!(writer, "{}", connection_string)?;
    let mut clipboard = Clipboard::new().expect("Failed to create clipboard");
    clipboard
        .set_text(connection_string)
        .expect("Failed to copy text");
    writeln!(writer, "\n{}", "Copied to clipboard!".blue().bold())?;

    Ok(())
}

pub fn create<W, F, C>(
    writer: &mut W,
    get_chroma_client_and_dbs: F,
    args: CreateArgs,
    profile_name: String,
    current_profile: Profile,
) -> Result<(), std::io::Error>
where
    W: Write,
    F: Fn(&mut W, &Profile, String, bool) -> Result<Option<(C, Vec<Database>)>, std::io::Error>,
    C: ChromaClientTrait,
{
    let (chroma_client, dbs) = match get_chroma_client_and_dbs(
        writer,
        &current_profile,
        profile_name.clone(),
        args.db_args.dev,
    )? {
        Some((client, dbs)) => (client, dbs),
        None => return Ok(()),
    };

    let name = match get_db_name_from_args(writer, args.name, "What is the name of your new DB?")? {
        Some(name) => name,
        None => return Ok(()),
    };

    if dbs.iter().any(|db| db.name == name) {
        let message = format!("DB with name {} already exists!", name);
        writeln!(writer, "{}", message.red())?;
        writeln!(
            writer,
            "If you want to delete it, use: {} {}",
            "chroma db delete".yellow(),
            name.yellow()
        )?;
        return Ok(());
    }

    writeln!(
        writer,
        "\n{} {}...",
        "Creating database".bold().blue(),
        name.bold().blue()
    )?;

    if chroma_client.create_database(name.clone()).is_err() {
        let message = format!("Failed to create database {}", name);
        writeln!(writer, "{}", message.red())?;
        return Ok(());
    };

    let success_message = format!("\nDatabase {} created successfully!", name);
    let instructions = format!(
        "To get a connection string, run:\n   {} {}",
        "chroma db connect".yellow(),
        name.yellow()
    );
    writeln!(writer, "{}", success_message.green())?;
    writeln!(writer, "{}", instructions)?;

    Ok(())
}

pub fn delete<W, F, C>(
    writer: &mut W,
    get_chroma_client_and_dbs: F,
    args: DeleteArgs,
    profile_name: String,
    current_profile: Profile,
) -> Result<(), std::io::Error>
where
    W: Write,
    F: Fn(&mut W, &Profile, String, bool) -> Result<Option<(C, Vec<Database>)>, std::io::Error>,
    C: ChromaClientTrait,
{
    let (chroma_client, dbs) = match get_chroma_client_and_dbs(
        writer,
        &current_profile,
        profile_name.clone(),
        args.db_args.dev,
    )? {
        Some((client, dbs)) => (client, dbs),
        None => return Ok(()),
    };

    let name = match get_db_name_from_args(writer, args.name, "What is the name of your new DB?")? {
        Some(name) => name,
        None => return Ok(()),
    };

    if !dbs.iter().any(|db| db.name == name) {
        let message = format!("DB {} not found", name);
        writeln!(writer, "{}", message.red())?;
        return Ok(());
    }

    writeln!(writer, "{}", "Are you sure you want to delete this DB?\nThis action cannot be reverted and you will lose all the data in this DB.\n\nIf you want to continue please type the name of DB to confirm.".red().bold())?;
    let confirm: String = Input::with_theme(&ColorfulTheme::default())
        .interact_text()
        .unwrap();

    if confirm == name {
        match chroma_client.delete_database(name.clone()) {
            Ok(_) => {}
            Err(_) => {
                let message = format!("\nFailed to delete DB {}", name);
                writeln!(writer, "{}", message.red())?;
            }
        }
        writeln!(writer, "\nDeleted DB {} successfully!", name)?;
    } else {
        writeln!(
            writer,
            "\n{} '{}' {} '{}'",
            "DB deletion cancelled. Confirmation input".yellow(),
            confirm.yellow(),
            "did not match DB name to delete: ".yellow(),
            name.yellow()
        )?;
    }

    Ok(())
}

pub fn list<W, F, C>(
    writer: &mut W,
    get_chroma_client_and_dbs: F,
    args: ListArgs,
    profile_name: String,
    current_profile: Profile,
) -> Result<(), std::io::Error>
where
    W: Write,
    F: Fn(&mut W, &Profile, String, bool) -> Result<Option<(C, Vec<Database>)>, std::io::Error>,
    C: ChromaClientTrait,
{
    let (_chroma_client, dbs) = match get_chroma_client_and_dbs(
        writer,
        &current_profile,
        profile_name.clone(),
        args.db_args.dev,
    )? {
        Some((client, dbs)) => (client, dbs),
        None => return Ok(()),
    };

    if dbs.is_empty() {
        writeln!(
            writer,
            "Profile {} has 0 DBs. To create a new Chroma DB use: {}",
            profile_name,
            "chroma db create <db name>".yellow()
        )?;
        return Ok(());
    }

    writeln!(
        writer,
        "{} {} {}",
        "Listing".blue().bold(),
        dbs.len().to_string().blue().bold(),
        "databases".blue().bold()
    )?;
    for db in dbs {
        writeln!(writer, "{} {}", ">".yellow(), db.name)?;
    }

    Ok(())
}

pub fn db_command<W: Write>(writer: &mut W, command: DbCommand) -> Result<(), std::io::Error> {
    let (profile_name, current_profile) = match get_current_profile() {
        Ok((profile_name, current_profile)) => (profile_name, current_profile),
        Err(_) => {
            writeln!(writer, "{}", "No current profile found.".red().bold())?;
            writeln!(
                writer,
                "To set a new profile use: {}",
                "chroma login".yellow()
            )?;
            return Ok(());
        }
    };

    match command {
        DbCommand::Connect(args) => connect(writer, args, profile_name, current_profile),
        DbCommand::Create(args) => create(
            writer,
            get_chroma_client_and_dbs,
            args,
            profile_name,
            current_profile,
        ),
        DbCommand::Delete(args) => delete(
            writer,
            get_chroma_client_and_dbs,
            args,
            profile_name,
            current_profile,
        ),
        DbCommand::List(args) => list(
            writer,
            get_chroma_client_and_dbs,
            args,
            profile_name,
            current_profile,
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::client::{ChromaCliClientError, ChromaClientTrait};
    use crate::commands::db::{create, list, CreateArgs, DbArgs, ListArgs};
    use crate::utils::Profile;
    use chroma_types::Database;
    use std::io::Cursor;
    use uuid::Uuid;

    #[derive(Default)]
    pub struct MockChromaClient;

    impl ChromaClientTrait for MockChromaClient {
        fn list_databases(&self) -> Result<Vec<Database>, ChromaCliClientError> {
            Ok(vec![])
        }

        fn create_database(&self, _name: String) -> Result<(), ChromaCliClientError> {
            Ok(())
        }

        fn delete_database(&self, _name: String) -> Result<(), ChromaCliClientError> {
            Ok(())
        }
    }

    fn fake_get_client_and_dbs_some<W: std::io::Write>(
        _writer: &mut W,
        _profile: &Profile,
        _profile_name: String,
        _dev: bool,
    ) -> Result<Option<(MockChromaClient, Vec<Database>)>, std::io::Error> {
        let fake_dbs = vec![
            Database {
                id: Uuid::new_v4(),
                name: "fake_db1".to_string(),
                tenant: "tenant1".to_string(),
            },
            Database {
                id: Uuid::new_v4(),
                name: "fake_db2".to_string(),
                tenant: "tenant2".to_string(),
            },
        ];
        Ok(Some((MockChromaClient, fake_dbs)))
    }

    fn fake_get_client_and_dbs_empty<W: std::io::Write>(
        _writer: &mut W,
        _profile: &Profile,
        _profile_name: String,
        _dev: bool,
    ) -> Result<Option<(MockChromaClient, Vec<Database>)>, std::io::Error> {
        Ok(Some((MockChromaClient, vec![])))
    }

    #[test]
    fn test_list_with_no_dbs() {
        let mut output = Cursor::new(Vec::new());
        let args = ListArgs {
            db_args: DbArgs { dev: true },
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        list(
            &mut output,
            fake_get_client_and_dbs_empty,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("list should succeed");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains("has 0 DBs"));
        assert!(result.contains("chroma db create <db name>"));
    }

    #[test]
    fn test_list_with_some_dbs() {
        let mut output = Cursor::new(Vec::new());
        let args = ListArgs {
            db_args: DbArgs { dev: true },
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        list(
            &mut output,
            fake_get_client_and_dbs_some,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("list should succeed");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains("Listing"));
        assert!(result.contains("fake_db1"));
        assert!(result.contains("fake_db2"));
    }

    #[test]
    fn test_create_with_invalid_name() {
        let mut output = Cursor::new(Vec::new());
        let args = CreateArgs {
            db_args: DbArgs { dev: true },
            name: Some("invalid@name".to_string()),
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        create(
            &mut output,
            fake_get_client_and_dbs_some,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("create should execute without error");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains(
            "Database name must contain only alphanumeric characters, hyphens, or underscores"
        ));
    }

    #[test]
    fn test_create_success() {
        let mut output = Cursor::new(Vec::new());
        let args = CreateArgs {
            db_args: DbArgs { dev: true },
            name: Some("new_valid_db".to_string()),
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        create(
            &mut output,
            fake_get_client_and_dbs_empty,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("create should execute without error");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains("Creating database"));
        assert!(result.contains("Database new_valid_db created successfully!"));
        assert!(result.contains("chroma db connect"));
    }

    #[test]
    fn test_create_with_name_that_already_exists() {
        let mut output = Cursor::new(Vec::new());
        let args = CreateArgs {
            db_args: DbArgs { dev: true },
            name: Some("fake_db1".to_string()),
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        create(
            &mut output,
            fake_get_client_and_dbs_some,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("create should execute without error");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains("DB with name fake_db1 already exists!"));
        assert!(result.contains("chroma db delete"));
    }

    #[test]
    fn test_delete_for_name_that_doesnt_exist() {
        use crate::commands::db::{delete, DbArgs, DeleteArgs};
        use std::io::Cursor;

        let mut output = Cursor::new(Vec::new());
        let args = DeleteArgs {
            db_args: DbArgs { dev: true },
            name: Some("nonexistent_db".to_string()),
        };
        let profile_name = "test_profile".to_string();
        let current_profile = Profile {
            api_key: "dummy_key".to_string(),
            team_id: "dummy_team".to_string(),
        };

        delete(
            &mut output,
            fake_get_client_and_dbs_some,
            args,
            profile_name.clone(),
            current_profile,
        )
        .expect("delete should execute without error");

        let result = String::from_utf8(output.into_inner()).unwrap();
        assert!(result.contains("DB nonexistent_db not found"));
    }
}

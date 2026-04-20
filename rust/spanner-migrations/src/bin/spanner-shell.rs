use std::io::{self, IsTerminal, Read, Write};

use chroma_tracing::{init_global_filter_layer, init_otel_layer, init_stdout_layer, init_tracing};
use chroma_types::Topology;
use clap::{Parser, ValueEnum};
use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use serde_json::Value;
use spanner_migrations::{
    connect_spanner, ddl_wait_retry_setting, RootConfig, RunMigrationsError, TopologySpannerConfig,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TargetDatabase {
    Sysdb,
    #[value(alias = "log")]
    Logdb,
}

impl TargetDatabase {
    fn slug(self) -> &'static str {
        match self {
            Self::Sysdb => "spanner_sysdb",
            Self::Logdb => "spanner_logdb",
        }
    }

    fn shell_name(self) -> &'static str {
        match self {
            Self::Sysdb => "sysdb",
            Self::Logdb => "log",
        }
    }

    fn from_repl_arg(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "sysdb" | "spanner_sysdb" => Some(Self::Sysdb),
            "log" | "logdb" | "spanner_logdb" => Some(Self::Logdb),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlKind {
    Query,
    Dml,
    Ddl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplCommand {
    Help,
    Quit,
    SwitchDatabase(TargetDatabase),
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Interactive SQL shell for the configured Spanner target"
)]
struct Args {
    /// Override CONFIG_PATH before loading migration config.
    #[arg(long)]
    config_path: Option<String>,

    /// Which configured database to target.
    #[arg(long, value_enum, default_value_t = TargetDatabase::Sysdb)]
    database: TargetDatabase,

    /// Topology to connect to. Defaults to the first configured topology.
    #[arg(long)]
    topology: Option<String>,

    /// Execute one SQL statement and exit.
    #[arg(long, short = 'e')]
    execute: Option<String>,
}

struct ShellConnection {
    client: Client,
    admin_client: AdminClient,
    database_path: String,
    admin_rpc_timeout_secs: u64,
}

struct ShellContext {
    connection: ShellConnection,
    topology: Topology<TopologySpannerConfig>,
    topology_name: String,
    target_database: TargetDatabase,
}

impl ShellContext {
    async fn connect(
        topology: &Topology<TopologySpannerConfig>,
        target_database: TargetDatabase,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let connection = connect_target(topology, target_database).await?;
        Ok(Self {
            connection,
            topology: topology.clone(),
            topology_name: topology.name.to_string(),
            target_database,
        })
    }

    fn prompt_label(&self) -> &'static str {
        self.target_database.shell_name()
    }

    async fn switch_database(
        &mut self,
        target_database: TargetDatabase,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.target_database == target_database {
            println!(
                "Already connected to {} via {} on {}.",
                self.target_database.slug(),
                self.topology_name,
                self.connection.database_path
            );
            return Ok(());
        }

        let new_connection = connect_target(&self.topology, target_database).await?;
        let old_connection = std::mem::replace(&mut self.connection, new_connection);
        self.target_database = target_database;
        old_connection.client.close().await;
        println!(
            "Switched to {} via {} on {}.",
            self.target_database.slug(),
            self.topology_name,
            self.connection.database_path
        );
        Ok(())
    }

    async fn close(self) {
        self.connection.client.close().await;
    }
}

async fn connect_target(
    topology: &Topology<TopologySpannerConfig>,
    target_database: TargetDatabase,
) -> Result<ShellConnection, Box<dyn std::error::Error>> {
    let spanner_config = match target_database {
        TargetDatabase::Sysdb => &topology.config.sysdb_spanner,
        TargetDatabase::Logdb => &topology.config.logdb_spanner,
    };

    let connected = connect_spanner(spanner_config)
        .await
        .map_err(render_connection_error)?;
    Ok(ShellConnection {
        client: connected.client,
        admin_client: connected.admin_client,
        database_path: connected.database_path,
        admin_rpc_timeout_secs: connected.admin_rpc_timeout_secs,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Some(config_path) = &args.config_path {
        std::env::set_var("CONFIG_PATH", config_path);
    }

    let config = RootConfig::load()?;
    init_shell_tracing(&config);

    let topology = select_topology(&config.topologies, args.topology.as_deref())?;
    let mut shell = ShellContext::connect(topology, args.database).await?;

    if let Some(sql) = args.execute {
        run_statement(&mut shell, &sql).await?;
        shell.close().await;
        return Ok(());
    }

    if !io::stdin().is_terminal() {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        if !input.trim().is_empty() {
            run_statement(&mut shell, &input).await?;
        }
        shell.close().await;
        return Ok(());
    }

    println!(
        "Connected to {} via {} on {}. Use 'use sysdb' or 'use log'. End statements with ';'. Type 'help' or 'quit'.",
        shell.target_database.slug(),
        shell.topology_name,
        shell.connection.database_path
    );

    repl(&mut shell).await?;
    shell.close().await;
    Ok(())
}

fn init_shell_tracing(config: &spanner_migrations::MigrationConfig) {
    let tracing_layers = vec![
        init_global_filter_layer(&config.otel_filters),
        init_otel_layer(&config.service_name, &config.otel_endpoint),
        init_stdout_layer(),
    ];
    init_tracing(tracing_layers);
}

fn select_topology<'a>(
    topologies: &'a [chroma_types::Topology<TopologySpannerConfig>],
    requested: Option<&str>,
) -> Result<&'a chroma_types::Topology<TopologySpannerConfig>, Box<dyn std::error::Error>> {
    if topologies.is_empty() {
        return Err("No topologies defined in configuration".into());
    }

    if let Some(requested) = requested {
        return topologies
            .iter()
            .find(|topology| topology.name.to_string() == requested)
            .ok_or_else(|| format!("Topology '{}' not found in configuration", requested).into());
    }

    Ok(&topologies[0])
}

async fn repl(shell: &mut ShellContext) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut buffer = String::new();

    loop {
        let prompt_label = shell.prompt_label();
        let prompt = if buffer.trim().is_empty() {
            format!("{prompt_label}> ")
        } else {
            format!("{:width$}> ", "", width = prompt_label.len())
        };
        print!("{}", prompt);
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes_read = stdin.read_line(&mut line)?;
        if bytes_read == 0 {
            if !buffer.trim().is_empty() {
                if let Err(err) = run_statement(shell, &buffer).await {
                    eprintln!("error: {err}");
                }
            }
            break;
        }

        let trimmed = line.trim();
        if buffer.trim().is_empty() {
            match parse_repl_command(trimmed) {
                Ok(Some(ReplCommand::Quit)) => break,
                Ok(Some(ReplCommand::Help)) => {
                    print_help();
                    continue;
                }
                Ok(Some(ReplCommand::SwitchDatabase(target_database))) => {
                    if let Err(err) = shell.switch_database(target_database).await {
                        eprintln!("error: {err}");
                    }
                    continue;
                }
                Ok(None) if trimmed.is_empty() => continue,
                Ok(None) => {}
                Err(err) => {
                    eprintln!("error: {err}");
                    continue;
                }
            }
        }

        buffer.push_str(&line);
        if !trimmed.ends_with(';') {
            continue;
        }

        if let Err(err) = run_statement(shell, &buffer).await {
            eprintln!("error: {err}");
        }
        buffer.clear();
    }

    Ok(())
}

fn print_help() {
    println!("Commands:");
    println!("  help          Show this help.");
    println!("  quit | exit   Exit the shell.");
    println!("  use <db>      Switch to 'sysdb' or 'log'.");
    println!("  \\c <db>       Alias for 'use <db>'.");
    println!("Notes:");
    println!("  One statement at a time.");
    println!("  Multi-line statements are supported and execute when the last line ends with ';'.");
}

fn parse_repl_command(line: &str) -> Result<Option<ReplCommand>, String> {
    let command = line.trim().trim_end_matches(';').trim();
    if command.is_empty() {
        return Ok(None);
    }

    match command {
        "help" => return Ok(Some(ReplCommand::Help)),
        "quit" | "exit" | "\\q" => return Ok(Some(ReplCommand::Quit)),
        _ => {}
    }

    let mut parts = command.split_whitespace();
    let Some(verb) = parts.next() else {
        return Ok(None);
    };
    match verb.to_ascii_lowercase().as_str() {
        "use" => parse_switch_command(parts),
        _ if verb == "\\c" => parse_switch_command(parts),
        _ => Ok(None),
    }
}

fn parse_switch_command<'a>(
    mut args: impl Iterator<Item = &'a str>,
) -> Result<Option<ReplCommand>, String> {
    const USAGE: &str = "Usage: use <sysdb|log> or \\c <sysdb|log>";

    let Some(target) = args.next() else {
        return Err(USAGE.to_string());
    };
    if args.next().is_some() {
        return Err(USAGE.to_string());
    }

    let target_database = TargetDatabase::from_repl_arg(target)
        .ok_or_else(|| format!("Unknown database '{}'. Use 'sysdb' or 'log'.", target))?;
    Ok(Some(ReplCommand::SwitchDatabase(target_database)))
}

async fn run_statement(
    shell: &mut ShellContext,
    raw_sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sql = normalize_sql(raw_sql, &shell.topology_name)?;
    if sql.is_empty() {
        return Ok(());
    }

    match classify_sql(&sql)? {
        SqlKind::Query => execute_query(&mut shell.connection.client, &sql).await?,
        SqlKind::Dml => execute_dml(&mut shell.connection.client, &sql).await?,
        SqlKind::Ddl => {
            execute_ddl(
                &shell.connection.admin_client,
                &shell.connection.database_path,
                shell.connection.admin_rpc_timeout_secs,
                &sql,
            )
            .await?
        }
    }

    Ok(())
}

fn normalize_sql(raw_sql: &str, topology_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let sql = raw_sql.trim();
    if sql.is_empty() {
        return Ok(String::new());
    }

    let sql = sql.trim_end_matches(';').trim().to_string();
    if sql.contains("@topo_name") {
        return Ok(sql.replace("@topo_name", &format!("'{}'", topology_name)));
    }
    Ok(sql)
}

fn classify_sql(sql: &str) -> Result<SqlKind, Box<dyn std::error::Error>> {
    let leading = strip_leading_comments(sql).to_ascii_lowercase();
    let keyword = leading
        .split_whitespace()
        .next()
        .ok_or_else(|| "SQL statement is empty".to_string())?;

    match keyword {
        "select" | "with" | "show" | "describe" | "explain" => Ok(SqlKind::Query),
        "insert" | "update" | "delete" => Ok(SqlKind::Dml),
        "create" | "alter" | "drop" | "truncate" => Ok(SqlKind::Ddl),
        other => Err(format!("Unsupported statement type '{}'", other).into()),
    }
}

fn strip_leading_comments(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if let Some(stripped) = rest.strip_prefix("--") {
            if let Some((_, remaining)) = stripped.split_once('\n') {
                rest = remaining.trim_start();
                continue;
            }
            return "";
        }

        if let Some(stripped) = rest.strip_prefix("/*") {
            if let Some((_, remaining)) = stripped.split_once("*/") {
                rest = remaining.trim_start();
                continue;
            }
            return "";
        }

        return rest;
    }
}

async fn execute_query(client: &mut Client, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let wrapped = wrap_query_for_json(sql);
    let stmt = Statement::new(&wrapped);
    let mut tx = client.single().await?;
    let mut iter = tx.query(stmt).await?;
    let mut row_count = 0usize;

    while let Some(row) = iter.next().await? {
        let row_json: String = row.column_by_name("row_json")?;
        print_row_json(&row_json)?;
        row_count += 1;
    }

    println!("{} row(s)", row_count);
    Ok(())
}

fn wrap_query_for_json(sql: &str) -> String {
    format!(
        "SELECT TO_JSON_STRING(TO_JSON((SELECT AS STRUCT row_data.*))) AS row_json FROM ({sql}) AS row_data"
    )
}

fn print_row_json(row_json: &str) -> Result<(), Box<dyn std::error::Error>> {
    match serde_json::from_str::<Value>(row_json) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value)?),
        Err(_) => println!("{row_json}"),
    }
    Ok(())
}

async fn execute_dml(client: &mut Client, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (_, rows_affected) = client
        .read_write_transaction::<i64, google_cloud_spanner::client::Error, _>(|tx| {
            let sql = sql.to_string();
            Box::pin(async move {
                let stmt = Statement::new(&sql);
                let rows_affected = tx.update(stmt).await?;
                Ok(rows_affected)
            })
        })
        .await?;

    println!("{} row(s) affected", rows_affected);
    Ok(())
}

async fn execute_ddl(
    admin_client: &AdminClient,
    database_path: &str,
    admin_rpc_timeout_secs: u64,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = UpdateDatabaseDdlRequest {
        database: database_path.to_string(),
        statements: vec![sql.to_string()],
        operation_id: String::new(),
        proto_descriptors: Vec::new(),
        throughput_mode: false,
    };

    let mut operation = admin_client
        .database()
        .update_database_ddl(request, None)
        .await?;
    operation
        .wait(Some(ddl_wait_retry_setting(admin_rpc_timeout_secs)))
        .await?;
    println!("DDL applied");
    Ok(())
}

fn render_connection_error(err: RunMigrationsError) -> Box<dyn std::error::Error> {
    err.to_string().into()
}

#[cfg(test)]
mod tests {
    use super::{
        classify_sql, parse_repl_command, strip_leading_comments, wrap_query_for_json, ReplCommand,
        SqlKind, TargetDatabase,
    };

    #[test]
    fn strips_leading_comments_before_classifying() {
        assert_eq!(
            strip_leading_comments("-- hello\n/* world */\nSELECT 1"),
            "SELECT 1"
        );
    }

    #[test]
    fn classifies_select_and_dml() {
        assert!(matches!(classify_sql("SELECT 1").unwrap(), SqlKind::Query));
        assert!(matches!(
            classify_sql("/* leading */ INSERT INTO t (id) VALUES (1)").unwrap(),
            SqlKind::Dml
        ));
    }

    #[test]
    fn wraps_queries_with_select_as_struct_json_conversion() {
        assert_eq!(
            wrap_query_for_json("select * from collections"),
            "SELECT TO_JSON_STRING(TO_JSON((SELECT AS STRUCT row_data.*))) AS row_json FROM (select * from collections) AS row_data"
        );
    }

    #[test]
    fn parses_database_switch_commands() {
        assert_eq!(
            parse_repl_command("use sysdb").unwrap(),
            Some(ReplCommand::SwitchDatabase(TargetDatabase::Sysdb))
        );
        assert_eq!(
            parse_repl_command("use log;").unwrap(),
            Some(ReplCommand::SwitchDatabase(TargetDatabase::Logdb))
        );
        assert_eq!(
            parse_repl_command("\\c spanner_logdb").unwrap(),
            Some(ReplCommand::SwitchDatabase(TargetDatabase::Logdb))
        );
    }

    #[test]
    fn rejects_invalid_switch_commands() {
        assert_eq!(
            parse_repl_command("use").unwrap_err(),
            "Usage: use <sysdb|log> or \\c <sysdb|log>"
        );
        assert_eq!(
            parse_repl_command("use other").unwrap_err(),
            "Unknown database 'other'. Use 'sysdb' or 'log'."
        );
    }
}

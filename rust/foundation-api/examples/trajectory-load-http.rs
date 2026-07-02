//! Load generated trajectories through the foundation-api HTTP routes.
//!
//! Run against a foundation-api server that has trajectory record I/O enabled:
//!
//! ```bash
//! CHROMA_API_KEY=ck-... \
//! cargo run -p foundation-api --example trajectory-load-http -- \
//!   --api-url http://localhost:8000 \
//!   ../foundation-research/trajectories/generate
//! ```
//!
//! Add `--incremental` to create each trajectory open, append entries, finalize,
//! and verify the finalized read through `GET /api/trajectories/{id}`. The
//! default mode is wholesale.

use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use clap::Parser;
use foundation_api::trajectories::{GenerateTrajectoryFile, TrajectoryEntry, WriteState};
use reqwest::{Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const DEFAULT_API_URL: &str = "http://localhost:8000";
const CHROMA_API_KEY_ENV: &str = "CHROMA_API_KEY";
const TOKEN_HEADER: &str = "x-chroma-token";

/// Load generated trajectory JSON files through foundation-api.
#[derive(Debug, Parser)]
struct Args {
    /// Base URL for foundation-api.
    #[arg(long, default_value = DEFAULT_API_URL, value_name = "URL")]
    api_url: String,

    /// Chroma API token sent as x-chroma-token. Defaults to CHROMA_API_KEY.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,

    /// Upload through open, append, and finalize routes instead of one-shot save.
    #[arg(long)]
    incremental: bool,

    /// Number of entries per append request when --incremental is set.
    #[arg(long, default_value_t = 1, value_name = "N")]
    append_batch: usize,

    /// Generated trajectory JSON files or directories recursively containing them.
    #[arg(required = true, value_name = "PATH")]
    paths: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct TrajectoryWriteResponse {
    trajectory_id: Uuid,
    write_state: WriteState,
    entry_count: usize,
    record_count: usize,
    first_inserted_record_offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct AppendTrajectoryEntriesRequest<'a> {
    expected_entry_index: usize,
    entries: &'a [TrajectoryEntry],
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(err) = run(args).await {
        eprintln!("FAIL {err}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    if args.append_batch == 0 {
        return Err("--append-batch must be at least 1".into());
    }

    let client =
        FoundationTrajectoryClient::new(resolve_api_url(&args.api_url), resolve_token(&args)?)?;
    let paths = collect_input_paths(&args.paths)?;
    if paths.is_empty() {
        return Err("no trajectory JSON files matched the provided paths".into());
    }

    for path in &paths {
        let file = parse_path(path)?;
        if args.incremental {
            save_incremental(&client, &file, args.append_batch).await?;
        } else {
            save_one_shot(&client, &file).await?;
        }
        verify_json_equivalent(&client, &file).await?;

        println!(
            "PASS {} {} entries",
            path.display(),
            file.trajectory.actions_and_observations.len()
        );
    }

    println!("PASS loaded {} trajectories via HTTP", paths.len());
    Ok(())
}

fn resolve_api_url(raw: &str) -> String {
    raw.trim_end_matches('/').to_string()
}

fn resolve_token(args: &Args) -> Result<String, Box<dyn Error>> {
    args.token
        .clone()
        .or_else(|| env::var(CHROMA_API_KEY_ENV).ok())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| format!("missing token: pass --token or set {CHROMA_API_KEY_ENV}").into())
}

struct FoundationTrajectoryClient {
    client: reqwest::Client,
    api_url: String,
    token: String,
}

impl FoundationTrajectoryClient {
    fn new(api_url: String, token: String) -> Result<Self, Box<dyn Error>> {
        if api_url.is_empty() {
            return Err("foundation-api URL cannot be empty".into());
        }

        Ok(Self {
            client: reqwest::Client::new(),
            api_url,
            token,
        })
    }

    async fn save(
        &self,
        file: &GenerateTrajectoryFile,
    ) -> Result<TrajectoryWriteResponse, Box<dyn Error>> {
        self.send_json(Method::POST, "/api/trajectories/save", file)
            .await
    }

    async fn open(
        &self,
        file: &GenerateTrajectoryFile,
    ) -> Result<TrajectoryWriteResponse, Box<dyn Error>> {
        self.send_json(Method::POST, "/api/trajectories/open", file)
            .await
    }

    async fn append_entries(
        &self,
        id: Uuid,
        request: &AppendTrajectoryEntriesRequest<'_>,
    ) -> Result<TrajectoryWriteResponse, Box<dyn Error>> {
        self.send_json(
            Method::POST,
            &format!("/api/trajectories/{id}/entries"),
            request,
        )
        .await
    }

    async fn finalize(
        &self,
        file: &GenerateTrajectoryFile,
    ) -> Result<TrajectoryWriteResponse, Box<dyn Error>> {
        self.send_json(
            Method::POST,
            &format!("/api/trajectories/{}/finalize", file.trajectory.id),
            file,
        )
        .await
    }

    async fn get_finalized(&self, id: Uuid) -> Result<GenerateTrajectoryFile, Box<dyn Error>> {
        self.request_json(
            Method::GET,
            &format!("/api/trajectories/{id}?require_finalized=true"),
        )
        .await
    }

    async fn send_json<Body, Response>(
        &self,
        method: Method,
        path: &str,
        body: &Body,
    ) -> Result<Response, Box<dyn Error>>
    where
        Body: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let url = format!("{}{}", self.api_url, path);
        let response = self
            .client
            .request(method, &url)
            .header(TOKEN_HEADER, &self.token)
            .json(body)
            .send()
            .await?;
        parse_response(response).await
    }

    async fn request_json<Response>(
        &self,
        method: Method,
        path: &str,
    ) -> Result<Response, Box<dyn Error>>
    where
        Response: DeserializeOwned,
    {
        let url = format!("{}{}", self.api_url, path);
        let response = self
            .client
            .request(method, &url)
            .header(TOKEN_HEADER, &self.token)
            .send()
            .await?;
        parse_response(response).await
    }
}

async fn parse_response<Response>(response: reqwest::Response) -> Result<Response, Box<dyn Error>>
where
    Response: DeserializeOwned,
{
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(http_error(status, text).into());
    }
    Ok(serde_json::from_str(&text)?)
}

fn http_error(status: StatusCode, body: String) -> String {
    if body.trim().is_empty() {
        format!("foundation-api request failed with status {status}")
    } else {
        format!("foundation-api request failed with status {status}: {body}")
    }
}

async fn save_one_shot(
    client: &FoundationTrajectoryClient,
    file: &GenerateTrajectoryFile,
) -> Result<(), Box<dyn Error>> {
    let response = client.save(file).await?;
    print_write("WRITE", &response);
    Ok(())
}

async fn save_incremental(
    client: &FoundationTrajectoryClient,
    file: &GenerateTrajectoryFile,
    append_batch: usize,
) -> Result<(), Box<dyn Error>> {
    let open_file = open_trajectory_skeleton(file);
    let open = client.open(&open_file).await?;
    print_write("OPEN", &open);

    let id = file.trajectory.id;
    let entries = &file.trajectory.actions_and_observations;
    for (chunk_index, chunk) in entries.chunks(append_batch).enumerate() {
        let expected_entry_index = chunk_index * append_batch;
        let request = AppendTrajectoryEntriesRequest {
            expected_entry_index,
            entries: chunk,
        };
        let response = client.append_entries(id, &request).await?;
        print_write("APPEND", &response);
    }

    let finalized = client.finalize(file).await?;
    print_write("FINALIZE", &finalized);
    Ok(())
}

fn print_write(prefix: &str, response: &TrajectoryWriteResponse) {
    println!(
        "{prefix} {} state={:?} entries={} records={} first_offset={:?}",
        response.trajectory_id,
        response.write_state,
        response.entry_count,
        response.record_count,
        response.first_inserted_record_offset
    );
}

fn open_trajectory_skeleton(file: &GenerateTrajectoryFile) -> GenerateTrajectoryFile {
    let mut open_file = file.clone();
    open_file.trajectory.actions_and_observations.clear();
    open_file.duration_seconds = None;
    open_file.status = None;
    open_file.error = None;
    open_file.usage = None;
    open_file.citations = None;
    open_file.final_todos = None;
    open_file
}

async fn verify_json_equivalent(
    client: &FoundationTrajectoryClient,
    expected: &GenerateTrajectoryFile,
) -> Result<(), Box<dyn Error>> {
    let actual = client.get_finalized(expected.trajectory.id).await?;
    let expected_json = serde_json::to_value(expected)?;
    let actual_json = serde_json::to_value(actual)?;
    if expected_json != actual_json {
        return Err(json_mismatch(expected.trajectory.id, &expected_json, &actual_json).into());
    }
    Ok(())
}

fn parse_path(path: &Path) -> Result<GenerateTrajectoryFile, Box<dyn Error>> {
    let file = File::open(path).map_err(|err| format!("open {}: {err}", path.display()))?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(|err| format!("parse {}: {err}", path.display()).into())
}

fn collect_input_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            collect_json_files(path, &mut out)?;
        } else if path.is_file() {
            out.push(path.clone());
        } else {
            return Err(format!("input path does not exist: {}", path.display()).into());
        }
    }
    out.sort();
    Ok(out)
}

fn collect_json_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir).map_err(|err| format!("list {}: {err}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_json_files(&path, out)?;
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "json") {
            out.push(path);
        }
    }
    Ok(())
}

fn json_mismatch(id: Uuid, expected: &Value, actual: &Value) -> String {
    let expected = serde_json::to_string_pretty(expected)
        .unwrap_or_else(|err| format!("<could not format expected json: {err}>"));
    let actual = serde_json::to_string_pretty(actual)
        .unwrap_or_else(|err| format!("<could not format actual json: {err}>"));
    format!("trajectory {id} readback was not JSON-equivalent\nexpected:\n{expected}\nactual:\n{actual}")
}

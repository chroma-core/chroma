//! Load generated trajectories into Chroma and verify projected reads.
//!
//! Run against a Chroma server that supports conditional transactions:
//!
//! ```bash
//! CHROMA_ENDPOINT=http://localhost:8000 \
//! cargo run -p foundation-api --example trajectory-load-chroma -- \
//!   --collection generated_trajectories path/to/trajectory.json
//! ```
//!
//! Use `--incremental` to create each trajectory open, append one projected
//! reasoning entry per committed transaction, finalize, and verify the finalized read.

use std::error::Error;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use chroma::{ChromaCollection, ChromaHttpClient};
use clap::Parser;
use foundation_api::trajectories::{
    chroma_create_open_trajectory, chroma_extend_open_trajectory_at,
    chroma_finalize_open_trajectory, chroma_load_generate_trajectory,
    chroma_save_generate_trajectory, ReasoningEntry, ReasoningTrajectoryFile,
};
use serde_json::Value;

const DEFAULT_COLLECTION_NAME: &str = "generated_trajectories";

/// Load generated trajectory JSON files into Chroma.
#[derive(Debug, Parser)]
struct Args {
    /// Chroma collection that stores generated trajectory records.
    #[arg(long, value_name = "NAME")]
    collection: Option<String>,

    /// Upload each trajectory by committing the open header, each entry, and finalization.
    #[arg(long)]
    incremental: bool,

    /// Generated trajectory JSON file or directory of JSON files.
    #[arg(required = true, value_name = "TRAJECTORY_JSON_OR_DIR")]
    paths: Vec<PathBuf>,
}

impl Args {
    /// Resolve the target collection name.
    fn collection_name(&self) -> String {
        self.collection
            .clone()
            .or_else(|| std::env::var("CHROMA_TRAJECTORY_COLLECTION").ok())
            .unwrap_or_else(|| DEFAULT_COLLECTION_NAME.to_string())
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(args).await {
        Ok(summary) => {
            println!(
                "PASS loaded and verified {} trajectories into collection {:?}",
                summary.trajectories, summary.collection
            );
        }
        Err(err) => {
            eprintln!("FAIL {err}");
            std::process::exit(1);
        }
    }
}

/// Parse arguments, connect to Chroma, and process every requested trajectory.
async fn run(args: Args) -> Result<Summary, Box<dyn Error>> {
    let collection_name = args.collection_name();
    let paths = collect_input_paths(&args.paths)?;
    if paths.is_empty() {
        return Err("no trajectory JSON files matched the provided paths".into());
    }

    let client = ChromaHttpClient::from_env()?;
    let collection = client
        .get_or_create_collection(&collection_name, None, None)
        .await?;

    for path in &paths {
        let file = parse_path(path)?;
        if args.incremental {
            save_incremental(&collection, &file).await?;
        } else {
            save_one_shot(&collection, &file).await?;
        }
        verify_json_equivalent(&collection, &file).await?;
        println!(
            "PASS {} {} entries",
            path.display(),
            file.trajectory.entries.len()
        );
    }

    Ok(Summary {
        collection: collection_name,
        trajectories: paths.len(),
    })
}

/// Successful load summary.
struct Summary {
    collection: String,
    trajectories: usize,
}

/// Save a complete finalized trajectory in one committed transaction.
async fn save_one_shot(
    collection: &ChromaCollection,
    file: &ReasoningTrajectoryFile,
) -> Result<(), Box<dyn Error>> {
    let mut txn = collection.conditional();
    chroma_save_generate_trajectory(&mut txn, file).await?;
    let committed = txn.commit().await?;
    println!(
        "WRITE {} one-shot {} records",
        file.trajectory.id, committed.record_count
    );
    Ok(())
}

/// Save a trajectory by committing the open header, each entry, and finalization.
async fn save_incremental(
    collection: &ChromaCollection,
    file: &ReasoningTrajectoryFile,
) -> Result<(), Box<dyn Error>> {
    let open_file = open_trajectory_skeleton(file);
    let entries = &file.trajectory.entries;

    let mut txn = collection.conditional();
    chroma_create_open_trajectory(&mut txn, &open_file).await?;
    let committed = txn.commit().await?;
    println!(
        "WRITE {} open {} records",
        file.trajectory.id, committed.record_count
    );

    for (index, entry) in entries.iter().enumerate() {
        append_one_entry(collection, file.trajectory.id, index, entry).await?;
    }

    let mut txn = collection.conditional();
    chroma_finalize_open_trajectory(&mut txn, file).await?;
    let committed = txn.commit().await?;
    println!(
        "WRITE {} finalize {} records",
        file.trajectory.id, committed.record_count
    );
    Ok(())
}

/// Build the initial open trajectory shape before terminal metadata exists.
fn open_trajectory_skeleton(file: &ReasoningTrajectoryFile) -> ReasoningTrajectoryFile {
    let mut open_file = file.clone();
    open_file.citations = None;
    open_file.trajectory.entries.clear();
    open_file
}

/// Append exactly one trajectory entry in its own committed transaction.
async fn append_one_entry(
    collection: &ChromaCollection,
    id: uuid::Uuid,
    index: usize,
    entry: &ReasoningEntry,
) -> Result<(), Box<dyn Error>> {
    let mut txn = collection.conditional();
    let next =
        chroma_extend_open_trajectory_at(&mut txn, id, index, std::iter::once(entry)).await?;
    let committed = txn.commit().await?;
    println!(
        "WRITE {id} entry {index} -> {next} {} records",
        committed.record_count
    );
    Ok(())
}

/// Load the finalized trajectory and compare canonical JSON values.
async fn verify_json_equivalent(
    collection: &ChromaCollection,
    expected: &ReasoningTrajectoryFile,
) -> Result<(), Box<dyn Error>> {
    let mut txn = collection.conditional();
    let actual = chroma_load_generate_trajectory(&mut txn, expected.trajectory.id, true).await?;
    let expected_json = serde_json::to_value(expected)?;
    let actual_json = serde_json::to_value(&actual)?;
    if actual_json != expected_json {
        return Err(json_mismatch(expected.trajectory.id, &expected_json, &actual_json).into());
    }
    Ok(())
}

/// Parse one generated trajectory JSON file.
fn parse_path(path: &Path) -> Result<ReasoningTrajectoryFile, Box<dyn Error>> {
    let file = File::open(path).map_err(|err| format!("open {}: {err}", path.display()))?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(|err| format!("parse {}: {err}", path.display()).into())
}

/// Expand explicit file and directory inputs into sorted JSON file paths.
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

/// Recursively collect JSON files from a directory.
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

/// Render a compact mismatch error with pretty expected and actual JSON.
fn json_mismatch(id: uuid::Uuid, expected: &Value, actual: &Value) -> String {
    let expected = serde_json::to_string_pretty(expected)
        .unwrap_or_else(|err| format!("<could not format expected json: {err}>"));
    let actual = serde_json::to_string_pretty(actual)
        .unwrap_or_else(|err| format!("<could not format actual json: {err}>"));
    format!("trajectory {id} readback was not JSON-equivalent\nexpected:\n{expected}\nactual:\n{actual}")
}

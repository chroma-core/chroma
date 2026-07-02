use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use foundation_api::trajectories::GenerateTrajectoryFile;

/// Run the trajectory parser over explicit paths or the default tree.
fn main() -> ExitCode {
    let paths: Vec<PathBuf> = env::args_os().skip(1).map(PathBuf::from).collect();
    if paths.is_empty() {
        return parse_default_generate_tree();
    }

    parse_paths(&paths, true)
}

/// Parse every generated trajectory JSON file found under the default root.
fn parse_default_generate_tree() -> ExitCode {
    let Some(root) = default_generate_root() else {
        eprintln!("FAIL could not find trajectories/generate from the current directory");
        return ExitCode::from(1);
    };

    let mut paths = Vec::new();
    if let Err(err) = collect_generate_json_files(&root, &mut paths) {
        eprintln!("FAIL could not list {}: {err}", root.display());
        return ExitCode::from(1);
    }
    paths.sort();

    let total = paths.len();
    let code = parse_paths(&paths, false);
    if code == ExitCode::SUCCESS {
        println!("PASS {total} generate trajectories");
    }
    code
}

/// Parse a list of files and return a failure exit code if any file fails.
fn parse_paths(paths: &[PathBuf], print_each: bool) -> ExitCode {
    let mut failures = 0usize;
    for path in paths {
        match parse_path(path) {
            Ok(()) => {
                if print_each {
                    println!("PASS {}", path.display());
                }
            }
            Err(err) => {
                failures = failures.saturating_add(1);
                println!("FAIL {}: {err}", path.display());
            }
        }
    }

    if failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Parse one JSON file as a generated trajectory file.
fn parse_path(path: &Path) -> Result<(), String> {
    let file = File::open(path).map_err(|err| format!("open: {err}"))?;
    let reader = BufReader::new(file);
    serde_json::from_reader::<_, GenerateTrajectoryFile>(reader)
        .map(|_| ())
        .map_err(|err| format!("parse: {err}"))
}

/// Find the default generated-trajectory directory from common working roots.
fn default_generate_root() -> Option<PathBuf> {
    [
        PathBuf::from("trajectories/generate"),
        PathBuf::from("../trajectories/generate"),
    ]
    .into_iter()
    .find(|candidate| candidate.is_dir())
}

/// Recursively collect generated trajectory JSON files from a directory.
fn collect_generate_json_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(dir).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| err.to_string())?;
        if file_type.is_dir() {
            collect_generate_json_files(&path, out)?;
        } else if file_type.is_file() && is_trajectory_json(entry.file_name()) {
            out.push(path);
        }
    }
    Ok(())
}

/// Recognize timestamp-like generated trajectory JSON file names.
fn is_trajectory_json(name: OsString) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    name.starts_with('2') && name.ends_with(".json")
}

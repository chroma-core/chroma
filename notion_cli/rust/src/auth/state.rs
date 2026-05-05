//! On-disk credential state: token_v2, file_token, and the pinned
//! workspace id (in `.env`). Mirrors the layout the python script wrote
//! so existing on-disk files keep working across the cutover.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// In-memory mirror of what we eventually serialize to disk. (Currently
/// unused -- the public surface is the per-field `save_*` helpers
/// below; kept around for callers that want to bundle the three writes
/// into one struct later.)
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub token_v2: String,
    pub file_token: Option<String>,
    /// Optional space id to pin in `.env`. None = leave as-is.
    pub space_id: Option<String>,
}

/// Resolve the directory that holds `notion-token-v2.txt` /
/// `notion-file-token.txt`. Mirrors `crate::token::notion_cli_dir` (which
/// is private). We re-derive here so `auth` doesn't have to plumb a path
/// through every public API; the CLI binary always runs against a known
/// repo layout.
pub fn notion_cli_dir() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("NOTION_CLI_DIR") {
        if !p.is_empty() {
            return Ok(PathBuf::from(p));
        }
    }
    if let Some(repo) = find_repo_root() {
        let cli = repo.join("notion_cli");
        if cli.is_dir() {
            return Ok(cli);
        }
    }
    let cwd = std::env::current_dir()?;
    if cwd.file_name().map(|n| n == "notion_cli").unwrap_or(false) {
        return Ok(cwd);
    }
    let direct = cwd.join("notion_cli");
    if direct.is_dir() {
        return Ok(direct);
    }
    // Last resort: write next to the binary's cwd. Better than failing
    // -- the user can always move the file later.
    Ok(cwd)
}

/// Walk up from CWD looking for a directory containing `notion_cli/`.
fn find_repo_root() -> Option<PathBuf> {
    let mut cwd = std::env::current_dir().ok()?;
    for _ in 0..10 {
        if cwd.join("notion_cli").is_dir() {
            return Some(cwd);
        }
        if !cwd.pop() {
            break;
        }
    }
    None
}

/// `<repo_root>/.env`. Used by `save_workspace_pin`.
pub fn env_path() -> Option<PathBuf> {
    find_repo_root().map(|r| r.join(".env"))
}

pub fn token_path() -> Result<PathBuf> {
    Ok(notion_cli_dir()?.join("notion-token-v2.txt"))
}

pub fn file_token_path() -> Result<PathBuf> {
    Ok(notion_cli_dir()?.join("notion-file-token.txt"))
}

/// Write `<value>\n` to `notion-token-v2.txt`. Returns the path.
pub fn save_token_v2(value: &str) -> Result<PathBuf> {
    let path = token_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    std::fs::write(&path, format!("{}\n", value.trim()))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

/// Write `<value>\n` to `notion-file-token.txt`. Returns the path.
pub fn save_file_token(value: &str) -> Result<PathBuf> {
    let path = file_token_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    std::fs::write(&path, format!("{}\n", value.trim()))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

/// Upsert `KEY=value` in `<repo_root>/.env`. Idempotent (rewrites the
/// file with the new value if the key already exists, else appends).
/// No-op if no `.env` location can be located (and no repo_root found,
/// which is unlikely in the deployed layout).
pub fn save_workspace_pin(space_id: &str) -> Result<Option<PathBuf>> {
    let Some(env) = env_path() else {
        return Ok(None);
    };
    persist_env_var(&env, "NOTION_INTERNAL_SPACE_ID", space_id)?;
    Ok(Some(env))
}

fn persist_env_var(env: &Path, key: &str, value: &str) -> Result<()> {
    let mut lines: Vec<String> = if env.exists() {
        std::fs::read_to_string(env)
            .with_context(|| format!("reading {}", env.display()))?
            .lines()
            .map(str::to_string)
            .collect()
    } else {
        Vec::new()
    };
    let new_line = format!("{key}={value}");
    let mut found = false;
    for l in lines.iter_mut() {
        let trimmed = l.trim_start();
        let body = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        if let Some((k, _)) = body.split_once('=') {
            if k.trim() == key {
                *l = new_line.clone();
                found = true;
                break;
            }
        }
    }
    if !found {
        lines.push(new_line);
    }
    let mut joined = lines.join("\n");
    joined.push('\n');
    if let Some(parent) = env.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(env, joined)
        .with_context(|| format!("writing {}", env.display()))?;
    Ok(())
}

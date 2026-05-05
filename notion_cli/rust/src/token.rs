//! Token loading: matches the on-disk locations the Python CLI writes.
//!
//! Resolution order (matches `_saved_token_v2` in `notion_auth.py`
//! plus a search for the `notion_cli/` directory relative to the binary's
//! working directory so users can run from anywhere in the repo):
//!
//! 1. `--token-v2 <hex>` (CLI flag, highest priority)
//! 2. `NOTION_TOKEN_V2` env var
//! 3. The `.env` file at `<repo_root>/.env` -- looked up by walking up from
//!    CWD looking for a directory containing `notion_cli/`
//! 4. `<notion_cli_dir>/notion-token-v2.txt`
//! 5. Fail with a pointer to `./notion_auth.sh login`
//!
//! The `file_token` cookie is similar but only ever lives in the
//! `notion-file-token.txt` file (no env var fallback).

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Tokens {
    pub token_v2: String,
    /// `file_token` cookie scoped to `.notion.so` path `/f`. Required to
    /// download the export zips from `file.notion.so` -- without it the
    /// proxy returns 403 even though `enqueueExportBlock` succeeds.
    pub file_token: Option<String>,
    /// Where the token_v2 came from (for diagnostics).
    pub source: String,
}

impl Tokens {
    /// Build the `Cookie:` header value for an /api/v3 call.
    pub fn cookie_header_for_api(&self) -> String {
        format!("token_v2={}", self.token_v2)
    }

    /// Build the `Cookie:` header value for a `file.notion.so` download.
    pub fn cookie_header_for_file_download(&self) -> String {
        match &self.file_token {
            Some(ft) => format!("token_v2={}; file_token={}", self.token_v2, ft),
            None => format!("token_v2={}", self.token_v2),
        }
    }
}

pub fn load(token_v2_arg: Option<&str>) -> Result<Tokens> {
    if let Some(t) = token_v2_arg.and_then(non_empty) {
        return Ok(Tokens {
            token_v2: t.to_string(),
            file_token: read_file_token().ok().flatten(),
            source: "--token-v2 flag".into(),
        });
    }
    if let Ok(t) = std::env::var("NOTION_TOKEN_V2") {
        if let Some(t) = non_empty(&t).map(str::to_string) {
            return Ok(Tokens {
                token_v2: t,
                file_token: read_file_token().ok().flatten(),
                source: "$NOTION_TOKEN_V2".into(),
            });
        }
    }
    if let Some(t) = read_dotenv_value("NOTION_TOKEN_V2")? {
        return Ok(Tokens {
            token_v2: t,
            file_token: read_file_token().ok().flatten(),
            source: format!(
                ".env at {}",
                find_repo_dotenv()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<missing>".into())
            ),
        });
    }
    if let Some(p) = notion_cli_dir() {
        let token_path = p.join("notion-token-v2.txt");
        if let Some(t) = read_one_line(&token_path)? {
            return Ok(Tokens {
                token_v2: t,
                file_token: read_one_line(&p.join("notion-file-token.txt"))?,
                source: token_path.display().to_string(),
            });
        }
    }
    Err(anyhow!(
        "no token_v2 found. Run `./notion_auth.sh login` (in \
         notion_cli/) to obtain a session, or pass --token-v2 / set \
         NOTION_TOKEN_V2."
    ))
}

pub fn load_space_id(arg: Option<&str>) -> Result<Option<String>> {
    if let Some(s) = arg.and_then(non_empty) {
        return Ok(Some(s.to_string()));
    }
    if let Ok(s) = std::env::var("NOTION_INTERNAL_SPACE_ID") {
        if let Some(s) = non_empty(&s) {
            return Ok(Some(s.to_string()));
        }
    }
    if let Some(s) = read_dotenv_value("NOTION_INTERNAL_SPACE_ID")? {
        return Ok(Some(s));
    }
    Ok(None)
}

fn read_file_token() -> Result<Option<String>> {
    let Some(p) = notion_cli_dir() else {
        return Ok(None);
    };
    read_one_line(&p.join("notion-file-token.txt"))
}

fn non_empty(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn read_one_line(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    for raw in content.lines() {
        let line = raw.trim();
        if !line.is_empty() && !line.starts_with('#') {
            return Ok(Some(line.to_string()));
        }
    }
    Ok(None)
}

/// Walk up from CWD looking for a directory that contains a `notion_cli/`
/// subdirectory or has a `.env` file. Returns the path to that directory.
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

fn find_repo_dotenv() -> Option<PathBuf> {
    find_repo_root().map(|r| r.join(".env"))
}

fn notion_cli_dir() -> Option<PathBuf> {
    // 1. Explicit env override (handy in tests / for split layouts).
    if let Ok(p) = std::env::var("NOTION_CLI_DIR") {
        if !p.is_empty() {
            return Some(PathBuf::from(p));
        }
    }
    // 2. Repo-relative.
    if let Some(repo) = find_repo_root() {
        let cli = repo.join("notion_cli");
        if cli.is_dir() {
            return Some(cli);
        }
    }
    // 3. CWD-relative.
    let cwd = std::env::current_dir().ok()?;
    if cwd.file_name().map(|n| n == "notion_cli").unwrap_or(false) {
        return Some(cwd);
    }
    let direct = cwd.join("notion_cli");
    if direct.is_dir() {
        return Some(direct);
    }
    None
}

fn read_dotenv_value(key: &str) -> Result<Option<String>> {
    let Some(p) = find_repo_dotenv() else {
        return Ok(None);
    };
    if !p.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&p)
        .with_context(|| format!("reading {}", p.display()))?;
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Accept both `FOO=bar` and `export FOO=bar`.
        let body = line.strip_prefix("export ").unwrap_or(line);
        if let Some((k, v)) = body.split_once('=') {
            if k.trim() == key {
                let v = v.trim().trim_matches(|c| c == '"' || c == '\'').to_string();
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }
    }
    Ok(None)
}

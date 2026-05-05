//! Cross-browser cookie store scan via `rookie`.
//!
//! Replaces ~470 LoC of python (`_extract_notion_cookies_*` etc.) with
//! the rookie crate's per-browser readers + a thin wrapper that:
//!
//!   1. calls `rookie::load(Some(vec!["notion".into()]))` to get every
//!      Notion cookie from every browser rookie knows about,
//!   2. groups cookies by browser identity (we don't actually get an
//!      explicit "this is which browser" tag from `load`, so we group
//!      by `(token_v2 value, file_token value)` -- two cookies live in
//!      the same session iff they came from the same browser jar),
//!   3. validates each candidate `token_v2` against `/api/v3/loadUserContent`
//!      (cheap, ~200ms each), drops dead ones,
//!   4. dedups by `user_id` (same notion identity in two browsers ->
//!      keep the one with the most complete cookie set).

use anyhow::Result;
use std::collections::HashMap;

use crate::auth::validate::{validate_token, UserContent};

/// One signed-in Notion identity, as discovered by `scan_all`. The
/// `label` is what we show the user when picking between sessions
/// ("rookie:any_browser" today; we'd be more specific if rookie ever
/// exposed the source-browser name).
#[derive(Debug, Clone)]
pub struct Session {
    pub label: String,
    pub token_v2: String,
    pub file_token: Option<String>,
    pub user: UserContent,
}

/// Scan every browser rookie knows about for Notion cookies, validate
/// each unique `token_v2`, and return the deduped, sorted list of
/// working sessions.
///
/// Sort key: prefer sessions that ALSO carry a `file_token` (so we can
/// download exports without a second cookie scrape later).
///
/// Windows note: when Chrome is running, the latest ~30s of cookie
/// writes may live in the Cookies-wal/-shm/-journal sidecar files.
/// rookie issue #47 reports that `rookie::load` can miss those (and
/// reading the main DB without admin can corrupt it back to old
/// values). On Windows we therefore additionally probe the standard
/// per-browser Cookies paths via `windows_wal_fallback`, copy each
/// `(Cookies, Cookies-wal, Cookies-shm, Cookies-journal)` quad into
/// a temp dir, and re-call `rookie::any_browser` on the snapshot.
pub async fn scan_all(verbose: bool) -> Result<Vec<Session>> {
    let cookies = match rookie::load(Some(vec!["notion".into()])) {
        Ok(c) => c,
        Err(e) => {
            if verbose {
                eprintln!("  scan: rookie::load failed: {e}");
            }
            return Ok(Vec::new());
        }
    };
    if verbose {
        eprintln!("  scan: rookie returned {} notion cookies", cookies.len());
    }
    let mut sessions = sessions_from_cookies(cookies, verbose).await?;
    if cfg!(target_os = "windows") && sessions.is_empty() {
        sessions = windows_wal_fallback(verbose).await?;
    }
    Ok(sessions)
}

/// Same as `scan_all` but pointed at one explicit cookie file (rookie's
/// `any_browser` escape hatch). Used by `--cookie-file <path>` for the
/// long-tail browsers rookie doesn't enumerate by default.
pub async fn scan_cookie_file(path: &std::path::Path, verbose: bool) -> Result<Vec<Session>> {
    // rookie::any_browser takes a PathBuf and an Option<Vec<String>> of
    // domains. We give it the same "notion" filter. If the caller's
    // file is a Firefox-style cookies.sqlite vs a Chromium-style
    // Cookies db, rookie sniffs the schema.
    let path_str = path.to_string_lossy();
    let cookies = match rookie::any_browser(&path_str, Some(vec!["notion".into()]), None) {
        Ok(c) => c,
        Err(e) => {
            if verbose {
                eprintln!("  scan: rookie::any_browser({}) failed: {e}", path.display());
            }
            return Ok(Vec::new());
        }
    };
    if verbose {
        eprintln!(
            "  scan: rookie::any_browser({}) returned {} cookies",
            path.display(),
            cookies.len()
        );
    }
    sessions_from_cookies(cookies, verbose).await
}

/// Windows-only mitigation for rookie issue #47. Locates the standard
/// Cookies paths for Chrome/Edge/Brave/Opera/Vivaldi/Arc, copies each
/// SQLite quad (`Cookies`, `Cookies-wal`, `Cookies-shm`,
/// `Cookies-journal`) into a private temp dir, and asks
/// `rookie::any_browser` to read the snapshot. The temp copy is
/// disposable so we never risk corrupting the live Cookies DB.
#[cfg(target_os = "windows")]
async fn windows_wal_fallback(verbose: bool) -> Result<Vec<Session>> {
    use std::fs;
    use std::path::PathBuf;
    let local = match std::env::var("LOCALAPPDATA") {
        Ok(p) => PathBuf::from(p),
        Err(_) => return Ok(Vec::new()),
    };
    let candidates: Vec<(&'static str, PathBuf)> = vec![
        ("Chrome",   local.join(r"Google\Chrome\User Data\Default\Network\Cookies")),
        ("Edge",     local.join(r"Microsoft\Edge\User Data\Default\Network\Cookies")),
        ("Brave",    local.join(r"BraveSoftware\Brave-Browser\User Data\Default\Network\Cookies")),
        ("Opera",    local.join(r"Opera Software\Opera Stable\Network\Cookies")),
        ("Vivaldi",  local.join(r"Vivaldi\User Data\Default\Network\Cookies")),
        ("Arc",      local.join(r"Packages\TheBrowserCompany.Arc_*\LocalCache\Local\Arc\User Data\Default\Network\Cookies")),
    ];
    let mut acc: Vec<Session> = Vec::new();
    for (name, src) in candidates {
        if !src.exists() {
            continue;
        }
        let scratch = std::env::temp_dir().join(format!("notion-cli-cookie-{}", uuid::Uuid::new_v4()));
        if fs::create_dir_all(&scratch).is_err() {
            continue;
        }
        let dst = scratch.join("Cookies");
        if fs::copy(&src, &dst).is_err() {
            continue;
        }
        for sidecar in ["Cookies-wal", "Cookies-shm", "Cookies-journal"] {
            let sidecar_src = src.with_file_name(sidecar);
            if sidecar_src.exists() {
                let _ = fs::copy(&sidecar_src, scratch.join(sidecar));
            }
        }
        if verbose {
            eprintln!("  scan: WAL fallback ({name}) snapshot at {}", dst.display());
        }
        match scan_cookie_file(&dst, verbose).await {
            Ok(mut sessions) => {
                for s in &mut sessions {
                    s.label = format!("{} (WAL snapshot)", name);
                }
                acc.extend(sessions);
            }
            Err(e) if verbose => {
                eprintln!("  scan: WAL fallback {name} failed: {e}");
            }
            Err(_) => {}
        }
        let _ = fs::remove_dir_all(&scratch);
    }
    Ok(acc)
}

#[cfg(not(target_os = "windows"))]
async fn windows_wal_fallback(_verbose: bool) -> Result<Vec<Session>> {
    Ok(Vec::new())
}

async fn sessions_from_cookies(
    cookies: Vec<rookie::common::enums::Cookie>,
    verbose: bool,
) -> Result<Vec<Session>> {
    // Group by token_v2 value -- one notion identity per unique cookie
    // string. file_token, if present in the same jar, joins it.
    let mut by_token: HashMap<String, (Option<String>, String)> = HashMap::new();
    for c in cookies {
        if c.name == "token_v2" {
            by_token.entry(c.value.clone()).or_insert((None, c.domain.clone()));
        }
    }
    if verbose {
        eprintln!("  scan: {} unique token_v2 candidate(s)", by_token.len());
    }
    if by_token.is_empty() {
        return Ok(Vec::new());
    }
    // Second pass: opportunistically attach file_token cookies. We can't
    // tell which browser a given cookie came from with rookie::load, so
    // we just attach any file_token to every candidate token_v2 -- if
    // they weren't from the same jar Notion will still accept the
    // file_token (it's scoped per-user, not per-session).
    //
    // Refetch (rookie::load consumed the vec).
    let again = rookie::load(Some(vec!["notion".into()])).unwrap_or_default();
    let any_file_token: Option<String> = again
        .iter()
        .find(|c| c.name == "file_token")
        .map(|c| c.value.clone());
    for entry in by_token.values_mut() {
        entry.0 = any_file_token.clone();
    }

    // Validate each candidate. Drop the dead ones, keep the live ones
    // tagged with their loadUserContent result.
    let mut sessions: Vec<Session> = Vec::new();
    for (token_v2, (file_token, _domain)) in by_token {
        match validate_token(&token_v2).await {
            Ok(uc) => sessions.push(Session {
                label: "browser cookie store (rookie)".into(),
                token_v2,
                file_token,
                user: uc,
            }),
            Err(e) => {
                if verbose {
                    eprintln!("  scan: candidate token rejected by /api/v3: {e}");
                }
            }
        }
    }

    // Dedup by user_id (same identity in multiple browsers -> keep the
    // one that brought a file_token). Sort: file_token-bearing first.
    let mut by_user: HashMap<String, Session> = HashMap::new();
    for s in sessions {
        let key = s.user.user_id.clone();
        match by_user.get(&key) {
            Some(prev) => {
                let prev_has_ft = prev.file_token.is_some();
                let curr_has_ft = s.file_token.is_some();
                if curr_has_ft && !prev_has_ft {
                    by_user.insert(key, s);
                }
            }
            None => {
                by_user.insert(key, s);
            }
        }
    }
    let mut out: Vec<Session> = by_user.into_values().collect();
    out.sort_by_key(|s| (s.file_token.is_none(), s.user.user_email.clone()));
    Ok(out)
}

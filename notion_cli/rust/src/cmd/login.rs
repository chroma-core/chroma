//! `login` subcommand: capture a Notion session token (`token_v2`) +
//! the file-download cookie (`file_token`) and write them to the
//! on-disk paths the rest of the CLI already reads from.
//!
//! Umbrella flow (mirrors python `cmd_login`):
//!
//!   1. Validate any token already on disk / in env. If it works, done.
//!      `--force` skips this step.
//!   2. Scan installed browsers via `rookie::load`. Prompt to pick if
//!      multiple identities are signed in. `--no-browser-scan` skips.
//!   3. Launch a managed Chromium-family browser via `chromiumoxide`
//!      with a persistent profile. User signs in, we capture cookies via
//!      CDP. `--paste` picks the manual stdin paste flow instead.
//!
//! Subcommand-style escape hatches:
//!
//!   --paste              skip 1+2+3, run only the stdin paste flow.
//!   --chrome             skip 1+2, jump straight to managed-browser CDP.
//!   --cookie-file PATH   skip 1+3, run rookie::any_browser on PATH.

use anyhow::{Context, Result};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use crate::auth::{
    catalog,
    cdp::{self, CdpOpts},
    paste::run_paste_flow,
    scan::{scan_all, scan_cookie_file, Session},
    state::{save_file_token, save_token_v2, save_workspace_pin},
    validate::{validate_token, UserContent, WorkspaceInfo},
};
use crate::token;

#[derive(clap::Args, Debug, Clone)]
pub struct Args {
    /// Force the umbrella to skip the disk fast-path even if a saved
    /// token would validate.
    #[arg(long)]
    pub force: bool,
    /// Skip the rookie cookie-store scan (avoids the macOS Touch ID /
    /// Linux keyring prompt).
    #[arg(long)]
    pub no_browser_scan: bool,
    /// Don't auto-pick / auto-pin a workspace even when only one is
    /// available. The dump path will fall back to prompting.
    #[arg(long)]
    pub no_pick: bool,
    /// Pin this `space_id` in `.env` if the chosen session has access
    /// to it. Otherwise we'll auto-pin or prompt depending on `--no-pick`.
    #[arg(long)]
    pub space_id: Option<String>,

    /// Bypass the umbrella; just run the stdin paste flow.
    #[arg(long, conflicts_with_all = ["chrome", "cookie_file"])]
    pub paste: bool,
    /// Bypass the umbrella; just run the managed-browser CDP flow.
    #[arg(long, conflicts_with_all = ["paste", "cookie_file"])]
    pub chrome: bool,
    /// Bypass the umbrella; just call rookie::any_browser on this path.
    #[arg(long, conflicts_with_all = ["paste", "chrome"])]
    pub cookie_file: Option<PathBuf>,

    /// Override the chromium binary the CDP flow launches (default:
    /// auto-detect).
    #[arg(long)]
    pub chrome_binary: Option<PathBuf>,
    /// Override the persistent profile dir the CDP flow uses (default:
    /// `~/.cache/notion-cli-chrome`).
    #[arg(long)]
    pub profile_dir: Option<PathBuf>,
    /// CDP poll timeout, seconds (default 300s).
    #[arg(long, default_value_t = 300.0)]
    pub timeout: f64,
    /// Leave the managed browser running after capture (debugging).
    #[arg(long)]
    pub keep_open: bool,

    /// Verbose: print extra detail from the scan + validation steps.
    #[arg(long, short)]
    pub verbose: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let pinned = args.space_id.clone().or_else(|| {
        std::env::var("NOTION_INTERNAL_SPACE_ID")
            .ok()
            .filter(|s| !s.is_empty())
    });

    // Branch out to the explicit flag-driven escape hatches first.
    if args.paste {
        return run_paste_only(&args, pinned.as_deref()).await;
    }
    if args.chrome {
        return run_chrome_only(&args, pinned.as_deref()).await;
    }
    if let Some(p) = args.cookie_file.clone() {
        return run_cookie_file_only(&args, &p, pinned.as_deref()).await;
    }

    // Default umbrella.
    println!();
    println!("=== Notion login ===");
    println!();
    println!("To dump your Notion data we use the same /api/v3 endpoints the web");
    println!("app uses, authenticated by your browser session cookie (`token_v2`).");
    println!("That cookie is HttpOnly so we have to read it from a browser cookie");
    println!("store rather than via JavaScript.");
    println!();
    println!("This flow will, in order:");
    println!("  1. Check for a token already saved by a previous run on this machine.");
    println!("  2. Scan your installed browsers for an active Notion session.");
    println!("  3. If neither works, open a browser window and ask you to sign in.");
    println!();

    // Step 1: disk
    if !args.force {
        if try_saved_token(pinned.as_deref(), args.no_pick).await?.is_some() {
            return Ok(());
        }
    } else {
        println!("Step 1: skipped (--force).");
    }

    // Step 2: rookie cookie scan
    if !args.no_browser_scan {
        if try_browser_scan(&args, pinned.as_deref()).await?.is_some() {
            return Ok(());
        }
    } else {
        println!("Step 2: skipped (--no-browser-scan).");
    }

    // Step 3: managed browser via CDP. If no Chromium-family binary is
    // installed, bail with the long-tail catalogue + paste fallback.
    println!();
    println!("Step 3: opening a managed browser so you can sign in ...");
    let chrome = cdp::find_chromium_binary();
    match chrome {
        Some(chrome) => {
            let profile = cdp::default_profile_dir();
            println!(
                "  found Chromium-family browser: {}",
                short_chrome_label(&chrome)
            );
            println!("  ({})", chrome.display());
            println!();
            println!("  We'll open a fresh window with a managed profile at");
            println!("    {}", profile.display());
            println!("  Sign in to Notion in that window. We'll detect the session over");
            println!("  CDP and close the window automatically.");
            println!();
            run_chrome_only(&args, pinned.as_deref()).await
        }
        None => {
            println!("  no Chromium-family browser found on this machine.");
            println!();
            println!("To finish login, choose one of:");
            println!("  - Install a browser we can drive over CDP (any one is fine):");
            print_install_links();
            println!("    Then re-run `notion-internal-dump login`.");
            println!();
            println!("  - Manual paste: open notion.so in any browser, sign in, copy");
            println!("    `token_v2` from DevTools (Application -> Cookies), and run");
            println!("    `notion-internal-dump login --paste`.");
            println!();
            println!("  - Or point us at a cookie file directly via");
            println!("    `notion-internal-dump login --cookie-file <path>`.");
            catalog::print_for_current_os();
            std::process::exit(5);
        }
    }
}

// -------------------------------------------------------------------------
// Step implementations
// -------------------------------------------------------------------------

/// Returns Some(()) if we successfully validated a saved token (caller
/// short-circuits the rest of the umbrella), None if there was nothing
/// on disk worth trying.
async fn try_saved_token(
    pinned: Option<&str>,
    no_pick: bool,
) -> Result<Option<()>> {
    let existing = match token::load(None) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    println!("Step 1: validating saved token on disk ...");
    match validate_token(&existing.token_v2).await {
        Ok(uc) => {
            println!("  ok, still valid.");
            print_session_summary("saved/env token", &uc);
            maybe_pick_and_save_space(&uc, pinned, no_pick)?;
            println!();
            println!("Already authenticated. Use --force to re-run the login flow.");
            Ok(Some(()))
        }
        Err(e) => {
            println!("  saved/env token didn't validate ({e}); continuing to next step.");
            Ok(None)
        }
    }
}

/// Returns Some(()) if a session was picked + saved, None to fall
/// through to the next step.
async fn try_browser_scan(
    args: &Args,
    pinned: Option<&str>,
) -> Result<Option<()>> {
    println!("Step 2: scanning installed browsers for an existing Notion session ...");
    if cfg!(target_os = "macos") {
        println!("  (macOS may prompt for Touch ID / your password to decrypt some cookie stores.)");
    } else if cfg!(target_os = "linux") {
        println!("  (Linux may prompt for your login keyring password to decrypt some cookie stores.)");
    }
    let sessions = scan_all(args.verbose).await?;
    if sessions.is_empty() {
        println!("  no active Notion session found.");
        return Ok(None);
    }
    pick_session_and_workspace(&sessions, pinned, args.no_pick)?;
    Ok(Some(()))
}

async fn run_chrome_only(args: &Args, pinned: Option<&str>) -> Result<()> {
    let opts = CdpOpts {
        chrome_binary: args.chrome_binary.as_deref(),
        profile_dir: args.profile_dir.as_deref(),
        poll_interval: Duration::from_millis(1500),
        timeout: Duration::from_secs_f64(args.timeout.max(1.0)),
        keep_open: args.keep_open,
    };
    let captured = cdp::run_cdp_login(opts).await?;
    println!(
        "  got token_v2 (len={}), file_token={}",
        captured.token_v2.len(),
        if captured.file_token.is_some() {
            "present"
        } else {
            "MISSING"
        }
    );
    let uc = validate_token(&captured.token_v2)
        .await
        .context("validating captured token_v2")?;
    accept_session(
        &Session {
            label: "chrome (CDP)".into(),
            token_v2: captured.token_v2,
            file_token: captured.file_token,
            user: uc,
        },
        pinned,
        args.no_pick,
    )
}

async fn run_paste_only(args: &Args, pinned: Option<&str>) -> Result<()> {
    let pasted = run_paste_flow(true).await?;
    accept_session(
        &Session {
            label: "manual paste".into(),
            token_v2: pasted.token_v2,
            file_token: None,
            user: pasted.user,
        },
        pinned,
        args.no_pick,
    )
}

async fn run_cookie_file_only(
    args: &Args,
    path: &std::path::Path,
    pinned: Option<&str>,
) -> Result<()> {
    println!();
    println!("=== Notion login (cookie file) ===");
    println!("  path: {}", path.display());
    let sessions = scan_cookie_file(path, args.verbose).await?;
    if sessions.is_empty() {
        println!("  no Notion session found in {}.", path.display());
        catalog::print_for_current_os();
        anyhow::bail!("no signed-in Notion session in cookie file");
    }
    pick_session_and_workspace(&sessions, pinned, args.no_pick)
}

// -------------------------------------------------------------------------
// Workspace picker / persistence
// -------------------------------------------------------------------------

fn pick_session_and_workspace(
    sessions: &[Session],
    pinned: Option<&str>,
    no_pick: bool,
) -> Result<()> {
    if sessions.len() == 1 {
        return accept_session(&sessions[0], pinned, no_pick);
    }
    println!();
    println!(
        "Found {} active Notion sessions across your browsers:",
        sessions.len()
    );
    for s in sessions {
        let nspaces = s.user.spaces.len();
        println!(
            "  - {:30}  ({})  ->  {} workspace(s)",
            s.user.user_email, s.label, nspaces
        );
    }

    if let Some(pin) = pinned {
        for s in sessions {
            if s.user.spaces.contains_key(pin) {
                let name = &s.user.spaces[pin].name;
                println!();
                println!(
                    "  Pinned NOTION_INTERNAL_SPACE_ID={pin} ({name}) matched session {}; using it.",
                    s.user.user_email
                );
                return accept_session(s, Some(pin), no_pick);
            }
        }
        println!(
            "  warning: pinned NOTION_INTERNAL_SPACE_ID={pin} not found in any of these sessions"
        );
    }

    if no_pick || !atty::is(atty::Stream::Stdin) {
        let s = &sessions[0];
        println!(
            "  --no-pick (or no tty): defaulting to {} ({})",
            s.user.user_email, s.label
        );
        return accept_session(s, pinned, no_pick);
    }

    // Flatten to (session, space_id, name) for the picker.
    let mut flat: Vec<(&Session, String, String)> = Vec::new();
    for s in sessions {
        for (sid, sp) in &s.user.spaces {
            flat.push((s, sid.clone(), sp.name.clone()));
        }
    }
    let name_w = flat.iter().map(|(_, _, n)| n.len()).max().unwrap_or(20);
    println!();
    println!("Pick the workspace to dump (token + space will be saved together):");
    for (i, (s, sid, name)) in flat.iter().enumerate() {
        println!(
            "  [{:2}] {:<width$}  ({}, {}) [{}]",
            i + 1,
            name,
            s.user.user_email,
            s.label,
            sid,
            width = name_w
        );
    }
    loop {
        print!("choice (1-{}): ", flat.len());
        io::stdout().flush().ok();
        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err() {
            anyhow::bail!("aborted");
        }
        let n: usize = match line.trim().parse() {
            Ok(n) => n,
            Err(_) => {
                println!("  invalid choice, try again");
                continue;
            }
        };
        if n < 1 || n > flat.len() {
            println!("  invalid choice, try again");
            continue;
        }
        let (chosen, sid, _) = &flat[n - 1];
        return accept_session(chosen, Some(sid.as_str()), no_pick);
    }
}

fn accept_session(s: &Session, pinned: Option<&str>, no_pick: bool) -> Result<()> {
    let path = save_token_v2(&s.token_v2)?;
    println!();
    println!("  saved token_v2 -> {}  (from {})", path.display(), s.label);
    if let Some(ft) = &s.file_token {
        let p = save_file_token(ft)?;
        println!("  saved file_token -> {}  (from {})", p.display(), s.label);
    }
    print_session_summary(&s.label, &s.user);
    maybe_pick_and_save_space(&s.user, pinned, no_pick)?;
    println!();
    println!("Done. Run `notion-internal-dump grab` next");
    println!("(or `notion-internal-dump sync` for incremental updates after the first run).");
    Ok(())
}

fn maybe_pick_and_save_space(
    uc: &UserContent,
    pinned: Option<&str>,
    no_pick: bool,
) -> Result<Option<String>> {
    if uc.spaces.is_empty() {
        return Ok(None);
    }
    if let Some(pin) = pinned {
        if let Some(ws) = uc.spaces.get(pin) {
            if let Some(env) = save_workspace_pin(pin)? {
                println!(
                    "  saved NOTION_INTERNAL_SPACE_ID={pin} ({}) -> {}",
                    ws.name,
                    env.display()
                );
            }
            return Ok(Some(pin.to_string()));
        }
        println!("  warning: --space-id {pin} not in your accessible spaces");
        return Ok(None);
    }
    if no_pick {
        return Ok(None);
    }
    if uc.spaces.len() == 1 {
        let (sid, ws) = uc.spaces.iter().next().unwrap();
        if let Some(env) = save_workspace_pin(sid)? {
            println!(
                "  saved NOTION_INTERNAL_SPACE_ID={sid} ({}) -> {}",
                ws.name,
                env.display()
            );
        }
        return Ok(Some(sid.clone()));
    }
    if !atty::is(atty::Stream::Stdin) {
        return Ok(None);
    }
    println!();
    println!("Multiple workspaces available. Which one will the dump target?");
    let items: Vec<(String, &WorkspaceInfo)> = uc
        .spaces
        .iter()
        .map(|(sid, ws)| (sid.clone(), ws))
        .collect();
    for (i, (sid, ws)) in items.iter().enumerate() {
        println!("  [{}] {}  ({sid})", i + 1, ws.name);
    }
    loop {
        print!("choice (1-{}, or empty to skip): ", items.len());
        io::stdout().flush().ok();
        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err() {
            return Ok(None);
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            println!("  skipped; you'll be prompted at dump time");
            return Ok(None);
        }
        match trimmed.parse::<usize>() {
            Ok(n) if (1..=items.len()).contains(&n) => {
                let (sid, ws) = &items[n - 1];
                if let Some(env) = save_workspace_pin(sid)? {
                    println!(
                        "  saved NOTION_INTERNAL_SPACE_ID={sid} ({}) -> {}",
                        ws.name,
                        env.display()
                    );
                }
                return Ok(Some(sid.clone()));
            }
            _ => println!("  invalid choice, try again"),
        }
    }
}

fn print_session_summary(label: &str, uc: &UserContent) {
    println!();
    println!("  signed in as: {} ({})", uc.user_email, uc.user_id);
    println!("  cookie source: {}", label);
    println!("  workspaces:    {}", uc.spaces.len());
    for (sid, ws) in &uc.spaces {
        println!("    - {:30}  {sid}", format!("'{}'", ws.name));
    }
}

fn print_install_links() {
    if cfg!(target_os = "macos") {
        println!("      Chrome:  https://www.google.com/chrome/");
        println!("      Edge:    https://www.microsoft.com/edge");
        println!("      Brave:   https://brave.com/download/");
    } else if cfg!(target_os = "linux") {
        println!("      Debian/Ubuntu:  sudo apt install chromium-browser");
        println!("      Fedora:         sudo dnf install chromium");
        println!("      Arch:           sudo pacman -S chromium");
        println!("      or Chrome .deb / .rpm from https://www.google.com/chrome/");
    } else if cfg!(target_os = "windows") {
        println!("      Chrome:  https://www.google.com/chrome/");
        println!("      Edge is preinstalled on Windows 10/11 -- did the scanner miss it?");
        println!("      If so, pass --chrome-binary explicitly.");
    }
}

fn short_chrome_label(p: &std::path::Path) -> String {
    let s = p.display().to_string();
    if let Some(idx) = s.find(".app/") {
        if let Some(slash) = s[..idx].rfind('/') {
            return s[slash + 1..idx].to_string();
        }
    }
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or(s)
}

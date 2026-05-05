//! Managed Chromium-family browser via CDP (`chromiumoxide`).
//!
//! Replaces the python `cmd_login_chrome` flow:
//!
//!   - find a Chromium-family binary in the well-known per-OS locations
//!   - launch it with `--user-data-dir=<persistent profile>`,
//!     `--remote-debugging-port=<free>` and the Notion login URL
//!   - poll cookies via the CDP `Network.getAllCookies` request until
//!     `token_v2` appears, then return both `token_v2` and (if present)
//!     `file_token`
//!
//! Subsequent runs reuse the persistent profile dir (`~/.cache/notion-cli-chrome`
//! by default) so the user stays signed in -- the next `login --chrome`
//! finishes in ~3 seconds without prompting.

use anyhow::{anyhow, Context, Result};
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::storage::GetCookiesParams;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CapturedTokens {
    pub token_v2: String,
    pub file_token: Option<String>,
}

pub struct CdpOpts<'a> {
    /// Optional override of the chromium binary. If None, we search the
    /// well-known per-OS install locations.
    pub chrome_binary: Option<&'a Path>,
    /// Persistent user-data-dir. If None, defaults to
    /// `~/.cache/notion-cli-chrome`.
    pub profile_dir: Option<&'a Path>,
    /// Per-poll interval for cookies (default 1.5s).
    pub poll_interval: Duration,
    /// Total timeout (default 300s -- enough for typing+2FA).
    pub timeout: Duration,
    /// If true, leave the browser running after capture for debugging.
    pub keep_open: bool,
}

impl<'a> Default for CdpOpts<'a> {
    fn default() -> Self {
        Self {
            chrome_binary: None,
            profile_dir: None,
            poll_interval: Duration::from_millis(1500),
            timeout: Duration::from_secs(300),
            keep_open: false,
        }
    }
}

pub async fn run_cdp_login(opts: CdpOpts<'_>) -> Result<CapturedTokens> {
    let chrome = match opts.chrome_binary {
        Some(p) => p.to_path_buf(),
        None => find_chromium_binary().ok_or_else(|| {
            anyhow!(
                "no Chromium-family browser found in well-known locations. \
                 Install Chrome / Edge / Brave (or pass --chrome-binary <path>), \
                 or use `login --paste` to paste your token_v2 from DevTools."
            )
        })?,
    };
    let profile_dir = match opts.profile_dir {
        Some(p) => p.to_path_buf(),
        None => default_profile_dir(),
    };
    std::fs::create_dir_all(&profile_dir)
        .with_context(|| format!("mkdir -p {}", profile_dir.display()))?;

    println!();
    println!("=== Notion login (managed Chrome via CDP) ===");
    println!("  binary:  {}", chrome.display());
    println!("  profile: {} (persistent across runs)", profile_dir.display());
    println!();

    // chromiumoxide's BrowserConfig builder takes care of picking a free
    // remote-debugging port + assembling the launch flags. We add a
    // couple of sane defaults (no first-run prompt, no default-browser
    // check) plus the Notion login URL as the initial tab so the user
    // doesn't have to type it.
    let config = BrowserConfig::builder()
        .chrome_executable(&chrome)
        .user_data_dir(&profile_dir)
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--disable-features=Translate")
        .arg("https://www.notion.so/login")
        // Don't run headless -- the whole point is "user signs in".
        .with_head()
        .build()
        .map_err(|e| anyhow!("BrowserConfig::build: {e}"))?;

    let (mut browser, mut handler) = Browser::launch(config)
        .await
        .context("Browser::launch")?;
    let handler_task = tokio::spawn(async move {
        while let Some(_evt) = handler.next().await {
            // Drain the event stream; we don't care about events, just
            // need to keep the connection alive.
        }
    });

    println!(
        "polling cookies every {:.1}s (timeout {:.0}s) ...",
        opts.poll_interval.as_secs_f64(),
        opts.timeout.as_secs_f64()
    );
    let deadline = Instant::now() + opts.timeout;
    let mut last_status = String::new();
    let captured = loop {
        if Instant::now() >= deadline {
            cleanup(&mut browser, opts.keep_open).await;
            handler_task.abort();
            return Err(anyhow!("timed out waiting for sign-in"));
        }
        match browser
            .execute(GetCookiesParams::default())
            .await
        {
            Ok(resp) => {
                let mut t2: Option<String> = None;
                let mut ft: Option<String> = None;
                let mut notion_names: Vec<String> = Vec::new();
                for c in resp.result.cookies.iter() {
                    if !c.domain.contains("notion") {
                        continue;
                    }
                    notion_names.push(c.name.clone());
                    if c.name == "token_v2" {
                        t2 = Some(c.value.clone());
                    } else if c.name == "file_token" {
                        ft = Some(c.value.clone());
                    }
                }
                if let Some(token) = t2 {
                    break CapturedTokens {
                        token_v2: token,
                        file_token: ft,
                    };
                }
                let status = if notion_names.is_empty() {
                    "  no notion cookies yet; waiting for sign-in...".to_string()
                } else {
                    format!(
                        "  {} notion cookie(s); waiting for token_v2: {:?}",
                        notion_names.len(),
                        notion_names
                    )
                };
                if status != last_status {
                    println!("{status}");
                    last_status = status;
                }
            }
            Err(e) => {
                println!("  cdp error: {e}; retrying...");
            }
        }
        tokio::time::sleep(opts.poll_interval).await;
    };

    cleanup(&mut browser, opts.keep_open).await;
    handler_task.abort();
    Ok(captured)
}

async fn cleanup(browser: &mut Browser, keep_open: bool) {
    if keep_open {
        println!("  (browser left running; quit it manually when done.)");
        return;
    }
    if let Err(e) = browser.close().await {
        eprintln!("  (warning: browser.close: {e})");
    }
    let _ = browser.wait().await;
}

/// Persistent profile dir. Stays the same across runs so the user only
/// has to sign in once.
pub fn default_profile_dir() -> PathBuf {
    if let Some(cache) = dirs::cache_dir() {
        cache.join("notion-cli-chrome")
    } else {
        std::env::temp_dir().join("notion-cli-chrome")
    }
}

/// Probe well-known Chromium-family install locations on each OS, in
/// roughly the order most users want (stable Chrome first, weirder
/// variants last).
pub fn find_chromium_binary() -> Option<PathBuf> {
    for cand in candidates() {
        let p = PathBuf::from(cand);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn candidates() -> Vec<&'static str> {
    if cfg!(target_os = "macos") {
        vec![
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta",
            "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            "/Applications/Microsoft Edge Beta.app/Contents/MacOS/Microsoft Edge Beta",
            "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
            "/Applications/Brave Browser Beta.app/Contents/MacOS/Brave Browser Beta",
            "/Applications/Brave Browser Nightly.app/Contents/MacOS/Brave Browser Nightly",
            "/Applications/Arc.app/Contents/MacOS/Arc",
            "/Applications/Atlas.app/Contents/MacOS/Atlas",
            "/Applications/Dia.app/Contents/MacOS/Dia",
            "/Applications/Vivaldi.app/Contents/MacOS/Vivaldi",
            "/Applications/Opera.app/Contents/MacOS/Opera",
            "/Applications/Opera GX.app/Contents/MacOS/Opera",
            "/Applications/Yandex.app/Contents/MacOS/Yandex",
            "/Applications/DuckDuckGo.app/Contents/MacOS/DuckDuckGo",
            "/Applications/Sidekick.app/Contents/MacOS/Sidekick",
            "/Applications/Comet.app/Contents/MacOS/Comet",
            "/Applications/Wavebox.app/Contents/MacOS/Wavebox",
        ]
    } else if cfg!(target_os = "linux") {
        vec![
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome-beta",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/microsoft-edge",
            "/usr/bin/microsoft-edge-stable",
            "/usr/bin/brave-browser",
            "/usr/bin/brave",
            "/usr/bin/vivaldi",
            "/usr/bin/opera",
            "/snap/bin/chromium",
            "/snap/bin/google-chrome",
            "/snap/bin/brave",
        ]
    } else if cfg!(target_os = "windows") {
        vec![
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
            r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
            r"C:\Program Files\Vivaldi\Application\vivaldi.exe",
            r"C:\Program Files\Opera\opera.exe",
        ]
    } else {
        Vec::new()
    }
}

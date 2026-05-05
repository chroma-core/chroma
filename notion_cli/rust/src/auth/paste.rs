//! Manual paste fallback. Used when:
//!
//!   - the user explicitly passes `--paste`,
//!   - or the umbrella `login` couldn't find anything via disk / scan /
//!     CDP and is degrading to "tell us what's in your clipboard".
//!
//! We auto-open `https://www.notion.so/` in the user's default browser
//! so they can pull `token_v2` from DevTools without us having to print
//! the long instruction block. If the open fails (headless box, weird
//! display setup), we just print the URL and instructions.

use anyhow::{anyhow, Context, Result};
use std::io::{self, BufRead, Write};

use crate::auth::validate::{validate_token, UserContent};

#[derive(Debug, Clone)]
pub struct PastedSession {
    pub token_v2: String,
    pub user: UserContent,
}

pub async fn run_paste_flow(open_browser: bool) -> Result<PastedSession> {
    println!();
    println!("=== Notion login (manual paste) ===");
    println!();
    println!("Steps:");
    println!("  1. Open https://www.notion.so/ and sign in (we'll open it for you).");
    println!("  2. DevTools (Cmd-Opt-I / Ctrl-Shift-I)");
    println!("       -> Application -> Storage -> Cookies -> https://www.notion.so");
    println!("  3. Copy the Value column for the cookie named `token_v2`");
    println!("       (a long opaque blob -- not the literal text 'token_v2').");
    println!("  4. Paste it below and hit return.");
    println!();
    if open_browser {
        if let Err(e) = webbrowser::open("https://www.notion.so/") {
            eprintln!("  (couldn't open browser automatically: {e})");
        }
    }

    print!("token_v2: ");
    io::stdout().flush().ok();
    let mut line = String::new();
    let stdin = io::stdin();
    stdin
        .lock()
        .read_line(&mut line)
        .context("reading token_v2 from stdin")?;
    let token = line.trim().to_string();
    if token.is_empty() {
        return Err(anyhow!("no token pasted; aborting"));
    }
    let user = validate_token(&token).await?;
    Ok(PastedSession {
        token_v2: token,
        user,
    })
}

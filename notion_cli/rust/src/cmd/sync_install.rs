//! `sync-install` / `sync-uninstall`: per-OS scheduler entry that runs
//! `notion-internal-dump sync` periodically.
//!
//! macOS  -> launchd user agent at ~/Library/LaunchAgents/com.chroma.notion-sync.plist
//! Linux  -> systemd user units at ~/.config/systemd/user/notion-sync.{service,timer}
//! Windows -> Task Scheduler task `ChromaNotionSync` via `schtasks /create`
//!
//! All paths and labels are stable so `sync-uninstall` can safely remove the
//! same things even from a different binary location.

use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;

use crate::DEFAULT_SYNC_INTERVAL_S;

const LAUNCHD_LABEL: &str = "com.chroma.notion-sync";
const SYSTEMD_UNIT_BASE: &str = "notion-sync";
const WINDOWS_TASK_NAME: &str = "ChromaNotionSync";

#[derive(clap::Args, Debug, Clone)]
pub struct InstallArgs {
    /// Output directory the scheduled `sync` should write to. Becomes part of
    /// the scheduler's command line, so changing this requires
    /// `sync-uninstall` then `sync-install` again.
    #[arg(long, default_value = "./notion-internal-dump")]
    pub output: PathBuf,

    /// Seconds between syncs.
    #[arg(long, default_value_t = DEFAULT_SYNC_INTERVAL_S)]
    pub interval: u64,

    /// Pass through to `sync` as `--space-id`. Optional; falls back to
    /// NOTION_INTERNAL_SPACE_ID resolution at run time.
    #[arg(long)]
    pub space_id: Option<String>,

    /// Path to the binary to schedule. Defaults to the running binary.
    #[arg(long)]
    pub binary: Option<PathBuf>,
}

#[derive(clap::Args, Debug, Clone)]
pub struct UninstallArgs {}

pub async fn run_install(a: InstallArgs) -> Result<()> {
    let bin = a
        .binary
        .clone()
        .map(Ok)
        .unwrap_or_else(|| std::env::current_exe().context("locating current exe"))?;
    let output = a.output.canonicalize().unwrap_or(a.output);

    if cfg!(target_os = "macos") {
        install_launchd(&bin, &output, a.interval, a.space_id.as_deref())
    } else if cfg!(target_os = "linux") {
        install_systemd_user(&bin, &output, a.interval, a.space_id.as_deref())
    } else if cfg!(target_os = "windows") {
        install_windows(&bin, &output, a.interval, a.space_id.as_deref())
    } else {
        Err(anyhow!("unsupported OS for sync-install"))
    }
}

pub async fn run_uninstall(_a: UninstallArgs) -> Result<()> {
    if cfg!(target_os = "macos") {
        uninstall_launchd()
    } else if cfg!(target_os = "linux") {
        uninstall_systemd_user()
    } else if cfg!(target_os = "windows") {
        uninstall_windows()
    } else {
        Err(anyhow!("unsupported OS for sync-uninstall"))
    }
}

// ---------- macOS / launchd ----------

fn launchd_plist_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("no $HOME"))?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

fn launchd_log_paths() -> Result<(PathBuf, PathBuf)> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("no $HOME"))?;
    let logs = home.join("Library").join("Logs");
    Ok((
        logs.join("notion-sync.out.log"),
        logs.join("notion-sync.err.log"),
    ))
}

fn install_launchd(
    bin: &PathBuf,
    output: &PathBuf,
    interval: u64,
    space_id: Option<&str>,
) -> Result<()> {
    let plist_path = launchd_plist_path()?;
    let (out_log, err_log) = launchd_log_paths()?;
    if let Some(p) = plist_path.parent() {
        std::fs::create_dir_all(p).ok();
    }
    if let Some(p) = out_log.parent() {
        std::fs::create_dir_all(p).ok();
    }

    let mut args_xml = String::new();
    args_xml.push_str(&format!("        <string>{}</string>\n", xml_escape(&bin.display().to_string())));
    args_xml.push_str("        <string>sync</string>\n");
    args_xml.push_str(&format!("        <string>--output</string>\n        <string>{}</string>\n", xml_escape(&output.display().to_string())));
    if let Some(s) = space_id {
        args_xml.push_str(&format!("        <string>--space-id</string>\n        <string>{}</string>\n", xml_escape(s)));
    }

    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{args}    </array>
    <key>StartInterval</key>
    <integer>{interval}</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{out_log}</string>
    <key>StandardErrorPath</key>
    <string>{err_log}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NOTION_LOG</key>
        <string>notion_internal_dump=info</string>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
    <key>KeepAlive</key>
    <false/>
</dict>
</plist>
"#,
        label = LAUNCHD_LABEL,
        args = args_xml,
        out_log = xml_escape(&out_log.display().to_string()),
        err_log = xml_escape(&err_log.display().to_string()),
    );
    std::fs::write(&plist_path, plist)
        .with_context(|| format!("writing {}", plist_path.display()))?;

    // Reload (unload then load) so changes take effect immediately.
    let _ = std::process::Command::new("launchctl")
        .args(["unload", &plist_path.display().to_string()])
        .output();
    let load = std::process::Command::new("launchctl")
        .args(["load", &plist_path.display().to_string()])
        .output()
        .context("launchctl load")?;
    if !load.status.success() {
        eprintln!(
            "launchctl load failed: {}",
            String::from_utf8_lossy(&load.stderr)
        );
    }

    println!("installed launchd agent: {}", plist_path.display());
    println!("  binary:    {}", bin.display());
    println!("  output:    {}", output.display());
    println!("  interval:  {}s", interval);
    println!("  stdout:    {}", out_log.display());
    println!("  stderr:    {}", err_log.display());
    println!("inspect: launchctl list | grep notion-sync");
    Ok(())
}

fn uninstall_launchd() -> Result<()> {
    let plist_path = launchd_plist_path()?;
    if !plist_path.exists() {
        println!("nothing to remove (no plist at {})", plist_path.display());
        return Ok(());
    }
    let _ = std::process::Command::new("launchctl")
        .args(["unload", &plist_path.display().to_string()])
        .output();
    std::fs::remove_file(&plist_path)
        .with_context(|| format!("removing {}", plist_path.display()))?;
    println!("removed: {}", plist_path.display());
    Ok(())
}

// ---------- Linux / systemd user ----------

fn systemd_user_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("no $HOME"))?;
    Ok(home.join(".config").join("systemd").join("user"))
}

fn install_systemd_user(
    bin: &PathBuf,
    output: &PathBuf,
    interval: u64,
    space_id: Option<&str>,
) -> Result<()> {
    let dir = systemd_user_dir()?;
    std::fs::create_dir_all(&dir)?;
    let svc = dir.join(format!("{SYSTEMD_UNIT_BASE}.service"));
    let timer = dir.join(format!("{SYSTEMD_UNIT_BASE}.timer"));

    let mut exec = format!(
        "{} sync --output {}",
        shell_escape(&bin.display().to_string()),
        shell_escape(&output.display().to_string()),
    );
    if let Some(s) = space_id {
        exec.push_str(&format!(" --space-id {}", shell_escape(s)));
    }

    let svc_body = format!(
        "[Unit]\n\
         Description=Chroma Notion incremental sync\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         Type=oneshot\n\
         ExecStart={exec}\n\
         Environment=NOTION_LOG=notion_internal_dump=info\n\
         "
    );
    let timer_body = format!(
        "[Unit]\n\
         Description=Run Chroma Notion sync every {interval}s\n\
         \n\
         [Timer]\n\
         OnBootSec=2min\n\
         OnUnitActiveSec={interval}s\n\
         Persistent=true\n\
         Unit={SYSTEMD_UNIT_BASE}.service\n\
         \n\
         [Install]\n\
         WantedBy=timers.target\n\
         "
    );
    std::fs::write(&svc, svc_body)?;
    std::fs::write(&timer, timer_body)?;

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    let st = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", &format!("{SYSTEMD_UNIT_BASE}.timer")])
        .status();
    match st {
        Ok(s) if s.success() => {
            println!("installed + enabled systemd user timer:");
            println!("  service: {}", svc.display());
            println!("  timer:   {}", timer.display());
            println!("  interval: {}s", interval);
            println!("inspect: systemctl --user list-timers | grep notion-sync");
        }
        _ => {
            println!("wrote unit files but `systemctl --user enable --now` failed.");
            println!("Run it yourself once your user systemd is up:");
            println!(
                "  systemctl --user daemon-reload && systemctl --user enable --now {SYSTEMD_UNIT_BASE}.timer"
            );
        }
    }
    Ok(())
}

fn uninstall_systemd_user() -> Result<()> {
    let dir = systemd_user_dir()?;
    let svc = dir.join(format!("{SYSTEMD_UNIT_BASE}.service"));
    let timer = dir.join(format!("{SYSTEMD_UNIT_BASE}.timer"));
    let _ = std::process::Command::new("systemctl")
        .args([
            "--user",
            "disable",
            "--now",
            &format!("{SYSTEMD_UNIT_BASE}.timer"),
        ])
        .status();
    let mut removed: Vec<String> = Vec::new();
    for p in [&timer, &svc] {
        if p.exists() {
            std::fs::remove_file(p).with_context(|| format!("removing {}", p.display()))?;
            removed.push(p.display().to_string());
        }
    }
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    if removed.is_empty() {
        println!("nothing to remove (no unit files in {})", dir.display());
    } else {
        for r in removed {
            println!("removed: {r}");
        }
    }
    Ok(())
}

// ---------- Windows / Task Scheduler ----------

fn install_windows(
    bin: &PathBuf,
    output: &PathBuf,
    interval: u64,
    space_id: Option<&str>,
) -> Result<()> {
    let interval_min = ((interval as f64) / 60.0).round().max(1.0) as u64;
    let mut tr = format!(
        "\"{}\" sync --output \"{}\"",
        bin.display(),
        output.display()
    );
    if let Some(s) = space_id {
        tr.push_str(&format!(" --space-id \"{}\"", s));
    }
    let st = std::process::Command::new("schtasks")
        .args([
            "/Create",
            "/TN",
            WINDOWS_TASK_NAME,
            "/TR",
            &tr,
            "/SC",
            "MINUTE",
            "/MO",
            &interval_min.to_string(),
            "/F",
        ])
        .status()
        .context("schtasks /Create")?;
    if !st.success() {
        anyhow::bail!("schtasks /Create exited with {st}");
    }
    println!("installed Task Scheduler task: {WINDOWS_TASK_NAME}");
    println!("  command:  {tr}");
    println!("  interval: {interval_min} min");
    println!("inspect:  schtasks /Query /TN {WINDOWS_TASK_NAME} /V /FO LIST");
    Ok(())
}

fn uninstall_windows() -> Result<()> {
    let st = std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", WINDOWS_TASK_NAME, "/F"])
        .status()
        .context("schtasks /Delete")?;
    if !st.success() {
        anyhow::bail!("schtasks /Delete exited with {st}");
    }
    println!("removed Task Scheduler task: {WINDOWS_TASK_NAME}");
    Ok(())
}

// ---------- helpers ----------

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

fn shell_escape(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '.' | ':')) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

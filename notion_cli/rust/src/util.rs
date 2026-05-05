//! Path & string helpers shared across modules.

use std::path::{Path, PathBuf};

/// Mirror of the Python `safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in label)`
/// used to build per-container directory names. Identical output is required so
/// the Rust binary keeps using the same `exports/<slug>__<id>/` directories
/// that the Python script populated.
pub fn slugify(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for c in label.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

pub fn container_dir_name(label: &str, container_id: &str) -> String {
    let safe = slugify(label);
    let safe = if safe.is_empty() { container_id.to_string() } else { safe };
    // Python truncates label to 40 chars before slugifying; replicate that.
    let safe: String = safe.chars().take(40).collect();
    format!("{safe}__{container_id}")
}

/// Atomic write: write to a sibling tempfile then rename into place. Used for
/// state files that must never be observed half-written by a concurrent reader.
pub fn write_atomic<P: AsRef<Path>>(path: P, bytes: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp: PathBuf = parent.join(format!(
        ".{}.tmp-{pid}-{nanos}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("out")
    ));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub fn truncate_str(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

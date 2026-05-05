//! `export_one`: enqueue → poll → download → unzip for a single container.
//!
//! Mirrors the Python `_export_one` helper. Returns a structured result so
//! `cmd_dump` can both print a summary line and feed the changelog/Merkle
//! pipeline downstream.

use anyhow::{Context, Result};
use futures::StreamExt;
use reqwest::header::{COOKIE, USER_AGENT};
use reqwest::header::{HeaderMap, HeaderValue};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use super::client::{NotionInternal, USER_AGENT_STR};
use super::rate_limit::{default_initial_backoff_s, RateLimitedError, MAX_BACKOFF_S};
use super::task_pool::TaskPool;
use super::types::Container;
use crate::util::{container_dir_name, truncate_str};

#[derive(Debug, Clone)]
pub struct ExportResult {
    pub container: Container,
    pub ok: bool,
    pub phase: Option<String>,
    pub task_id: Option<String>,
    pub zip_path: Option<PathBuf>,
    pub unzipped_dir: Option<PathBuf>,
    pub bytes: u64,
    pub pages_exported: u64,
    pub elapsed_s: f64,
    pub error: Option<String>,
    /// `None` for a normal full-container export. `Some(block_id)` when this
    /// was a subtree export rooted at that block.
    pub subtree_block_id: Option<String>,
    /// For subtree exports: the count of pages in the subtree, used in the
    /// per-container progress line.
    pub subtree_page_count: Option<usize>,
}

/// Lower-level building block: enqueue an export for an arbitrary block id,
/// poll via the shared task pool, download the ZIP into `dest_dir/zip_name`,
/// optionally extract into `dest_dir/unzip_subdir`. Used by both
/// `export_one` (full container) and `export_subtree` (LCA-rooted re-export).
async fn export_block_to_dir(
    client: &NotionInternal,
    block_id: &str,
    block_label: &str,
    space_id: &str,
    dest_dir: &Path,
    zip_name: &str,
    unzip_subdir: Option<&str>,
    task_pool: &TaskPool,
    task_timeout_s: f64,
    include_files: &str,
) -> std::result::Result<RawExport, RawExportFailure> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs_f64(task_timeout_s);

    let task_id = match enqueue_with_backoff_for_block(
        client,
        block_id,
        block_label,
        space_id,
        include_files,
        deadline,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            return Err(RawExportFailure {
                phase: "enqueue".into(),
                task_id: None,
                error: e.to_string(),
                elapsed_s: started.elapsed().as_secs_f64(),
            });
        }
    };

    let notify = task_pool.register(&task_id);
    let now = Instant::now();
    let remaining = deadline.saturating_duration_since(now);
    let completed = tokio::time::timeout(remaining, notify.notified()).await.is_ok();
    let final_state = task_pool.status(&task_id);
    let state = final_state
        .as_ref()
        .map(|s| s.state.as_str())
        .unwrap_or("in_progress");
    let status = final_state
        .as_ref()
        .map(|s| s.status.clone())
        .unwrap_or(serde_json::Value::Null);
    let pages_exported = status
        .get("pagesExported")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .unwrap_or(0);
    if !completed {
        return Err(RawExportFailure {
            phase: "timeout".into(),
            task_id: Some(task_id),
            error: format!("task did not finish in {task_timeout_s:.0}s (state={state})"),
            elapsed_s: started.elapsed().as_secs_f64(),
        });
    }
    if state == "failure" {
        return Err(RawExportFailure {
            phase: "task".into(),
            task_id: Some(task_id),
            error: serde_json::to_string(&status).unwrap_or_default(),
            elapsed_s: started.elapsed().as_secs_f64(),
        });
    }
    let export_url = match status.get("exportURL").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return Err(RawExportFailure {
                phase: "no_url".into(),
                task_id: Some(task_id),
                error: format!(
                    "task succeeded but no exportURL (status={})",
                    serde_json::to_string(&status).unwrap_or_default()
                ),
                elapsed_s: started.elapsed().as_secs_f64(),
            });
        }
    };

    if let Err(e) = tokio::fs::create_dir_all(dest_dir).await {
        return Err(RawExportFailure {
            phase: "mkdir".into(),
            task_id: Some(task_id),
            error: format!("creating {}: {e}", dest_dir.display()),
            elapsed_s: started.elapsed().as_secs_f64(),
        });
    }
    let zip_path = dest_dir.join(zip_name);
    let bytes = match download(client, &export_url, &zip_path).await {
        Ok(n) => n,
        Err(e) => {
            return Err(RawExportFailure {
                phase: "download".into(),
                task_id: Some(task_id),
                error: e.to_string(),
                elapsed_s: started.elapsed().as_secs_f64(),
            });
        }
    };

    let mut unzipped_dir: Option<PathBuf> = None;
    let mut unzip_err: Option<String> = None;
    if let Some(subdir) = unzip_subdir {
        let target = dest_dir.join(subdir);
        let tmp = dest_dir.join(format!(".{subdir}.tmp"));
        let _ = tokio::fs::remove_dir_all(&tmp).await;
        let res = tokio::task::spawn_blocking({
            let zip_path = zip_path.clone();
            let tmp = tmp.clone();
            move || -> std::io::Result<()> {
                let f = std::fs::File::open(&zip_path)?;
                let mut archive = zip::ZipArchive::new(f)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                std::fs::create_dir_all(&tmp)?;
                archive
                    .extract(&tmp)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                Ok(())
            }
        })
        .await;
        match res {
            Ok(Ok(())) => {
                let _ = tokio::fs::remove_dir_all(&target).await;
                if let Err(e) = tokio::fs::rename(&tmp, &target).await {
                    unzip_err =
                        Some(format!("rename {} -> {}: {e}", tmp.display(), target.display()));
                } else {
                    unzipped_dir = Some(target);
                }
            }
            Ok(Err(e)) => unzip_err = Some(format!("extract: {e}")),
            Err(e) => unzip_err = Some(format!("join: {e}")),
        }
    }

    Ok(RawExport {
        task_id,
        zip_path,
        unzipped_dir,
        bytes,
        pages_exported,
        unzip_err,
        elapsed_s: started.elapsed().as_secs_f64(),
    })
}

#[derive(Debug, Clone)]
pub struct RawExport {
    pub task_id: String,
    pub zip_path: PathBuf,
    pub unzipped_dir: Option<PathBuf>,
    pub bytes: u64,
    pub pages_exported: u64,
    pub unzip_err: Option<String>,
    pub elapsed_s: f64,
}

#[derive(Debug, Clone)]
pub struct RawExportFailure {
    pub phase: String,
    pub task_id: Option<String>,
    pub error: String,
    pub elapsed_s: f64,
}

/// Subtree export: enqueue any block id, dump the ZIP under
/// `<container_dir>/.subtree.tmp/`, unzip alongside it. Caller is responsible
/// for stitching the result into the container's `unzipped/` tree and
/// cleaning the temp dir.
pub async fn export_subtree(
    client: &NotionInternal,
    container: Container,
    block_id: String,
    block_label: String,
    space_id: &str,
    output_root: &Path,
    task_pool: &TaskPool,
    task_timeout_s: f64,
    include_files: &str,
    subtree_page_count: usize,
) -> ExportResult {
    let container_dir = output_root
        .join("exports")
        .join(crate::util::container_dir_name(
            if container.title.is_empty() {
                &container.id
            } else {
                &container.title
            },
            &container.id,
        ));
    let temp_dir = container_dir.join(".subtree.tmp");
    // Best-effort wipe of any leftover from a previous interrupted run.
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    let started_total = Instant::now();
    match export_block_to_dir(
        client,
        &block_id,
        &block_label,
        space_id,
        &temp_dir,
        "subtree.zip",
        Some("unzipped"),
        task_pool,
        task_timeout_s,
        include_files,
    )
    .await
    {
        Ok(raw) => ExportResult {
            container,
            ok: true,
            phase: raw.unzip_err.as_ref().map(|_| "unzip_failed".into()),
            task_id: Some(raw.task_id),
            zip_path: Some(raw.zip_path),
            unzipped_dir: raw.unzipped_dir,
            bytes: raw.bytes,
            pages_exported: raw.pages_exported,
            elapsed_s: raw.elapsed_s,
            error: raw.unzip_err,
            subtree_block_id: Some(block_id),
            subtree_page_count: Some(subtree_page_count),
        },
        Err(f) => ExportResult {
            container,
            ok: false,
            phase: Some(f.phase),
            task_id: f.task_id,
            zip_path: None,
            unzipped_dir: None,
            bytes: 0,
            pages_exported: 0,
            elapsed_s: started_total.elapsed().as_secs_f64().max(f.elapsed_s),
            error: Some(f.error),
            subtree_block_id: Some(block_id),
            subtree_page_count: Some(subtree_page_count),
        },
    }
}

async fn enqueue_with_backoff_for_block(
    client: &NotionInternal,
    block_id: &str,
    _label: &str,
    space_id: &str,
    include_files: &str,
    deadline: Instant,
) -> Result<String> {
    let mut backoff = default_initial_backoff_s();
    loop {
        match client
            .enqueue_export_block(block_id, space_id, include_files)
            .await
        {
            Ok(id) => return Ok(id),
            Err(e) => {
                if let Some(rl) = e.downcast_ref::<RateLimitedError>() {
                    if Instant::now() >= deadline {
                        return Err(e.context("enqueue: timed out under rate limiting"));
                    }
                    let sleep_for = backoff.max(rl.retry_after).min(MAX_BACKOFF_S);
                    tokio::time::sleep(Duration::from_secs_f64(sleep_for)).await;
                    backoff = (backoff * 2.0).min(MAX_BACKOFF_S);
                    continue;
                }
                return Err(e);
            }
        }
    }
}

pub async fn export_one(
    client: &NotionInternal,
    container: Container,
    space_id: &str,
    output_root: &Path,
    task_pool: &TaskPool,
    task_timeout_s: f64,
    include_files: &str,
    unzip: bool,
) -> ExportResult {
    let label = truncate_str(
        if container.title.is_empty() {
            &container.id
        } else {
            &container.title
        },
        40,
    );
    let dest_dir = output_root
        .join("exports")
        .join(container_dir_name(&label, &container.id));

    let started_total = Instant::now();
    match export_block_to_dir(
        client,
        &container.id,
        &label,
        space_id,
        &dest_dir,
        "export.zip",
        if unzip { Some("unzipped") } else { None },
        task_pool,
        task_timeout_s,
        include_files,
    )
    .await
    {
        Ok(raw) => ExportResult {
            container,
            ok: true,
            phase: raw.unzip_err.as_ref().map(|_| "unzip_failed".into()),
            task_id: Some(raw.task_id),
            zip_path: Some(raw.zip_path),
            unzipped_dir: raw.unzipped_dir,
            bytes: raw.bytes,
            pages_exported: raw.pages_exported,
            elapsed_s: raw.elapsed_s,
            error: raw.unzip_err,
            subtree_block_id: None,
            subtree_page_count: None,
        },
        Err(f) => ExportResult {
            container,
            ok: false,
            phase: Some(f.phase),
            task_id: f.task_id,
            zip_path: None,
            unzipped_dir: None,
            bytes: 0,
            pages_exported: 0,
            elapsed_s: started_total.elapsed().as_secs_f64().max(f.elapsed_s),
            error: Some(f.error),
            subtree_block_id: None,
            subtree_page_count: None,
        },
    }
}

async fn download(
    client: &NotionInternal,
    url: &str,
    dest: &Path,
) -> Result<u64> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_STR));
    let cookie = client.tokens.cookie_header_for_file_download();
    headers.insert(COOKIE, HeaderValue::from_str(&cookie).context("cookie header")?);
    let resp = client
        .http
        .get(url)
        .headers(headers)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("download HTTP {} for {}", resp.status(), url);
    }
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let mut f = File::create(dest)
        .await
        .with_context(|| format!("creating {}", dest.display()))?;
    let mut total: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("download chunk")?;
        total += chunk.len() as u64;
        f.write_all(&chunk).await.context("write chunk")?;
    }
    f.flush().await.ok();
    Ok(total)
}

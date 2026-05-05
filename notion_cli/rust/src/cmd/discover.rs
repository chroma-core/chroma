//! `discover`: walk /api/v3/search, write sidebar.jsonl + discovery.jsonl.
//! Same on-disk shape as the Python `cmd_discover` (so the two binaries are
//! interchangeable).

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::path::PathBuf;

use crate::api::{
    search::{block_title, derive_sidebar, search_all},
    NotionInternal, RateLimitGate, TokenBucket,
};
use crate::sync::state::pages_from_search;
use crate::token;
use crate::util::write_atomic;
use crate::{DEFAULT_MAX_PAGES, DEFAULT_OUTPUT, DEFAULT_PAGE_SIZE, DEFAULT_RPS};

#[derive(clap::Args, Debug, Clone)]
pub struct Args {
    #[arg(long)]
    pub token_v2: Option<String>,
    #[arg(long)]
    pub space_id: Option<String>,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: PathBuf,
    #[arg(long, default_value_t = DEFAULT_RPS)]
    pub rps: f64,
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub page_size: u32,
    #[arg(long, default_value_t = DEFAULT_MAX_PAGES)]
    pub max_pages: u32,
}

pub async fn run(args: Args) -> Result<()> {
    let tokens = token::load(args.token_v2.as_deref())?;
    println!("auth: token_v2 source = {}", tokens.source);
    if tokens.file_token.is_some() {
        println!("auth: file_token loaded (downloads will work)");
    } else {
        println!(
            "auth: WARNING no file_token on disk -- enqueueing exports will \
             succeed but downloads from file.notion.so will return HTTP 403."
        );
    }
    let space_id = token::load_space_id(args.space_id.as_deref())?
        .ok_or_else(|| {
            anyhow!(
                "no space_id. Pass --space-id, set NOTION_INTERNAL_SPACE_ID, \
                 or run `notion-internal-dump login` which writes the pinned \
                 space to .env."
            )
        })?;

    let bucket = TokenBucket::new(args.rps);
    let gate = RateLimitGate::new();
    let client = NotionInternal::new(tokens, bucket, gate)?;
    println!("space:  {}", space_id);

    println!(
        "--- /api/v3/search (page_size {}, max_pages {}) ---",
        args.page_size, args.max_pages
    );
    let t0 = std::time::Instant::now();
    let (blocks, teams) = search_all(&client, &space_id, args.page_size, args.max_pages, true)
        .await
        .context("search_all")?;
    let dt = t0.elapsed().as_secs_f64();
    println!("blocks discovered: {}", blocks.len());
    println!("teams discovered:  {}", teams.len());
    println!("discovery wall:    {:.1}s", dt);

    let sidebar = derive_sidebar(&blocks, &teams, &space_id);
    let n_private = sidebar.iter().filter(|s| s.kind == "space_page").count();
    let n_team = sidebar.iter().filter(|s| s.kind == "teamspace_page").count();
    let mut teamspace_names: std::collections::BTreeSet<String> = Default::default();
    for s in &sidebar {
        if let Some(n) = &s.teamspace_name {
            teamspace_names.insert(n.clone());
        }
    }
    println!();
    println!(
        "sidebar (top-level containers): {}  (space_page={}  teamspace_page={})",
        sidebar.len(),
        n_private,
        n_team
    );
    if !teamspace_names.is_empty() {
        println!(
            "teamspaces represented: {}",
            teamspace_names.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    tokio::fs::create_dir_all(&args.output).await?;
    let sidebar_path = args.output.join("sidebar.jsonl");
    let mut buf: Vec<u8> = Vec::new();
    for s in &sidebar {
        let mut v = serde_json::to_value(s).unwrap();
        if let Some(map) = v.as_object_mut() {
            map.insert("space_id".into(), Value::String(space_id.clone()));
        }
        let line = serde_json::to_string(&v)?;
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    write_atomic(&sidebar_path, &buf)?;

    let discovery_path = args.output.join("discovery.jsonl");
    let curr_pages = pages_from_search(&blocks, &space_id);
    let mut buf: Vec<u8> = Vec::new();
    for (_id, b) in &blocks {
        let title = block_title(b);
        let v = serde_json::json!({
            "id": b.get("id").and_then(Value::as_str).unwrap_or(""),
            "title": title,
            "type": b.get("type").and_then(Value::as_str),
            "parent_table": b.get("parent_table").and_then(Value::as_str),
            "parent_id": b.get("parent_id").and_then(Value::as_str),
            "last_edited_time": b.get("last_edited_time").and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))),
            "space_id": space_id.clone(),
        });
        let line = serde_json::to_string(&v)?;
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    write_atomic(&discovery_path, &buf)?;

    println!("wrote: {}", sidebar_path.display());
    println!("wrote: {} ({} pages)", discovery_path.display(), curr_pages.len());
    Ok(())
}

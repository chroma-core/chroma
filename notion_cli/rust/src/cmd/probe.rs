//! `probe`: hit each /api/v3 endpoint we use and dump shape + samples to
//! `probe.*.json`. Diagnostic when something has shifted server-side and
//! the dump path is failing in a way that smells server-side
//! (schema drift, perms change, etc.).
//!
//! Mirrors python `cmd_probe`.

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::path::PathBuf;

use crate::api::{NotionInternal, RateLimitGate, TokenBucket};
use crate::token;
use crate::DEFAULT_RPS;

#[derive(clap::Args, Debug, Clone)]
pub struct Args {
    #[arg(long)]
    pub token_v2: Option<String>,
    #[arg(long)]
    pub space_id: Option<String>,
    #[arg(long, default_value = "./notion-internal-probe")]
    pub output: PathBuf,
    #[arg(long, default_value_t = DEFAULT_RPS)]
    pub rps: f64,
}

pub async fn run(args: Args) -> Result<()> {
    let tokens = token::load(args.token_v2.as_deref()).map_err(|e| {
        anyhow!(
            "{e}\n\n  Run `notion-internal-dump login` first to capture a session."
        )
    })?;
    let bucket = TokenBucket::new(args.rps);
    let gate = RateLimitGate::new();
    let client = NotionInternal::new(tokens, bucket, gate)?;

    let space_id = match token::load_space_id(args.space_id.as_deref())? {
        Some(s) => s,
        None => resolve_space_from_user_content(&client).await?,
    };

    std::fs::create_dir_all(&args.output)
        .with_context(|| format!("mkdir -p {}", args.output.display()))?;
    let save = |name: &str, obj: &Value| -> Result<()> {
        let p = args.output.join(format!("probe.{name}.json"));
        let serialized = serde_json::to_string_pretty(obj)?;
        std::fs::write(&p, serialized)
            .with_context(|| format!("writing {}", p.display()))?;
        println!("  wrote {}", p.display());
        Ok(())
    };

    println!("=== loadUserContent ===");
    match client.load_user_content().await {
        Ok(uc) => {
            save("loadUserContent", &uc)?;
            if let Some(rmap) = uc.get("recordMap").and_then(Value::as_object) {
                let mut keys: Vec<&String> = rmap.keys().collect();
                keys.sort();
                for table in keys {
                    let n = rmap
                        .get(table)
                        .and_then(Value::as_object)
                        .map(|o| o.len())
                        .unwrap_or(0);
                    let ids: Vec<&str> = rmap
                        .get(table)
                        .and_then(Value::as_object)
                        .map(|o| o.keys().take(2).map(String::as_str).collect())
                        .unwrap_or_default();
                    println!("  recordMap.{table}: {n} record(s)  e.g. {ids:?}");
                }
            }
        }
        Err(e) => println!("  ERROR: {e}"),
    }

    println!();
    println!("=== getSpaces ===");
    match client.get_spaces().await {
        Ok(gs) => {
            save("getSpaces", &gs)?;
            if let Some(obj) = gs.as_object() {
                for (k, v) in obj {
                    if let Some(rm) = v.get("recordMap").and_then(Value::as_object) {
                        let mut keys: Vec<&String> = rm.keys().collect();
                        keys.sort();
                        println!("  spaces[{k}].recordMap tables: {keys:?}");
                    } else if k == "recordMap" {
                        if let Some(rm) = v.as_object() {
                            let mut keys: Vec<&String> = rm.keys().collect();
                            keys.sort();
                            println!("  recordMap tables: {keys:?}");
                        }
                    }
                }
            }
        }
        Err(e) => println!("  ERROR: {e}"),
    }

    println!();
    println!("=== syncRecordValues space:{space_id} ===");
    match client
        .sync_record_values(vec![serde_json::json!({
            "pointer": { "table": "space", "id": space_id },
            "version": -1
        })])
        .await
    {
        Ok(sr) => {
            save("syncRecordValues_space", &sr)?;
            if let Some(rmap) = sr.get("recordMap").and_then(Value::as_object) {
                let mut keys: Vec<&String> = rmap.keys().collect();
                keys.sort();
                for table in &keys {
                    let n = rmap
                        .get(*table)
                        .and_then(Value::as_object)
                        .map(|o| o.len())
                        .unwrap_or(0);
                    println!("  recordMap.{table}: {n} record(s)");
                }
                if let Some(space) = rmap
                    .get("space")
                    .and_then(Value::as_object)
                    .and_then(|o| o.get(&space_id))
                {
                    let inner = space.get("value").unwrap_or(space);
                    let inner = inner.get("value").unwrap_or(inner);
                    let name = inner.get("name").and_then(Value::as_str).unwrap_or("?");
                    let pages = inner
                        .get("pages")
                        .and_then(Value::as_array)
                        .map(|a| a.len())
                        .unwrap_or(0);
                    let teams = inner
                        .get("teams")
                        .and_then(Value::as_array)
                        .map(|a| a.len())
                        .unwrap_or(0);
                    println!("  space.name = {name:?}");
                    println!("  space.pages = {pages}");
                    println!("  space.teams = {teams}");
                }
            }
        }
        Err(e) => println!("  ERROR: {e}"),
    }

    println!();
    println!("=== search variants ===");
    for variant in ["minimal", "legacy"] {
        println!("  variant={variant}");
        match client.search(&space_id, "", 5, variant).await {
            Ok(r) => {
                save(&format!("search_{variant}"), &r)?;
                let nresults = r
                    .get("results")
                    .and_then(Value::as_array)
                    .map(|a| a.len())
                    .unwrap_or(0);
                let nblocks = r
                    .get("recordMap")
                    .and_then(|m| m.get("block"))
                    .and_then(Value::as_object)
                    .map(|o| o.len())
                    .unwrap_or(0);
                println!("    OK results={nresults}  recordMap.block={nblocks}");
            }
            Err(e) => println!("    ERROR: {e}"),
        }
    }
    Ok(())
}

/// If neither `--space-id` nor `NOTION_INTERNAL_SPACE_ID` is set, fall
/// back to the first reachable space from `loadUserContent`. Mirrors
/// python `_resolve_space(None)` for the probe path.
async fn resolve_space_from_user_content(client: &NotionInternal) -> Result<String> {
    let uc = client.load_user_content().await?;
    let parsed = crate::auth::validate::parse_user_content(&uc)?;
    parsed
        .spaces
        .keys()
        .next()
        .cloned()
        .ok_or_else(|| anyhow!("no spaces in loadUserContent; can't probe"))
}

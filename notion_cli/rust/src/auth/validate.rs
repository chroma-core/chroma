//! Token validation: does this `token_v2` actually authenticate against
//! `/api/v3/loadUserContent`? If yes, return a small typed view of the
//! user identity + the workspaces (`spaces`) they have access to.

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::api::{NotionInternal, RateLimitGate, TokenBucket};
use crate::token::Tokens;

/// What the user sees printed after a successful login. Identity +
/// reachable workspaces.
#[derive(Debug, Clone)]
pub struct UserContent {
    pub user_id: String,
    pub user_email: String,
    /// Insertion order matches Notion's `recordMap` order (we sort by name
    /// for display, but keep the original here for the "single workspace?
    /// auto-pin" path).
    pub spaces: BTreeMap<String, WorkspaceInfo>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceInfo {
    /// Same value as the `BTreeMap` key in `UserContent::spaces`. Kept
    /// for API completeness so callers can pass a `&WorkspaceInfo`
    /// around without losing the id.
    #[allow(dead_code)]
    pub id: String,
    pub name: String,
}

/// Hit `loadUserContent` with `token_v2`. Returns the parsed identity +
/// spaces if the token authenticates. Errors otherwise (could be expired,
/// could be a network failure, could be a malformed cookie -- caller
/// distinguishes by message text only).
pub async fn validate_token(token_v2: &str) -> Result<UserContent> {
    let tokens = Tokens {
        token_v2: token_v2.to_string(),
        file_token: None,
        source: "auth::validate".into(),
    };
    // High RPS bucket -- this is a one-shot validation, no need to throttle.
    let bucket = TokenBucket::new(10.0);
    let gate = RateLimitGate::new();
    let client = NotionInternal::new(tokens, bucket, gate)?;
    let v = client
        .load_user_content()
        .await
        .context("POST /api/v3/loadUserContent")?;
    parse_user_content(&v)
}

pub fn parse_user_content(v: &Value) -> Result<UserContent> {
    let rmap = v
        .get("recordMap")
        .ok_or_else(|| anyhow!("loadUserContent response missing recordMap"))?;
    let users = walk_record_map(rmap, "notion_user");
    let (user_id, user_email) = users
        .into_iter()
        .next()
        .map(|(id, val)| {
            let email = val
                .get("email")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            (id, email)
        })
        .unwrap_or_else(|| ("?".into(), "?".into()));
    let spaces_raw = walk_record_map(rmap, "space");
    let mut spaces = BTreeMap::new();
    for (id, val) in spaces_raw {
        let name = val
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("(untitled)")
            .to_string();
        spaces.insert(id.clone(), WorkspaceInfo { id, name });
    }
    Ok(UserContent {
        user_id,
        user_email,
        spaces,
    })
}

/// Pull the inner `value` of every record in `recordMap.<table>`.
/// Notion's recordMap entries are wrapped in `{"role": ..., "value": ...}`;
/// the value sometimes has another inner `value` field as well. Mirrors
/// `_walk_record_map` in the python script.
fn walk_record_map(rmap: &Value, table: &str) -> Vec<(String, Value)> {
    let Some(table_obj) = rmap.get(table).and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (id, wrapper) in table_obj {
        let mut v = wrapper
            .get("value")
            .cloned()
            .unwrap_or_else(|| wrapper.clone());
        if let Some(inner) = v.get("value").cloned() {
            if inner.is_object() {
                v = inner;
            }
        }
        out.push((id.clone(), v));
    }
    out
}

//! Idempotent tenant bootstrap through the data-plane HTTP API.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::{Client, StatusCode, Url};
use serde_json::json;

use crate::config::Config;

/// Create the configured tenant and initial database. Existing resources are
/// verified with GET after a conflict; other failures stop the installation.
pub async fn install(config: &Config, endpoint: &str) -> Result<()> {
    let base = Url::parse(endpoint).context("Invalid frontend URL")?;
    if !matches!(base.scheme(), "http" | "https")
        || base.host_str().is_none()
        || base.path() != "/"
        || base.query().is_some()
        || base.fragment().is_some()
        || !base.username().is_empty()
        || base.password().is_some()
    {
        bail!(
            "Frontend URL must be an HTTP(S) origin without credentials, path, query, or fragment"
        );
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let tenant_url = base.join(&format!("api/v2/tenants/{}", config.tenant))?;
    create(
        &client,
        &config.api_keys[0],
        base.join("api/v2/tenants")?,
        tenant_url,
        &config.tenant,
        "tenant",
    )
    .await?;
    let databases_url = base.join(&format!("api/v2/tenants/{}/databases", config.tenant))?;
    let database_url = base.join(&format!(
        "api/v2/tenants/{}/databases/{}",
        config.tenant, config.database
    ))?;
    create(
        &client,
        &config.api_keys[0],
        databases_url,
        database_url,
        &config.database,
        "database",
    )
    .await?;
    Ok(())
}

async fn create(
    client: &Client,
    key: &str,
    collection: Url,
    resource: Url,
    name: &str,
    kind: &str,
) -> Result<()> {
    let response = client
        .post(collection)
        .header("x-chroma-token", key)
        .json(&json!({"name": name}))
        .send()
        .await
        .context("Data-plane create request failed")?;
    if response.status().is_success() {
        return Ok(());
    }
    if response.status() == StatusCode::CONFLICT {
        let existing = client
            .get(resource)
            .header("x-chroma-token", key)
            .send()
            .await
            .context("Cannot verify existing resource")?;
        if existing.status().is_success() {
            let body: serde_json::Value = existing
                .json()
                .await
                .context("Invalid existing resource response")?;
            if body.get("name").and_then(|v| v.as_str()) == Some(name) {
                return Ok(());
            }
        }
        bail!("Could not verify existing {kind}");
    }
    bail!("Creating {kind} failed with HTTP {}", response.status());
}

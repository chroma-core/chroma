//! OAuth protected-resource discovery for the Foundation MCP endpoint.
//!
//! Serves the protected-resource metadata document and derives the public URLs
//! (the resource identifier and the authorization server) advertised to MCP
//! clients during OAuth discovery.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

use crate::{config::FoundationApiConfig, server::FoundationApiServer};

use super::{FOUNDATION_SCOPE, MCP_SCOPED_PATH};
use crate::routes::{
    whoami::{validate_foundation_name, validate_path_tenant},
    FoundationScope,
};

#[derive(Debug, Serialize)]
pub(super) struct ProtectedResourceMetadata {
    resource: String,
    authorization_servers: Vec<String>,
    scopes_supported: Vec<String>,
}

/// Discovery for an explicitly named default Foundation. The tenant and name
/// are validated before they are placed in the advertised resource URL.
pub(super) async fn explicit_protected_resource_metadata(
    State(server): State<FoundationApiServer>,
    Path(scope): Path<FoundationScope>,
) -> Result<Json<ProtectedResourceMetadata>, StatusCode> {
    let tenant = scope.tenant;
    let foundation = scope.foundation;
    validate_path_tenant(&tenant).map_err(|_| StatusCode::BAD_REQUEST)?;
    validate_foundation_name(&foundation).map_err(|_| StatusCode::BAD_REQUEST)?;
    if foundation != server.config.foundation.database_name {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(ProtectedResourceMetadata {
        resource: explicit_mcp_resource_url(&server.config, &tenant, &foundation),
        authorization_servers: vec![mcp_authorization_server_url(&server.config)],
        scopes_supported: vec![FOUNDATION_SCOPE.to_string()],
    }))
}

pub(super) fn explicit_mcp_resource_url(
    config: &FoundationApiConfig,
    tenant: &str,
    foundation: &str,
) -> String {
    format!(
        "{}{path}",
        mcp_resource_origin(config),
        path = MCP_SCOPED_PATH
            .replace("{tenant}", tenant)
            .replace("{foundation}", foundation)
    )
}

/// The public origin (`scheme://host[:port]`) this service is reachable at,
/// from the configured `api_public_origin`. Used to build both the MCP resource
/// URL and the OAuth metadata URL.
pub(super) fn mcp_resource_origin(config: &FoundationApiConfig) -> String {
    if let Some(public_origin) = &config.foundation.api_public_origin {
        return match reqwest::Url::parse(public_origin) {
            Ok(url) => url.origin().ascii_serialization(),
            Err(_) => public_origin.trim_end_matches('/').to_string(),
        };
    }

    let host = match config.base.listen_address.as_str() {
        "0.0.0.0" | "::" => "localhost",
        host => host,
    };
    format!("http://{}:{}", host, config.base.port)
}

/// The OAuth authorization server URL advertised in the protected-resource
/// metadata, from the configured `mcp_authorization_server_url`.
fn mcp_authorization_server_url(config: &FoundationApiConfig) -> String {
    config
        .foundation
        .mcp_authorization_server_url
        .clone()
        .unwrap_or_else(|| "http://localhost:8002".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a config with the two MCP-relevant fields set, leaving the rest at
    /// their defaults (`listen_address = "0.0.0.0"`, `port = 8000`).
    fn config_with(public_origin: Option<&str>, auth_server: Option<&str>) -> FoundationApiConfig {
        let mut config = FoundationApiConfig::default();
        config.foundation.api_public_origin = public_origin.map(str::to_string);
        config.foundation.mcp_authorization_server_url = auth_server.map(str::to_string);
        config
    }

    #[test]
    fn resource_origin_prefers_configured_public_origin() {
        let config = config_with(Some("https://foundation.trychroma.com"), None);
        assert_eq!(
            mcp_resource_origin(&config),
            "https://foundation.trychroma.com"
        );
    }

    #[test]
    fn resource_origin_falls_back_to_listen_address() {
        // Default config binds 0.0.0.0:8000, which is advertised as localhost.
        let config = config_with(None, None);
        assert_eq!(mcp_resource_origin(&config), "http://localhost:8000");
    }

    #[test]
    fn authorization_server_url_uses_configured_value() {
        let config = config_with(None, Some("https://dashboard.trychroma.com"));
        assert_eq!(
            mcp_authorization_server_url(&config),
            "https://dashboard.trychroma.com"
        );
    }
}

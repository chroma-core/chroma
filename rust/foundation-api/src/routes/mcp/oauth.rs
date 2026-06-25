//! OAuth protected-resource discovery for the Foundation MCP endpoint.
//!
//! Serves the protected-resource metadata document and derives the public URLs
//! (the resource identifier and the authorization server) advertised to MCP
//! clients during OAuth discovery.

use axum::{extract::State, Json};
use serde::Serialize;

use crate::{config::FoundationApiConfig, server::FoundationApiServer};

use super::{FOUNDATION_SCOPE, MCP_PATH};

#[derive(Debug, Serialize)]
pub(super) struct ProtectedResourceMetadata {
    resource: String,
    authorization_servers: Vec<String>,
    scopes_supported: Vec<String>,
}

pub(super) async fn protected_resource_metadata(
    State(server): State<FoundationApiServer>,
) -> Json<ProtectedResourceMetadata> {
    Json(protected_resource_metadata_doc(&server.config))
}

/// Builds the protected-resource metadata document advertised at
/// `PROTECTED_RESOURCE_METADATA_PATH`. Pure (config in, document out) so it is
/// unit-testable without standing up a server.
fn protected_resource_metadata_doc(config: &FoundationApiConfig) -> ProtectedResourceMetadata {
    ProtectedResourceMetadata {
        resource: mcp_resource_url(config),
        authorization_servers: vec![mcp_authorization_server_url(config)],
        scopes_supported: vec![FOUNDATION_SCOPE.to_string()],
    }
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

/// The MCP resource URL advertised as the protected resource identifier.
fn mcp_resource_url(config: &FoundationApiConfig) -> String {
    format!("{}{}", mcp_resource_origin(config), MCP_PATH)
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
    fn resource_url_appends_mcp_path_to_origin() {
        let config = config_with(Some("https://foundation.trychroma.com"), None);
        assert_eq!(
            mcp_resource_url(&config),
            "https://foundation.trychroma.com/mcp/foundation"
        );
    }

    #[test]
    fn authorization_server_url_uses_configured_value() {
        let config = config_with(None, Some("https://dashboard.trychroma.com"));
        assert_eq!(
            mcp_authorization_server_url(&config),
            "https://dashboard.trychroma.com"
        );
    }

    #[test]
    fn protected_resource_metadata_advertises_resource_auth_server_and_scope() {
        let config = config_with(
            Some("https://foundation.trychroma.com"),
            Some("https://dashboard.trychroma.com"),
        );

        let doc = protected_resource_metadata_doc(&config);

        assert_eq!(
            doc.resource,
            "https://foundation.trychroma.com/mcp/foundation"
        );
        assert_eq!(
            doc.authorization_servers,
            vec!["https://dashboard.trychroma.com".to_string()]
        );
        assert_eq!(doc.scopes_supported, vec![FOUNDATION_SCOPE.to_string()]);
    }
}

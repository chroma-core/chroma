use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        request::Parts,
        HeaderMap, HeaderValue, Request, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{any, get},
    Json, Router,
};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    RoleServer, ServerHandler,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower::ServiceExt;
use validator::Validate;

use crate::{
    auth::AuthzAction,
    routes::{
        agent::{default_model, default_system_prompt, run_agent_to_final_text, AgentRequest},
        search::{run_search, SearchRequest, SearchResponseBody},
        whoami::whoami_and_authorize,
        CHROMA_TOKEN_HEADER,
    },
    server::FoundationApiServer,
};

const MCP_PATH: &str = "/mcp/foundation";
const PROTECTED_RESOURCE_METADATA_PATH: &str =
    "/.well-known/oauth-protected-resource/mcp/foundation";
const FOUNDATION_SCOPE: &str = "foundation";

pub(crate) fn router() -> Router<FoundationApiServer> {
    Router::new()
        .route(
            PROTECTED_RESOURCE_METADATA_PATH,
            get(protected_resource_metadata),
        )
        .route(MCP_PATH, any(handle_mcp))
}

#[derive(Debug, Serialize)]
struct ProtectedResourceMetadata {
    resource: String,
    authorization_servers: Vec<String>,
    scopes_supported: Vec<String>,
}

async fn protected_resource_metadata(
    State(server): State<FoundationApiServer>,
) -> Json<ProtectedResourceMetadata> {
    Json(ProtectedResourceMetadata {
        resource: mcp_resource_url(&server),
        authorization_servers: vec![mcp_authorization_server_url(&server)],
        scopes_supported: vec![FOUNDATION_SCOPE.to_string()],
    })
}

async fn handle_mcp(
    State(server): State<FoundationApiServer>,
    mut request: Request<Body>,
) -> Response {
    let Some(token) = bearer_token(request.headers()).map(str::to_string) else {
        return mcp_unauthorized(&server);
    };

    let Ok(value) = HeaderValue::from_str(&token) else {
        return mcp_unauthorized(&server);
    };

    request.headers_mut().insert(CHROMA_TOKEN_HEADER, value);

    let service_server = server.clone();
    let service = StreamableHttpService::new(
        move || Ok(FoundationMcpServer::new(service_server.clone())),
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig::default()
            .disable_allowed_hosts()
            .with_stateful_mode(false)
            .with_json_response(true),
    );

    match service.oneshot(request).await {
        Ok(response) => response.map(Body::new),
        Err(never) => match never {},
    }
}

fn mcp_unauthorized(server: &FoundationApiServer) -> Response {
    let metadata_url = format!(
        "{}{}",
        mcp_resource_origin(server),
        PROTECTED_RESOURCE_METADATA_PATH
    );
    (
        StatusCode::UNAUTHORIZED,
        [(
            WWW_AUTHENTICATE,
            format!("Bearer resource_metadata=\"{metadata_url}\""),
        )],
        Json(json!({
            "jsonrpc": "2.0",
            "error": { "code": -32000, "message": "Unauthorized" },
            "id": null
        })),
    )
        .into_response()
}

#[derive(Clone)]
struct FoundationMcpServer {
    server: FoundationApiServer,
    tool_router: ToolRouter<Self>,
}

impl FoundationMcpServer {
    fn new(server: FoundationApiServer) -> Self {
        Self {
            server,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct AskFoundationParams {
    #[schemars(description = "Question to ask the selected Chroma Foundation knowledge base.")]
    query: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchParams {
    #[schemars(description = "Search query for the selected Chroma Foundation knowledge base.")]
    query: String,
    #[schemars(description = "Maximum number of hits to return. Defaults to 10.")]
    limit: Option<u32>,
}

#[tool_router]
impl FoundationMcpServer {
    #[tool(
        name = "ask_foundation",
        description = "Ask the selected Chroma Foundation knowledge base."
    )]
    async fn ask_foundation(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<AskFoundationParams>,
    ) -> CallToolResult {
        let headers = match request_headers(&ctx) {
            Ok(headers) => headers,
            Err(message) => return CallToolResult::error(vec![Content::text(message)]),
        };
        let identity =
            match whoami_and_authorize(&*self.server.auth, &headers, AuthzAction::ViewFoundation)
                .await
            {
                Ok(identity) => identity,
                Err(_) => {
                    return CallToolResult::error(vec![Content::text(
                        "Foundation access is no longer available.",
                    )]);
                }
            };

        let request = AgentRequest {
            input: params.query,
            model: default_model(),
            system: default_system_prompt(),
        };
        let _guard = match self.server.scorecard_request(&[
            "op:foundation_mcp_ask",
            &format!("tenant:{}", identity.tenant),
        ]) {
            Ok(guard) => guard,
            Err(err) => return CallToolResult::error(vec![Content::text(err.to_string())]),
        };

        match run_agent_to_final_text(&self.server, &headers, &identity.tenant, &request).await {
            Ok(answer) => CallToolResult::success(vec![Content::text(answer)]),
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }

    #[tool(
        name = "search",
        description = "Search the selected Chroma Foundation knowledge base."
    )]
    async fn search(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SearchParams>,
    ) -> CallToolResult {
        let headers = match request_headers(&ctx) {
            Ok(headers) => headers,
            Err(message) => return CallToolResult::error(vec![Content::text(message)]),
        };
        let identity =
            match whoami_and_authorize(&*self.server.auth, &headers, AuthzAction::ViewFoundation)
                .await
            {
                Ok(identity) => identity,
                Err(_) => {
                    return CallToolResult::error(vec![Content::text(
                        "Foundation access is no longer available.",
                    )]);
                }
            };

        let request = SearchRequest {
            query: params.query,
            limit: params
                .limit
                .unwrap_or_else(crate::routes::search::default_limit),
        };
        if let Err(err) = request.validate() {
            return CallToolResult::error(vec![Content::text(err.to_string())]);
        }

        let _guard = match self.server.scorecard_request(&[
            "op:foundation_mcp_search",
            &format!("tenant:{}", identity.tenant),
        ]) {
            Ok(guard) => guard,
            Err(err) => return CallToolResult::error(vec![Content::text(err.to_string())]),
        };

        match run_search(&self.server, &headers, &identity.tenant, &request).await {
            Ok(hits) => {
                let body = SearchResponseBody { hits };
                match serde_json::to_value(body) {
                    Ok(value) => CallToolResult::structured(value),
                    Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
                }
            }
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FoundationMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Search and ask questions over Chroma Foundation.")
    }
}

fn request_headers(ctx: &RequestContext<RoleServer>) -> Result<HeaderMap, String> {
    let parts = ctx
        .extensions
        .get::<Parts>()
        .ok_or_else(|| "missing HTTP request context".to_string())?;
    let token = parts
        .headers
        .get(CHROMA_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| "missing bearer token".to_string())?;
    let value = HeaderValue::from_str(token).map_err(|_| "invalid bearer token".to_string())?;
    let mut headers = HeaderMap::new();
    headers.insert(CHROMA_TOKEN_HEADER, value);
    Ok(headers)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

fn mcp_resource_url(server: &FoundationApiServer) -> String {
    if let Some(public_origin) = &server.config.foundation.api_public_origin {
        let origin = match reqwest::Url::parse(public_origin) {
            Ok(url) => url.origin().ascii_serialization(),
            Err(_) => public_origin.trim_end_matches('/').to_string(),
        };
        return format!("{}{}", origin, MCP_PATH);
    }

    let host = match server.config.base.listen_address.as_str() {
        "0.0.0.0" | "::" => "localhost",
        host => host,
    };
    format!("http://{}:{}{}", host, server.config.base.port, MCP_PATH)
}

fn mcp_resource_origin(server: &FoundationApiServer) -> String {
    let resource = mcp_resource_url(server);
    match reqwest::Url::parse(&resource) {
        Ok(url) => url.origin().ascii_serialization(),
        Err(_) => resource.trim_end_matches(MCP_PATH).to_string(),
    }
}

fn mcp_authorization_server_url(server: &FoundationApiServer) -> String {
    server
        .config
        .foundation
        .mcp_authorization_server_url
        .clone()
        .unwrap_or_else(|| "http://localhost:8002".to_string())
}

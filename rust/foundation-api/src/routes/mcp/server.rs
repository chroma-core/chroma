//! The Foundation MCP server: the [`FoundationMcpServer`] handler plus its
//! `ask_foundation` / `search_pages` / `read_page` tools and server
//! instructions.

use axum::http::{request::Parts, HeaderMap};
use mdac::ScorecardGuard;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Icon, Implementation, ServerCapabilities, ServerInfo},
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router, RoleServer, ServerHandler,
};
use serde::Deserialize;
use validator::Validate;

use crate::{
    auth::AuthzAction,
    routes::{
        agent::{default_model, default_system_prompt, run_agent_to_final_text, AgentRequest},
        links::page_link_instructions,
        read_page::{run_read_page, ReadPageRequest},
        search::{run_page_search, PageSearchResponseBody, SearchRequest},
        whoami::whoami_and_authorize,
        CHROMA_TOKEN_HEADER,
    },
    server::FoundationApiServer,
};

use super::{MCP_SERVER_ICON_URL, MCP_SERVER_NAME, MCP_SERVER_VERSION};

#[derive(Clone)]
pub(super) struct FoundationMcpServer {
    server: FoundationApiServer,
    tool_router: ToolRouter<Self>,
}

impl FoundationMcpServer {
    pub(super) fn new(server: FoundationApiServer) -> Self {
        Self {
            server,
            tool_router: Self::tool_router(),
        }
    }

    /// Shared prelude for every MCP tool: lift the caller's token out of the
    /// request context, authorize it for `ViewFoundation`, and open a
    /// scorecard slot tagged with `op`. Returns the per-request headers, the
    /// resolved tenant, and the scorecard guard — which the caller must hold
    /// for the duration of the tool run. On failure the `Err` is the
    /// `CallToolResult` to return verbatim.
    async fn authorize_and_meter(
        &self,
        ctx: &RequestContext<RoleServer>,
        op: &str,
    ) -> Result<(HeaderMap, String, ScorecardGuard), CallToolResult> {
        let headers = request_headers(ctx)
            .map_err(|message| CallToolResult::error(vec![Content::text(message)]))?;
        let identity =
            whoami_and_authorize(&*self.server.auth, &headers, AuthzAction::ViewFoundation)
                .await
                .map_err(|_| {
                    CallToolResult::error(vec![Content::text(
                        "Foundation access is no longer available.",
                    )])
                })?;
        let guard = self
            .server
            .scorecard_request(&[op, &format!("tenant:{}", identity.tenant)])
            .map_err(|err| CallToolResult::error(vec![Content::text(err.to_string())]))?;
        Ok((headers, identity.tenant, guard))
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct AskFoundationParams {
    #[schemars(description = "Question to ask the company's Foundation wiki.")]
    query: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchParams {
    #[schemars(description = "Search query for the company's Foundation wiki.")]
    query: String,
    #[schemars(description = "Maximum number of unique pages to return. Defaults to 10.")]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadPageParams {
    #[schemars(
        description = "Slug of the Foundation wiki page to read in full, taken \
            from a `search_pages` result."
    )]
    slug: String,
}

#[tool_router]
impl FoundationMcpServer {
    #[tool(
        name = "ask_foundation",
        description = "Ask a question and get a synthesized, cited answer grounded \
            in Foundation, the organization-wide wiki of the company's data. Use \
            this for questions that may be answered by internal company knowledge \
            rather than general knowledge.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn ask_foundation(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<AskFoundationParams>,
    ) -> CallToolResult {
        let (headers, tenant, _guard) = match self
            .authorize_and_meter(&ctx, "op:foundation_mcp_ask")
            .await
        {
            Ok(prelude) => prelude,
            Err(result) => return result,
        };

        // When a valid ui origin is configured, instruct the agent how to build
        // web page links so its cited pages become references the user can
        // follow; otherwise fall back to the link-free default prompt.
        let link_instructions = self
            .server
            .config
            .foundation
            .foundation_ui_origin
            .as_deref()
            .and_then(|origin| page_link_instructions(origin, &tenant));
        let system = match link_instructions {
            Some(instructions) => default_system_prompt() + &instructions,
            None => default_system_prompt(),
        };
        let request = AgentRequest {
            input: params.query,
            model: default_model(),
            system,
        };
        if let Err(err) = request.validate() {
            return CallToolResult::error(vec![Content::text(err.to_string())]);
        }

        match run_agent_to_final_text(&self.server, &headers, &tenant, &request).await {
            Ok(answer) => CallToolResult::success(vec![Content::text(answer)]),
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }

    #[tool(
        name = "search_pages",
        description = "Search the company's Foundation wiki and return a ranked \
            list of pages relevant to the query, each with its slug, title, \
            categories, and a snippet of the best-matching text; then call \
            `read_page` with a slug to read a page in full. Foundation is the \
            organization's internal knowledge, synthesized from its docs, Slack, \
            GitHub, and AI sessions. Use this whenever a request touches \
            company-specific or internal information (projects, decisions, \
            processes, architecture, conventions, team knowledge) that would not \
            be in the current codebase or public sources. Use `ask_foundation` \
            instead when you just want a synthesized answer rather than the \
            source pages.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_pages(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SearchParams>,
    ) -> CallToolResult {
        let (headers, tenant, _guard) = match self
            .authorize_and_meter(&ctx, "op:foundation_mcp_search")
            .await
        {
            Ok(prelude) => prelude,
            Err(result) => return result,
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

        match run_page_search(
            &self.server,
            &headers,
            &tenant,
            &request.query,
            request.limit,
        )
        .await
        {
            Ok(hits) => {
                let body = PageSearchResponseBody { hits };
                match serde_json::to_value(body) {
                    Ok(value) => CallToolResult::structured(value),
                    Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
                }
            }
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }

    #[tool(
        name = "read_page",
        description = "Read a single Foundation wiki page in full by its slug \
            (as returned by `search_pages`), including its complete markdown \
            content, title, and categories. Use this to pull the source material \
            behind a search hit so you can read and cite it directly.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_page(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<ReadPageParams>,
    ) -> CallToolResult {
        let (headers, tenant, _guard) = match self
            .authorize_and_meter(&ctx, "op:foundation_mcp_read_page")
            .await
        {
            Ok(prelude) => prelude,
            Err(result) => return result,
        };

        let request = ReadPageRequest { slug: params.slug };
        if let Err(err) = request.validate() {
            return CallToolResult::error(vec![Content::text(err.to_string())]);
        }

        match run_read_page(&self.server, &headers, &tenant, &request.slug).await {
            Ok(Some(page)) => match serde_json::to_value(page) {
                Ok(value) => CallToolResult::structured(value),
                Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
            },
            Ok(None) => CallToolResult::error(vec![Content::text(format!(
                "No Foundation page found for slug '{}'.",
                request.slug
            ))]),
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FoundationMcpServer {
    fn get_info(&self) -> ServerInfo {
        let implementation = Implementation::new(MCP_SERVER_NAME, MCP_SERVER_VERSION).with_icons(
            vec![Icon::new(MCP_SERVER_ICON_URL)
                .with_mime_type("image/png")
                .with_sizes(vec!["256x256".to_string()])],
        );
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(implementation)
            .with_instructions(
                "Foundation is an organization-wide wiki built by synthesizing a \
                 company's own data — its documentation, Slack chats, GitHub code, \
                 and AI session traces. It is the place to look up shared \
                 institutional knowledge: \
                 company processes, policies, projects, decisions, products, and \
                 the facts that live inside the organization rather than on the \
                 public internet.\n\n\
                 Use these tools whenever a question might be answered by the \
                 company's own knowledge instead of general world knowledge. Use \
                 `ask_foundation` to get a synthesized, cited answer to a \
                 natural-language question. To read the source material yourself, \
                 use `search_pages` to find the most relevant pages (each \
                 result has a slug, title, and snippet), then `read_page` with a \
                 slug to fetch that page's full content. Prefer Foundation over \
                 guessing when a query concerns internal or company-specific \
                 information.",
            )
    }
}

fn request_headers(ctx: &RequestContext<RoleServer>) -> Result<HeaderMap, String> {
    let parts = ctx
        .extensions
        .get::<Parts>()
        .ok_or_else(|| "missing HTTP request context".to_string())?;
    // `mcp_authenticate` already validated and inserted this header, so reuse the
    // stored `HeaderValue` directly rather than re-parsing it from a string.
    let token = parts
        .headers
        .get(CHROMA_TOKEN_HEADER)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "missing bearer token".to_string())?;
    let mut headers = HeaderMap::new();
    headers.insert(CHROMA_TOKEN_HEADER, token.clone());
    Ok(headers)
}

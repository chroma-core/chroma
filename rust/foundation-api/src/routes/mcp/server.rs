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
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    auth::AuthzAction,
    routes::{
        caller_token,
        links::page_redirect_url,
        read_page::{run_read_page, ReadPageRequest},
        search::{run_page_search, PageSearchResponseBody, SearchRequest},
        subagent_search::{collect_subagent_search_final, RankedDocument, SubagentSearchCreds},
        whoami::whoami_and_authorize,
        CHROMA_TOKEN_HEADER,
    },
    server::FoundationApiServer,
    wiki::chunking::slug_from_chunk_id,
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
    async fn authorize_and_scorecard(
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

    /// Turns the subagent's ranked chunk documents into client-facing pages:
    /// resolve each chunk id to its page slug and stamp each page's web `url`
    /// from the configured `foundation_ui_origin` (buildable from the slug
    /// alone, so no per-page lookup is needed).
    ///
    /// Rank order is preserved, and duplicate slugs are kept (the subagent may
    /// rank several chunks of the same page). A document whose id is not a chunk
    /// id is skipped (there is no page the caller could open).
    fn enrich_ranked_documents(
        &self,
        tenant: &str,
        documents: Vec<RankedDocument>,
    ) -> AskFoundationResponseBody {
        let origin = self
            .server
            .config
            .foundation
            .foundation_ui_origin
            .as_deref();
        AskFoundationResponseBody {
            documents: pages_from_ranked_documents(documents, origin, tenant),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct AskFoundationParams {
    #[schemars(description = "Question to ask the company's Foundation wiki.")]
    query: String,
}

/// One page the deep-research subagent surfaced for an `ask_foundation` query,
/// in rank order (most relevant first). The raw chunk id the subagent returns
/// is deliberately dropped — it points at a chunk the caller cannot fetch — and
/// resolved to the page `slug` (usable with `read_page`) and `url` instead. To
/// read the page's title and content, pass the slug to `read_page`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AskFoundationDocument {
    /// Page slug, as accepted by `read_page`.
    slug: String,
    /// Absolute web URL to view the page. `None` when `foundation_ui_origin`
    /// is unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    /// The subagent's justification for ranking this page.
    justification: String,
}

/// Structured `ask_foundation` result: the subagent's ranked, justified pages.
#[derive(Debug, Serialize)]
struct AskFoundationResponseBody {
    documents: Vec<AskFoundationDocument>,
}

/// Resolves each ranked chunk document into a client-facing
/// [`AskFoundationDocument`] in a single pass, preserving rank order and
/// stamping each `url` from `origin`. Duplicate slugs are kept: the subagent may
/// rank several chunks of the same page, and each is a distinct justified hit
/// the caller can surface. A document whose id is not a chunk id
/// (`{slug}-{chunk_id}`) carries no page the caller could open, so it is
/// dropped. Pure (no I/O) so it is unit-testable.
fn pages_from_ranked_documents(
    documents: Vec<RankedDocument>,
    origin: Option<&str>,
    tenant: &str,
) -> Vec<AskFoundationDocument> {
    documents
        .into_iter()
        .filter_map(|doc| {
            let Some(slug) = slug_from_chunk_id(&doc.id) else {
                tracing::debug!(
                    id = %doc.id,
                    "subagent returned a non-chunk document id; skipping"
                );
                return None;
            };
            let url = origin.and_then(|origin| page_redirect_url(origin, tenant, &slug));
            Some(AskFoundationDocument {
                slug,
                url,
                justification: doc.justification,
            })
        })
        .collect()
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
        description = "Ask an open-ended question and get back a ranked, justified \
            set of Foundation wiki pages, gathered by a deep-research subagent \
            that explores the company's Foundation - the organization-wide wiki \
            of the company's data - over multiple steps. Each result carries the \
            page's slug (pass it to `read_page` to read the page in full) and a \
            justification for why it is relevant. Use this for questions that may \
            be answered by internal company knowledge rather than general \
            knowledge.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn ask_foundation(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<AskFoundationParams>,
    ) -> CallToolResult {
        let (headers, tenant, _guard) = match self
            .authorize_and_scorecard(&ctx, "op:foundation_mcp_ask")
            .await
        {
            Ok(prelude) => prelude,
            Err(result) => return result,
        };

        // `ask_foundation` is backed by the deep-research subagent, so it is only
        // available when the deep-research dependency is configured.
        let Some(url) = self.server.config.foundation.deep_research_api_url.clone() else {
            return CallToolResult::error(vec![Content::text("deep research is not configured")]);
        };
        let Some(token) = caller_token(&headers).map(str::to_string) else {
            return CallToolResult::error(vec![Content::text("missing bearer token")]);
        };

        let creds = SubagentSearchCreds {
            chroma_api_key: token,
            chroma_tenant: tenant.clone(),
            chroma_database: self.server.config.foundation.database_name.clone(),
            collection_name: self.server.config.foundation.wiki_collection.clone(),
        };

        let documents = match collect_subagent_search_final(
            self.server.shared_http_client.clone(),
            url,
            creds,
            params.query,
        )
        .await
        {
            Ok(documents) => documents,
            Err(err) => return CallToolResult::error(vec![Content::text(err.to_string())]),
        };

        let body = self.enrich_ranked_documents(&tenant, documents);
        match serde_json::to_value(body) {
            Ok(value) => CallToolResult::structured(value),
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
            instead when you want a deep-researched ranked set of source pages \
            rather than a targeted search result list.",
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
            .authorize_and_scorecard(&ctx, "op:foundation_mcp_search")
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
            .authorize_and_scorecard(&ctx, "op:foundation_mcp_read_page")
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
                 `ask_foundation` to hand an open-ended question to a \
                 deep-research subagent and get back the most relevant pages, \
                 each with its slug and a justification. To search and read the \
                 source material yourself, use `search_pages` to find the most \
                 relevant pages (each result has a slug, title, and snippet), \
                 then `read_page` with a slug to fetch that page's full content. \
                 Prefer Foundation over guessing when a query concerns internal \
                 or company-specific information.",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: &str, justification: &str) -> RankedDocument {
        RankedDocument {
            id: id.to_string(),
            justification: justification.to_string(),
        }
    }

    #[test]
    fn pages_keep_every_chunk_including_duplicate_slugs_in_rank_order() {
        // The first page's chunks are ranked 1st and 3rd. Both are kept as
        // separate rows, each with its own justification, in rank order — a
        // page may be surfaced more than once when several of its chunks rank.
        let pages = pages_from_ranked_documents(
            vec![
                doc("onboarding-0", "first"),
                doc("gc-hard-delete-2", "second"),
                doc("onboarding-4", "third"),
            ],
            None,
            "t-1",
        );

        let rows: Vec<(&str, &str)> = pages
            .iter()
            .map(|p| (p.slug.as_str(), p.justification.as_str()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("onboarding", "first"),
                ("gc-hard-delete", "second"),
                ("onboarding", "third"),
            ]
        );
        // No origin configured, so no page carries a url.
        assert!(pages.iter().all(|p| p.url.is_none()));
    }

    #[test]
    fn pages_skip_non_chunk_ids_and_stamp_url_from_origin() {
        // An id without a numeric chunk suffix can't be resolved to a page, so
        // it is dropped rather than surfaced with no locator. The surviving page
        // gets a url built from the configured origin.
        let pages = pages_from_ranked_documents(
            vec![
                doc("not-a-chunk-id", "dropped"),
                doc("onboarding-0", "kept"),
            ],
            Some("https://wiki.example.com"),
            "t-1",
        );

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].slug, "onboarding");
        assert_eq!(pages[0].justification, "kept");
        assert_eq!(
            pages[0].url.as_deref(),
            Some("https://wiki.example.com/~/page-redirect?tenant_uuid=t-1&slug=onboarding")
        );
    }

    #[test]
    fn pages_from_empty_documents_is_empty() {
        assert!(pages_from_ranked_documents(vec![], None, "t-1").is_empty());
    }
}

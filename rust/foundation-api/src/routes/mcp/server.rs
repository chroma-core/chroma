//! The Foundation MCP server: the [`FoundationMcpServer`] handler plus its
//! `subagent_search` / `search` / `read_page` tools and server
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
        links::page_url,
        read_page::{run_read_page, ReadPageRequest},
        search::{run_page_search, PageSearchResponseBody, SearchRequest},
        subagent_search::{
            collect_subagent_search_final, RankedDocument, SubagentSearchCreds, SubagentSearchError,
        },
        ui_origin_for,
        whoami::{authorize_scope, ScopePolicy},
        FoundationScope, CHROMA_TOKEN_HEADER,
    },
    server::FoundationApiServer,
    wiki::chunking::ChunkRecordId,
};

use super::{McpScope, MCP_SERVER_ICON_URL, MCP_SERVER_NAME, MCP_SERVER_VERSION};

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
    /// request context, resolve the Foundation the request addresses, and open
    /// a scorecard slot tagged with `op`. On failure the `Err` is the
    /// `CallToolResult` to return verbatim.
    ///
    /// Invariants:
    /// 1. A request that named a tenant and Foundation in its path uses exactly
    ///    that pair. The authentication gate authorized the caller against it
    ///    already, so no second authorization call is made and the configured
    ///    default is never consulted.
    /// 2. A request on the bare endpoint names no Foundation, so the empty
    ///    scope resolves to the key's tenant and the configured default
    ///    Foundation, and is authorized here.
    /// 3. A request carrying no scope at all never reached the gate, so it is
    ///    refused rather than resolved against the default Foundation: silently
    ///    answering with a Foundation the caller did not ask for is worse than
    ///    an error.
    /// 4. Rate limiting is per tenant on both paths, so the tags do not name the
    ///    Foundation. The tenant they carry is the path's on the prefixed path,
    ///    and it is the key's own tenant only because the authorization
    ///    implementation refuses any other — the Cloud one does, the no-op one
    ///    the open-source binary runs does not.
    async fn authorize_and_scorecard(
        &self,
        ctx: &RequestContext<RoleServer>,
        op: &str,
    ) -> Result<ToolPrelude, CallToolResult> {
        let headers = request_headers(ctx)
            .map_err(|message| CallToolResult::error(vec![Content::text(message)]))?;
        let scope = request_scope(ctx).ok_or_else(|| {
            CallToolResult::error(vec![Content::text(
                "This Foundation request carries no tenant or Foundation.",
            )])
        })?;
        let ui_origin =
            ui_origin_for(&self.server, &scope.as_foundation_scope()).map(str::to_string);
        let (tenant, database) = match scope {
            McpScope::Named { tenant, database } => (tenant, database),
            McpScope::Bare => {
                let (tenant, database, _identity) = authorize_scope(
                    &*self.server.auth,
                    &headers,
                    AuthzAction::ViewFoundation,
                    &FoundationScope::default(),
                    &self.server.config.foundation.database_name,
                    ScopePolicy::DefaultToConfig,
                )
                .await
                .map_err(|_| {
                    CallToolResult::error(vec![Content::text(
                        "Foundation access is no longer available.",
                    )])
                })?;
                (tenant, database)
            }
        };
        let guard = self
            .server
            .scorecard_request(&[op, &format!("tenant:{tenant}")])
            .map_err(|err| CallToolResult::error(vec![Content::text(err.to_string())]))?;
        Ok(ToolPrelude {
            headers,
            tenant,
            database,
            ui_origin,
            _guard: guard,
        })
    }
}

/// What an MCP tool needs before it runs, resolved once by
/// [`FoundationMcpServer::authorize_and_scorecard`].
///
/// The rate-limit slot is held for as long as this value lives, which is the
/// whole tool run, and released when it is dropped.
struct ToolPrelude {
    /// Per-request headers carrying the caller's token, forwarded downstream.
    headers: HeaderMap,
    /// The tenant the request resolved to.
    tenant: String,
    /// The database holding the Foundation the request resolved to.
    database: String,
    /// The origin a page link is built from, or `None` when no link can be
    /// built for this Foundation.
    ui_origin: Option<String>,
    _guard: ScorecardGuard,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SubagentSearchParams {
    #[schemars(description = "Question to ask the company's Foundation knowledge base.")]
    query: String,
}

/// One page the deep-research subagent surfaced for a `subagent_search` query,
/// in rank order (most relevant first). The raw chunk id the subagent returns
/// is deliberately dropped — it points at a chunk the caller cannot fetch — and
/// resolved to the page `slug` (usable with `read_page`) and `url` instead. To
/// read the page's title and content, pass the slug to `read_page`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubagentSearchHit {
    /// Page slug, as accepted by `read_page`.
    slug: String,
    /// Absolute web URL to view the page. `None` when `foundation_ui_origin`
    /// is unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    /// The subagent's justification for ranking this page.
    justification: String,
}

/// Structured `subagent_search` result: the subagent's ranked, justified pages,
/// keyed `hits` to match the `search` tool's result shape.
#[derive(Debug, Serialize)]
struct SubagentSearchResponseBody {
    hits: Vec<SubagentSearchHit>,
}

/// Resolves each ranked chunk document into a client-facing
/// [`SubagentSearchHit`] in a single pass, preserving rank order and
/// stamping each `url` from `origin`. Duplicate slugs are kept: the subagent may
/// rank several chunks of the same page, and each is a distinct justified hit
/// the caller can surface. A document whose id is not a chunk id
/// (`{slug}-{chunk_id}`) carries no page the caller could open, so it is
/// dropped. Pure (no I/O) so it is unit-testable.
fn pages_from_ranked_documents(
    documents: Vec<RankedDocument>,
    origin: Option<&str>,
    tenant: &str,
) -> Vec<SubagentSearchHit> {
    documents
        .into_iter()
        .filter_map(|doc| {
            let Some(slug) = ChunkRecordId::slug_from_id(&doc.id) else {
                tracing::debug!(
                    id = %doc.id,
                    "subagent returned a non-chunk document id; skipping"
                );
                return None;
            };
            let url = page_url(origin, tenant, slug);
            Some(SubagentSearchHit {
                slug: slug.to_string(),
                url,
                justification: doc.justification,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchParams {
    #[schemars(description = "Search query for the company's Foundation knowledge base.")]
    query: String,
    #[schemars(description = "Maximum number of unique pages to return. Defaults to 10.")]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadPageParams {
    #[schemars(
        description = "Slug of the Foundation knowledge base page to read in full, taken \
            from a `search` result."
    )]
    slug: String,
}

#[tool_router]
impl FoundationMcpServer {
    #[tool(
        name = "subagent_search",
        description = "Ask an open-ended question and get back a ranked, justified \
            set of Foundation knowledge base pages, gathered by a deep-research \
            subagent that explores the company's Foundation - the organization-wide \
            knowledge base of the company's data - over multiple steps. Each result \
            carries the \
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
    async fn subagent_search(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SubagentSearchParams>,
    ) -> CallToolResult {
        let prelude = match self
            .authorize_and_scorecard(&ctx, "op:foundation_mcp_subagent_search")
            .await
        {
            Ok(prelude) => prelude,
            Err(result) => return result,
        };

        // `subagent_search` is backed by the deep-research subagent, so it is only
        // available when the deep-research dependency is configured. Reuse the
        // route's typed preconditions so the messages can't drift from it.
        let Some(url) = self.server.config.foundation.deep_research_api_url.clone() else {
            return CallToolResult::error(vec![Content::text(
                SubagentSearchError::RouteDisabled.to_string(),
            )]);
        };
        let Some(token) = caller_token(&prelude.headers).map(str::to_string) else {
            return CallToolResult::error(vec![Content::text(
                SubagentSearchError::MissingToken.to_string(),
            )]);
        };

        let creds = SubagentSearchCreds::new(
            prelude.tenant.clone(),
            prelude.database.clone(),
            &self.server.config.foundation.wiki_collection,
            token,
        );

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

        let body = SubagentSearchResponseBody {
            hits: pages_from_ranked_documents(
                documents.documents,
                prelude.ui_origin.as_deref(),
                &prelude.tenant,
            ),
        };
        match serde_json::to_value(body) {
            Ok(value) => CallToolResult::structured(value),
            Err(err) => CallToolResult::error(vec![Content::text(err.to_string())]),
        }
    }

    #[tool(
        name = "search",
        description = "Search the company's Foundation knowledge base and return a ranked \
            list of pages relevant to the query, each with its slug, title, \
            categories, and a snippet of the best-matching text; then call \
            `read_page` with a slug to read a page in full. Foundation is the \
            organization's internal knowledge, synthesized from its docs, Slack, \
            GitHub, and AI sessions. Use this whenever a request touches \
            company-specific or internal information (projects, decisions, \
            processes, architecture, conventions, team knowledge) that would not \
            be in the current codebase or public sources. Use `subagent_search` \
            instead when you want a deep-researched ranked set of source pages \
            rather than a targeted search result list.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn search(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(params): Parameters<SearchParams>,
    ) -> CallToolResult {
        let prelude = match self
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
            &prelude.headers,
            &prelude.tenant,
            &prelude.database,
            prelude.ui_origin.as_deref(),
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
        description = "Read a single Foundation knowledge base page in full by its slug \
            (as returned by `search`), including its complete markdown \
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
        let prelude = match self
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

        match run_read_page(
            &self.server,
            &prelude.headers,
            &prelude.tenant,
            &prelude.database,
            prelude.ui_origin.as_deref(),
            &request.slug,
        )
        .await
        {
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
                "Foundation is an organization-wide knowledge base built by synthesizing a \
                 company's own data — its documentation, Slack chats, GitHub code, \
                 and AI session traces. It is the place to look up shared \
                 institutional knowledge: \
                 company processes, policies, projects, decisions, products, and \
                 the facts that live inside the organization rather than on the \
                 public internet.\n\n\
                 Use these tools whenever a question might be answered by the \
                 company's own knowledge instead of general world knowledge. Use \
                 `subagent_search` to hand an open-ended question to a \
                 deep-research subagent and get back the most relevant pages, \
                 each with its slug and a justification. To search and read the \
                 source material yourself, use `search` to find the most \
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

/// Which Foundation the authentication gate read off the request path, or
/// `None` when no gate ran.
///
/// The MCP library splits the HTTP request and, in the stateless mode this
/// server runs, puts the whole request parts value — extensions included — into
/// the tool request context, which is what carries a value inserted by an HTTP
/// layer into a tool.
fn request_scope(ctx: &RequestContext<RoleServer>) -> Option<McpScope> {
    scope_from_parts(ctx.extensions.get::<Parts>()?)
}

/// Reads the scope back out of one request parts value. Split out of
/// [`request_scope`] so the round trip through the extensions can be tested
/// without a live tool call.
fn scope_from_parts(parts: &Parts) -> Option<McpScope> {
    parts.extensions.get::<McpScope>().cloned()
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

    /// Builds the request parts value the MCP library hands a tool, the way the
    /// authentication gate leaves it.
    fn parts_with(scope: Option<McpScope>) -> Parts {
        let (mut parts, _) = axum::http::Request::builder()
            .uri("/mcp/f/team-1/wiki_team")
            .body(())
            .expect("request should build")
            .into_parts();
        if let Some(scope) = scope {
            parts.extensions.insert(scope);
        }
        parts
    }

    fn named(database: &str) -> McpScope {
        McpScope::Named {
            tenant: "team-1".to_string(),
            database: database.to_string(),
        }
    }

    #[test]
    fn the_resolved_pair_survives_the_trip_through_the_request_parts() {
        // The authentication gate inserts the pair into the request extensions
        // and a tool reads it back from the parts value the MCP library
        // carries. Pin that round trip here rather than through a full
        // JSON-RPC call.
        let parts = parts_with(Some(named("wiki_team")));

        let scope = scope_from_parts(&parts).expect("the inserted scope should be readable");

        let McpScope::Named { tenant, database } = &scope else {
            panic!("expected a named scope, got {scope:?}");
        };
        assert_eq!(tenant, "team-1");
        assert_eq!(database, "wiki_team");
        let as_scope = scope.as_foundation_scope();
        assert_eq!(as_scope.tenant.as_deref(), Some("team-1"));
        assert_eq!(as_scope.foundation.as_deref(), Some("wiki_team"));
    }

    #[test]
    fn a_bare_request_carries_a_scope_that_names_no_foundation() {
        let parts = parts_with(Some(McpScope::Bare));

        let scope = scope_from_parts(&parts).expect("the inserted scope should be readable");

        assert!(matches!(scope, McpScope::Bare));
        let as_scope = scope.as_foundation_scope();
        assert_eq!(as_scope.tenant, None);
        assert_eq!(as_scope.foundation, None);
    }

    #[test]
    fn a_request_that_never_reached_the_gate_carries_no_scope() {
        // The gate inserts a scope on both mounts, so finding none means no
        // gate ran. A tool refuses rather than resolving the default
        // Foundation for a caller who may have asked for another one.
        assert!(scope_from_parts(&parts_with(None)).is_none());
    }

    fn server_with_page_links() -> FoundationApiServer {
        use chroma_sysdb::{SysDb, TestSysDb};
        use chroma_system::System;
        use std::sync::Arc;

        let mut config = crate::config::FoundationApiConfig::default();
        config.foundation.foundation_ui_origin = Some("https://wiki.example.com".to_string());
        FoundationApiServer::new(
            config,
            Arc::new(()),
            SysDb::Test(TestSysDb::new()),
            vec![],
            System::new(),
        )
    }

    #[test]
    fn only_a_foundation_the_page_link_cannot_resolve_loses_its_links() {
        // This is the expression the tool prelude evaluates to decide whether a
        // result carries page URLs. A page link resolves a tenant and a slug
        // and carries no Foundation, so it always opens the default
        // Foundation's page: a result from any other Foundation must carry no
        // link at all, while naming the default one in the path keeps its
        // links, since it addresses the database the bare path does.
        let server = server_with_page_links();

        assert_eq!(
            ui_origin_for(&server, &McpScope::Bare.as_foundation_scope()),
            Some("https://wiki.example.com")
        );
        assert_eq!(
            ui_origin_for(&server, &named("FOUNDATION").as_foundation_scope()),
            Some("https://wiki.example.com")
        );
        assert_eq!(
            ui_origin_for(&server, &named("other_foundation").as_foundation_scope()),
            None
        );
    }
}

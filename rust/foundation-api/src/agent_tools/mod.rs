//! `chroma-agent` tool implementations backed by foundation-api's retrieval
//! cores.
//!
//! Each tool reuses a route core ([`crate::routes::search`] /
//! [`crate::routes::read_page`] / [`crate::routes::subagent_search`]) so the
//! agent loop and the bare HTTP routes share one retrieval implementation.
//! Per-request state (resolved collection, caller token, deep-research creds)
//! is captured as struct fields when the `/api/agent` handler builds the
//! toolset.

mod read_page_tool;
mod search_tool;
mod subagent_search_tool;

pub(crate) use read_page_tool::ReadPageTool;
pub(crate) use search_tool::SearchTool;
pub(crate) use subagent_search_tool::SubagentSearchTool;

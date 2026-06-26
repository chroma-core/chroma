//! Building wiki page redirect links.
//!
//! `foundation-api` resolves only the tenant UUID from the caller's token, so a
//! page link is keyed by that UUID plus the page slug; the configured web
//! origin resolves it to the page. [`page_redirect_url`] builds the link;
//! [`page_link_instructions`] is the prompt guidance that teaches the
//! `ask_foundation` agent to build the same links for the pages it cites.

use reqwest::Url;

/// Root-relative path of the wiki redirect route that resolves a tenant UUID +
/// page slug to the page URL.
const PAGE_REDIRECT_PATH: &str = "/~/page-redirect";

/// The `{origin}/~/page-redirect` base URL, or `None` when `origin` does not
/// parse as a valid URL (e.g. a deploy that forgot the scheme).
fn page_redirect_base(origin: &str) -> Option<String> {
    let base = format!("{}{}", origin.trim_end_matches('/'), PAGE_REDIRECT_PATH);
    Url::parse(&base).ok()?;
    Some(base)
}

/// Builds the absolute redirect URL for a wiki page:
/// `{origin}/~/page-redirect?tenant_uuid=<tenant_uuid>&slug=<slug>`.
///
/// `tenant_uuid` and `slug` are query params (not path segments) so the slug —
/// which may contain `:` (category pages) and other path-fragile characters —
/// is a single percent-encoded value, and so `tenant_uuid` stays cleanly
/// optional. Returns `None` only when the configured origin fails to parse as a
/// URL.
pub(crate) fn page_redirect_url(origin: &str, tenant: &str, slug: &str) -> Option<String> {
    let base = page_redirect_base(origin)?;
    Url::parse_with_params(&base, &[("tenant_uuid", tenant), ("slug", slug)])
        .ok()
        .map(String::from)
}

/// Guidance appended to the `ask_foundation` agent's system prompt so the
/// synthesized answer can link the pages it cites.
pub(crate) fn page_link_instructions(origin: &str, tenant: &str) -> Option<String> {
    let base = page_redirect_base(origin)?;
    Some(format!(
        "\n\nWhen you cite a Foundation page, link to it for the user as a \
         markdown link using this URL template:\n\
         {base}?tenant_uuid={tenant}&slug=<slug>\n\
         Substitute <slug> with the page's exact slug (each search result \
         reports its `slug=`). Use the `tenant_uuid` value above verbatim; do \
         not change it. For example, cite a page as \
         `[Onboarding]({base}?tenant_uuid={tenant}&slug=onboarding)`."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_url_has_tenant_uuid_and_slug_query_params() {
        let url = page_redirect_url("https://wiki.example.com", "tenant-uuid-123", "onboarding")
            .expect("url");
        assert_eq!(
            url,
            "https://wiki.example.com/~/page-redirect?tenant_uuid=tenant-uuid-123&slug=onboarding"
        );
    }

    #[test]
    fn redirect_url_percent_encodes_category_slug() {
        let url =
            page_redirect_url("https://wiki.example.com", "t-1", "category:eng").expect("url");
        // The `:` in a category slug must be encoded so it survives as a single
        // query value rather than being read as a scheme/path delimiter.
        assert!(
            url.ends_with("slug=category%3Aeng"),
            "expected encoded slug, got: {url}"
        );
    }

    #[test]
    fn redirect_url_trims_trailing_slash_on_origin() {
        let url = page_redirect_url("https://wiki.example.com/", "t-1", "p").expect("url");
        assert!(
            url.starts_with("https://wiki.example.com/~/page-redirect?"),
            "expected single slash before path, got: {url}"
        );
    }

    #[test]
    fn redirect_url_none_for_unparseable_origin() {
        // A scheme-less origin (a plausible deploy misconfiguration) is not a valid URL.
        assert_eq!(page_redirect_url("wiki.example.com", "t-1", "p"), None);
    }

    #[test]
    fn link_instructions_embed_origin_and_tenant() {
        let text = page_link_instructions("https://wiki.example.com", "tenant-9").expect("text");
        assert!(text.contains("https://wiki.example.com/~/page-redirect?tenant_uuid=tenant-9"));
        assert!(text.contains("<slug>"));
    }

    #[test]
    fn link_instructions_none_for_unparseable_origin() {
        // Mirrors `page_redirect_url`: a bad origin emits no link guidance, so
        // the agent is never told to build links the other tools can't.
        assert_eq!(page_link_instructions("wiki.example.com", "t-1"), None);
    }
}

//! Building wiki page redirect links.
//!
//! `foundation-api` resolves only the tenant UUID from the caller's token, so a
//! page link is keyed by that UUID plus the page slug; the configured web
//! origin resolves it to the page. [`page_redirect_url`] builds the link.

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
}

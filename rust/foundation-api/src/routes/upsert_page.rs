//! `POST /api/upsert-page` — replace a wiki page's chunks.
//!
//! This module currently implements the request/response contract, coarse
//! foundation authorization, and input validation. The replace flow itself
//! (resolve the wiki collection, read `{slug}-0`, delete-by-slug, re-chunk +
//! embed, batched add) lands in a later change; until then the handler returns
//! a stub response after authorizing and validating the request.

use super::whoami::whoami_and_authorize;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::ChromaValidationError;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::LazyLock;
use validator::{Validate, ValidationError};

/// `^(?:[a-z0-9][a-z0-9-]*|category:[a-z0-9][a-z0-9-]*|)$` — the wiki slug
/// shape (empty root, lowercase alnum/hyphen, or a `category:<slug>`). The
/// empty alternative makes the root slug valid.
static SLUG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:[a-z0-9][a-z0-9-]*|category:[a-z0-9][a-z0-9-]*|)$")
        .expect("the wiki slug regex should be valid")
});

/// `^[a-z0-9][a-z0-9-]*$` — the category-name shape.
static CATEGORY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-z0-9][a-z0-9-]*$").expect("the category-name regex should be valid")
});

/// Request body for `POST /api/upsert-page`.
#[derive(Debug, Deserialize, Validate)]
pub struct UpsertPageRequest {
    /// Page slug. Empty string is the wiki root; otherwise lowercase
    /// alnum/hyphen, or `category:<slug>`.
    #[validate(custom(function = "validate_slug"))]
    pub slug: String,
    /// Full markdown content. The first non-blank line is the title.
    #[validate(length(min = 1, message = "content must not be empty"))]
    pub content: String,
    /// Citation ids (`<collection>:<record_id>`) for every source whose
    /// information appears in `content`. May be empty.
    #[validate(custom(function = "validate_source_ids"))]
    pub source_ids: Vec<String>,
    /// Optional category tags (lowercase alnum/hyphen); deduplicated and
    /// sorted before they are stamped onto the page.
    #[serde(default)]
    #[validate(custom(function = "validate_categories"))]
    pub categories: Vec<String>,
}

/// Response body for `POST /api/upsert-page`.
#[derive(Debug, Serialize)]
pub struct UpsertPageResponse {
    /// The page slug that was written.
    pub slug: String,
    /// `"added"` for a new page or `"updated"` for an existing one (the stub
    /// returns `"stub"` until the replace flow is wired up).
    pub op: String,
    /// Monotonic page version (1 for a new page, previous + 1 on update).
    pub version: u32,
    /// Page creation time, unix seconds.
    pub created_at: i64,
    /// Last write time, unix seconds.
    pub updated_at: i64,
    /// Number of chunks written for the page.
    pub num_chunks: usize,
}

/// `POST /api/upsert-page` handler.
pub async fn foundation_upsert_page(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<UpsertPageRequest>,
) -> Result<Json<UpsertPageResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_upsert_page", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;

    // Normalize now so the replace flow (added later) can rely on a well-formed
    // request; `_categories` is the deduped/sorted list it stamps onto chunks.
    let _categories = normalize_categories(&request.categories);

    Ok(Json(UpsertPageResponse {
        slug: request.slug,
        op: "stub".to_string(),
        version: 0,
        created_at: 0,
        updated_at: 0,
        num_chunks: 0,
    }))
}

/// Validates the slug against [`SLUG_RE`].
fn validate_slug(slug: &str) -> Result<(), ValidationError> {
    if SLUG_RE.is_match(slug) {
        Ok(())
    } else {
        Err(ValidationError::new("slug").with_message(
            format!("invalid slug '{slug}': must be lowercase alnum/hyphen, or 'category:<slug>'")
                .into(),
        ))
    }
}

/// Validates each source id is `<collection>:<record_id>` with a non-empty
/// collection. The record id (after the first `:`) may be empty (the wiki
/// root's citation id).
fn validate_source_ids(source_ids: &[String]) -> Result<(), ValidationError> {
    for source_id in source_ids {
        match source_id.split_once(':') {
            Some((collection, _record_id)) if !collection.is_empty() => {}
            _ => {
                return Err(ValidationError::new("source_ids").with_message(
                    format!(
                    "invalid source id '{source_id}': expected format '<collection>:<record_id>'"
                )
                    .into(),
                ))
            }
        }
    }
    Ok(())
}

/// Validates each category against [`CATEGORY_RE`].
fn validate_categories(categories: &[String]) -> Result<(), ValidationError> {
    for category in categories {
        if !CATEGORY_RE.is_match(category) {
            return Err(ValidationError::new("categories").with_message(
                format!("invalid category name '{category}': must be lowercase alnum/hyphen")
                    .into(),
            ));
        }
    }
    Ok(())
}

/// Deduplicates and lexicographically sorts the (already-validated) category
/// list.
fn normalize_categories(categories: &[String]) -> Vec<String> {
    categories
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        slug: &str,
        content: &str,
        source_ids: &[&str],
        categories: &[&str],
    ) -> UpsertPageRequest {
        UpsertPageRequest {
            slug: slug.to_string(),
            content: content.to_string(),
            source_ids: source_ids.iter().map(|s| s.to_string()).collect(),
            categories: categories.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn validate_slug_accepts_root_and_well_formed_slugs() {
        validate_slug("").unwrap();
        validate_slug("foo").unwrap();
        validate_slug("foo-bar-1").unwrap();
        validate_slug("category:foo-bar").unwrap();
        validate_slug("0").unwrap();
    }

    #[test]
    fn validate_slug_rejects_malformed_slugs() {
        for slug in [
            "Foo",          // uppercase
            "foo_bar",      // underscore
            "-foo",         // leading hyphen
            "foo:bar",      // colon outside category prefix
            "category:",    // empty category body
            "category:Foo", // uppercase category body
            " foo",         // leading space
        ] {
            assert!(
                validate_slug(slug).is_err(),
                "expected {slug:?} to be rejected"
            );
        }
    }

    #[test]
    fn validate_source_ids_checks_collection_and_separator() {
        validate_source_ids(&[]).unwrap();
        validate_source_ids(&["slack_master:C094_177.992".to_string()]).unwrap();
        // Empty record id is allowed (root page citation id).
        validate_source_ids(&["wiki_master:".to_string()]).unwrap();

        assert!(validate_source_ids(&["norecord".to_string()]).is_err());
        assert!(validate_source_ids(&[":record".to_string()]).is_err());
    }

    #[test]
    fn validate_categories_rejects_malformed_names() {
        validate_categories(&[]).unwrap();
        validate_categories(&["foo".to_string(), "bar-1".to_string()]).unwrap();
        assert!(validate_categories(&["Archive".to_string()]).is_err());
    }

    #[test]
    fn normalize_categories_dedups_and_sorts() {
        assert_eq!(
            normalize_categories(&["b".to_string(), "a".to_string(), "a".to_string()]),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(normalize_categories(&[]), Vec::<String>::new());
    }

    #[test]
    fn request_validate_rejects_empty_content() {
        assert!(request("foo", "", &["c:r"], &[]).validate().is_err());
    }

    #[test]
    fn request_validate_rejects_bad_fields() {
        assert!(request("Foo", "body", &[], &[]).validate().is_err());
        assert!(request("foo", "body", &["norecord"], &[])
            .validate()
            .is_err());
        assert!(request("foo", "body", &[], &["Archive"])
            .validate()
            .is_err());
    }

    #[test]
    fn request_validate_accepts_well_formed_request() {
        request("foo", "# Title\n\nBody", &["slack_master:abc"], &["z", "a"])
            .validate()
            .unwrap();
    }
}

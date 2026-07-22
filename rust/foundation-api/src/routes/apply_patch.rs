//! `POST /api/apply-patch` — exact-string patch for an existing wiki page.
//!
//! This route intentionally builds on `/api/upsert-page`: it reads the current
//! page, applies one exact string replacement, unions metadata additions, and
//! writes the complete patched page through the same transactional upsert path.

use crate::routes::read_page::{run_read_page, FoundationPage, ReadPageError};
use crate::routes::upsert_page::{
    normalize_categories, run_upsert_page, validate_categories as validate_upsert_categories,
    validate_slug as validate_upsert_slug, validate_source_ids as validate_upsert_source_ids,
    UpsertPageError, UpsertPageRequest, UpsertPageResponse,
};
use crate::routes::whoami::whoami_and_authorize;
use crate::{auth::AuthzAction, errors::ServerError, server::FoundationApiServer};
use axum::{extract::State, http::HeaderMap, Json};
use chroma_error::{ChromaError, ChromaValidationError, ErrorCodes};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use validator::{Validate, ValidationError};

/// Request body for `POST /api/apply-patch`.
#[derive(Debug, Deserialize, Validate)]
pub struct ApplyPatchRequest {
    /// Existing page slug. Empty string is the wiki root; otherwise lowercase
    /// alnum/hyphen, or `category:<slug>`.
    #[validate(custom(function = "validate_apply_patch_slug"))]
    pub slug: String,
    /// Exact non-empty stored markdown text to replace. Must match once.
    #[validate(length(min = 1, message = "old_str must not be empty"))]
    pub old_str: String,
    /// Replacement text. May be empty.
    pub new_str: String,
    /// Additional citation ids introduced by `new_str`; unioned with existing
    /// page `source_ids`.
    #[serde(default)]
    #[validate(custom(function = "validate_apply_patch_source_ids"))]
    pub source_ids: Vec<String>,
    /// Additional categories; unioned with existing page categories.
    #[serde(default)]
    #[validate(custom(function = "validate_apply_patch_categories"))]
    pub categories: Vec<String>,
    /// Optional version the caller expects to patch. When absent, the route
    /// patches the current version it just read.
    pub expected_version: Option<u32>,
    /// Optional caller-supplied reason for this change. Forwarded to
    /// `/api/upsert-page`, which logs only presence.
    #[validate(length(max = 350, message = "reason must be at most 350 characters"))]
    pub reason: Option<String>,
}

/// Response body for `POST /api/apply-patch`.
#[derive(Debug, Serialize)]
pub struct ApplyPatchResponse {
    /// The underlying upsert result for the patched page.
    #[serde(flatten)]
    pub upsert: UpsertPageResponse,
    /// Length of the exact text that was replaced.
    pub old_str_chars: usize,
    /// Length of the replacement text.
    pub new_str_chars: usize,
}

/// Errors raised while applying an exact-string page patch.
#[derive(Debug, thiserror::Error)]
pub enum ApplyPatchError {
    /// Reading the page failed.
    #[error(transparent)]
    Read(#[from] ReadPageError),
    /// The target page does not exist.
    #[error("cannot apply patch to missing page '{slug}'; use upsert-page to create it")]
    MissingPage { slug: String },
    /// `old_str` was empty.
    #[error("old_str must not be empty")]
    EmptyOldStr,
    /// `old_str` did not match exactly once.
    #[error("old_str matched {matches} times in page '{slug}'; expected exactly one match")]
    PatchMatch { slug: String, matches: usize },
    /// Writing the patched page failed.
    #[error(transparent)]
    Upsert(#[from] UpsertPageError),
}

impl ChromaError for ApplyPatchError {
    fn code(&self) -> ErrorCodes {
        match self {
            ApplyPatchError::Read(err) => err.code(),
            ApplyPatchError::MissingPage { .. } => ErrorCodes::NotFound,
            ApplyPatchError::EmptyOldStr => ErrorCodes::InvalidArgument,
            ApplyPatchError::PatchMatch { .. } => ErrorCodes::FailedPrecondition,
            ApplyPatchError::Upsert(err) => err.code(),
        }
    }
}

/// `POST /api/apply-patch` handler.
#[tracing::instrument(skip(headers, server, request))]
pub async fn foundation_apply_patch(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(request): Json<ApplyPatchRequest>,
) -> Result<Json<ApplyPatchResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;

    let _guard =
        server.scorecard_request(&["op:foundation_apply_patch", &format!("tenant:{tenant}")])?;

    request.validate().map_err(ChromaValidationError::from)?;

    let response = run_apply_patch(&server, &headers, &tenant, &request).await?;
    Ok(Json(response))
}

/// Reads, patches, and writes a wiki page through the upsert-page flow.
pub(crate) async fn run_apply_patch(
    server: &FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
    request: &ApplyPatchRequest,
) -> Result<ApplyPatchResponse, ApplyPatchError> {
    let page = run_read_page(server, headers, tenant, &request.slug)
        .await?
        .ok_or_else(|| ApplyPatchError::MissingPage {
            slug: request.slug.clone(),
        })?;
    let patched = patch_page(page, request)?;

    let upsert = UpsertPageRequest {
        slug: request.slug.clone(),
        content: patched.content,
        source_ids: patched.source_ids,
        categories: patched.categories,
        reason: request.reason.clone(),
        expected_version: request.expected_version.unwrap_or(patched.base_version),
    };
    let categories = normalize_categories(&upsert.categories);
    let upsert = run_upsert_page(server, headers, tenant, &upsert, &categories).await?;

    Ok(ApplyPatchResponse {
        upsert,
        old_str_chars: request.old_str.chars().count(),
        new_str_chars: request.new_str.chars().count(),
    })
}

#[derive(Debug)]
struct PatchedPage {
    content: String,
    source_ids: Vec<String>,
    categories: Vec<String>,
    base_version: u32,
}

fn patch_page(
    page: FoundationPage,
    request: &ApplyPatchRequest,
) -> Result<PatchedPage, ApplyPatchError> {
    if request.old_str.is_empty() {
        return Err(ApplyPatchError::EmptyOldStr);
    }

    let matches = page.content.matches(&request.old_str).count();
    if matches != 1 {
        return Err(ApplyPatchError::PatchMatch {
            slug: request.slug.clone(),
            matches,
        });
    }

    Ok(PatchedPage {
        content: page.content.replacen(&request.old_str, &request.new_str, 1),
        source_ids: dedupe_preserve_order(page.source_ids, &request.source_ids),
        categories: normalize_union(page.categories, &request.categories),
        base_version: page.version,
    })
}

fn dedupe_preserve_order(mut base: Vec<String>, additions: &[String]) -> Vec<String> {
    let mut seen: HashSet<String> = base.iter().cloned().collect();
    for addition in additions {
        if seen.insert(addition.clone()) {
            base.push(addition.clone());
        }
    }
    base
}

fn normalize_union(mut base: Vec<String>, additions: &[String]) -> Vec<String> {
    base.extend(additions.iter().cloned());
    normalize_categories(&base)
}

fn validate_apply_patch_slug(slug: &str) -> Result<(), ValidationError> {
    validate_upsert_slug(slug)
}

fn validate_apply_patch_source_ids(source_ids: &[String]) -> Result<(), ValidationError> {
    validate_upsert_source_ids(source_ids)
}

fn validate_apply_patch_categories(categories: &[String]) -> Result<(), ValidationError> {
    validate_upsert_categories(categories)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(content: &str) -> FoundationPage {
        FoundationPage {
            slug: "alpha".to_string(),
            title: "Alpha".to_string(),
            categories: vec!["infra".to_string()],
            source_ids: vec!["slack_master:old".to_string()],
            version: 7,
            updated_at: Some(1700),
            content: content.to_string(),
            url: None,
        }
    }

    fn request(old_str: &str, new_str: &str) -> ApplyPatchRequest {
        ApplyPatchRequest {
            slug: "alpha".to_string(),
            old_str: old_str.to_string(),
            new_str: new_str.to_string(),
            source_ids: vec![
                "slack_master:new".to_string(),
                "slack_master:old".to_string(),
            ],
            categories: vec!["product".to_string(), "infra".to_string()],
            expected_version: None,
            reason: None,
        }
    }

    #[test]
    fn patch_page_replaces_one_match_and_unions_metadata() {
        let patched = patch_page(page("# Alpha\n\nOld body."), &request("Old", "New")).unwrap();

        assert_eq!(patched.content, "# Alpha\n\nNew body.");
        assert_eq!(
            patched.source_ids,
            vec![
                "slack_master:old".to_string(),
                "slack_master:new".to_string(),
            ]
        );
        assert_eq!(
            patched.categories,
            vec!["infra".to_string(), "product".to_string()]
        );
        assert_eq!(patched.base_version, 7);
    }

    #[test]
    fn patch_page_allows_empty_replacement() {
        let patched = patch_page(page("# Alpha\n\nOld body."), &request("Old ", "")).unwrap();

        assert_eq!(patched.content, "# Alpha\n\nbody.");
    }

    #[test]
    fn patch_page_rejects_missing_and_ambiguous_matches() {
        let missing = patch_page(page("# Alpha\n\nBody."), &request("Old", "New")).unwrap_err();
        assert!(matches!(
            missing,
            ApplyPatchError::PatchMatch { matches: 0, .. }
        ));
        assert_eq!(missing.code(), ErrorCodes::FailedPrecondition);

        let ambiguous =
            patch_page(page("# Alpha\n\nOld Old."), &request("Old", "New")).unwrap_err();
        assert!(matches!(
            ambiguous,
            ApplyPatchError::PatchMatch { matches: 2, .. }
        ));
    }

    #[test]
    fn request_validation_matches_upsert_constraints() {
        assert!(request("", "New").validate().is_err());

        let mut invalid_source = request("Old", "New");
        invalid_source.source_ids = vec!["not-a-source-id".to_string()];
        assert!(invalid_source.validate().is_err());

        let mut invalid_category = request("Old", "New");
        invalid_category.categories = vec!["BadCategory".to_string()];
        assert!(invalid_category.validate().is_err());
    }
}

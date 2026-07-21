//! HTTP routes for storing and reading generated wiki trajectories.
//!
//! The request and response bodies intentionally mirror
//! [`crate::trajectories`] types. Axum concerns stay here: Foundation auth,
//! scorecard metering, caller-token extraction, collection resolution, and
//! cache invalidation for stale collection ids. The transaction boundaries and
//! trajectory invariants live next to the trajectory model.

use std::future::Future;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    auth::AuthzAction,
    errors::ServerError,
    foundation_chroma::{FoundationChromaClient, FoundationChromaClientError},
    routes::{caller_token, whoami::whoami_and_authorize},
    server::FoundationApiServer,
    trajectories::{
        append_open_generate_trajectory, create_open_generate_trajectory,
        finalize_open_generate_trajectory, load_generate_trajectory, save_generate_trajectory,
        AppendTrajectoryEntriesRequest, ReasoningTrajectoryFile, TrajectoryError,
        TrajectoryWriteResponse,
    },
};

/// Query parameters for `GET /api/trajectories/{id}`.
#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
pub struct ReadTrajectoryQuery {
    /// Reject open trajectories when true. Defaults false so callers can
    /// inspect a partial trajectory while it executes.
    #[serde(default)]
    pub require_finalized: bool,
}

/// Query parameters for `GET /api/trajectories/{id}/reasoning`.
#[derive(Debug, Default, PartialEq, Eq, Deserialize)]
pub struct ReadTrajectoryReasoningQuery {
    /// Wiki page slug to find in trajectory write calls. Present-but-empty is
    /// valid and targets the wiki root page.
    pub slug: Option<String>,
    /// Reject open trajectories when true. Defaults false so callers can
    /// inspect a partial trajectory while it executes.
    #[serde(default)]
    pub require_finalized: bool,
}

/// Reasoning traces associated with one page write.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct TrajectoryReasoningResponse {
    /// The requested page slug.
    pub slug: String,
    /// Trimmed non-empty reasoning traces through the final write action.
    pub reasoning: Vec<String>,
    /// Other slugs written by the same final action, excluding `slug`.
    pub other_slugs: Vec<String>,
}

/// Errors raised while running trajectory routes after request extraction.
#[derive(Debug, thiserror::Error)]
pub enum TrajectoryRouteError {
    /// `frontend_ingress_url` is unset, so the proxying client was never built.
    #[error("trajectory record I/O is not configured")]
    RouteDisabled,
    /// The caller's request carried no usable `x-chroma-token`.
    #[error("missing or invalid x-chroma-token header")]
    MissingToken,
    /// Resolving the trajectory collection through the proxy failed.
    #[error(transparent)]
    Resolve(#[from] FoundationChromaClientError),
    /// The trajectory operation failed.
    #[error(transparent)]
    Trajectory(#[from] TrajectoryError),
    /// The reasoning route requires a slug query parameter.
    #[error("missing slug query parameter")]
    MissingSlug,
}

impl ChromaError for TrajectoryRouteError {
    fn code(&self) -> ErrorCodes {
        match self {
            TrajectoryRouteError::RouteDisabled => ErrorCodes::Internal,
            TrajectoryRouteError::MissingToken | TrajectoryRouteError::MissingSlug => {
                ErrorCodes::InvalidArgument
            }
            TrajectoryRouteError::Resolve(err) if err.is_not_found() => ErrorCodes::NotFound,
            TrajectoryRouteError::Resolve(err) => err.code(),
            TrajectoryRouteError::Trajectory(err) => err.code(),
        }
    }
}

/// `POST /api/trajectories/save` writes a complete finalized trajectory.
pub async fn foundation_save_trajectory(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(file): Json<ReasoningTrajectoryFile>,
) -> Result<Json<TrajectoryWriteResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;
    let _guard = server
        .scorecard_request(&["op:foundation_save_trajectory", &format!("tenant:{tenant}")])?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let response = trajectory_op(
        client,
        &tenant,
        save_generate_trajectory(&collection, &file),
    )
    .await?;
    Ok(Json(response))
}

/// `POST /api/trajectories/open` creates an open trajectory with zero entries.
pub async fn foundation_open_trajectory(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Json(file): Json<ReasoningTrajectoryFile>,
) -> Result<Json<TrajectoryWriteResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;
    let _guard = server
        .scorecard_request(&["op:foundation_open_trajectory", &format!("tenant:{tenant}")])?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let response = trajectory_op(
        client,
        &tenant,
        create_open_generate_trajectory(&collection, &file),
    )
    .await?;
    Ok(Json(response))
}

/// `POST /api/trajectories/{id}/entries` appends complete entries.
pub async fn foundation_append_trajectory_entries(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(id): Path<Uuid>,
    Json(request): Json<AppendTrajectoryEntriesRequest>,
) -> Result<Json<TrajectoryWriteResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;
    let _guard = server.scorecard_request(&[
        "op:foundation_append_trajectory_entries",
        &format!("tenant:{tenant}"),
    ])?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let response = trajectory_op(
        client,
        &tenant,
        append_open_generate_trajectory(&collection, id, &request),
    )
    .await?;
    Ok(Json(response))
}

/// `POST /api/trajectories/{id}/finalize` finalizes an open trajectory.
pub async fn foundation_finalize_trajectory(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(id): Path<Uuid>,
    Json(file): Json<ReasoningTrajectoryFile>,
) -> Result<Json<TrajectoryWriteResponse>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::UpsertFoundation).await?;
    let tenant = identity.tenant;
    let _guard = server.scorecard_request(&[
        "op:foundation_finalize_trajectory",
        &format!("tenant:{tenant}"),
    ])?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let response = trajectory_op(
        client,
        &tenant,
        finalize_open_generate_trajectory(&collection, id, &file),
    )
    .await?;
    Ok(Json(response))
}

/// `GET /api/trajectories/{id}` returns a full or partial trajectory.
pub async fn foundation_get_trajectory(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(id): Path<Uuid>,
    Query(query): Query<ReadTrajectoryQuery>,
) -> Result<Json<ReasoningTrajectoryFile>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;
    let _guard =
        server.scorecard_request(&["op:foundation_get_trajectory", &format!("tenant:{tenant}")])?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let response = trajectory_op(
        client,
        &tenant,
        load_generate_trajectory(&collection, id, query.require_finalized),
    )
    .await?;
    Ok(Json(response))
}

/// `GET /api/trajectories/{id}/reasoning` returns reasoning for one page write.
pub async fn foundation_get_trajectory_reasoning(
    headers: HeaderMap,
    State(server): State<FoundationApiServer>,
    Path(id): Path<Uuid>,
    Query(query): Query<ReadTrajectoryReasoningQuery>,
) -> Result<Json<Option<TrajectoryReasoningResponse>>, ServerError> {
    let identity =
        whoami_and_authorize(&*server.auth, &headers, AuthzAction::ViewFoundation).await?;
    let tenant = identity.tenant;
    let _guard = server.scorecard_request(&[
        "op:foundation_get_trajectory_reasoning",
        &format!("tenant:{tenant}"),
    ])?;
    let slug = query.slug.ok_or(TrajectoryRouteError::MissingSlug)?;

    let (client, collection) = trajectory_collection(&server, &headers, &tenant).await?;
    let file = trajectory_op(
        client,
        &tenant,
        load_generate_trajectory(&collection, id, query.require_finalized),
    )
    .await?;
    Ok(Json(reasoning_for_slug(&file, &slug)))
}

fn reasoning_for_slug(
    file: &ReasoningTrajectoryFile,
    slug: &str,
) -> Option<TrajectoryReasoningResponse> {
    let entries = &file.trajectory.entries;
    let mut reasoning_prefix = Vec::new();
    let mut last_match = None;

    for (index, entry) in entries.iter().enumerate() {
        if let Some(reasoning) = entry.reasoning.as_ref() {
            reasoning_prefix.push(reasoning.clone());
        }
        let reasoning_len_after_entry = reasoning_prefix.len();

        if entry.writes.iter().any(|write| write.slug == slug) {
            last_match = Some((index, reasoning_len_after_entry));
        }
    }

    let Some((entry_index, reasoning_len)) = last_match else {
        return None;
    };
    let other_slugs = entries[entry_index]
        .writes
        .iter()
        .filter(|write| write.slug != slug)
        .map(|write| write.slug.clone())
        .collect();

    Some(TrajectoryReasoningResponse {
        slug: slug.to_string(),
        reasoning: reasoning_prefix.into_iter().take(reasoning_len).collect(),
        other_slugs,
    })
}

async fn trajectory_collection<'a>(
    server: &'a FoundationApiServer,
    headers: &HeaderMap,
    tenant: &str,
) -> Result<(&'a FoundationChromaClient, chroma::ChromaCollection), TrajectoryRouteError> {
    let client = server
        .foundation_chroma_client
        .as_ref()
        .ok_or(TrajectoryRouteError::RouteDisabled)?;
    let token = caller_token(headers).ok_or(TrajectoryRouteError::MissingToken)?;
    let collection = client.trajectories_collection(tenant, token).await?;
    Ok((client, collection))
}

async fn trajectory_op<T, F>(
    client: &FoundationChromaClient,
    tenant: &str,
    fut: F,
) -> Result<T, TrajectoryRouteError>
where
    F: Future<Output = Result<T, TrajectoryError>>,
{
    fut.await.map_err(|err| {
        if err.is_chroma_not_found() {
            client.invalidate_trajectories(tenant);
        }
        TrajectoryRouteError::Trajectory(err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trajectories::{
        ReasoningEntry, ReasoningTrajectory, ReasoningTrajectoryFile, ReasoningWrite,
    };
    use chroma::client::ChromaHttpClientError;
    use serde_json::json;

    fn minimal_file(entries: Vec<ReasoningEntry>) -> ReasoningTrajectoryFile {
        ReasoningTrajectoryFile {
            citations: None,
            trajectory: ReasoningTrajectory {
                id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
                entries,
            },
        }
    }

    fn entry(reasoning: Option<&str>, slugs: &[&str]) -> ReasoningEntry {
        ReasoningEntry {
            reasoning: reasoning.map(str::to_string),
            writes: slugs
                .iter()
                .map(|slug| ReasoningWrite {
                    slug: (*slug).to_string(),
                })
                .collect(),
        }
        .normalized()
        .expect("test entry should have reasoning or writes after normalization")
    }

    #[test]
    fn route_errors_map_complete_contract_codes() {
        let id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        assert_eq!(
            vec![
                TrajectoryRouteError::RouteDisabled.code(),
                TrajectoryRouteError::MissingToken.code(),
                TrajectoryRouteError::MissingSlug.code(),
                TrajectoryRouteError::Resolve(FoundationChromaClientError::InvalidToken(
                    "bad token".to_string(),
                ))
                .code(),
                TrajectoryRouteError::Resolve(FoundationChromaClientError::Client(
                    ChromaHttpClientError::ApiError(
                        "missing".to_string(),
                        reqwest::StatusCode::NOT_FOUND,
                    ),
                ))
                .code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::NotFound { tid: id }).code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::AlreadyExists { tid: id }).code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::EmptyAppend { tid: id }).code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::IdMismatch {
                    path: id,
                    body: Uuid::nil(),
                })
                .code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::FinalizedRequired { tid: id })
                    .code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::EntryCountMismatch {
                    tid: id,
                    expected: 1,
                    actual: 0,
                })
                .code(),
                TrajectoryRouteError::Trajectory(TrajectoryError::NotOpen {
                    tid: id,
                    write_state: crate::trajectories::WriteState::Finalized,
                })
                .code(),
            ],
            vec![
                ErrorCodes::Internal,
                ErrorCodes::InvalidArgument,
                ErrorCodes::InvalidArgument,
                ErrorCodes::InvalidArgument,
                ErrorCodes::NotFound,
                ErrorCodes::NotFound,
                ErrorCodes::AlreadyExists,
                ErrorCodes::InvalidArgument,
                ErrorCodes::InvalidArgument,
                ErrorCodes::FailedPrecondition,
                ErrorCodes::FailedPrecondition,
                ErrorCodes::FailedPrecondition,
            ]
        );
    }

    #[test]
    fn read_query_defaults_to_partial_reads() {
        assert_eq!(
            ReadTrajectoryQuery::default(),
            ReadTrajectoryQuery {
                require_finalized: false,
            }
        );
        assert_eq!(
            serde_json::from_value::<ReadTrajectoryQuery>(json!({})).unwrap(),
            ReadTrajectoryQuery {
                require_finalized: false,
            }
        );
    }

    #[test]
    fn read_query_deserializes_finalized_requirement_opt_in() {
        assert_eq!(
            serde_json::from_value::<ReadTrajectoryQuery>(json!({
                "require_finalized": true
            }))
            .unwrap(),
            ReadTrajectoryQuery {
                require_finalized: true,
            }
        );
    }

    #[test]
    fn reasoning_query_requires_slug_but_allows_root_slug() {
        assert_eq!(
            serde_json::from_value::<ReadTrajectoryReasoningQuery>(json!({
                "require_finalized": true
            }))
            .unwrap(),
            ReadTrajectoryReasoningQuery {
                slug: None,
                require_finalized: true,
            }
        );
        assert_eq!(
            serde_json::from_value::<ReadTrajectoryReasoningQuery>(json!({
                "slug": "",
            }))
            .unwrap(),
            ReadTrajectoryReasoningQuery {
                slug: Some(String::new()),
                require_finalized: false,
            }
        );
    }

    #[test]
    fn reasoning_for_slug_returns_none_when_slug_was_not_written() {
        let file = minimal_file(vec![entry(Some("thinking"), &["other"])]);

        assert_eq!(reasoning_for_slug(&file, "target"), None);
    }

    #[test]
    fn reasoning_for_slug_uses_latest_write_and_prefix_reasoning() {
        let file = minimal_file(vec![
            entry(Some("  first target  "), &["target"]),
            entry(Some("intermediate"), &["other"]),
            entry(Some("final target"), &["target", "sibling", "sibling"]),
            entry(Some("after final target"), &["later"]),
        ]);

        assert_eq!(
            reasoning_for_slug(&file, "target"),
            Some(TrajectoryReasoningResponse {
                slug: "target".to_string(),
                reasoning: vec![
                    "first target".to_string(),
                    "intermediate".to_string(),
                    "final target".to_string(),
                ],
                other_slugs: vec!["sibling".to_string()],
            })
        );
    }

    #[test]
    fn reasoning_for_slug_returns_written_slug_without_reasoning() {
        let file = minimal_file(vec![
            entry(Some("before"), &["other"]),
            entry(None, &["target"]),
            entry(Some("after target"), &["other"]),
        ]);

        assert_eq!(
            reasoning_for_slug(&file, "target"),
            Some(TrajectoryReasoningResponse {
                slug: "target".to_string(),
                reasoning: vec!["before".to_string()],
                other_slugs: Vec::new(),
            })
        );
    }
}

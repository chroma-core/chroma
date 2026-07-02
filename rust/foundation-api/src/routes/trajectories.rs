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
use serde::Deserialize;
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
        AppendTrajectoryEntriesRequest, GenerateTrajectoryFile, TrajectoryError,
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
}

impl ChromaError for TrajectoryRouteError {
    fn code(&self) -> ErrorCodes {
        match self {
            TrajectoryRouteError::RouteDisabled => ErrorCodes::Internal,
            TrajectoryRouteError::MissingToken => ErrorCodes::InvalidArgument,
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
    Json(file): Json<GenerateTrajectoryFile>,
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
    Json(file): Json<GenerateTrajectoryFile>,
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
    Json(file): Json<GenerateTrajectoryFile>,
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
) -> Result<Json<GenerateTrajectoryFile>, ServerError> {
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
    use chroma::client::ChromaHttpClientError;
    use serde_json::json;

    #[test]
    fn route_errors_map_complete_contract_codes() {
        let id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        assert_eq!(
            vec![
                TrajectoryRouteError::RouteDisabled.code(),
                TrajectoryRouteError::MissingToken.code(),
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
}

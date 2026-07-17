use chroma::{types::ConditionalCommitResult, ChromaCollection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::chroma_store::{
    chroma_create_open_trajectory, chroma_extend_open_trajectory_at,
    chroma_finalize_open_trajectory, chroma_load_generate_trajectory,
    chroma_save_generate_trajectory,
};
use super::error::TrajectoryError;
use super::model::{ReasoningEntry, ReasoningTrajectoryFile, WriteState};

/// Request body for appending complete entries to an open trajectory.
///
/// `entries` uses the exact [`ReasoningEntry`] JSON shape. It must be
/// non-empty; callers with no displayable reasoning entry to append should skip
/// the request.
#[derive(Debug, Deserialize)]
pub struct AppendTrajectoryEntriesRequest {
    /// Entry count the caller expects the stored open trajectory to have.
    pub expected_entry_index: usize,
    /// Pruned reasoning entries to append atomically.
    pub entries: Vec<ReasoningEntry>,
}

/// Compact response returned by trajectory write endpoints.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct TrajectoryWriteResponse {
    /// UUID of the logical trajectory resource.
    pub trajectory_id: Uuid,
    /// Durable trajectory storage state after the write.
    pub write_state: WriteState,
    /// Number of committed trajectory entries after the write.
    pub entry_count: usize,
    /// Number of Chroma storage records committed by this transaction.
    pub record_count: usize,
    /// First inserted log offset reported by Chroma, when available.
    pub first_inserted_record_offset: Option<i64>,
}

impl TrajectoryWriteResponse {
    fn from_commit(
        trajectory_id: Uuid,
        write_state: WriteState,
        entry_count: usize,
        commit: ConditionalCommitResult,
    ) -> Self {
        Self {
            trajectory_id,
            write_state,
            entry_count,
            record_count: commit.record_count,
            first_inserted_record_offset: commit.first_inserted_record_offset,
        }
    }
}

/// Save a complete trajectory as finalized records in one transaction.
///
/// This is an overwrite/upsert operation for backfills, reprocessing, and
/// one-shot imports. Use [`create_open_generate_trajectory`] plus appends for
/// live incremental writes.
pub async fn save_generate_trajectory(
    collection: &ChromaCollection,
    file: &ReasoningTrajectoryFile,
) -> Result<TrajectoryWriteResponse, TrajectoryError> {
    let mut txn = collection.conditional();
    chroma_save_generate_trajectory(&mut txn, file).await?;
    let commit = txn.commit().await?;
    Ok(TrajectoryWriteResponse::from_commit(
        file.trajectory.id,
        WriteState::Finalized,
        file.trajectory.entries.len(),
        commit,
    ))
}

/// Create an open trajectory with zero committed entries.
///
/// The supplied [`ReasoningTrajectoryFile`] must already be an open skeleton:
/// `trajectory.entries` must be empty. The API deliberately does not strip
/// entries from the body. If the caller has a complete file, use
/// [`save_generate_trajectory`] instead.
pub async fn create_open_generate_trajectory(
    collection: &ChromaCollection,
    file: &ReasoningTrajectoryFile,
) -> Result<TrajectoryWriteResponse, TrajectoryError> {
    let mut txn = collection.conditional();
    chroma_create_open_trajectory(&mut txn, file).await?;
    let commit = txn.commit().await?;
    Ok(TrajectoryWriteResponse::from_commit(
        file.trajectory.id,
        WriteState::Open,
        0,
        commit,
    ))
}

/// Append complete entries to an open trajectory.
///
/// The append is conditional on `request.expected_entry_index` matching the
/// stored open header's entry count. A stale expectation fails with a
/// precondition error instead of being retried inside the server.
pub async fn append_open_generate_trajectory(
    collection: &ChromaCollection,
    tid: Uuid,
    request: &AppendTrajectoryEntriesRequest,
) -> Result<TrajectoryWriteResponse, TrajectoryError> {
    if request.entries.is_empty() {
        return Err(TrajectoryError::EmptyAppend { tid });
    }

    let mut txn = collection.conditional();
    let next_entry = chroma_extend_open_trajectory_at(
        &mut txn,
        tid,
        request.expected_entry_index,
        &request.entries,
    )
    .await?;
    let commit = txn.commit().await?;
    Ok(TrajectoryWriteResponse::from_commit(
        tid,
        WriteState::Open,
        next_entry,
        commit,
    ))
}

/// Finalize an existing open trajectory using a complete final file.
///
/// The path UUID and body UUID must match, and the stored entry count must
/// equal the number of entries in `file`. If a caller wants to write a complete
/// trajectory without first appending every entry, use
/// [`save_generate_trajectory`].
pub async fn finalize_open_generate_trajectory(
    collection: &ChromaCollection,
    tid: Uuid,
    file: &ReasoningTrajectoryFile,
) -> Result<TrajectoryWriteResponse, TrajectoryError> {
    if file.trajectory.id != tid {
        return Err(TrajectoryError::IdMismatch {
            path: tid,
            body: file.trajectory.id,
        });
    }

    let mut txn = collection.conditional();
    chroma_finalize_open_trajectory(&mut txn, file).await?;
    let commit = txn.commit().await?;
    Ok(TrajectoryWriteResponse::from_commit(
        tid,
        WriteState::Finalized,
        file.trajectory.entries.len(),
        commit,
    ))
}

/// Load a trajectory by UUID.
///
/// Set `require_finalized` to reject open trajectories. The default HTTP read
/// route leaves this false so callers can inspect a live partial trajectory.
pub async fn load_generate_trajectory(
    collection: &ChromaCollection,
    tid: Uuid,
    require_finalized: bool,
) -> Result<ReasoningTrajectoryFile, TrajectoryError> {
    let mut txn = collection.conditional();
    let file = chroma_load_generate_trajectory(&mut txn, tid, require_finalized).await?;
    let _ = txn.commit().await?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use chroma::{
        client::{ChromaHttpClientOptions, ChromaRetryOptions},
        types::ConditionalCommitResult,
        ChromaHttpClient,
    };
    use chroma_error::{ChromaError, ErrorCodes};
    use chroma_types::Collection;

    use super::*;
    use crate::trajectories::{ReasoningTrajectory, ReasoningWrite};

    fn dummy_collection() -> ChromaCollection {
        let client = ChromaHttpClient::new(ChromaHttpClientOptions {
            endpoint: "http://127.0.0.1:9".parse().unwrap(),
            retry_options: ChromaRetryOptions {
                max_retries: 0,
                ..Default::default()
            },
            ..Default::default()
        });
        ChromaCollection::from_collection_model(
            client,
            Collection {
                tenant: "tenant".to_string(),
                database: "FOUNDATION".to_string(),
                ..Default::default()
            },
        )
    }

    fn minimal_file(id: Uuid, entries: Vec<ReasoningEntry>) -> ReasoningTrajectoryFile {
        ReasoningTrajectoryFile {
            citations: None,
            trajectory: ReasoningTrajectory { id, entries },
        }
    }

    fn reasoning_entry() -> ReasoningEntry {
        ReasoningEntry {
            reasoning: Some("reason".to_string()),
            writes: vec![ReasoningWrite {
                slug: "page".to_string(),
            }],
        }
    }

    #[test]
    fn write_response_from_commit_preserves_complete_commit_result() {
        let trajectory_id = Uuid::parse_str("00000000-0000-0000-0000-000000000011").unwrap();
        assert_eq!(
            TrajectoryWriteResponse::from_commit(
                trajectory_id,
                WriteState::Finalized,
                7,
                ConditionalCommitResult {
                    first_inserted_record_offset: Some(42),
                    record_count: 19,
                },
            ),
            TrajectoryWriteResponse {
                trajectory_id,
                write_state: WriteState::Finalized,
                entry_count: 7,
                record_count: 19,
                first_inserted_record_offset: Some(42),
            }
        );
    }

    #[tokio::test]
    async fn append_empty_entries_is_rejected_before_chroma() {
        let collection = dummy_collection();
        let trajectory_id = Uuid::parse_str("00000000-0000-0000-0000-000000000012").unwrap();
        let request = AppendTrajectoryEntriesRequest {
            expected_entry_index: 0,
            entries: Vec::new(),
        };

        let error = append_open_generate_trajectory(&collection, trajectory_id, &request)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            TrajectoryError::EmptyAppend { tid } if tid == trajectory_id
        ));
        assert_eq!(error.code(), ErrorCodes::InvalidArgument);
    }

    #[tokio::test]
    async fn finalize_rejects_path_body_id_mismatch_before_chroma() {
        let collection = dummy_collection();
        let path_id = Uuid::parse_str("00000000-0000-0000-0000-000000000013").unwrap();
        let body_id = Uuid::parse_str("00000000-0000-0000-0000-000000000014").unwrap();
        let file = minimal_file(body_id, vec![reasoning_entry()]);

        let error = finalize_open_generate_trajectory(&collection, path_id, &file)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            TrajectoryError::IdMismatch { path, body } if path == path_id && body == body_id
        ));
        assert_eq!(error.code(), ErrorCodes::InvalidArgument);
    }

    #[tokio::test]
    async fn open_create_rejects_non_empty_file_before_chroma() {
        let collection = dummy_collection();
        let trajectory_id = Uuid::parse_str("00000000-0000-0000-0000-000000000015").unwrap();
        let file = minimal_file(trajectory_id, vec![reasoning_entry()]);

        let error = create_open_generate_trajectory(&collection, &file)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid value: chroma_create_open_trajectory requires zero committed entries, got 1"
        );
        assert_eq!(error.code(), ErrorCodes::InvalidArgument);
    }
}

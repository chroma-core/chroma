use chroma_types::chroma_proto::{
    compactor_client::CompactorClient, CollectionIds, CompactRequest,
    GetCollectionAssignmentRequest, ListInProgressJobsRequest, RebuildRequest, SegmentScope,
};
use clap::{Parser, Subcommand};
use std::io::Write;
use thiserror::Error;
use tonic::transport::Channel;
use uuid::Uuid;

/// Error for compaction client
#[derive(Debug, Error)]
pub enum CompactionClientError {
    #[error("Compactor failed: {0}")]
    Compactor(String),
    #[error("Unable to connect to compactor: {0}")]
    Connection(#[from] tonic::transport::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Tool to control compaction service
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct CompactionClient {
    /// Url of the target compactor
    #[arg(short, long)]
    url: String,
    /// Subcommand for compaction
    #[command(subcommand)]
    command: CompactionCommand,
}

#[derive(Debug, Subcommand)]
pub enum CompactionCommand {
    /// Trigger a one-off compaction
    Compact {
        /// Specify Uuids of the collections to compact
        #[arg(short, long)]
        id: Vec<Uuid>,
    },
    Rebuild {
        /// Specify Uuids of the collections to rebuild
        #[arg(short, long)]
        id: Vec<Uuid>,
        /// Specify which segment scopes to rebuild (metadata, vector)
        /// Can be specified multiple times. If not specified, rebuilds all segments.
        #[arg(long = "segment", value_parser = ["metadata", "vector"])]
        segment_scopes: Vec<String>,
    },
    /// List all in-progress compaction jobs
    ListInProgressJobs,
    /// Get collection assignment info (which node would handle a collection)
    GetCollectionAssignment {
        /// Collection ID to check assignment for
        #[arg(short, long)]
        collection_id: Uuid,
    },
}

impl CompactionClient {
    async fn grpc_client(&self) -> Result<CompactorClient<Channel>, CompactionClientError> {
        Ok(CompactorClient::connect(self.url.clone()).await?)
    }

    pub async fn run(&self, w: &mut dyn Write) -> Result<(), CompactionClientError> {
        match &self.command {
            CompactionCommand::Compact { id } => {
                let mut client = self.grpc_client().await?;
                let response = client
                    .compact(CompactRequest {
                        ids: Some(CollectionIds {
                            ids: id.iter().map(ToString::to_string).collect(),
                        }),
                    })
                    .await;
                if let Err(status) = response {
                    return Err(CompactionClientError::Compactor(status.to_string()));
                }
            }
            CompactionCommand::Rebuild { id, segment_scopes } => {
                let mut client = self.grpc_client().await?;
                // Convert CLI strings to proto SegmentScope i32 values
                let mut proto_scopes: Vec<i32> = segment_scopes
                    .iter()
                    .map(|scope| match scope.as_str() {
                        "metadata" => SegmentScope::Metadata as i32,
                        "vector" => SegmentScope::Vector as i32,
                        _ => unreachable!(), // value_parser guarantees valid values
                    })
                    .collect();
                proto_scopes.sort();
                proto_scopes.dedup();

                let response = client
                    .rebuild(RebuildRequest {
                        ids: Some(CollectionIds {
                            ids: id.iter().map(ToString::to_string).collect(),
                        }),
                        segment_scopes: proto_scopes,
                    })
                    .await;
                if let Err(status) = response {
                    return Err(CompactionClientError::Compactor(status.to_string()));
                }
            }
            CompactionCommand::ListInProgressJobs => {
                let mut client = self.grpc_client().await?;
                let response = client
                    .list_in_progress_jobs(ListInProgressJobsRequest {})
                    .await
                    .map_err(|e| CompactionClientError::Compactor(e.to_string()))?;

                let jobs = response.into_inner().jobs;
                writeln!(w, "In-progress compaction jobs:")?;
                if jobs.is_empty() {
                    writeln!(w, "  None")?;
                } else {
                    for job in jobs {
                        writeln!(
                            w,
                            "  job_id={} database={} expires_at_epoch_secs={}",
                            job.job_id, job.database_name, job.expires_at_epoch_secs
                        )?;
                    }
                }
            }
            CompactionCommand::GetCollectionAssignment { collection_id } => {
                let mut client = self.grpc_client().await?;
                let response = client
                    .get_collection_assignment(GetCollectionAssignmentRequest {
                        collection_id: collection_id.to_string(),
                    })
                    .await
                    .map_err(|e| CompactionClientError::Compactor(e.to_string()))?;

                let assignment = response.into_inner();
                writeln!(w, "Collection assignment information:")?;
                writeln!(w, "  Collection ID: {}", collection_id)?;
                writeln!(w, "  Assigned to node: {}", assignment.assigned_node)?;
                writeln!(w, "  Current memberlist:")?;
                if assignment.memberlist.is_empty() {
                    writeln!(w, "    (empty)")?;
                } else {
                    for member in assignment.memberlist {
                        writeln!(w, "    - {}", member)?;
                    }
                }
            }
        };
        Ok(())
    }
}

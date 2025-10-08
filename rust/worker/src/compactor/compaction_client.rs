use chroma_types::chroma_proto::{
    compactor_client::CompactorClient, CollectionIds, CompactRequest, ListDeadJobsRequest,
    RebuildRequest,
};
use clap::{Parser, Subcommand};
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
    },
    /// List all dead jobs (collections with failed compactions)
    ListDeadJobs,
}

impl CompactionClient {
    async fn grpc_client(&self) -> Result<CompactorClient<Channel>, CompactionClientError> {
        Ok(CompactorClient::connect(self.url.clone()).await?)
    }

    pub async fn run(&self) -> Result<(), CompactionClientError> {
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
            CompactionCommand::Rebuild { id } => {
                let mut client = self.grpc_client().await?;
                let response = client
                    .rebuild(RebuildRequest {
                        ids: Some(CollectionIds {
                            ids: id.iter().map(ToString::to_string).collect(),
                        }),
                    })
                    .await;
                if let Err(status) = response {
                    return Err(CompactionClientError::Compactor(status.to_string()));
                }
            }
            CompactionCommand::ListDeadJobs => {
                let mut client = self.grpc_client().await?;
                let response = client
                    .list_dead_jobs(ListDeadJobsRequest {})
                    .await
                    .map_err(|e| CompactionClientError::Compactor(e.to_string()))?;

                let dead_jobs = response.into_inner();
                if let Some(ids) = dead_jobs.ids {
                    println!("Dead jobs (collections with failed compactions):");
                    if ids.ids.is_empty() {
                        println!("  None");
                    } else {
                        for id in ids.ids {
                            println!("  {}", id);
                        }
                    }
                } else {
                    println!("No dead jobs response didn't contain an ids field");
                }
            }
        };
        Ok(())
    }
}

use chroma_types::{CollectionFlushInfo, CollectionUuid, DatabaseName};
use clap::{Parser, Subcommand};
use std::str::FromStr;
use std::sync::Arc;
use worker::work_queue::work_queue_client::WorkQueueClient;

#[derive(Parser)]
#[command(name = "work-queue-cli")]
#[command(about = "CLI for interacting with the work queue service", long_about = None)]
struct Cli {
    /// Work queue service endpoint
    #[arg(short, long, default_value = "http://localhost:50058")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Push work to the queue
    Push {
        /// Function ID (UUID format)
        #[arg(short, long)]
        function_id: String,

        /// Collection ID (UUID format)
        #[arg(short, long)]
        collection_id: String,

        /// Completion offset
        #[arg(short, long)]
        offset: i64,
    },

    /// Get work from the queue
    Get {
        /// Shard ID (worker identifier)
        #[arg(short, long, default_value = "worker-0")]
        shard_id: String,

        /// Maximum number of items to retrieve
        #[arg(short, long, default_value = "10")]
        limit: u32,
    },

    /// Mark work as finished
    Finish {
        /// Function ID (UUID format)
        #[arg(short, long)]
        function_id: String,

        /// Collection ID (UUID format)
        #[arg(short, long)]
        collection_id: String,

        /// New completion offset
        #[arg(short, long)]
        offset: i64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut client = WorkQueueClient::new(cli.endpoint.clone()).await?;

    match cli.command {
        Commands::Push {
            function_id,
            collection_id,
            offset,
        } => {
            client.push_work(function_id, collection_id, offset).await?;
            println!("✓ Work pushed successfully");
        }

        Commands::Get { shard_id, limit } => {
            let response = client.get_work(shard_id, limit).await?;

            if response.items.is_empty() {
                println!("No work items available");
            } else {
                println!("Found {} work items:", response.items.len());
                for item in response.items {
                    println!(
                        "  - fn_id: {}, coll_id: {}, offset: {}",
                        item.fn_id, item.input_coll_id, item.completion_offset
                    );
                }
            }
        }

        Commands::Finish {
            function_id,
            collection_id,
            offset,
        } => {
            let output_collection_flush = CollectionFlushInfo {
                tenant_id: "default_tenant".to_string(),
                database_name: DatabaseName::new("default_database".to_string()).unwrap(),
                collection_id: CollectionUuid::from_str(&collection_id)?,
                log_position: offset,
                collection_version: 0,
                segment_flush_info: Arc::from([]),
                total_records_post_compaction: 0,
                size_bytes_post_compaction: 0,
                schema: None,
            };
            client
                .finish_work(function_id, collection_id, offset, output_collection_flush)
                .await?;
            println!("✓ Work marked as finished");
        }
    }

    Ok(())
}

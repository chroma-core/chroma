use clap::{Parser, Subcommand};
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

        /// Compaction offset
        #[arg(long)]
        compaction_offset: i64,
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

    /// Reset a dead-lettered function's failure count so it can be processed again
    Undlq {
        /// Function ID (UUID format)
        #[arg(short, long)]
        function_id: String,

        /// Collection ID (UUID format)
        #[arg(short, long)]
        collection_id: String,
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
            compaction_offset,
        } => {
            client
                .push_work(function_id, collection_id, offset, compaction_offset)
                .await?;
            println!("✓ Work pushed successfully");
        }

        Commands::Get { shard_id, limit } => {
            let response = client
                .get_work_with_failure_limit(shard_id, limit, i32::MAX)
                .await?;

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
            client
                .finish_work(function_id, collection_id, offset)
                .await?;
            println!("✓ Work marked as finished");
        }

        Commands::Undlq {
            function_id,
            collection_id,
        } => {
            client
                .set_function_failure_count(function_id, collection_id, 0)
                .await?;
            println!("✓ Function removed from DLQ");
        }
    }

    Ok(())
}

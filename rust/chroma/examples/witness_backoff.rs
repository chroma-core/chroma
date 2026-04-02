//! Witness write backpressure and inspect the embedded indexing status.
//!
//! Set `CHROMA_ENDPOINT` (and auth env vars if needed), then run:
//!
//! ```bash
//! CHROMA_BACKPRESSURE_COLLECTION=backpressure-demo cargo run --example witness_backoff
//! ```
//!
//! This example keeps writing batches until the server returns a 429-style
//! backpressure response. When that happens, it prints the embedded indexing
//! status that can be used to decide how long to back off before retrying.

use chroma::client::ChromaHttpClientError;
use chroma::ChromaHttpClient;
use reqwest::StatusCode;

const BATCH_SIZE: usize = 256;
const MAX_RECORDS: usize = 10_000_000;
type DynError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let collection_name = std::env::var("CHROMA_BACKPRESSURE_COLLECTION")
        .unwrap_or_else(|_| "backpressure-demo".to_string());
    let client = ChromaHttpClient::from_env().map_err(|err| Box::new(err) as DynError)?;
    let collection = client
        .get_or_create_collection(collection_name, None, None)
        .await
        .map_err(|err| Box::new(err) as DynError)?;

    for start in (0..MAX_RECORDS).step_by(BATCH_SIZE) {
        let ids = (start..start + BATCH_SIZE)
            .map(|offset| format!("backpressure-{offset}"))
            .collect::<Vec<_>>();
        let embeddings = (start..start + BATCH_SIZE)
            .map(|offset| vec![offset as f32, 0.0, 1.0])
            .collect::<Vec<_>>();

        match collection.add(ids, embeddings, None, None, None).await {
            Ok(_) => {
                if start > 0 && start % 10_000 == 0 {
                    println!("wrote {start} records without backpressure");
                }
            }
            Err(ChromaHttpClientError::ApiError {
                status,
                message,
                indexing_status: Some(indexing_status),
            }) if status == StatusCode::TOO_MANY_REQUESTS => {
                println!("{message}");
                println!(
                    "indexing progress: {:.1}% (indexed {} / total {})",
                    indexing_status.op_indexing_progress * 100.0,
                    indexing_status.num_indexed_ops,
                    indexing_status.total_ops,
                );
                println!(
                    "unindexed ops remaining: {}",
                    indexing_status.num_unindexed_ops
                );
                return Ok(());
            }
            Err(ChromaHttpClientError::ApiError {
                status, message, ..
            }) if status == StatusCode::TOO_MANY_REQUESTS => {
                println!("{message}");
                println!("backpressure did not include indexing status");
                return Ok(());
            }
            Err(err) => return Err(Box::new(err) as DynError),
        }
    }

    println!("did not observe backpressure before MAX_RECORDS={MAX_RECORDS}");
    Ok(())
}

use std::io::Error;
use std::sync::Arc;
use std::time::Instant;

use chroma::bench::{create_client, GaussianMixtureModel};
use chroma::types::{Key, QueryVector, RankExpr, SearchPayload};
use clap::Parser;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const DEFAULT_TOTAL_OPS: usize = 1_000_000;
const DEFAULT_BATCH_SIZE: usize = 300;
const DEFAULT_MAX_OUTSTANDING_OPS: usize = 10;

#[derive(Parser, Debug)]
#[command(name = "fill_collection_to_1e6")]
struct Args {
    #[arg(long, default_value = "https://api.devchroma.com:443")]
    endpoint: String,

    #[arg(long, default_value = "test-disabled-compaction")]
    collection: String,

    #[arg(long, default_value_t = DEFAULT_TOTAL_OPS)]
    total_ops: usize,

    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,

    #[arg(long, default_value_t = DEFAULT_MAX_OUTSTANDING_OPS)]
    max_outstanding_ops: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.batch_size == 0 {
        return Err(Error::other("--batch-size must be greater than 0").into());
    }
    if args.max_outstanding_ops == 0 {
        return Err(Error::other("--max-outstanding-ops must be greater than 0").into());
    }

    let client = create_client(&args.endpoint)?;
    let collection = client
        .get_or_create_collection(&args.collection, None, None)
        .await?;

    let total_batches = args.total_ops.div_ceil(args.batch_size);

    println!("Collection: {} ({})", args.collection, collection.id());
    println!("Endpoint: {}", args.endpoint);
    println!("Total ops: {}", args.total_ops);
    println!("Batch size: {}", args.batch_size);
    println!("Outstanding ops: {}", args.max_outstanding_ops);
    println!("Total batches: {}", total_batches);

    let gmm = Arc::new(GaussianMixtureModel::new(42));

    println!(
        "Starting query verification loop ({} outstanding)...",
        args.max_outstanding_ops
    );
    let mut query_rng = StdRng::seed_from_u64(12345);
    let mut query_count: u64 = 0;
    let mut in_flight = FuturesUnordered::new();

    loop {
        while in_flight.len() < args.max_outstanding_ops {
            let batch_idx = query_rng.gen_range(0..total_batches);
            let start_idx = batch_idx * args.batch_size;
            let batch_size = (args.total_ops - start_idx).min(args.batch_size);

            let mut batch_rng = StdRng::seed_from_u64(42 + batch_idx as u64);
            let embeddings = gmm.generate_batch(&mut batch_rng, batch_size);

            let pick = query_rng.gen_range(0..batch_size);
            let query_embedding = embeddings[pick].clone();
            let expected_id = format!("id_{:07}", start_idx + pick);

            let search = SearchPayload::default()
                .rank(RankExpr::Knn {
                    query: QueryVector::Dense(query_embedding),
                    key: Key::Embedding,
                    limit: 10,
                    default: None,
                    return_rank: false,
                })
                .limit(Some(10), 0)
                .select([Key::Score]);

            let collection = collection.clone();
            let query_start = Instant::now();
            in_flight.push(async move {
                let result = collection.search(vec![search]).await;
                (expected_id, query_start, result)
            });
        }

        if let Some((expected_id, query_start, result)) = in_flight.next().await {
            query_count += 1;
            match result {
                Ok(response) => {
                    let elapsed_ms = query_start.elapsed().as_millis();
                    let found = response
                        .ids
                        .first()
                        .map(|ids| ids.contains(&expected_id))
                        .unwrap_or(false);
                    let top_ids: Vec<&str> = response
                        .ids
                        .first()
                        .map(|ids| ids.iter().map(|s| s.as_str()).collect())
                        .unwrap_or_default();
                    if found {
                        println!(
                            "[query {}] OK  expected={} found in top-10  ({}ms)  top: {:?}",
                            query_count, expected_id, elapsed_ms, top_ids
                        );
                    } else {
                        println!(
                            "[query {}] MISS expected={} NOT in top-10  ({}ms)  top: {:?}",
                            query_count, expected_id, elapsed_ms, top_ids
                        );
                    }
                }
                Err(err) => {
                    println!("[query {}] ERROR: {}", query_count, err);
                }
            }
        }
    }
}

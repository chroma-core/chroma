use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::AsArray;
use futures::{stream, StreamExt, TryStreamExt};
use hf_hub::api::tokio::Api;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

pub struct TheStackDedupRust {
    pub shard_paths: Vec<PathBuf>,
}

impl TheStackDedupRust {
    pub async fn init() -> Result<Self> {
        let mut shard_paths = Vec::new();
        let api = Api::new()?;
        let dataset = api.dataset("bigcode/the-stack-dedup".to_string());
        for i in 0..21 {
            let shard_path = format!("data/rust/data-{:05}-of-00021.parquet", i);
            let local_path = dataset.get(&shard_path).await?;
            shard_paths.push(local_path);
        }

        Ok(Self { shard_paths })
    }

    pub async fn documents(&self) -> Result<Vec<String>> {
        let mut shard_streams = Vec::new();
        for shard_path in &self.shard_paths {
            let file = File::open(shard_path).await?;
            let shard_stream = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;
            shard_streams.push(shard_stream);
        }
        let batches = stream::iter(shard_streams)
            .flatten()
            .try_collect::<Vec<_>>()
            .await?;
        let mut documents = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
        for batch in batches {
            documents.extend(
                batch
                    .column_by_name("content")
                    .context("Inspecting content column")?
                    .as_string::<i32>()
                    .iter()
                    .map(|doc| doc.unwrap_or_default().to_string()),
            );
        }
        Ok(documents)
    }
}

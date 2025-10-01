use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::{Array, AsArray};
use futures::{stream, StreamExt, TryStreamExt};
use hf_hub::api::tokio::Api;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

pub struct WikipediaSplade {
    pub shard_paths: Vec<PathBuf>,
}

impl WikipediaSplade {
    pub async fn init() -> Result<Self> {
        let mut shard_paths = Vec::new();
        let api = Api::new()?;
        let dataset = api.dataset("Sicheng-Chroma/wikipedia-en-splade-bge".to_string());

        // Download all 7 shards
        for i in 0..1 {
            let shard_path = format!("train-{:05}-of-00007.parquet", i);
            let local_path = dataset.get(&shard_path).await?;
            shard_paths.push(local_path);
        }

        Ok(Self { shard_paths })
    }

    pub async fn documents(&self) -> Result<Vec<SparseDocument>> {
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
            // Extract required fields
            let texts = batch
                .column_by_name("text")
                .context("Missing text column")?
                .as_string::<i32>();

            let titles = batch
                .column_by_name("title")
                .context("Missing title column")?
                .as_string::<i32>();

            let urls = batch
                .column_by_name("url")
                .context("Missing url column")?
                .as_string::<i32>();

            let sparse_indices = batch
                .column_by_name("sparse_embedding_indices")
                .context("Missing sparse_embedding_indices column")?
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .context("sparse_embedding_indices is not a list array")?;

            let sparse_values = batch
                .column_by_name("sparse_embedding_values")
                .context("Missing sparse_embedding_values column")?
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .context("sparse_embedding_values is not a list array")?;

            for i in 0..batch.num_rows() {
                let text = texts.value(i).to_string();
                let title = titles.value(i).to_string();
                let url = urls.value(i).to_string();

                // Extract sparse vector indices and values
                let indices_array = sparse_indices.value(i);
                let values_array = sparse_values.value(i);

                let indices = indices_array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .context("indices is not Int32Array")?;

                let values = values_array
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .context("values is not Float32Array")?;

                // Build sparse vector
                let mut sparse_indices_vec = Vec::new();
                let mut sparse_values_vec = Vec::new();

                for j in 0..indices.len() {
                    sparse_indices_vec.push(indices.value(j) as usize);
                    sparse_values_vec.push(values.value(j));
                }

                let sparse_vector = sprs::CsVec::new(30522, sparse_indices_vec, sparse_values_vec); // SPLADE vocab size is 30522

                documents.push(SparseDocument {
                    doc_id: url.clone(), // Use URL as doc_id for consistency
                    url,
                    title,
                    body: text,
                    sparse_vector,
                });
            }
        }

        Ok(documents)
    }

    pub async fn queries(query_path: &str) -> Result<Vec<SparseQuery>> {
        let file = File::open(query_path).await?;
        let stream = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        let mut queries = Vec::new();

        for batch in batches {
            let topics = batch
                .column_by_name("topic")
                .context("Missing topic column")?
                .as_string::<i32>();

            let query_texts = batch
                .column_by_name("query")
                .context("Missing query column")?
                .as_string::<i32>();

            let sparse_indices = batch
                .column_by_name("sparse_embedding_indices")
                .context("Missing sparse_embedding_indices column")?
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .context("sparse_embedding_indices is not a list array")?;

            let sparse_values = batch
                .column_by_name("sparse_embedding_values")
                .context("Missing sparse_embedding_values column")?
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .context("sparse_embedding_values is not a list array")?;

            for i in 0..batch.num_rows() {
                let topic = topics.value(i).to_string();
                let query_text = query_texts.value(i).to_string();

                // Extract sparse vector indices and values
                let indices_array = sparse_indices.value(i);
                let values_array = sparse_values.value(i);

                let indices = indices_array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .context("indices is not Int32Array")?;

                let values = values_array
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .context("values is not Float32Array")?;

                // Build sparse vector
                let mut sparse_indices_vec = Vec::new();
                let mut sparse_values_vec = Vec::new();

                for j in 0..indices.len() {
                    sparse_indices_vec.push(indices.value(j) as usize);
                    sparse_values_vec.push(values.value(j));
                }

                let sparse_vector = sprs::CsVec::new(30522, sparse_indices_vec, sparse_values_vec);

                queries.push(SparseQuery {
                    query_id: format!("{}_{}", topic, i),
                    text: query_text,
                    sparse_vector,
                });
            }
        }

        Ok(queries)
    }
}

#[derive(Debug, Clone)]
pub struct SparseDocument {
    pub doc_id: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub sparse_vector: sprs::CsVec<f32>,
}

#[derive(Debug, Clone)]
pub struct SparseQuery {
    pub query_id: String,
    pub text: String,
    pub sparse_vector: sprs::CsVec<f32>,
}

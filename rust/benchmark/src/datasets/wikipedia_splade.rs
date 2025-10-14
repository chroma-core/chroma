use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::{
    array::AsArray,
    datatypes::{Float32Type, Int32Type},
    record_batch::RecordBatch,
};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use hf_hub::api::tokio::Api;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use sprs::CsVec;
use tokio::fs::File;

const SPLADE_VOCAB_SIZE: usize = 30522;

pub struct WikipediaSplade {
    pub train_paths: Vec<PathBuf>,
    pub test_path: PathBuf,
}

impl WikipediaSplade {
    pub async fn init() -> Result<Self> {
        let api = Api::new()?;
        let dataset = api.dataset("Sicheng-Chroma/wikipedia-en-splade-bge".to_string());

        // Download first shard from train directory
        let mut train_paths = Vec::new();
        let train_shard = "train/train-00000-of-00007.parquet";
        let train_path = dataset.get(train_shard).await?;
        train_paths.push(train_path);

        // Download test queries
        let test_path = dataset.get("test/test-00000-of-00001.parquet").await?;

        Ok(Self {
            train_paths,
            test_path,
        })
    }

    pub async fn documents(&self) -> Result<impl Stream<Item = Result<SparseDocument>> + '_> {
        let mut shard_streams = Vec::new();
        for shard_path in &self.train_paths {
            let file = File::open(shard_path).await?;
            let shard_stream = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;
            shard_streams.push(shard_stream);
        }

        Ok(stream::iter(shard_streams)
            .flatten()
            .map(|res| {
                res.map_err(Into::into).and_then(|batch| {
                    Self::batch_to_documents(batch)
                        .map(|docs| stream::iter(docs.into_iter().map(Ok)))
                })
            })
            .try_flatten())
    }

    // Helper to convert a batch to documents
    // Returns Vec for now since we need to access columns multiple times
    fn batch_to_documents(batch: RecordBatch) -> Result<Vec<SparseDocument>> {
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
            .as_list::<i32>();

        let sparse_values = batch
            .column_by_name("sparse_embedding_values")
            .context("Missing sparse_embedding_values column")?
            .as_list::<i32>();

        let mut documents = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let text = texts.value(i).to_string();
            let title = titles.value(i).to_string();
            let url = urls.value(i).to_string();

            let indices = sparse_indices.value(i);
            let values = sparse_values.value(i);

            let indices_array = indices.as_primitive::<Int32Type>();
            let values_array = values.as_primitive::<Float32Type>();

            let mut sparse_indices_vec = Vec::with_capacity(indices_array.len());
            let mut sparse_values_vec = Vec::with_capacity(values_array.len());

            for j in 0..indices_array.len() {
                sparse_indices_vec.push(indices_array.value(j) as usize);
                sparse_values_vec.push(values_array.value(j));
            }

            let sparse_vector =
                CsVec::new(SPLADE_VOCAB_SIZE, sparse_indices_vec, sparse_values_vec);

            documents.push(SparseDocument {
                doc_id: url.clone(),
                url,
                title,
                body: text,
                sparse_vector,
            });
        }

        Ok(documents)
    }

    pub async fn queries(&self) -> Result<Vec<SparseQuery>> {
        // Use the already downloaded test file
        let file = File::open(&self.test_path).await?;
        let stream = ParquetRecordBatchStreamBuilder::new(file).await?.build()?;

        let batches = stream.try_collect::<Vec<_>>().await?;
        let mut queries = Vec::new();

        for batch in batches {
            // Try to get query_id if it exists, otherwise generate it
            let query_ids = batch
                .column_by_name("query_id")
                .map(|col| col.as_string::<i32>());

            let query_texts = batch
                .column_by_name("text")
                .or_else(|| batch.column_by_name("query"))
                .context("Missing text/query column")?
                .as_string::<i32>();

            let sparse_indices = batch
                .column_by_name("sparse_embedding_indices")
                .context("Missing sparse_embedding_indices column")?
                .as_list::<i32>();

            let sparse_values = batch
                .column_by_name("sparse_embedding_values")
                .context("Missing sparse_embedding_values column")?
                .as_list::<i32>();

            for i in 0..batch.num_rows() {
                let query_id = if let Some(ids) = query_ids {
                    ids.value(i).to_string()
                } else {
                    format!("query_{}", i)
                };
                let query_text = query_texts.value(i).to_string();

                // Extract sparse vector indices and values
                let indices_array = sparse_indices.value(i);
                let values_array = sparse_values.value(i);

                let indices = indices_array.as_primitive::<Int32Type>();
                let values = values_array.as_primitive::<Float32Type>();

                // Build sparse vector
                let mut sparse_indices_vec = Vec::new();
                let mut sparse_values_vec = Vec::new();

                for j in 0..indices.len() {
                    sparse_indices_vec.push(indices.value(j) as usize);
                    sparse_values_vec.push(values.value(j));
                }

                let sparse_vector = CsVec::new(30522, sparse_indices_vec, sparse_values_vec);

                queries.push(SparseQuery {
                    query_id,
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
    pub sparse_vector: CsVec<f32>,
}

#[derive(Debug, Clone)]
pub struct SparseQuery {
    pub query_id: String,
    pub text: String,
    pub sparse_vector: CsVec<f32>,
}

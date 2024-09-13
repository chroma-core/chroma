use std::{collections::HashMap, path::PathBuf};

use crate::traits::{get_dataset_cache_path, TestDataset, TestDatasetDocument};
use async_compression::tokio::bufread::GzipDecoder;
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::{wrappers::LinesStream, Stream};
use tokio_util::io::StreamReader;

#[derive(Deserialize, Debug)]
struct SciDocsCorpusLine {
    _id: String,
    title: String,
    text: String,
}

/// Dataset from https://huggingface.co/datasets/BeIR/scidocs
pub struct SciDocsDataset {}

impl SciDocsDataset {
    async fn get_filepath() -> Result<PathBuf, std::io::Error> {
        let dataset_dir = get_dataset_cache_path("scidocs").await?;
        Ok(dataset_dir.join("corpus.jsonl"))
    }
}

impl TestDataset for SciDocsDataset {
    async fn init() -> Result<Self, std::io::Error> {
        let corpus_filepath = SciDocsDataset::get_filepath().await?;

        if !corpus_filepath.exists() {
            let file = File::create(corpus_filepath).await?;

            let client = reqwest::Client::new();
            let response = client
                .get("https://huggingface.co/datasets/BeIR/scidocs/resolve/main/corpus.jsonl.gz")
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;

            if !response.status().is_success() {
                panic!("Failed to download SciDocs dataset");
            }

            let byte_stream = response.bytes_stream();
            let stream_reader = StreamReader::new(
                byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            let mut decoder = GzipDecoder::new(stream_reader);
            let mut file_writer = tokio::io::BufWriter::new(file);
            tokio::io::copy(&mut decoder, &mut file_writer).await?;
        }

        Ok(SciDocsDataset {})
    }

    async fn create_documents_stream(
        &self,
    ) -> Result<impl Stream<Item = TestDatasetDocument>, std::io::Error> {
        let file = File::open(SciDocsDataset::get_filepath().await?).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        Ok(lines.map(|line| {
            let line = line.unwrap();
            let parsed = serde_json::from_str::<SciDocsCorpusLine>(&line).unwrap();

            let mut metadata = HashMap::new();
            metadata.insert("id".to_string(), parsed._id);
            metadata.insert("title".to_string(), parsed.title);

            TestDatasetDocument {
                content: parsed.text,
                metadata,
            }
        }))
    }
}

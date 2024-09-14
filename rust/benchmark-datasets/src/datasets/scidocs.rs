use std::{collections::HashMap, path::PathBuf};

use crate::{
    types::{BenchmarkDataset, BenchmarkDatasetDocument},
    util::get_or_populate_cached_dataset,
};
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use futures::{FutureExt, StreamExt, TryStreamExt};
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
pub struct SciDocsDataset {
    file_path: PathBuf,
}

impl BenchmarkDataset for SciDocsDataset {
    async fn init() -> Result<Self> {
        let file_path = get_or_populate_cached_dataset("scidocs", "corpus.jsonl", |mut writer| {
            async move {
                let client = reqwest::Client::new();
                let response = client
                    .get(
                        "https://huggingface.co/datasets/BeIR/scidocs/resolve/main/corpus.jsonl.gz",
                    )
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Failed to download SciDocs dataset, got status code {}",
                        response.status()
                    ));
                }

                let byte_stream = response.bytes_stream();
                let stream_reader = StreamReader::new(
                    byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                );

                let mut decoder = GzipDecoder::new(stream_reader);
                tokio::io::copy(&mut decoder, &mut writer).await?;

                Ok(())
            }
            .boxed()
        })
        .await?;

        Ok(SciDocsDataset { file_path })
    }

    async fn create_documents_stream(
        &self,
    ) -> Result<impl Stream<Item = Result<BenchmarkDatasetDocument>>> {
        let file = File::open(self.file_path.clone()).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        Ok(lines.map(|line| match line {
            Ok(line) => {
                let parsed = serde_json::from_str::<SciDocsCorpusLine>(&line)?;
                let mut metadata = HashMap::new();
                metadata.insert("id".to_string(), parsed._id);
                metadata.insert("title".to_string(), parsed.title);

                Ok(BenchmarkDatasetDocument {
                    content: parsed.text,
                    metadata,
                })
            }
            Err(e) => Err(e.into()),
        }))
    }
}

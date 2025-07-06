use std::{collections::HashMap, path::PathBuf};

use super::{
    types::{Record, RecordDataset},
    util::get_or_populate_cached_dataset_file,
};
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use futures::{FutureExt, TryStreamExt};
use serde::Deserialize;
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::{wrappers::LinesStream, Stream, StreamExt};
use tokio_util::io::StreamReader;

#[derive(Deserialize, Debug)]
struct SciDocsCorpusLine {
    _id: String,
    title: String,
    text: String,
}

/// Dataset from <https://huggingface.co/datasets/BeIR/scidocs>.
/// Metadata:
/// - id: The record ID.
/// - title: The title of the record.
pub struct SciDocsDataset {
    file_path: PathBuf,
}

impl RecordDataset for SciDocsDataset {
    const NAME: &'static str = "scidocs";
    const DISPLAY_NAME: &'static str = "SciDocs";

    async fn init() -> Result<Self> {
        let file_path =
            get_or_populate_cached_dataset_file("scidocs", "corpus.jsonl", None, |mut writer| {
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

    async fn create_records_stream(&self) -> Result<impl Stream<Item = Result<Record>>> {
        let file = File::open(self.file_path.clone()).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        Ok(lines
            .map(|line| match line {
                Ok(line) => {
                    let parsed = serde_json::from_str::<SciDocsCorpusLine>(&line)?;
                    let mut metadata = HashMap::new();
                    metadata.insert("id".to_string(), parsed._id);
                    metadata.insert("title".to_string(), parsed.title);

                    Ok(Record {
                        document: parsed.text,
                        metadata,
                        embedding: None,
                    })
                }
                Err(e) => Err(e.into()),
            })
            .filter(|record| match record {
                Ok(record) => record.document.is_ascii(),
                Err(_) => true,
            }))
    }
}

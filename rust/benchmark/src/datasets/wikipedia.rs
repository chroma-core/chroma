use std::{collections::HashMap, path::PathBuf};

use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::BzDecoder;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::Deserialize;
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;

use super::{
    types::{Record, RecordDataset},
    util::get_or_populate_cached_dataset_file,
};

#[derive(Deserialize, Debug)]
struct WikipediaArticlesLine {
    url: String,
    title: String,
    body: String,
}

/// This is the same dataset that tantivy uses in its examples.
/// Metadata:
/// - url: The URL of the article.
/// - title: The title of the article.
pub struct WikipediaDataset {
    file_path: PathBuf,
}

impl RecordDataset for WikipediaDataset {
    const NAME: &'static str = "wikipedia";
    const DISPLAY_NAME: &'static str = "Wikipedia";

    async fn init() -> Result<Self> {
        let file_path = get_or_populate_cached_dataset_file(
            "wikipedia",
            "articles.jsonl",
            None,
            |mut writer| {
                async move {
                    let client = reqwest::Client::new();
                    let response = client
                        .get(
                            "https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=1", // todo: less sketchy source
                        )
                        .send()
                        .await?;

                    if !response.status().is_success() {
                        return Err(anyhow!(
                            "Failed to download Wikipedia dataset, got status code {}",
                            response.status()
                        ));
                    }

                    let byte_stream = response.bytes_stream();
                    let stream_reader = StreamReader::new(
                        byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                    );

                    let mut decoder = BzDecoder::new(stream_reader);
                    tokio::io::copy(&mut decoder, &mut writer).await?;

                    Ok(())
                }
                .boxed()
            },
        )
        .await?;

        Ok(WikipediaDataset { file_path })
    }

    async fn create_records_stream(&self) -> Result<impl futures::Stream<Item = Result<Record>>> {
        let file = File::open(self.file_path.clone()).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        Ok(lines.map(|line| match line {
            Ok(line) => {
                let parsed = serde_json::from_str::<WikipediaArticlesLine>(&line)?;
                let mut metadata = HashMap::new();
                metadata.insert("url".to_string(), parsed.url);
                metadata.insert("title".to_string(), parsed.title);

                Ok(Record {
                    document: parsed.body,
                    metadata,
                    embedding: None,
                })
            }
            Err(e) => Err(e.into()),
        }))
    }
}

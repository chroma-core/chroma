use std::{collections::HashMap, path::PathBuf};

use async_compression::tokio::bufread::BzDecoder;
use futures::{FutureExt, StreamExt, TryStreamExt};
use serde::Deserialize;
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;

use crate::traits::{get_or_populate_cached_dataset, TestDataset, TestDatasetDocument};

#[derive(Deserialize, Debug)]
struct WikipediaArticlesLine {
    url: String,
    title: String,
    body: String,
}

/// This is the same dataset that tantivy uses in its examples.
pub struct WikipediaDataset {
    file_path: PathBuf,
}

impl TestDataset for WikipediaDataset {
    async fn init() -> Result<Self, std::io::Error> {
        let file_path =
            get_or_populate_cached_dataset("wikipedia", "articles.jsonl", |mut writer| {
                async move {
                    let client = reqwest::Client::new();
                    let response = client
                        .get(
                            "https://www.dropbox.com/s/wwnfnu441w1ec9p/wiki-articles.json.bz2?dl=1", // todo: less sketchy source
                        )
                        .send()
                        .await
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))
                        })?;

                    if !response.status().is_success() {
                        panic!("Failed to download SciDocs dataset");
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
            })
            .await?;

        Ok(WikipediaDataset { file_path })
    }

    async fn create_documents_stream(
        &self,
    ) -> Result<impl futures::Stream<Item = crate::traits::TestDatasetDocument>, std::io::Error>
    {
        let file = File::open(self.file_path.clone()).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        Ok(lines.map(|line| {
            let line = line.unwrap();
            let parsed = serde_json::from_str::<WikipediaArticlesLine>(&line).unwrap();

            let mut metadata = HashMap::new();
            metadata.insert("url".to_string(), parsed.url);
            metadata.insert("title".to_string(), parsed.title);

            TestDatasetDocument {
                content: parsed.body,
                metadata,
            }
        }))
    }
}

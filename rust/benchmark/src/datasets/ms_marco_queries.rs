use std::{collections::HashMap, path::PathBuf};

use super::{
    types::{QueryDataset, Record, RecordDataset},
    util::get_or_populate_cached_dataset_file,
};
use anyhow::{anyhow, Result};
use bloom::{BloomFilter, ASMS};
use futures::FutureExt;
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::{wrappers::LinesStream, Stream, StreamExt};
use tokio_util::io::StreamReader;

/// Dataset from <https://github.com/microsoft/MS-MARCO-Web-Search>.
/// Metadata:
/// - id: The query ID.
/// - language_codes: The language codes of the query (e.g. en-US).
pub struct MicrosoftMarcoQueriesDataset {
    file_path: PathBuf,
}

impl RecordDataset for MicrosoftMarcoQueriesDataset {
    const NAME: &'static str = "microsoft_marco_queries";
    const DISPLAY_NAME: &'static str = "Microsoft MARCO Queries";

    async fn init() -> Result<Self> {
        let file_path = get_or_populate_cached_dataset_file("microsoft_marco_queries", "queries.tsv", None, |mut writer| {
            async move {
                let client = reqwest::Client::new();
                let response = client
                    .get(
                         "https://msmarco.z22.web.core.windows.net/msmarcowebsearch/100M_queries/queries_train.tsv"
                    )
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Failed to download Microsoft MARCO dataset, got status code {}",
                        response.status()
                    ));
                }

                let byte_stream = response.bytes_stream();
                let mut stream_reader = StreamReader::new(
                    futures::TryStreamExt::map_err(byte_stream, |e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                );
                tokio::io::copy(&mut stream_reader, &mut writer).await?;

                Ok(())
            }
            .boxed()
        })
        .await?;

        Ok(MicrosoftMarcoQueriesDataset { file_path })
    }

    async fn create_records_stream(&self) -> Result<impl Stream<Item = Result<Record>>> {
        let file = File::open(self.file_path.clone()).await?;
        let buffered_reader = tokio::io::BufReader::new(file);
        let lines = LinesStream::new(buffered_reader.lines());

        let mut unique_content_filter = BloomFilter::with_rate(0.01, 10_000_000);

        Ok(lines
            .map(|line| match line {
                Ok(line) => {
                    let columns = line.split('\t').collect::<Vec<&str>>();
                    let mut metadata = HashMap::new();
                    metadata.insert("id".to_string(), columns[0].to_string());
                    metadata.insert("language_codes".to_string(), columns[2].to_string());

                    let content = columns[1].to_string();
                    let content = content
                        .chars()
                        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
                        .collect::<String>()
                        .split_whitespace()
                        .flat_map(|word| [word, " "])
                        .collect::<String>()
                        .trim()
                        .to_string();

                    Ok(Record {
                        document: content,
                        metadata,
                    })
                }
                Err(e) => Err(e.into()),
            })
            .filter(move |record| match record.as_ref() {
                Ok(record) => {
                    if record.document.len() < 3 {
                        return false;
                    }

                    if unique_content_filter.contains(&record.document) {
                        return false;
                    }

                    unique_content_filter.insert(&record.document);

                    let language_codes = record.metadata.get("language_codes").unwrap();
                    language_codes.contains("en-US") && record.document.is_ascii()
                }
                Err(_) => false,
            }))
    }
}

impl QueryDataset for MicrosoftMarcoQueriesDataset {}

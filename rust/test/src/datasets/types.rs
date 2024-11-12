use anyhow::Result;
use rand::prelude::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, path::PathBuf, sync::Arc};
use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    tokenizer::WhitespaceTokenizer,
    Index, ReloadPolicy,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use super::util::{get_dir_for_persistent_dataset_files, get_or_populate_cached_dataset_file};

#[derive(Debug, Clone)]
pub struct Record {
    pub document: String,
    pub metadata: HashMap<String, String>,
}

/// The base trait that all datasets must implement.
pub trait RecordDataset
where
    Self: Sized,
{
    const NAME: &'static str;
    const DISPLAY_NAME: &'static str;

    fn init() -> impl Future<Output = Result<Self>> + Send;
    fn create_records_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Record>>>> + Send;
}

impl<T: RecordDataset> RecordDataset for Arc<T> {
    const NAME: &'static str = T::NAME;
    const DISPLAY_NAME: &'static str = T::DISPLAY_NAME;

    async fn init() -> Result<Self> {
        Ok(Arc::new(T::init().await?))
    }

    fn create_records_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Record>>>> + Send {
        self.as_ref().create_records_stream()
    }
}

/// Represents a "known good" subset of queries from a query dataset that have at least `min_results_per_query` results in a corpus dataset.
#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenQuerySubset {
    pub queries: Vec<String>,
    pub queries_from_dataset: String,
    pub dataset_queries_tested_against: String,
    pub min_results_per_query: usize,
}

trait TantivyIndexable {
    fn build_tantivy_index(&mut self) -> impl Future<Output = Result<(Index, Schema)>>;
}

impl<T: Stream<Item = Result<Record>> + Unpin> TantivyIndexable for T {
    async fn build_tantivy_index(&mut self) -> Result<(Index, Schema)> {
        let mut schema_builder = Schema::builder();

        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("whitespace")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();

        schema_builder.add_text_field("content", text_options);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());
        index
            .tokenizers()
            .register("whitespace", WhitespaceTokenizer::default());

        let mut index_writer = index.writer(50_000_000)?;
        let content = schema.get_field("content")?;

        while let Some(record) = self.try_next().await? {
            index_writer.add_document(doc!(content => record.document))?;
        }
        index_writer.commit()?;

        Ok((index, schema))
    }
}

/// A query dataset is a specialized `RecordDataset` where the records are queries.
pub trait QueryDataset: RecordDataset
where
    Self: Sized,
{
    /// Returns a subset of queries from the dataset that have at least `min_results_per_query` results in the `corpus_dataset`.
    /// The subset will contain at most `max_num_of_queries` queries.
    ///
    /// Because constructing this subset can be expensive (and different subsets may lead to different downstream test results), by default the constructed subset is stored in the `dataset_files/` directory in the root of this crate.
    fn get_or_create_frozen_query_subset<CorpusDataset: RecordDataset>(
        &self,
        corpus_dataset: &CorpusDataset,
        min_results_per_query: usize,
        max_num_of_queries: usize,
        cache_dir: Option<PathBuf>,
    ) -> impl Future<Output = Result<FrozenQuerySubset>> {
        async move {
            let file_name = format!(
                "frozen_query_subset_{}_{}_{}.bin",
                Self::NAME,
                min_results_per_query,
                max_num_of_queries
            );

            let file = get_or_populate_cached_dataset_file(
                CorpusDataset::NAME,
                &file_name,
                Some(cache_dir.unwrap_or(get_dir_for_persistent_dataset_files())),
                |mut file| async move {
                    let (corpus_index, corpus_index_schema) = futures::StreamExt::boxed_local(
                        corpus_dataset.create_records_stream().await?,
                    )
                    .build_tantivy_index()
                    .await?;

                    let mut shuffled_queries = self
                        .create_records_stream()
                        .await?
                        .map(|doc| doc.map(|doc| doc.document))
                        .collect::<Result<Vec<String>>>()
                        .await?;

                    shuffled_queries.shuffle(&mut rand::thread_rng());

                    let reader = corpus_index
                        .reader_builder()
                        .reload_policy(ReloadPolicy::OnCommit)
                        .try_into()?;

                    let searcher = reader.searcher();
                    let mut query_parser = QueryParser::for_index(
                        &corpus_index,
                        vec![corpus_index_schema.get_field("content")?],
                    );
                    query_parser.set_conjunction_by_default();

                    let mut frozen_query_subset = FrozenQuerySubset {
                        queries: Vec::new(),
                        queries_from_dataset: Self::NAME.to_string(),
                        dataset_queries_tested_against: CorpusDataset::NAME.to_string(),
                        min_results_per_query,
                    };

                    for query_text in shuffled_queries.iter() {
                        let query =
                            query_parser.parse_query(format!("\"{}\"", query_text).as_str())?;

                        let top_docs =
                            searcher.search(&query, &TopDocs::with_limit(min_results_per_query))?;
                        if top_docs.len() >= min_results_per_query {
                            frozen_query_subset.queries.push(query_text.to_string());
                        }

                        if frozen_query_subset.queries.len() == max_num_of_queries {
                            break;
                        }
                    }

                    let serialized = bincode::serialize(&frozen_query_subset)?;
                    file.write_all(&serialized).await?;

                    Ok(())
                },
            )
            .await?;

            let serialized = tokio::fs::read(file).await?;

            Ok(bincode::deserialize(&serialized)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use async_tempfile::TempDir;

    use super::*;

    struct TestDataset {
        records: Vec<Record>,
    }

    impl RecordDataset for TestDataset {
        const NAME: &'static str = "test";
        const DISPLAY_NAME: &'static str = "Test";

        async fn init() -> Result<Self> {
            Ok(TestDataset { records: vec![] })
        }

        async fn create_records_stream(&self) -> Result<impl Stream<Item = Result<Record>>> {
            let records = self.records.clone();
            Ok(futures::stream::iter(records.into_iter().map(Ok)))
        }
    }

    impl QueryDataset for TestDataset {}

    #[tokio::test]
    async fn test_frozen_query_subset() {
        let mut test_dataset = TestDataset::init().await.unwrap();
        test_dataset.records = ["foo 0", "foo 1", "foo 3", "bar 0", "bar 2"]
            .iter()
            .map(|&content| Record {
                document: content.to_string(),
                metadata: HashMap::new(),
            })
            .collect();

        let mut test_query_dataset = TestDataset::init().await.unwrap();
        test_query_dataset.records = ["foo", "bar", "baz"]
            .iter()
            .map(|&content| Record {
                document: content.to_string(),
                metadata: HashMap::new(),
            })
            .collect();

        let temp_dir = TempDir::new().await.unwrap();

        let frozen_query_subset = test_query_dataset
            .get_or_create_frozen_query_subset(&test_dataset, 1, 100, Some(temp_dir.to_path_buf()))
            .await
            .unwrap();

        // There are no records with "baz" in the dataset
        assert_eq!(frozen_query_subset.queries.len(), 2);
        assert!(frozen_query_subset.queries.contains(&"foo".to_string()));
        assert!(frozen_query_subset.queries.contains(&"bar".to_string()));

        // Require at least 3 results per query
        let frozen_query_subset = test_query_dataset
            .get_or_create_frozen_query_subset(&test_dataset, 3, 100, Some(temp_dir.to_path_buf()))
            .await
            .unwrap();

        // "foo" is the only query with at least 3 results
        assert_eq!(frozen_query_subset.queries.len(), 1);
        assert!(frozen_query_subset.queries.contains(&"foo".to_string()));
    }
}

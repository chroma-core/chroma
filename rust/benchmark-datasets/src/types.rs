use anyhow::Result;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, path::PathBuf};
use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Schema, STORED, TEXT},
    Index, ReloadPolicy,
};
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use crate::util::{get_dir_for_persistent_dataset_files, get_or_populate_cached_dataset_file};

trait TantivyIndexable {
    fn build_tantivy_index(&mut self) -> impl Future<Output = Result<(Index, Schema)>>;
}

impl<T: Stream<Item = Document> + Unpin> TantivyIndexable for T {
    async fn build_tantivy_index(&mut self) -> Result<(Index, Schema)> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("content", TEXT | STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer(50_000_000)?;
        let content = schema.get_field("content")?;

        while let Some(document) = self.next().await {
            index_writer.add_document(doc!(content => document.content))?;
        }
        index_writer.commit()?;

        Ok((index, schema))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenQuerySubset {
    queries: Vec<String>,
    queries_from_dataset: String,
    dataset_queries_tested_against: String,
    min_results_per_query: usize,
}

#[derive(Debug, Clone)]
pub struct Document {
    pub content: String,
    pub metadata: HashMap<String, String>,
}

pub trait DocumentDataset
where
    Self: Sized,
{
    fn init() -> impl Future<Output = Result<Self>> + Send;
    fn get_name(&self) -> &'static str;
    fn create_documents_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Document>>>> + Send;
}

/// A query dataset is a specialized `DocumentDataset` where the documents are queries.
pub trait QueryDataset: DocumentDataset
where
    Self: Sized,
{
    /// Returns a subset of queries from the dataset that have at least `min_results_per_query` results in the `corpus_dataset`.
    /// The subset will contain at most `max_num_of_queries` queries.
    ///
    /// Because constructing this subset can be expensive (and different subsets may lead to different downstream test results), by default the constructed subset is stored in the `dataset_files/` directory in the root of this crate.
    fn get_or_create_frozen_query_subset(
        &self,
        corpus_dataset: &impl DocumentDataset,
        min_results_per_query: usize,
        max_num_of_queries: usize,
        cache_dir: Option<PathBuf>,
    ) -> impl Future<Output = Result<FrozenQuerySubset>> {
        async move {
            let file_name = format!(
                "frozen_query_subset_{}_{}_{}.bin",
                corpus_dataset.get_name(),
                min_results_per_query,
                max_num_of_queries
            );

            let file = get_or_populate_cached_dataset_file(
                self.get_name(),
                &file_name,
                Some(cache_dir.unwrap_or(get_dir_for_persistent_dataset_files())),
                |mut file| async move {
                    let (corpus_index, corpus_index_schema) = futures::StreamExt::boxed_local(
                        corpus_dataset.create_documents_stream().await?.filter_map(
                            |doc| match doc {
                                Ok(doc) => Some(doc),
                                Err(_) => None,
                            },
                        ),
                    )
                    .build_tantivy_index()
                    .await?;

                    let mut shuffled_queries = self
                        .create_documents_stream()
                        .await?
                        .filter_map(|doc| match doc {
                            Ok(doc) => Some(doc.content),
                            Err(_) => None,
                        })
                        .collect::<Vec<String>>()
                        .await;
                    shuffled_queries.shuffle(&mut rand::thread_rng());

                    let reader = corpus_index
                        .reader_builder()
                        .reload_policy(ReloadPolicy::OnCommit)
                        .try_into()?;

                    let searcher = reader.searcher();
                    let query_parser = QueryParser::for_index(
                        &corpus_index,
                        vec![corpus_index_schema.get_field("content")?],
                    );

                    let mut frozen_query_subset = FrozenQuerySubset {
                        queries: Vec::new(),
                        queries_from_dataset: self.get_name().to_string(),
                        dataset_queries_tested_against: corpus_dataset.get_name().to_string(),
                        min_results_per_query,
                    };

                    for query_text in shuffled_queries.iter() {
                        let query = query_parser.parse_query(query_text)?;
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
        documents: Vec<Document>,
    }

    impl DocumentDataset for TestDataset {
        fn init() -> impl Future<Output = Result<Self>> + Send {
            async move { Ok(TestDataset { documents: vec![] }) }
        }

        fn get_name(&self) -> &'static str {
            "test"
        }

        fn create_documents_stream(
            &self,
        ) -> impl Future<Output = Result<impl Stream<Item = Result<Document>>>> + Send {
            async move {
                let documents = self.documents.clone();
                Ok(futures::stream::iter(documents.into_iter().map(Ok)))
            }
        }
    }

    impl QueryDataset for TestDataset {}

    #[tokio::test]
    async fn test_frozen_query_subset() {
        let mut test_dataset = TestDataset::init().await.unwrap();
        test_dataset.documents = vec!["foo 0", "foo 1", "foo 3", "bar 0", "bar 2"]
            .iter()
            .map(|&content| Document {
                content: content.to_string(),
                metadata: HashMap::new(),
            })
            .collect();

        let mut test_query_dataset = TestDataset::init().await.unwrap();
        test_query_dataset.documents = vec!["foo", "bar", "baz"]
            .iter()
            .map(|&content| Document {
                content: content.to_string(),
                metadata: HashMap::new(),
            })
            .collect();

        let temp_dir = TempDir::new().await.unwrap();

        let frozen_query_subset = test_query_dataset
            .get_or_create_frozen_query_subset(&test_dataset, 1, 100, Some(temp_dir.to_path_buf()))
            .await
            .unwrap();

        // There are no documents with "baz" in the dataset
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

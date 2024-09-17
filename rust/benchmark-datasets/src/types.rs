use anyhow::Result;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::Future, path::PathBuf};
use tantivy::{collector::TopDocs, doc, query::QueryParser, schema::*, Index, ReloadPolicy};
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use crate::util::get_or_populate_cached_dataset;

fn get_git_checked_in_dataset_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("datasets")
}

trait TantivyIndexable {
    fn build_tantivy_index(&mut self) -> impl Future<Output = Result<(Index, Schema)>>;
}

impl<T: Stream<Item = BenchmarkDatasetDocument> + Unpin> TantivyIndexable for T {
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
pub enum BenchmarkDatasets {
    Wikipedia,
    SciDocs,
    MsMarcoQueries,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenQueryset {
    queries: Vec<String>,
    against_dataset: BenchmarkDatasets,
    min_results_per_query: usize,
}

#[derive(Debug, Clone)]
pub struct BenchmarkDatasetDocument {
    pub content: String,
    pub metadata: HashMap<String, String>,
}

pub trait BenchmarkDataset
where
    Self: Sized,
{
    fn init() -> impl Future<Output = Result<Self>> + Send;
    fn get_name(&self) -> &'static str;
    fn create_documents_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<BenchmarkDatasetDocument>>>> + Send;
}

pub trait QueryDataset: BenchmarkDataset
where
    Self: Sized,
{
    fn get_or_create_frozen_subset(
        &self,
        corpus_dataset: &impl BenchmarkDataset,
        min_results_per_query: usize,
        max_num_of_queries: usize,
    ) -> impl Future<Output = Result<FrozenQueryset>> {
        async move {
            let file_name = format!(
                "frozen_queryset_{}_{}_{}.bin",
                corpus_dataset.get_name(),
                min_results_per_query,
                max_num_of_queries
            );

            let file = get_or_populate_cached_dataset(
                self.get_name(),
                &file_name,
                Some(get_git_checked_in_dataset_dir()),
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

                    let mut frozen_queryset = FrozenQueryset {
                        queries: Vec::new(),
                        against_dataset: BenchmarkDatasets::Wikipedia, // todo
                        min_results_per_query,
                    };

                    for query_text in shuffled_queries.iter() {
                        let query = query_parser.parse_query(query_text)?;
                        let top_docs =
                            searcher.search(&query, &TopDocs::with_limit(min_results_per_query))?;
                        if top_docs.len() >= 2 {
                            frozen_queryset.queries.push(query_text.to_string());
                        }

                        if frozen_queryset.queries.len() == max_num_of_queries {
                            break;
                        }
                    }

                    let serialized = bincode::serialize(&frozen_queryset)?;
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

// pub trait StreamReservoirSample {
//     type Item;
//     fn reservoir_sample(self, n: usize) -> impl Future<Output = Vec<Self::Item>>;
// }

// impl<T> StreamReservoirSample for T
// where
//     T: Stream + Unpin,
// {
//     type Item = T::Item;

//     fn reservoir_sample(self, n: usize) -> impl Future<Output = Vec<Self::Item>> {
//         async move {
//             let mut rng = rand::thread_rng();
//             let mut stream = self;
//             let mut reservoir = Vec::with_capacity(n);

//             let random_index = Uniform::new(0, n);

//             let mut w: f64 = (rng.gen::<f64>().ln() / n as f64).exp();
//             let mut next_sample_i = n;

//             let mut i = 0;
//             while let Some(doc) = stream.next().await {
//                 if i < n {
//                     reservoir.push(doc);
//                 } else {
//                     if i == next_sample_i {
//                         next_sample_i +=
//                             (rng.gen::<f64>().ln() / (1.0 - w).ln()).floor() as usize + 1;
//                         w *= (rng.gen::<f64>().ln() / n as f64).exp();
//                         reservoir[random_index.sample(&mut rng)] = doc;
//                     }
//                 }

//                 i += 1;
//             }

//             reservoir
//         }
//     }
// }

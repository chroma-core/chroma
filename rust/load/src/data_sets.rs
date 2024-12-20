use std::sync::Arc;

use chromadb::collection::{GetOptions, QueryOptions};
use chromadb::ChromaClient;
use guacamole::combinators::*;
use guacamole::Guacamole;
use tracing::Instrument;

use crate::{bit_difference, DataSet, Error, GetQuery, QueryQuery, UpsertQuery};

//////////////////////////////////////////////// Nop ///////////////////////////////////////////////

/// A data set that does nothing.
#[derive(Debug)]
pub struct NopDataSet;

#[async_trait::async_trait]
impl DataSet for NopDataSet {
    fn name(&self) -> String {
        "nop".into()
    }

    fn description(&self) -> String {
        "No operation data set".into()
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!("nop")
    }

    async fn get(
        &self,
        _: &ChromaClient,
        _: GetQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("nop get");
        Ok(())
    }

    async fn query(
        &self,
        _: &ChromaClient,
        qq: QueryQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("nop query {qq:?}", qq = qq);
        Ok(())
    }

    async fn upsert(
        &self,
        _: &ChromaClient,
        _: UpsertQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("nop upsert");
        Ok(())
    }
}

/////////////////////////////////////////// Tiny Stories ///////////////////////////////////////////

/// A data set of tiny stories.
#[derive(Clone, Debug)]
pub struct TinyStoriesDataSet {
    name: &'static str,
    model: &'static str,
    size: usize,
}

impl TinyStoriesDataSet {
    pub const fn new(name: &'static str, model: &'static str, size: usize) -> Self {
        Self { name, model, size }
    }
}

#[async_trait::async_trait]
impl DataSet for TinyStoriesDataSet {
    fn name(&self) -> String {
        let size = match self.size {
            100_000 => "100K".to_string(),
            1_000_000 => "1M".to_string(),
            25_000 => "25K".to_string(),
            50_000 => "50K".to_string(),
            _ => format!("{}", self.size),
        };
        format!("{}-{}-{}", self.name, self.model, size)
    }

    fn description(&self) -> String {
        format!(
            "TinyStories dataset with {} stories and model {}",
            self.size, self.model
        )
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "tiny_stories": {
                "name": self.name,
                "model": self.model,
                "size": self.size,
            }
        })
    }

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let collection = client.get_collection(&self.name()).await?;
        let limit = gq.limit.sample(guac);
        let where_metadata = gq.metadata.map(|m| m.to_json(guac));
        let where_document = gq.document.map(|m| m.to_json(guac));
        let results = collection
            .get(GetOptions {
                ids: vec![],
                where_metadata,
                limit: Some(limit),
                offset: None,
                where_document,
                include: None,
            })
            .instrument(tracing::info_span!("get", limit = limit))
            .await;
        let _results = results?;
        Ok(())
    }

    async fn query(
        &self,
        client: &ChromaClient,
        qq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let collection = client.get_collection(&self.name()).await?;
        let limit = qq.limit.sample(guac);
        let size = match self.model {
            ALL_MINILM_L6_V2 => 384,
            DISTILUSE_BASE_MULTILINGUAL_CASED_V2 => 512,
            PARAPHRASE_MINILM_L3_V2 => 384,
            PARAPHRASE_ALBERT_SMALL_V2 => 768,
            _ => Err(Error::InvalidRequest(format!(
                "Unknown model: {}",
                self.model
            )))?,
        };
        let mut point = vec![0.0; size];
        for x in point.iter_mut() {
            *x = any(guac);
        }
        let results = collection
            .query(
                QueryOptions {
                    query_texts: None,
                    query_embeddings: Some(vec![point]),
                    where_metadata: None,
                    where_document: None,
                    n_results: Some(limit),
                    include: None,
                },
                None,
            )
            .instrument(tracing::info_span!("query::embedding", limit = limit))
            .await;
        let _results = results?;
        Ok(())
    }

    async fn upsert(
        &self,
        _: &ChromaClient,
        _: UpsertQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err(Error::InvalidRequest("Upsert not supported".into()).into())
    }
}

const ALL_MINILM_L6_V2: &str = "all-MiniLM-L6-v2";
const DISTILUSE_BASE_MULTILINGUAL_CASED_V2: &str = "distiluse-base-multilingual-cased-v2";
const PARAPHRASE_MINILM_L3_V2: &str = "paraphrase-MiniLM-L3-v2";
const PARAPHRASE_ALBERT_SMALL_V2: &str = "paraphrase-albert-small-v2";

const TINY_STORIES_DATA_SETS: &[TinyStoriesDataSet] = &[
    TinyStoriesDataSet::new("stories1", ALL_MINILM_L6_V2, 100_000),
    TinyStoriesDataSet::new("stories1", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 100_000),
    TinyStoriesDataSet::new("stories1", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 1_000_000),
    TinyStoriesDataSet::new("stories1", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories1", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_MINILM_L3_V2, 100_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_MINILM_L3_V2, 1_000_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_ALBERT_SMALL_V2, 100_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_ALBERT_SMALL_V2, 1_000_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories1", PARAPHRASE_ALBERT_SMALL_V2, 100_000),
    TinyStoriesDataSet::new("stories10", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories10", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories10", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories10", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories10", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories10", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories2", ALL_MINILM_L6_V2, 100_000),
    TinyStoriesDataSet::new("stories2", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 100_000),
    TinyStoriesDataSet::new("stories2", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 1_000_000),
    TinyStoriesDataSet::new("stories2", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories2", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_MINILM_L3_V2, 100_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_MINILM_L3_V2, 1_000_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_ALBERT_SMALL_V2, 100_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_ALBERT_SMALL_V2, 1_000_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories2", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories3", ALL_MINILM_L6_V2, 100_000),
    TinyStoriesDataSet::new("stories3", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories3", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories3", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories3", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories3", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories3", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories4", ALL_MINILM_L6_V2, 100_000),
    TinyStoriesDataSet::new("stories4", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories4", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories4", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories4", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories4", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories4", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories5", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories5", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories5", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories5", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories5", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories5", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories6", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories6", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories6", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories6", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories6", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories6", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories7", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories7", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories7", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories7", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories7", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories7", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories8", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories8", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories8", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories8", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories8", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories8", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
    TinyStoriesDataSet::new("stories9", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 25_000),
    TinyStoriesDataSet::new("stories9", DISTILUSE_BASE_MULTILINGUAL_CASED_V2, 50_000),
    TinyStoriesDataSet::new("stories9", PARAPHRASE_MINILM_L3_V2, 25_000),
    TinyStoriesDataSet::new("stories9", PARAPHRASE_MINILM_L3_V2, 50_000),
    TinyStoriesDataSet::new("stories9", PARAPHRASE_ALBERT_SMALL_V2, 25_000),
    TinyStoriesDataSet::new("stories9", PARAPHRASE_ALBERT_SMALL_V2, 50_000),
];

/////////////////////////////////////////// All Data Sets //////////////////////////////////////////

/// Get all data sets.
pub fn all_data_sets() -> Vec<Arc<dyn DataSet>> {
    let mut data_sets = vec![Arc::new(NopDataSet) as _];
    for data_set in TINY_STORIES_DATA_SETS {
        data_sets.push(Arc::new(data_set.clone()) as _);
    }
    for num_clusters in [10_000, 100_000] {
        for (seed_idx, seed_clusters) in [
            0xab1cd5b6a5173d40usize,
            0x415c2b5b6451416dusize,
            0x7bfbf398fb74d56usize,
            0xed11fe8e8655591eusize,
            0xcb86c32c95df5657usize,
            0xa869711d201b98a4usize,
            0xe2a276bde1c91d1ausize,
            0x866a7f8100ccf78usize,
            0xa23e0b862d45e227usize,
            0x59f651f54a5ffe1usize,
        ]
        .into_iter()
        .enumerate()
        {
            for max_adjacent in [1, 10, 100] {
                let adjacent_theta = 0.99;
                let eo = bit_difference::EmbeddingOptions {
                    num_clusters,
                    seed_clusters,
                    clustering: bit_difference::ClusterOptions {
                        max_adjacent,
                        adjacent_theta,
                    },
                };
                let collection = format!(
                    "bit-difference-scale-{:e}-seed-{}-adj-{}",
                    num_clusters, seed_idx, max_adjacent
                );
                let data_set = Arc::new(bit_difference::SyntheticDataSet::new(collection, eo));
                data_sets.push(data_set as _);
            }
        }
    }
    data_sets
}

/// Get a data set from a particular JSON value.
pub fn from_json(json: &serde_json::Value) -> Option<Arc<dyn DataSet>> {
    // NOTE(rescrv):  I don't like that we use json attributes to identify data sets, but it's the
    // only robust way I can think of that's not encoding everything to strings or reworking the
    // data set type to be an enum.
    all_data_sets()
        .into_iter()
        .find(|data_set| data_set.json() == *json)
}

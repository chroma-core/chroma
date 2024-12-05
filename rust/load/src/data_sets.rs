use std::sync::Arc;

use chromadb::v2::collection::{GetOptions, QueryOptions};
use chromadb::v2::ChromaClient;
use guacamole::combinators::*;
use guacamole::Guacamole;
use tracing::Instrument;

use crate::{DataSet, Error, GetQuery, QueryQuery};

//////////////////////////////////////////////// Nop ///////////////////////////////////////////////

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
        _: QueryQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("nop query");
        Ok(())
    }
}

/////////////////////////////////////////// Tiny Stories ///////////////////////////////////////////

#[derive(Clone, Debug)]
struct TinyStoriesDataSet {
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
            "name": self.name,
            "model": self.model,
            "size": self.size,
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
        let where_metadata = gq.metadata.map(|m| m.into_where_metadata(guac));
        let where_document = gq.document.map(|m| m.into_where_document(guac));
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

pub fn all_data_sets() -> Vec<Arc<dyn DataSet>> {
    let mut data_sets = vec![Arc::new(NopDataSet) as _];
    for data_set in TINY_STORIES_DATA_SETS {
        data_sets.push(Arc::new(data_set.clone()) as _);
    }
    data_sets
}

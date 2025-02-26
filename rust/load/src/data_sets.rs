use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use chromadb::collection::{CollectionEntries, GetOptions, GetResult, QueryOptions};
use chromadb::ChromaClient;
use guacamole::combinators::*;
use guacamole::Guacamole;
use tracing::Instrument;

use crate::{bit_difference, DataSet, Error, GetQuery, KeySelector, QueryQuery, UpsertQuery};

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

    fn cardinality(&self) -> usize {
        0
    }

    async fn get(
        &self,
        _: &ChromaClient,
        _: GetQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        tracing::info!("nop get");
        Ok(())
    }

    async fn query(
        &self,
        _: &ChromaClient,
        qq: QueryQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        tracing::info!("nop query {qq:?}", qq = qq);
        Ok(())
    }

    async fn upsert(
        &self,
        _: &ChromaClient,
        _: UpsertQuery,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        tracing::info!("nop upsert");
        Ok(())
    }
}

////////////////////////////////////// TinyStoriesDataSetType //////////////////////////////////////

/// A type of tiny stories data set.
///
/// In the initial load (Classic), we loaded variable numbers of stories from the Tiny Stories data
/// set in a variety of collections.  Some work, some don't.  They are handy to have around.  We'll
/// use the for garbage collection and other tests in the limit.
///
/// In order to support writes, we needed to have a way to index the data set to e.g. return the
/// N'th item.  The classic data sets use a set of random UUIDs to index the data set.  The
/// reference data sets use a set of sequential numbers to index the data set.  This allows for a
/// workload to create a new collection and write to it according to some hybrid workload, because
/// the writer can select point-wise from the reference set and insert into the referred-to set.
#[derive(Clone, Debug)]
pub enum TinyStoriesDataSetType {
    Classic {
        name: &'static str,
        model: &'static str,
        size: usize,
    },
    Reference {
        name: &'static str,
        model: &'static str,
        size: usize,
    },
}

impl TinyStoriesDataSetType {
    pub const fn classic(name: &'static str, model: &'static str, size: usize) -> Self {
        Self::Classic { name, model, size }
    }

    pub const fn reference(name: &'static str, model: &'static str, size: usize) -> Self {
        Self::Reference { name, model, size }
    }

    pub fn model_size(&self) -> Result<usize, Error> {
        fn func_of_model(model: &str) -> Result<usize, Error> {
            match model {
                ALL_MINILM_L6_V2 => Ok(384),
                DISTILUSE_BASE_MULTILINGUAL_CASED_V2 => Ok(512),
                PARAPHRASE_MINILM_L3_V2 => Ok(384),
                PARAPHRASE_ALBERT_SMALL_V2 => Ok(768),
                _ => Err(Error::InvalidRequest(format!("Unknown model: {}", model)))?,
            }
        }
        match self {
            Self::Classic { model, .. } => func_of_model(model),
            Self::Reference { model, .. } => func_of_model(model),
        }
    }

    pub fn name(&self) -> String {
        fn humanize(size: usize) -> String {
            match size {
                100_000 => "100K".to_string(),
                1_000_000 => "1M".to_string(),
                25_000 => "25K".to_string(),
                50_000 => "50K".to_string(),
                _ => format!("{}", size),
            }
        }
        match self {
            Self::Classic { name, model, size } => {
                format!("{}-{}-{}", name, model, humanize(*size))
            }
            Self::Reference {
                name,
                model,
                size: _,
            } => {
                format!("{}-{}", name, model)
            }
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::Classic { name, model, size } => {
                format!(
                    "{} tiny stories from {} with model {} (classic collection)",
                    size, name, model
                )
            }
            Self::Reference { name, model, size } => {
                format!(
                    "{} tiny stories from {} with model {} (reference collection)",
                    size, name, model
                )
            }
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Classic { size, .. } => *size,
            Self::Reference { size, .. } => *size,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        match self {
            Self::Classic { name, model, size } => serde_json::json!({
                "tiny_stories": {
                    "name": name,
                    "model": model,
                    "size": size,
                }
            }),
            Self::Reference { name, model, size } => serde_json::json!({
                "tiny_stories": {
                    "name": name,
                    "model": model,
                    "size": size,
                }
            }),
        }
    }
}

/////////////////////////////////////////// Tiny Stories ///////////////////////////////////////////

/// A data set of tiny stories.
#[derive(Clone, Debug)]
pub struct TinyStoriesDataSet {
    data_set_type: TinyStoriesDataSetType,
}

impl TinyStoriesDataSet {
    pub const fn new(data_set_type: TinyStoriesDataSetType) -> Self {
        Self { data_set_type }
    }
}

#[async_trait::async_trait]
impl DataSet for TinyStoriesDataSet {
    fn name(&self) -> String {
        self.data_set_type.name()
    }

    fn description(&self) -> String {
        self.data_set_type.description()
    }

    fn json(&self) -> serde_json::Value {
        self.data_set_type.json()
    }

    fn cardinality(&self) -> usize {
        self.data_set_type.size()
    }

    async fn get_by_key(
        &self,
        client: &ChromaClient,
        ids: &[&str],
    ) -> Result<Option<GetResult>, Box<dyn std::error::Error + Send>> {
        let collection = client.get_collection(&self.name()).await?;
        let ids = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
        Ok(Some(
            collection
                .get(GetOptions {
                    ids,
                    where_metadata: None,
                    limit: None,
                    offset: None,
                    where_document: None,
                    include: Some(vec![
                        "documents".to_string(),
                        "metadatas".to_string(),
                        "embeddings".to_string(),
                    ]),
                })
                .instrument(tracing::info_span!("get_by_key"))
                .await?,
        ))
    }

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
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
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_collection(&self.name()).await?;
        let limit = qq.limit.sample(guac);
        let size = self
            .data_set_type
            .model_size()
            .map_err(|err| -> Box<dyn std::error::Error + Send> { Box::new(err) as _ })?;
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
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Err(Box::new(Error::InvalidRequest(
            "Upsert not supported".into(),
        )))
    }
}

pub const ALL_MINILM_L6_V2: &str = "all-MiniLM-L6-v2";
pub const DISTILUSE_BASE_MULTILINGUAL_CASED_V2: &str = "distiluse-base-multilingual-cased-v2";
pub const PARAPHRASE_MINILM_L3_V2: &str = "paraphrase-MiniLM-L3-v2";
pub const PARAPHRASE_ALBERT_SMALL_V2: &str = "paraphrase-albert-small-v2";

const TINY_STORIES_DATA_SETS: &[TinyStoriesDataSet] = &[
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        ALL_MINILM_L6_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_MINILM_L3_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_MINILM_L3_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_ALBERT_SMALL_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_ALBERT_SMALL_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories1",
        PARAPHRASE_ALBERT_SMALL_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories10",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        ALL_MINILM_L6_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_MINILM_L3_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_MINILM_L3_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_ALBERT_SMALL_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_ALBERT_SMALL_V2,
        1_000_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories2",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        ALL_MINILM_L6_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories3",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        ALL_MINILM_L6_V2,
        100_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories4",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories5",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories6",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories7",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories8",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        DISTILUSE_BASE_MULTILINGUAL_CASED_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        PARAPHRASE_MINILM_L3_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        PARAPHRASE_MINILM_L3_V2,
        50_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        PARAPHRASE_ALBERT_SMALL_V2,
        25_000,
    )),
    TinyStoriesDataSet::new(TinyStoriesDataSetType::classic(
        "stories9",
        PARAPHRASE_ALBERT_SMALL_V2,
        50_000,
    )),
];

//////////////////////////////////////// ReferencingDataSet ////////////////////////////////////////

/// A referencing data set refers to some _other_ data set and re-uses its data.
#[derive(Debug)]
pub struct ReferencingDataSet {
    references: Arc<dyn DataSet>,
    operates_on: String,
    cardinality: usize,
}

#[async_trait::async_trait]
impl DataSet for ReferencingDataSet {
    fn name(&self) -> String {
        self.operates_on.clone()
    }

    fn description(&self) -> String {
        format!(
            "referencing data set {}, operating on {}",
            self.references.name(),
            self.operates_on
        )
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json! {
            {
                "references": self.references.json(),
                "operates_on": self.operates_on,
            }
        }
    }

    fn cardinality(&self) -> usize {
        self.cardinality
    }

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut keys = vec![];
        let num_keys = gq.limit.sample(guac);
        for _ in 0..num_keys {
            keys.push(KeySelector::Random(gq.skew).select(guac, self));
        }
        let collection = client.get_collection(&self.operates_on).await?;
        // TODO(rescrv):  from the reference collection, pull the documents and embeddings and
        // generate where_document and where_metadata mixins.
        collection
            .get(GetOptions {
                ids: keys,
                where_metadata: None,
                limit: None,
                offset: None,
                where_document: None,
                include: None,
            })
            .await?;
        Ok(())
    }

    async fn query(
        &self,
        client: &ChromaClient,
        qq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut keys = vec![];
        let num_keys = qq.limit.sample(guac);
        for _ in 0..num_keys {
            keys.push(KeySelector::Random(qq.skew).select(guac, self));
        }
        let keys = keys.iter().map(|k| k.as_str()).collect::<Vec<_>>();
        if let Some(res) = self.references.get_by_key(client, &keys).await? {
            let mut embeddings = vec![];
            if let Some(embeds) = res.embeddings {
                for (idx, embed) in embeds.iter().enumerate() {
                    if let Some(embed) = embed {
                        embeddings.push(embed.clone());
                    } else {
                        return Err(Box::new(Error::InvalidRequest(format!(
                            "Missing document for {}",
                            idx
                        ))));
                    }
                }
            } else {
                return Err(Box::new(Error::InvalidRequest("No documents".into())));
            }
            let collection = client.get_collection(&self.operates_on).await?;
            collection
                .query(
                    QueryOptions {
                        query_texts: None,
                        query_embeddings: Some(embeddings),
                        where_metadata: None,
                        where_document: None,
                        n_results: Some(num_keys),
                        include: None,
                    },
                    None,
                )
                .await?;
            Ok(())
        } else {
            return Err(Box::new(Error::InvalidRequest("No results".into())));
        }
    }

    async fn upsert(
        &self,
        client: &ChromaClient,
        uq: UpsertQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_collection(&self.operates_on).await?;
        let mut keys = vec![];
        for offset in 0..uq.batch_size {
            keys.push(uq.key.select_with_offset(guac, self, offset));
        }
        let keys = keys.iter().map(|k| k.as_str()).collect::<Vec<_>>();
        if let Some(res) = self.references.get_by_key(client, &keys).await? {
            let mut keys = vec![];
            for id in res.ids.iter() {
                keys.push(id.as_str());
            }
            let mut documents = vec![];
            if let Some(docs) = res.documents {
                for (idx, doc) in docs.into_iter().enumerate() {
                    if let Some(doc) = doc {
                        documents.push(doc);
                    } else {
                        return Err(Box::new(Error::InvalidRequest(format!(
                            "Missing document for {}",
                            idx
                        ))));
                    }
                }
            } else {
                return Err(Box::new(Error::InvalidRequest("No documents".into())));
            }
            let documents = documents.iter().map(|d| d.as_str()).collect::<Vec<_>>();
            let mut embeddings = vec![];
            if let Some(embeds) = res.embeddings {
                for (idx, embed) in embeds.iter().enumerate() {
                    if let Some(embed) = embed {
                        embeddings.push(embed.clone());
                    } else {
                        return Err(Box::new(Error::InvalidRequest(format!(
                            "Missing document for {}",
                            idx
                        ))));
                    }
                }
            } else {
                return Err(Box::new(Error::InvalidRequest("No documents".into())));
            }
            let entries = CollectionEntries {
                ids: keys,
                metadatas: res.metadatas,
                documents: Some(documents),
                embeddings: Some(embeddings),
            };
            collection.upsert(entries, None).await?;
        } else {
            return Err(Box::new(Error::InvalidRequest("No results".into())));
        }
        Ok(())
    }
}

//////////////////////////////////////////// RoundRobin ////////////////////////////////////////////

/// A data set that round-robins between other data sets.
#[derive(Debug)]
pub struct RoundRobinDataSet {
    name: String,
    description: String,
    data_sets: Vec<Arc<dyn DataSet>>,
    index: AtomicUsize,
}

#[async_trait::async_trait]
impl DataSet for RoundRobinDataSet {
    fn name(&self) -> String {
        format!("round-robin-{}", self.name)
    }

    fn description(&self) -> String {
        format!("round robin between other data sets; {}", self.description)
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!("round-robin")
    }

    fn cardinality(&self) -> usize {
        self.data_sets.iter().map(|ds| ds.cardinality()).sum()
    }

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let index = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.data_sets.len();
        self.data_sets[index].get(client, gq, guac).await
    }

    async fn query(
        &self,
        client: &ChromaClient,
        qq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let index = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.data_sets.len();
        self.data_sets[index].query(client, qq, guac).await
    }

    async fn upsert(
        &self,
        client: &ChromaClient,
        uq: UpsertQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let index = self
            .index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.data_sets.len();
        self.data_sets[index].upsert(client, uq, guac).await
    }
}

/////////////////////////////////////////// All Data Sets //////////////////////////////////////////

/// Get all data sets.
pub fn all_data_sets() -> Vec<Arc<dyn DataSet>> {
    let mut data_sets = vec![Arc::new(NopDataSet) as _];
    for data_set in TINY_STORIES_DATA_SETS {
        data_sets.push(Arc::new(data_set.clone()) as _);
    }
    // NOTE(rescrv):  When extending chroma-load to a new data set (or experiment), add it here.
    // Give it a unique name (not enforced because we may want to simulate a scenario in which
    // someone crosses embedding dimension), a description, and a cardinality.  The cardinality
    // should be less than 1e6 because the reference data sets are only 1e6 in size.
    //
    // This will, for each listed data set, create a writable data set that refers to the reference
    // data set that contains the data from hugging face as loaded by the perf test suite.
    for (cardinality, model, data_set_name) in &[
        (
            10_000,
            PARAPHRASE_ALBERT_SMALL_V2,
            "tiny-stories-paraphrase-albert-small-v2-10k-writable",
        ),
        (
            25_000,
            PARAPHRASE_ALBERT_SMALL_V2,
            "tiny-stories-paraphrase-albert-small-v2-25k-writable",
        ),
        (
            50_000,
            PARAPHRASE_ALBERT_SMALL_V2,
            "tiny-stories-paraphrase-albert-small-v2-50k-writable",
        ),
        (
            100_000,
            PARAPHRASE_ALBERT_SMALL_V2,
            "tiny-stories-paraphrase-albert-small-v2-100k-writable",
        ),
        (
            1_000_000,
            PARAPHRASE_ALBERT_SMALL_V2,
            "tiny-stories-paraphrase-albert-small-v2-1M-writable",
        ),
    ] {
        let reference = Arc::new(TinyStoriesDataSet::new(TinyStoriesDataSetType::reference(
            "reference",
            model,
            1_000_000,
        )));
        data_sets.push(Arc::new(ReferencingDataSet {
            references: reference,
            operates_on: data_set_name.to_string(),
            cardinality: *cardinality,
        }) as _);
    }
    data_sets.push(Arc::new(RoundRobinDataSet {
        name: "tiny-stories".to_string(),
        description: "tiny stories data sets".to_string(),
        data_sets: TINY_STORIES_DATA_SETS
            .iter()
            .map(|ds| Arc::new(ds.clone()) as _)
            .collect(),
        index: AtomicUsize::new(0),
    }) as _);
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

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct References {
    references: serde_json::Value,
    operates_on: String,
    cardinality: usize,
}

/// Get a data set from a particular JSON value.
pub fn from_json(json: &serde_json::Value) -> Option<Arc<dyn DataSet>> {
    // NOTE(rescrv):  I don't like that we use json attributes to identify data sets, but it's the
    // only robust way I can think of that's not encoding everything to strings or reworking the
    // data set type to be an enum.
    if let Some(data_set) = all_data_sets()
        .into_iter()
        .find(|data_set| data_set.json() == *json)
    {
        Some(data_set)
    } else {
        let references: Result<References, _> = serde_json::from_value(json.clone());
        if let Ok(references) = references {
            Some(Arc::new(ReferencingDataSet {
                references: from_json(&references.references)?,
                operates_on: references.operates_on,
                cardinality: references.cardinality,
            }))
        } else {
            None
        }
    }
}

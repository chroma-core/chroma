//! This is a synthetic data set that generates documents by using a sparse embedding over a fixed
//! set of words.  Essentially, each dimension of the embedding corresponds to a word and is zero
//! when the word is absent and one when the word is present.  In two dimensions, the space looks
//! like this:
//!
//! ```text
//! │ 01  11
//! │        
//! │ 00  10
//! └────────
//! ```
//!
//! The algorithm is simple:  Pick a random point in space, which we shall call the center of a
//! cluster, and then generate a number of documents around that point.  The number of documents is
//! parameterized.  The idea being that these documents, by nature of the randomness of the center,
//! will be more alike to each other than to any random document in the space.  Given a random
//! point, we can recall that point measure the recall of the documents in the cluster.
//!
//! To generate clusters randomly, we use the `guacamole` crate.  Guacamole provides a fast way to
//! seed a random number generator, and provides predictability in the seed so that if you pick two
//! numbers that are far apart, the streams are predictably far apart.  Given a guacamole stream,
//! we can generate the same clusters deterministically by using the same set of seeds, one per
//! cluster (this is why being quick to seek is important).
//!
//! Internally, guacamole provides primitives that make it easy to manage the set of seeds to get a
//! variety of data sets out of the synthetic data.

use chromadb::collection::{CollectionEntries, GetOptions, QueryOptions};
use chromadb::ChromaClient;
use guacamole::combinators::*;
use guacamole::{FromGuacamole, Guacamole};
use siphasher::sip::SipHasher24;
use std::collections::HashSet;
use tracing::Instrument;

use crate::words::MANY_WORDS;
use crate::{DataSet, Error, GetQuery, KeySelector, QueryQuery, Skew, UpsertQuery, ZIPF_CACHE};

const EMBEDDING_BYTES: usize = 128;
const EMBEDDING_SIZE: usize = 8 * EMBEDDING_BYTES;

/// This magic constant for hashing gives a consistent way to compute the document ID from the
/// document content when paired with siphasher.
pub const MAGIC_CONSTANT_FOR_HASHING: [u8; 16] = [
    0x63, 0x68, 0x72, 0x6f, 0x6d, 0x61, 0x20, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x6d, 0x61, 0x72, 0x6b,
];

fn embedding(embedding: [u8; EMBEDDING_BYTES]) -> Vec<f32> {
    let mut result = vec![];
    for byte in embedding.into_iter() {
        for j in 0..8 {
            if byte & (1 << j) != 0 {
                result.push(1.0);
            } else {
                result.push(0.0);
            }
        }
    }
    result
}

/// Options for generating synthetic data.
#[derive(Clone, Debug)]
pub struct EmbeddingOptions {
    /// The number of clusters to generate.
    pub num_clusters: usize,
    /// The seed for the random number generator for the clusters.  Given the same seed, the
    /// clusters will overlap.  For example, if you generate 10 clusters with the same seed and
    /// then update the num_clusters to 100, you will generate the same 10 clusters and 90 new
    /// clusters.
    pub seed_clusters: usize,
    /// Clustering options.
    pub clustering: ClusterOptions,
}

/// Options for generating a single cluster.
#[derive(Clone, Debug)]
pub struct ClusterOptions {
    /// The maximum number of adjacent documents.
    pub max_adjacent: u32,
    /// The theta of the zipf distribution for the number of adjacent documents.
    pub adjacent_theta: f64,
}

/// A document is a string of content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    /// The content of the document.
    pub content: String,
}

impl Document {
    /// The ID of the document.  This ID is a deterministic function of document content.
    pub fn id(&self) -> String {
        let hasher = SipHasher24::new_with_key(&MAGIC_CONSTANT_FOR_HASHING);
        let h = hasher.hash(self.content.as_bytes());
        format!("doc:{}", h)
    }

    /// The embedding of the document.  This embedding is a deterministic function of document
    /// content.
    pub fn embedding(&self) -> Vec<f32> {
        let mut result = vec![];
        let words = self.content.split_whitespace().collect::<Vec<_>>();
        for word in MANY_WORDS.iter() {
            if words.contains(word) {
                result.push(1.0);
            } else {
                result.push(0.0);
            }
        }
        result
    }
}

impl From<[u8; EMBEDDING_BYTES]> for Document {
    fn from(embedding: [u8; EMBEDDING_BYTES]) -> Document {
        let document = MANY_WORDS
            .iter()
            .enumerate()
            .filter_map(|(idx, word)| {
                // If the idx'th bit is set...
                if embedding[idx >> 3] & (1 << (idx & 0x7)) != 0 {
                    Some(*word)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        Document { content: document }
    }
}

/// The representation of a single cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cluster {
    pub cluster_id: u64,
    pub center: [u8; EMBEDDING_BYTES],
    pub docs: Vec<Document>,
}

impl FromGuacamole<ClusterOptions> for Cluster {
    fn from_guacamole(co: &mut ClusterOptions, guac: &mut Guacamole) -> Cluster {
        let cluster_id: u64 = any(guac);
        let mut embedding = [0u8; EMBEDDING_BYTES];
        guac.generate(&mut embedding);
        let center = embedding;
        let num_adjacent = range_to(co.max_adjacent)(guac) + 1;
        let mut bits_to_flip = vec![];
        while bits_to_flip.len() < num_adjacent as usize {
            let bit_to_flip: u16 = any(guac);
            let bit_to_flip = bit_to_flip as usize & (EMBEDDING_SIZE - 1);
            if !bits_to_flip.contains(&bit_to_flip) {
                bits_to_flip.push(bit_to_flip);
            }
        }
        let mut docs = vec![];
        for bit_to_flip in bits_to_flip.into_iter() {
            let mut embedding = embedding;
            embedding[bit_to_flip >> 3] ^= 1 << (bit_to_flip & 0x7);
            docs.push(Document::from(embedding));
        }
        Cluster {
            cluster_id,
            center,
            docs,
        }
    }
}

/// A synthetic data set that generates documents and sparse embeddings.
#[derive(Clone, Debug)]
pub struct SyntheticDataSet {
    collection: String,
    embedding_options: EmbeddingOptions,
}

impl SyntheticDataSet {
    /// Create a new synthetic data set.  The collection name will be the name of the document.
    pub fn new(collection: String, embedding_options: EmbeddingOptions) -> SyntheticDataSet {
        SyntheticDataSet {
            collection,
            embedding_options,
        }
    }

    /// Generate cluster by index.  If the index is greater than the number of clusters, it will
    /// wrap.
    fn cluster_by_index(&self, idx: usize) -> Cluster {
        let mut eo = self.embedding_options.clone();
        let unique_index = unique_set_index(eo.seed_clusters)(idx % eo.num_clusters);
        let cluster: Cluster = from_seed(from(&mut eo.clustering))(unique_index);
        cluster
    }

    /// Generate a cluster according to the skew function.
    fn cluster_by_skew(&self, skew: Skew, guac: &mut Guacamole) -> Cluster {
        let eo = self.embedding_options.clone();
        match skew {
            Skew::Uniform => {
                set_element(range_to(eo.num_clusters), |idx| self.cluster_by_index(idx))(guac)
            }
            Skew::Zipf { theta } => {
                let zipf = ZIPF_CACHE.from_theta(eo.num_clusters as u64, theta);
                let cluster: Cluster = set_element(
                    |guac| zipf.next(guac) as usize,
                    |idx| self.cluster_by_index(idx),
                )(guac);
                cluster
            }
        }
    }

    fn sample_ids(&self, skew: Skew, guac: &mut Guacamole, limit: usize) -> Vec<String> {
        let mut ids = vec![];
        for _ in 0..limit {
            let cluster = self.cluster_by_skew(skew, guac);
            let doc_idx = (any::<u32>(guac) as u64 * cluster.docs.len() as u64) >> 32;
            ids.push(cluster.docs[doc_idx as usize].id());
        }
        ids
    }

    async fn upsert_sequential(
        &self,
        client: &ChromaClient,
        _: UpsertQuery,
        idx: usize,
        _: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_or_create_collection(&self.name(), None).await?;
        let mut ids = vec![];
        let mut embeddings = vec![];
        let mut docs = vec![];
        let cluster = self.cluster_by_index(idx);
        for doc in cluster.docs.iter() {
            ids.push(doc.id());
            embeddings.push(doc.embedding());
            docs.push(doc.content.as_str());
        }
        let ids = ids.iter().map(String::as_str).collect();
        let entries = CollectionEntries {
            ids,
            embeddings: Some(embeddings),
            metadatas: None,
            documents: Some(docs),
        };
        let results = collection
            .upsert(entries, None)
            .instrument(tracing::info_span!("upsert_sequential"))
            .await;
        let _results = results?;
        Ok(())
    }

    async fn upsert_random(
        &self,
        client: &ChromaClient,
        uq: UpsertQuery,
        skew: Skew,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_or_create_collection(&self.name(), None).await?;
        let mut ids = vec![];
        let mut embeddings = vec![];
        let mut docs = vec![];
        let mut seen: HashSet<String> = HashSet::default();
        let cluster = self.cluster_by_skew(skew, guac);
        let num_this_cluster = (cluster.docs.len() as f64 * uq.associativity).ceil() as usize;
        for _ in 0..num_this_cluster {
            let doc_idx = (any::<u32>(guac) as u64 * cluster.docs.len() as u64) >> 32;
            if seen.contains(&cluster.docs[doc_idx as usize].id()) {
                continue;
            }
            seen.insert(cluster.docs[doc_idx as usize].id());
            ids.push(cluster.docs[doc_idx as usize].id());
            embeddings.push(cluster.docs[doc_idx as usize].embedding());
            docs.push(cluster.docs[doc_idx as usize].content.clone());
        }
        let ids = ids.iter().map(String::as_str).collect();
        let docs = docs.iter().map(String::as_str).collect();
        let entries = CollectionEntries {
            ids,
            embeddings: Some(embeddings),
            documents: Some(docs),
            ..Default::default()
        };
        let results = collection
            .upsert(entries, None)
            .instrument(tracing::info_span!("upsert_random"))
            .await;
        let _results = results?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataSet for SyntheticDataSet {
    fn name(&self) -> String {
        self.collection.clone()
    }

    fn description(&self) -> String {
        "a synthetic data set".to_string()
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json! {{
            "bit_difference": self.collection,
        }}
    }

    fn cardinality(&self) -> usize {
        // NOTE(rescrv):  This will report low.  There is currently no means by which a synthetic
        // can be referenced through the built-in data sets, so we just let this be a broken API.
        self.embedding_options.num_clusters
    }

    async fn get(
        &self,
        client: &ChromaClient,
        gq: GetQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_or_create_collection(&self.name(), None).await?;
        let limit = gq.limit.sample(guac);
        let mut ids = self.sample_ids(gq.skew, guac, limit);
        let where_metadata = gq.metadata.map(|m| m.to_json(guac));
        let where_document = gq.document.map(|m| m.to_json(guac));
        let results = collection
            .get(GetOptions {
                ids: ids.clone(),
                where_metadata: where_metadata.clone(),
                limit: Some(limit),
                offset: None,
                where_document: where_document.clone(),
                include: None,
            })
            .instrument(tracing::info_span!("get", limit = limit))
            .await;
        let mut results = results?;
        ids.sort();
        results.ids.sort();
        if where_metadata.is_none() && where_document.is_none() && results.ids != ids {
            return Err(Box::new(Error::InvalidRequest(format!(
                "expected {:?} but got {:?}",
                ids, results.ids
            ))));
        }
        Ok(())
    }

    async fn query(
        &self,
        client: &ChromaClient,
        vq: QueryQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let collection = client.get_or_create_collection(&self.name(), None).await?;
        let cluster = self.cluster_by_skew(vq.skew, guac);
        let where_metadata = vq.metadata.map(|m| m.to_json(guac));
        let where_document = vq.document.map(|m| m.to_json(guac));
        let results = collection
            .query(
                QueryOptions {
                    query_embeddings: Some(vec![embedding(cluster.center)]),
                    n_results: Some(cluster.docs.len()),
                    include: None,
                    query_texts: None,
                    where_metadata: where_metadata.clone(),
                    where_document: where_document.clone(),
                },
                None,
            )
            .instrument(tracing::info_span!("query"))
            .await;
        let results = results?;
        if where_metadata.is_none() && where_document.is_none() {
            println!("expected {:?} but got {:?}", cluster.docs, results.ids);
        }
        Ok(())
    }

    async fn upsert(
        &self,
        client: &ChromaClient,
        uq: UpsertQuery,
        guac: &mut Guacamole,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        match uq.key {
            KeySelector::Index(idx) => self.upsert_sequential(client, uq, idx, guac).await,
            KeySelector::Random(skew) => self.upsert_random(client, uq, skew, guac).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants() {
        assert_eq!(EMBEDDING_SIZE, MANY_WORDS.len());
    }

    mod synthethic {
        use std::time::Instant;

        use super::*;

        /// This test generates a synthetic data set of one cluster.
        ///
        /// It tests that no matter the index or skew, we get that one cluster.
        #[test]
        fn one() {
            let eo = EmbeddingOptions {
                num_clusters: 1,
                // Randomly chosen seed.
                seed_clusters: 10570396521983666744,
                clustering: ClusterOptions {
                    max_adjacent: 1,
                    adjacent_theta: 0.999,
                },
            };
            let sd = SyntheticDataSet {
                collection: "test".to_string(),
                embedding_options: eo,
            };
            let reference = sd.cluster_by_index(0);
            for i in 0..10 {
                let cluster = sd.cluster_by_index(i);
                assert_eq!(reference, cluster);
            }
            for _ in 0..50 {
                let cluster = sd.cluster_by_skew(
                    Skew::Uniform,
                    &mut Guacamole::new(Instant::now().elapsed().as_nanos() as u64),
                );
                assert_eq!(reference, cluster);
            }
        }

        /// This test generates one hundred clusters.
        ///
        /// It tests that they are different from each other.
        #[test]
        fn hundred() {
            let eo = EmbeddingOptions {
                num_clusters: 100,
                // Randomly chosen seed.
                seed_clusters: 16252348095511272702,
                clustering: ClusterOptions {
                    max_adjacent: 10,
                    adjacent_theta: 0.999,
                },
            };
            let sd = SyntheticDataSet {
                collection: "test".to_string(),
                embedding_options: eo,
            };
            let clusters = (0..100).map(|i| sd.cluster_by_index(i)).collect::<Vec<_>>();
            for (i, c1) in clusters.iter().enumerate() {
                println!("cluster {}", c1.cluster_id);
                for (j, c2) in clusters.iter().enumerate() {
                    if i == j {
                        assert_eq!(c1, c2);
                    } else {
                        assert_ne!(c1, c2, "{} {}", i, j);
                    }
                }
            }
        }
    }
}

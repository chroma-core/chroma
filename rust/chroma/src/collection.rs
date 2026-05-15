//! Collection operations for managing and querying vector embeddings.
//!
//! This module provides the [`ChromaCollection`] type, which represents a handle to a specific
//! collection within a Chroma database. Collections are the primary storage unit for vector
//! embeddings and their associated metadata, documents, and URIs.
//!
//! # Operations
//!
//! Collections support the following categories of operations:
//!
//! - **Metadata access**: [`database()`](ChromaCollection::database), [`metadata()`](ChromaCollection::metadata), [`schema()`](ChromaCollection::schema), [`tenant()`](ChromaCollection::tenant)
//! - **Read operations**: [`count()`](ChromaCollection::count), [`get()`](ChromaCollection::get), [`get_indexing_status()`](ChromaCollection::get_indexing_status), [`query()`](ChromaCollection::query), [`search()`](ChromaCollection::search)
//! - **Write operations**: [`add()`](ChromaCollection::add), [`update()`](ChromaCollection::update), [`upsert()`](ChromaCollection::upsert), [`delete()`](ChromaCollection::delete), [`modify()`](ChromaCollection::modify)

use std::sync::Arc;

use chroma_api_types::ForkCollectionPayload;
use chroma_types::{
    operator::{Key, QueryVector, RankExpr},
    plan::{ReadLevel, SearchPayload},
    AddCollectionRecordsRequest, AddCollectionRecordsResponse, Collection, CollectionUuid,
    DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, GetRequest, GetResponse,
    IncludeList, IndexStatusResponse, Metadata, MetadataValue, QueryRequest, QueryResponse, Schema,
    SearchRequest, SearchResponse, SparseVectorIndexConfig, UpdateCollectionConfiguration,
    UpdateCollectionPayload, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateMetadata, UpdateMetadataValue, UpsertCollectionRecordsRequest,
    UpsertCollectionRecordsResponse, Where, DOCUMENT_KEY,
};
use reqwest::Method;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    client::ChromaHttpClientError,
    embed::{
        dense_embedding_function_from_config, sparse_embedding_function_from_config,
        DenseEmbeddingFunction, EmbeddingError,
    },
    ChromaHttpClient,
};

#[derive(Deserialize)]
struct ForkCountResponse {
    count: usize,
}

/// A handle to a specific collection within a Chroma database.
///
/// Collections are the primary storage unit in Chroma for vector embeddings and associated metadata.
/// Each collection belongs to a specific tenant and database, and contains records that can be
/// queried by similarity, filtered by metadata, and managed through CRUD operations.
///
/// # Architecture
///
/// A `ChromaCollection` is a lightweight reference to collection metadata that shares the underlying
/// HTTP client. Operations on the collection are executed immediately against the server.
///
/// # Examples
///
/// ```
/// # use chroma::ChromaCollection;
/// # use chroma::client::ChromaHttpClientError;
/// # async fn example(collection: ChromaCollection) -> Result<(), ChromaHttpClientError> {
/// let count = collection.count().await?;
/// println!("Collection contains {} records", count);
///
/// let embeddings = vec![vec![0.1, 0.2, 0.3]];
/// let results = collection.query(embeddings, Some(10), None, None, None).await?;
/// println!("Found {} similar records", results.ids.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ChromaCollection {
    pub(crate) client: ChromaHttpClient,
    pub(crate) collection: Arc<Collection>,
    pub(crate) dense_embedding_function: Option<Arc<dyn DenseEmbeddingFunction>>,
}

impl std::fmt::Debug for ChromaCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChromaCollection")
            .field("database", &self.collection.database)
            .field("tenant", &self.collection.tenant)
            .field("name", &self.collection.name)
            .field("collection_id", &self.collection.collection_id)
            .field("version", &self.collection.version)
            .finish()
    }
}

/// Options for adding records with client-side embedding.
#[derive(Clone)]
pub struct AddRecordsOptions {
    /// Record IDs.
    pub ids: Vec<String>,
    /// Documents to store and to use for client-side embedding.
    pub documents: Vec<String>,
    /// Optional URIs to store with records.
    pub uris: Option<Vec<Option<String>>>,
    /// Optional metadata to store with records.
    pub metadatas: Option<Vec<Option<Metadata>>>,
}

/// Options for upserting records with client-side embedding.
#[derive(Clone)]
pub struct UpsertRecordsOptions {
    /// Record IDs.
    pub ids: Vec<String>,
    /// Documents to store and to use for client-side embedding.
    pub documents: Vec<String>,
    /// Optional URIs to store with records.
    pub uris: Option<Vec<Option<String>>>,
    /// Optional metadata updates to store with records.
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

/// Options for updating records with optional client-side embedding.
#[derive(Clone)]
pub struct UpdateRecordsOptions {
    /// Record IDs.
    pub ids: Vec<String>,
    /// Optional document updates. Present documents are embedded client-side.
    pub documents: Option<Vec<Option<String>>>,
    /// Optional URI updates.
    pub uris: Option<Vec<Option<String>>>,
    /// Optional metadata updates.
    pub metadatas: Option<Vec<Option<UpdateMetadata>>>,
}

/// Options for querying records with client-side query text embedding.
#[derive(Clone)]
pub struct QueryRecordsOptions {
    /// Query texts to embed client-side.
    pub query_texts: Vec<String>,
    /// Number of nearest neighbors to return per query.
    pub n_results: Option<u32>,
    /// Optional metadata filter.
    pub r#where: Option<Where>,
    /// Optional candidate ID restriction.
    pub ids: Option<Vec<String>>,
    /// Optional response include list.
    pub include: Option<IncludeList>,
}

/// Options for hybrid search with client-side dense and sparse query embedding.
#[derive(Clone)]
pub struct SearchRecordsOptions {
    /// Query texts used to embed dense and sparse query vectors.
    pub query_texts: Vec<String>,
    /// Metadata key that stores sparse vectors. When absent, the collection schema is used.
    pub sparse_key: Option<String>,
    /// Optional metadata filter.
    pub r#where: Option<Where>,
    /// Optional candidate ID restriction.
    pub ids: Option<Vec<String>>,
    /// Optional number of results to return per search payload.
    pub limit: Option<u32>,
    /// Number of results to skip per search payload.
    pub offset: u32,
    /// Optional KNN candidate limit. Defaults to `limit`, then the server KNN default.
    pub rank_limit: Option<u32>,
    /// Optional keys to return from each search payload.
    pub select: Option<Vec<Key>>,
    /// Optional dense KNN weight. Defaults to 1.0.
    pub dense_weight: Option<f32>,
    /// Optional sparse KNN weight. Defaults to 1.0.
    pub sparse_weight: Option<f32>,
    /// Controls whether to read from the write-ahead log.
    pub read_level: ReadLevel,
}

/// Options for modifying a collection.
#[derive(Clone, Default)]
pub struct ModifyCollectionOptions {
    /// Optional new collection name.
    pub new_name: Option<String>,
    /// Optional metadata update.
    pub new_metadata: Option<UpdateMetadata>,
    /// Optional collection configuration update.
    pub new_configuration: Option<UpdateCollectionConfiguration>,
    /// Optional dense embedding function to attach and persist in `new_configuration`.
    pub embedding_function: Option<Arc<dyn DenseEmbeddingFunction>>,
}

impl ChromaCollection {
    pub(crate) fn new(
        client: ChromaHttpClient,
        collection: Collection,
        dense_embedding_function: Option<Arc<dyn DenseEmbeddingFunction>>,
    ) -> Self {
        Self {
            client,
            collection: Arc::new(collection),
            dense_embedding_function,
        }
    }

    /// Returns the database ID that contains this collection.
    pub fn database(&self) -> &str {
        &self.collection.database
    }

    /// Returns the collection's metadata, if any was specified during creation.
    pub fn metadata(&self) -> &Option<Metadata> {
        &self.collection.metadata
    }

    /// Returns the collection's internal schema definition, if available.
    pub fn schema(&self) -> &Option<Schema> {
        &self.collection.schema
    }

    /// Returns the tenant ID that owns this collection.
    pub fn tenant(&self) -> &str {
        &self.collection.tenant
    }

    /// Returns the user-assigned name of this collection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # fn example(collection: ChromaCollection) {
    /// assert_eq!(collection.name(), "my_collection");
    /// # }
    /// ```
    pub fn name(&self) -> &str {
        &self.collection.name
    }

    /// Returns the unique identifier assigned to this collection by Chroma.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # fn example(collection: ChromaCollection) {
    /// let id = collection.id();
    /// println!("Collection ID: {}", id);
    /// # }
    /// ```
    pub fn id(&self) -> CollectionUuid {
        self.collection.collection_id
    }

    /// Returns the version number of this collection's schema or configuration.
    ///
    /// The version increments when the collection's structure or settings change.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # fn example(collection: ChromaCollection) {
    /// let version = collection.version();
    /// assert!(version >= 0);
    /// # }
    /// ```
    pub fn version(&self) -> i32 {
        self.collection.version
    }

    /// Computes the total number of records stored in this collection.
    ///
    /// # Errors
    ///
    /// Returns an error if network communication fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = collection.count().await?;
    /// assert!(count >= 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count(&self) -> Result<u32, ChromaHttpClientError> {
        self.count_with_options(ReadLevel::IndexAndWal).await
    }

    /// Count with custom read level controlling whether to read from the write-ahead log.
    ///
    /// By default, counts read from both the compacted index and the write-ahead log (WAL),
    /// ensuring all committed writes are visible. For higher throughput at the cost of
    /// potentially missing recent uncommitted writes, use `ReadLevel::IndexOnly` to skip
    /// the WAL and read only from the compacted index.
    ///
    /// # Arguments
    ///
    /// * `read_level` - Controls data sources for the query:
    ///   - [`ReadLevel::IndexAndWal`] - Read from both the compacted index and WAL (default).
    ///     All committed writes will be visible.
    ///   - [`ReadLevel::IndexOnly`] - Read only from the compacted index, skipping the WAL.
    ///     Faster, but recent writes that haven't been compacted may not be visible.
    ///   - [`ReadLevel::IndexAndBoundedWal`] - Read from the index and up to a server-configured
    ///     number of WAL entries for bounded query latency.
    ///
    /// # Example
    ///
    /// ```
    /// use chroma::types::ReadLevel;
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Skip WAL for faster count (may miss recent writes)
    /// let count = collection.count_with_options(ReadLevel::IndexOnly).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_with_options(
        &self,
        read_level: ReadLevel,
    ) -> Result<u32, ChromaHttpClientError> {
        #[derive(Serialize)]
        struct CountQueryParams {
            read_level: ReadLevel,
        }
        self.send_with_query::<(), CountQueryParams, u32>(
            true,
            "count",
            "count",
            Method::GET,
            None,
            Some(CountQueryParams { read_level }),
        )
        .await
    }

    /// Gets the indexing status of this collection.
    ///
    /// Returns information about how many operations have been indexed versus how many are
    /// pending. This is useful for monitoring the progress of bulk data ingestion.
    ///
    /// # Errors
    ///
    /// Returns an error if network communication fails, or if unauthenticated.
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let status = collection.get_indexing_status().await?;
    /// println!("Indexing progress: {:.1}%", status.op_indexing_progress * 100.0);
    /// println!("Indexed: {} / Total: {}", status.num_indexed_ops, status.total_ops);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_indexing_status(&self) -> Result<IndexStatusResponse, ChromaHttpClientError> {
        self.send::<(), IndexStatusResponse>(
            true,
            "indexing_status",
            "indexing_status",
            Method::GET,
            None,
        )
        .await
    }

    /// Modifies the collection's name or metadata.
    ///
    /// Updates the collection's configuration on the server and synchronizes the local
    /// collection handle with the new values. At least one of `new_name` or `new_metadata`
    /// should be specified; passing `None` for both parameters is valid but has no effect.
    ///
    /// Note that this method takes a mutable reference to self because it updates the
    /// local collection metadata after the server-side modification succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A collection with the new name already exists in the same database
    /// - Network communication fails
    /// - The authenticated user lacks sufficient permissions
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # use chroma_types::Metadata;
    /// # async fn example(mut collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut metadata = Metadata::new();
    /// metadata.insert("version".to_string(), "2.0".into());
    ///
    /// collection.modify(
    ///     Some("my_renamed_collection"),
    ///     Some(metadata)
    /// ).await?;
    ///
    /// assert_eq!(collection.name(), "my_renamed_collection");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn modify(
        &mut self,
        new_name: Option<impl AsRef<str>>,
        new_metadata: Option<Metadata>,
    ) -> Result<(), ChromaHttpClientError> {
        let new_name = new_name.map(|name| name.as_ref().to_string());
        let update_metadata = new_metadata.clone().map(|metadata| {
            metadata
                .into_iter()
                .map(|(key, value)| (key, UpdateMetadataValue::from(value)))
                .collect()
        });
        self.modify_with_options(ModifyCollectionOptions {
            new_name: new_name.clone(),
            new_metadata: update_metadata,
            ..Default::default()
        })
        .await?;

        if let Some(metadata) = new_metadata {
            let mut updated_collection = (*self.collection).clone();
            updated_collection.metadata = Some(metadata);
            self.collection = Arc::new(updated_collection);
        }

        Ok(())
    }

    /// Modifies collection name, metadata, or configuration.
    pub async fn modify_with_options(
        &mut self,
        options: ModifyCollectionOptions,
    ) -> Result<(), ChromaHttpClientError> {
        let ModifyCollectionOptions {
            new_name,
            new_metadata,
            mut new_configuration,
            embedding_function,
        } = options;

        if let Some(embedding_function) = embedding_function.as_ref() {
            let embedding_configuration = embedding_function.configuration();
            match new_configuration.as_mut() {
                Some(configuration) if configuration.embedding_function.is_some() => {
                    return Err(EmbeddingError::Configuration(
                        "embedding function provided when already defined in collection configuration update"
                            .to_string(),
                    )
                    .into());
                }
                Some(configuration) => {
                    configuration.embedding_function = Some(embedding_configuration);
                }
                None => {
                    new_configuration = Some(UpdateCollectionConfiguration {
                        hnsw: None,
                        spann: None,
                        embedding_function: Some(embedding_configuration),
                    });
                }
            }
        }

        // Returns empty map ({})
        self.send::<_, serde_json::Value>(
            false,
            "modify",
            "",
            Method::PUT,
            Some(UpdateCollectionPayload {
                new_name: new_name.clone(),
                new_metadata: new_metadata.clone(),
                new_configuration: new_configuration.clone(),
            }),
        )
        .await?;

        let mut updated_collection = (*self.collection).clone();
        if let Some(name) = new_name.as_ref() {
            updated_collection.name = name.clone();
        }
        if let Some(metadata_update) = new_metadata {
            let mut metadata = updated_collection.metadata.take().unwrap_or_default();
            for (key, value) in metadata_update {
                if matches!(value, UpdateMetadataValue::None) {
                    metadata.remove(&key);
                } else {
                    let value = MetadataValue::try_from(&value)
                        .map_err(|err| EmbeddingError::InvalidInput(err.to_string()))?;
                    metadata.insert(key, value);
                }
            }
            updated_collection.metadata = Some(metadata);
        }

        self.collection = Arc::new(updated_collection);
        if let Some(embedding_function) = embedding_function {
            self.dense_embedding_function = Some(embedding_function);
        }

        Ok(())
    }

    /// Retrieves records from the collection by ID or metadata filter.
    ///
    /// At least one of `ids` or `where` must be specified. The `include` parameter controls
    /// which fields are returned (embeddings, documents, metadata, URIs).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # use chroma_types::IncludeList;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.get(
    ///     Some(vec!["id1".to_string(), "id2".to_string()]),
    ///     None,
    ///     Some(10),
    ///     Some(0),
    ///     Some(IncludeList::default_get())
    /// ).await?;
    /// println!("Retrieved {} records", response.ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
        offset: Option<u32>,
        include: Option<IncludeList>,
    ) -> Result<GetResponse, ChromaHttpClientError> {
        let request = GetRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
            limit,
            offset.unwrap_or_default(),
            include.unwrap_or_else(IncludeList::default_get),
        )?;
        let request = request.into_payload()?;
        self.send(true, "get", "get", Method::POST, Some(request))
            .await
    }

    /// Performs vector similarity search against the collection.
    ///
    /// Finds the `n_results` nearest neighbors for each query embedding using the collection's
    /// configured distance metric. Results can be filtered by metadata conditions via `where`
    /// or restricted to a subset of IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Embedding dimensions don't match the collection's schema
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = vec![vec![0.1, 0.2, 0.3]];
    /// let results = collection.query(
    ///     query,
    ///     Some(5),
    ///     None,
    ///     None,
    ///     None
    /// ).await?;
    ///
    /// for (i, ids) in results.ids.iter().enumerate() {
    ///     println!("Query {} found {} neighbors", i, ids.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(
        &self,
        query_embeddings: Vec<Vec<f32>>,
        n_results: Option<u32>,
        r#where: Option<Where>,
        ids: Option<Vec<String>>,
        include: Option<IncludeList>,
    ) -> Result<QueryResponse, ChromaHttpClientError> {
        let request = QueryRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
            query_embeddings,
            n_results.unwrap_or(10),
            include.unwrap_or_else(IncludeList::default_query),
        )?;
        let request = request.into_payload()?;
        self.send(true, "query", "query", Method::POST, Some(request))
            .await
    }

    /// Performs similarity search with client-side query text embedding.
    pub async fn query_records(
        &self,
        options: QueryRecordsOptions,
    ) -> Result<QueryResponse, ChromaHttpClientError> {
        let QueryRecordsOptions {
            query_texts,
            n_results,
            r#where,
            ids,
            include,
        } = options;

        let query_embeddings = self.embed_query_texts(&query_texts).await?;

        self.query(query_embeddings, n_results, r#where, ids, include)
            .await
    }

    /// Performs hybrid search with client-side dense and sparse query embedding.
    pub async fn search_records(
        &self,
        options: SearchRecordsOptions,
    ) -> Result<SearchResponse, ChromaHttpClientError> {
        let SearchRecordsOptions {
            query_texts,
            sparse_key,
            r#where,
            ids,
            limit,
            offset,
            rank_limit,
            select,
            dense_weight,
            sparse_weight,
            read_level,
        } = options;

        let dense_embeddings = self.embed_query_texts(&query_texts).await?;
        if dense_embeddings.is_empty() {
            return Err(EmbeddingError::InvalidInput(
                "search_records requires at least one query".to_string(),
            )
            .into());
        }
        let (sparse_key, sparse_config) =
            self.sparse_query_embedding_target(sparse_key.as_deref())?;
        let sparse_vectors = self
            .sparse_embed(&sparse_config, &query_texts, true)
            .await?;
        ensure_len(
            "sparse query vectors",
            sparse_vectors.len(),
            dense_embeddings.len(),
        )?;

        let dense_weight = dense_weight.unwrap_or(1.0);
        if !dense_weight.is_finite() {
            return Err(EmbeddingError::InvalidInput(
                "dense_weight must be a finite number".to_string(),
            )
            .into());
        }
        let sparse_weight = sparse_weight.unwrap_or(1.0);
        if !sparse_weight.is_finite() {
            return Err(EmbeddingError::InvalidInput(
                "sparse_weight must be a finite number".to_string(),
            )
            .into());
        }
        let rank_limit = rank_limit
            .or(limit)
            .unwrap_or_else(RankExpr::default_knn_limit);

        let searches = dense_embeddings
            .into_iter()
            .zip(sparse_vectors)
            .map(|(dense_embedding, sparse_vector)| {
                let dense_knn = RankExpr::Knn {
                    query: QueryVector::Dense(dense_embedding),
                    key: Key::Embedding,
                    limit: rank_limit,
                    default: None,
                    return_rank: false,
                };
                let sparse_knn = RankExpr::Knn {
                    query: QueryVector::Sparse(sparse_vector),
                    key: Key::field(sparse_key.clone()),
                    limit: rank_limit,
                    default: None,
                    return_rank: false,
                };
                let dense_rank = if dense_weight == 1.0 {
                    dense_knn
                } else {
                    dense_knn * dense_weight
                };
                let sparse_rank = if sparse_weight == 1.0 {
                    sparse_knn
                } else {
                    sparse_knn * sparse_weight
                };
                let rank = dense_rank + sparse_rank;
                let mut search = SearchPayload::default().rank(rank).limit(limit, offset);
                if let Some(r#where) = r#where.clone() {
                    search = search.r#where(r#where);
                }
                if let Some(ids) = ids.clone() {
                    search.filter.query_ids = Some(ids);
                }
                if let Some(select) = select.clone() {
                    search = search.select(select);
                }
                search
            })
            .collect();

        self.search_with_options(searches, read_level).await
    }

    /// Performs hybrid search on the collection using the Search API.
    ///
    /// The Search API provides a powerful, flexible interface for vector similarity search
    /// combined with metadata filtering and custom ranking expressions.
    ///
    /// # Arguments
    ///
    /// * `searches` - One or more search payloads to execute in a single request
    ///
    /// # Returns
    ///
    /// A `SearchResponse` containing results for each search payload
    ///
    /// # Examples
    ///
    /// ## Basic similarity search
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Search with a query vector
    /// let search = SearchPayload::default()
    ///     .rank(RankExpr::Knn {
    ///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///         key: Key::Embedding,
    ///         limit: 100,
    ///         default: None,
    ///         return_rank: false,
    ///     })
    ///     .limit(Some(10), 0)
    ///     .select([Key::Document, Key::Score]);
    ///
    /// let results = collection.search(vec![search]).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Filtered search with metadata
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Filter by category and year, then rank by similarity
    /// let search = SearchPayload::default()
    ///     .r#where(
    ///         Key::field("category").eq("science")
    ///             & Key::field("year").gte(2020)
    ///     )
    ///     .rank(RankExpr::Knn {
    ///         query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///         key: Key::Embedding,
    ///         limit: 200,
    ///         default: None,
    ///         return_rank: false,
    ///     })
    ///     .limit(Some(5), 0)
    ///     .select([Key::Document, Key::Score, Key::field("title")]);
    ///
    /// let results = collection.search(vec![search]).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Hybrid search with custom ranking
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Combine two KNN searches with weights
    /// let dense_knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///     key: Key::Embedding,
    ///     limit: 200,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// let sparse_knn = RankExpr::Knn {
    ///     query: QueryVector::Dense(vec![0.1, 0.2, 0.3]), // Use sparse vector in practice
    ///     key: Key::field("sparse_embedding"),
    ///     limit: 200,
    ///     default: None,
    ///     return_rank: false,
    /// };
    ///
    /// // Weighted combination: 70% dense + 30% sparse
    /// let hybrid_rank = dense_knn * 0.7 + sparse_knn * 0.3;
    ///
    /// let search = SearchPayload::default()
    ///     .rank(hybrid_rank)
    ///     .limit(Some(10), 0)
    ///     .select([Key::Document, Key::Score]);
    ///
    /// let results = collection.search(vec![search]).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Batch operations
    ///
    /// ```
    /// use chroma_types::plan::SearchPayload;
    /// use chroma_types::operator::{RankExpr, QueryVector, Key};
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Run multiple searches in one request
    /// let searches = vec![
    ///     SearchPayload::default()
    ///         .r#where(Key::field("category").eq("tech"))
    ///         .rank(RankExpr::Knn {
    ///             query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///             key: Key::Embedding,
    ///             limit: 100,
    ///             default: None,
    ///             return_rank: false,
    ///         })
    ///         .limit(Some(5), 0),
    ///     SearchPayload::default()
    ///         .r#where(Key::field("category").eq("science"))
    ///         .rank(RankExpr::Knn {
    ///             query: QueryVector::Dense(vec![0.1, 0.2, 0.3]),
    ///             key: Key::Embedding,
    ///             limit: 100,
    ///             default: None,
    ///             return_rank: false,
    ///         })
    ///         .limit(Some(5), 0),
    /// ];
    ///
    /// let results = collection.search(searches).await?;
    /// // results.results[0] contains first search results
    /// // results.results[1] contains second search results
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(
        &self,
        searches: Vec<SearchPayload>,
    ) -> Result<SearchResponse, ChromaHttpClientError> {
        self.search_with_options(searches, ReadLevel::IndexAndWal)
            .await
    }

    /// Search with custom read level controlling whether to read from the write-ahead log.
    ///
    /// By default, searches read from both the compacted index and the write-ahead log (WAL),
    /// ensuring all committed writes are visible. For higher throughput at the cost of
    /// potentially missing recent uncommitted writes, use `ReadLevel::IndexOnly` to skip
    /// the WAL and read only from the compacted index.
    ///
    /// # Arguments
    ///
    /// * `searches` - Vector of search payloads to execute
    /// * `read_level` - Controls data sources for the query:
    ///   - [`ReadLevel::IndexAndWal`] - Read from both the compacted index and WAL (default).
    ///     All committed writes will be visible.
    ///   - [`ReadLevel::IndexOnly`] - Read only from the compacted index, skipping the WAL.
    ///     Faster, but recent writes that haven't been compacted may not be visible.
    ///   - [`ReadLevel::IndexAndBoundedWal`] - Read from the index and up to a server-configured
    ///     number of WAL entries for bounded query latency.
    ///
    /// # Example
    ///
    /// ```
    /// use chroma::types::{SearchPayload, ReadLevel};
    ///
    /// # async fn example(collection: &chroma::ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let search = SearchPayload::default().limit(Some(10), 0);
    ///
    /// // Skip WAL for faster queries (may miss recent writes)
    /// let results = collection
    ///     .search_with_options(vec![search], ReadLevel::IndexOnly)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_with_options(
        &self,
        searches: Vec<SearchPayload>,
        read_level: ReadLevel,
    ) -> Result<SearchResponse, ChromaHttpClientError> {
        let request = SearchRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            searches,
            read_level,
        )?;
        let request = request.into_payload();
        self.send(true, "search", "search", Method::POST, Some(request))
            .await
    }

    /// Inserts new records into the collection.
    ///
    /// All provided vectors must have lengths equal: `ids`, `embeddings`, and optionally
    /// `documents`, `uris`, and `metadatas`. Records with duplicate IDs will cause an error.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Embedding dimensions don't match the collection's schema
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.add(
    ///     vec!["doc1".to_string(), "doc2".to_string()],
    ///     vec![vec![0.1, 0.2], vec![0.3, 0.4]],
    ///     Some(vec![Some("First document".to_string()), Some("Second document".to_string())]),
    ///     None,
    ///     None
    /// ).await?;
    /// println!("Added records successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<AddCollectionRecordsResponse, ChromaHttpClientError> {
        let request = AddCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send(false, "add", "add", Method::POST, Some(request))
            .await
    }

    /// Inserts records with client-side dense and sparse embedding.
    pub async fn add_records(
        &self,
        options: AddRecordsOptions,
    ) -> Result<AddCollectionRecordsResponse, ChromaHttpClientError> {
        let AddRecordsOptions {
            ids,
            documents,
            uris,
            metadatas,
        } = options;
        let record_count = ids.len();
        let documents = documents.into_iter().map(Some).collect::<Vec<_>>();
        let embeddings = self
            .embed_insert_documents(Some(&documents), record_count)
            .await?;
        let metadatas = self
            .apply_sparse_embeddings_to_metadatas(metadatas, Some(&documents), record_count)
            .await?;

        self.add(ids, embeddings, Some(documents), uris, metadatas)
            .await
    }

    /// Modifies existing records in the collection.
    ///
    /// Updates only the specified fields for records matching the provided IDs. Fields set to
    /// `None` or `Some(None)` remain unchanged. All non-`None` vectors must match the length of `ids`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Embedding dimensions don't match the collection's schema
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.update(
    ///     vec!["doc1".to_string()],
    ///     Some(vec![Some(vec![0.5, 0.6])]),
    ///     Some(vec![Some("Updated document text".to_string())]),
    ///     None,
    ///     None
    /// ).await?;
    /// println!("Updated records successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update(
        &self,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpdateCollectionRecordsResponse, ChromaHttpClientError> {
        let request = UpdateCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send(false, "update", "update", Method::POST, Some(request))
            .await
    }

    /// Updates records with client-side dense and sparse embedding.
    pub async fn update_records(
        &self,
        options: UpdateRecordsOptions,
    ) -> Result<UpdateCollectionRecordsResponse, ChromaHttpClientError> {
        let UpdateRecordsOptions {
            ids,
            documents,
            uris,
            metadatas,
        } = options;
        let record_count = ids.len();
        let embeddings = self
            .embed_update_documents(documents.as_deref(), record_count)
            .await?;
        let metadatas = self
            .apply_sparse_embeddings_to_metadatas(metadatas, documents.as_deref(), record_count)
            .await?;

        self.update(ids, embeddings, documents, uris, metadatas)
            .await
    }

    /// Inserts new records or updates existing ones based on ID.
    ///
    /// For each ID: if the record exists, updates it; otherwise, inserts a new record.
    /// This combines the semantics of [`add`](Self::add) and [`update`](Self::update) in a single operation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Embedding dimensions don't match the collection's schema
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.upsert(
    ///     vec!["doc1".to_string(), "doc2".to_string()],
    ///     vec![vec![0.1, 0.2], vec![0.3, 0.4]],
    ///     Some(vec![Some("Document 1".to_string()), Some("Document 2".to_string())]),
    ///     None,
    ///     None
    /// ).await?;
    /// println!("Upserted records successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upsert(
        &self,
        ids: Vec<String>,
        embeddings: Vec<Vec<f32>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpsertCollectionRecordsResponse, ChromaHttpClientError> {
        let request = UpsertCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        let request = request.into_payload();
        self.send(false, "upsert", "upsert", Method::POST, Some(request))
            .await
    }

    /// Inserts or updates records with client-side dense and sparse embedding.
    pub async fn upsert_records(
        &self,
        options: UpsertRecordsOptions,
    ) -> Result<UpsertCollectionRecordsResponse, ChromaHttpClientError> {
        let UpsertRecordsOptions {
            ids,
            documents,
            uris,
            metadatas,
        } = options;
        let record_count = ids.len();
        let documents = documents.into_iter().map(Some).collect::<Vec<_>>();
        let embeddings = self
            .embed_insert_documents(Some(&documents), record_count)
            .await?;
        let metadatas = self
            .apply_sparse_embeddings_to_metadatas(metadatas, Some(&documents), record_count)
            .await?;

        self.upsert(ids, embeddings, Some(documents), uris, metadatas)
            .await
    }

    /// Removes records from the collection by ID or metadata filter.
    ///
    /// At least one of `ids` or `where` must be specified to prevent accidental deletion
    /// of all records.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network communication fails
    /// - Request validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.delete(
    ///     Some(vec!["doc1".to_string(), "doc2".to_string()]),
    ///     None,
    ///     None,
    /// ).await?;
    /// println!("Deleted {} records", response.deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
    ) -> Result<DeleteCollectionRecordsResponse, ChromaHttpClientError> {
        let request = DeleteCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
            limit,
        )?;
        let request = request.into_payload()?;
        self.send(false, "delete", "delete", Method::POST, Some(request))
            .await
    }

    /// Creates a shallow copy of this collection under a new name.
    ///
    /// The forked collection resides in the same database and tenant but receives a distinct
    /// collection ID and name. The fork shares the same configuration and initial records
    /// as the source collection at the time of forking, but subsequent modifications to either
    /// collection do not affect the other.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A collection with the target name already exists
    /// - Network communication fails
    /// - The authenticated user lacks sufficient permissions
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let forked = collection.fork("experimental_version").await?;
    /// assert_eq!(forked.name(), "experimental_version");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fork(
        &self,
        new_name: impl Into<String>,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        let request = ForkCollectionPayload {
            new_name: new_name.into(),
        };
        let collection: Collection = self
            .send(false, "fork", "fork", Method::POST, Some(request))
            .await?;
        Ok(ChromaCollection::new(
            self.client.clone(),
            collection,
            self.dense_embedding_function.clone(),
        ))
    }

    /// Returns the number of forks that exist for this collection.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collection no longer exists on the server
    /// - Network communication fails
    /// - The authenticated user lacks sufficient permissions
    ///
    /// # Examples
    ///
    /// ```
    /// # use chroma::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = collection.fork_count().await?;
    /// println!("Collection has {} forks", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fork_count(&self) -> Result<usize, ChromaHttpClientError> {
        let response: ForkCountResponse = self
            .send::<(), _>(true, "fork_count", "fork_count", Method::GET, None)
            .await?;
        Ok(response.count)
    }

    async fn embed_insert_documents(
        &self,
        documents: Option<&[Option<String>]>,
        record_count: usize,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let documents = documents.ok_or_else(|| {
            EmbeddingError::InvalidInput(
                "documents are required when embeddings are omitted".to_string(),
            )
        })?;
        ensure_len("documents", documents.len(), record_count)?;

        let mut texts = Vec::with_capacity(record_count);
        for (index, document) in documents.iter().enumerate() {
            let document = document.as_ref().ok_or_else(|| {
                EmbeddingError::InvalidInput(format!(
                    "documents[{index}] is required when embeddings are omitted"
                ))
            })?;
            texts.push(document.clone());
        }

        self.embed_documents(&texts).await
    }

    async fn embed_update_documents(
        &self,
        documents: Option<&[Option<String>]>,
        record_count: usize,
    ) -> Result<Option<Vec<Option<Vec<f32>>>>, EmbeddingError> {
        let Some(documents) = documents else {
            return Ok(None);
        };
        ensure_len("documents", documents.len(), record_count)?;

        let mut positions = Vec::new();
        let mut texts = Vec::new();
        for (index, document) in documents.iter().enumerate() {
            if let Some(document) = document {
                positions.push(index);
                texts.push(document.clone());
            }
        }

        if texts.is_empty() {
            return Ok(None);
        }

        let embeddings = self.embed_documents(&texts).await?;
        ensure_embedding_count(texts.len(), embeddings.len())?;

        let mut result = vec![None; record_count];
        for (position, embedding) in positions.into_iter().zip(embeddings) {
            result[position] = Some(embedding);
        }
        Ok(Some(result))
    }

    async fn embed_documents(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let embedder = self.dense_embedding_function()?;
        let refs = texts.iter().map(String::as_str).collect::<Vec<_>>();
        let embeddings = embedder.embed_documents(&refs).await?;
        ensure_embedding_count(texts.len(), embeddings.len())?;
        Ok(embeddings)
    }

    async fn embed_query_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let embedder = self.dense_embedding_function()?;
        let refs = texts.iter().map(String::as_str).collect::<Vec<_>>();
        let embeddings = embedder.embed_query(&refs).await?;
        ensure_embedding_count(texts.len(), embeddings.len())?;
        Ok(embeddings)
    }

    fn dense_embedding_function(&self) -> Result<Arc<dyn DenseEmbeddingFunction>, EmbeddingError> {
        if let Some(embedding_function) = self.dense_embedding_function.as_ref() {
            return Ok(Arc::clone(embedding_function));
        }

        if let Some(configuration) = self.collection.dense_embedding_function() {
            return dense_embedding_function_from_config(
                configuration,
                self.client.chroma_cloud_api_key(),
            );
        }

        Err(EmbeddingError::InvalidInput(format!(
            "no embedding function found for collection '{}'",
            self.collection.name
        )))
    }

    fn sparse_embedding_targets(&self) -> Vec<(String, SparseVectorIndexConfig)> {
        let Some(schema) = self.collection.schema.as_ref() else {
            return Vec::new();
        };

        schema
            .sparse_vector_indices()
            .filter_map(|(key, sparse_index)| {
                if !sparse_index.enabled
                    || sparse_index.config.embedding_function.is_none()
                    || sparse_index.config.source_key.is_none()
                {
                    return None;
                }
                Some((key.to_string(), sparse_index.config.clone()))
            })
            .collect()
    }

    fn sparse_query_key(&self, sparse_key: Option<&str>) -> Result<String, EmbeddingError> {
        if let Some(sparse_key) = sparse_key {
            return Ok(sparse_key.to_string());
        }

        let schema = self.collection.schema.as_ref().ok_or_else(|| {
            EmbeddingError::InvalidInput(
                "sparse_key is required when the collection schema is unavailable".to_string(),
            )
        })?;

        let mut sparse_keys = schema
            .sparse_vector_indices()
            .filter_map(|(key, sparse_index)| sparse_index.enabled.then(|| key.to_string()));
        let sparse_key = sparse_keys.next().ok_or_else(|| {
            EmbeddingError::InvalidInput(
                "sparse_key is required because the collection has no sparse vector index"
                    .to_string(),
            )
        })?;
        if sparse_keys.next().is_some() {
            return Err(EmbeddingError::InvalidInput(
                "sparse_key is required because multiple sparse vector indexes are available"
                    .to_string(),
            ));
        }
        Ok(sparse_key)
    }

    fn sparse_query_embedding_target(
        &self,
        sparse_key: Option<&str>,
    ) -> Result<(String, SparseVectorIndexConfig), EmbeddingError> {
        let sparse_key = self.sparse_query_key(sparse_key)?;
        let schema = self.collection.schema.as_ref().ok_or_else(|| {
            EmbeddingError::InvalidInput(
                "collection schema is required to embed sparse query text".to_string(),
            )
        })?;
        let sparse_index = schema.sparse_vector_index(&sparse_key).ok_or_else(|| {
            EmbeddingError::InvalidInput(format!(
                "key '{sparse_key}' is not configured as a sparse vector index"
            ))
        })?;
        if !sparse_index.enabled {
            return Err(EmbeddingError::InvalidInput(format!(
                "sparse vector index for key '{sparse_key}' is disabled"
            )));
        }
        if sparse_index.config.embedding_function.is_none() {
            return Err(EmbeddingError::InvalidInput(format!(
                "sparse vector index for key '{sparse_key}' has no embedding function"
            )));
        }
        Ok((sparse_key, sparse_index.config.clone()))
    }

    async fn apply_sparse_embeddings_to_metadatas<M>(
        &self,
        metadatas: Option<Vec<Option<M>>>,
        documents: Option<&[Option<String>]>,
        record_count: usize,
    ) -> Result<Option<Vec<Option<M>>>, EmbeddingError>
    where
        M: SparseEmbeddingMetadata,
    {
        let targets = self.sparse_embedding_targets();
        if targets.is_empty() {
            return Ok(metadatas);
        }
        if let Some(metadatas) = metadatas.as_ref() {
            ensure_len("metadatas", metadatas.len(), record_count)?;
        }
        if let Some(documents) = documents {
            ensure_len("documents", documents.len(), record_count)?;
        }

        let Some(metadatas) = metadatas
            .or_else(|| documents.map(|_| (0..record_count).map(|_| None).collect::<Vec<_>>()))
        else {
            return Ok(None);
        };

        let mut updated = metadatas
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect::<Vec<_>>();

        for (target_key, config) in targets {
            let source_key = config.source_key.as_deref().unwrap_or_default();
            let mut inputs = Vec::new();
            let mut positions = Vec::new();

            if source_key == DOCUMENT_KEY {
                let Some(documents) = documents else {
                    continue;
                };
                for (index, metadata) in updated.iter().enumerate() {
                    if metadata.has_key(&target_key) {
                        continue;
                    }
                    if let Some(Some(document)) = documents.get(index) {
                        inputs.push(document.clone());
                        positions.push(index);
                    }
                }
            } else {
                for (index, metadata) in updated.iter().enumerate() {
                    if metadata.has_key(&target_key) {
                        continue;
                    }
                    if let Some(value) = metadata.get_str(source_key) {
                        inputs.push(value.clone());
                        positions.push(index);
                    }
                }
            }

            if inputs.is_empty() {
                continue;
            }

            let sparse_embeddings = self.sparse_embed(&config, &inputs, false).await?;
            ensure_embedding_count(positions.len(), sparse_embeddings.len())?;
            for (position, sparse_embedding) in positions.into_iter().zip(sparse_embeddings) {
                updated[position].insert_sparse(target_key.clone(), sparse_embedding);
            }
        }

        Ok(Some(
            updated
                .into_iter()
                .map(|metadata| {
                    if metadata.is_empty_metadata() {
                        None
                    } else {
                        Some(metadata)
                    }
                })
                .collect(),
        ))
    }

    async fn sparse_embed(
        &self,
        config: &SparseVectorIndexConfig,
        inputs: &[String],
        is_query: bool,
    ) -> Result<Vec<chroma_types::SparseVector>, EmbeddingError> {
        let embedding_function_config = config.embedding_function.as_ref().ok_or_else(|| {
            EmbeddingError::Configuration(
                "sparse embedding target has no embedding function".to_string(),
            )
        })?;
        let embedding_function = sparse_embedding_function_from_config(
            embedding_function_config,
            self.client.chroma_cloud_api_key(),
        )?;
        let refs = inputs.iter().map(String::as_str).collect::<Vec<_>>();
        if is_query {
            embedding_function.embed_query(&refs).await
        } else {
            embedding_function.embed_documents(&refs).await
        }
    }

    /// Internal transport method that constructs collection-specific API paths and delegates to the client.
    async fn send<Body: Serialize, Response: DeserializeOwned>(
        &self,
        read_only: bool,
        operation: &str,
        path: &str,
        method: Method,
        body: Option<Body>,
    ) -> Result<Response, ChromaHttpClientError> {
        self.send_with_query::<Body, (), Response>(
            read_only, operation, path, method, body, None::<()>,
        )
        .await
    }

    /// Internal transport method with query parameter support.
    async fn send_with_query<
        Body: Serialize,
        QueryParams: Serialize,
        Response: DeserializeOwned,
    >(
        &self,
        read_only: bool,
        operation: &str,
        path: &str,
        method: Method,
        body: Option<Body>,
        query_params: Option<QueryParams>,
    ) -> Result<Response, ChromaHttpClientError> {
        let operation_name = format!("collection_{operation}");
        let path = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/{}",
            self.collection.tenant, self.collection.database, self.collection.collection_id, path
        );
        let path = path.trim_end_matches("/");

        if read_only {
            self.client
                .send_read_only(&operation_name, method, path, body, query_params)
                .await
        } else {
            self.client
                .send(&operation_name, method, path, body, query_params)
                .await
        }
    }
}

trait SparseEmbeddingMetadata: Default {
    fn has_key(&self, key: &str) -> bool;
    fn get_str(&self, key: &str) -> Option<&String>;
    fn insert_sparse(&mut self, key: String, value: chroma_types::SparseVector);
    fn is_empty_metadata(&self) -> bool;
}

impl SparseEmbeddingMetadata for Metadata {
    fn has_key(&self, key: &str) -> bool {
        self.contains_key(key)
    }

    fn get_str(&self, key: &str) -> Option<&String> {
        match self.get(key) {
            Some(MetadataValue::Str(value)) => Some(value),
            _ => None,
        }
    }

    fn insert_sparse(&mut self, key: String, value: chroma_types::SparseVector) {
        self.insert(key, value.into());
    }

    fn is_empty_metadata(&self) -> bool {
        self.is_empty()
    }
}

impl SparseEmbeddingMetadata for UpdateMetadata {
    fn has_key(&self, key: &str) -> bool {
        self.contains_key(key)
    }

    fn get_str(&self, key: &str) -> Option<&String> {
        match self.get(key) {
            Some(UpdateMetadataValue::Str(value)) => Some(value),
            _ => None,
        }
    }

    fn insert_sparse(&mut self, key: String, value: chroma_types::SparseVector) {
        self.insert(key, value.into());
    }

    fn is_empty_metadata(&self) -> bool {
        self.is_empty()
    }
}

fn ensure_len(name: &str, actual: usize, expected: usize) -> Result<(), EmbeddingError> {
    if actual != expected {
        return Err(EmbeddingError::InvalidInput(format!(
            "{name} length {actual} does not match ids length {expected}"
        )));
    }
    Ok(())
}

fn ensure_embedding_count(expected: usize, actual: usize) -> Result<(), EmbeddingError> {
    if expected != actual {
        return Err(EmbeddingError::LengthMismatch { expected, actual });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::embed::{DenseEmbeddingFunction, EmbeddingError};
    use crate::tests::{unique_collection_name, with_client};
    use chroma_types::operator::{Key, QueryVector, RankExpr};
    use chroma_types::plan::{ReadLevel, SearchPayload};
    use chroma_types::{
        Collection, EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration, Include,
        IncludeList, Metadata, MetadataComparison, MetadataExpression, MetadataValue,
        PrimitiveOperator, SparseIndexAlgorithm, SparseVector, SparseVectorIndexConfig,
        UpdateMetadata, UpdateMetadataValue, Where,
    };
    use httpmock::prelude::HttpMockRequest;
    use httpmock::MockServer;

    struct MockDenseEmbeddingFunction {
        wrong_length: bool,
    }

    #[async_trait::async_trait]
    impl DenseEmbeddingFunction for MockDenseEmbeddingFunction {
        fn name(&self) -> &str {
            "mock_dense"
        }

        async fn embed_documents(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            let mut embeddings = input
                .iter()
                .map(|text| vec![text.len() as f32])
                .collect::<Vec<_>>();
            if self.wrong_length {
                embeddings.push(vec![999.0]);
            }
            Ok(embeddings)
        }

        async fn embed_query(&self, input: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            Ok(input
                .iter()
                .map(|text| vec![100.0 + text.len() as f32])
                .collect())
        }
    }

    fn test_collection(
        server: &MockServer,
        schema: Option<chroma_types::Schema>,
        dense_embedding_function: Option<Arc<dyn DenseEmbeddingFunction>>,
    ) -> ChromaCollection {
        let mut collection = Collection::default();
        collection.name = "test".to_string();
        collection.tenant = "tenant".to_string();
        collection.database = "database".to_string();
        collection.schema = schema;
        let client = ChromaHttpClient::new(crate::client::ChromaHttpClientOptions {
            endpoint: server.base_url().parse().unwrap(),
            tenant_id: Some("tenant".to_string()),
            database_name: Some("database".to_string()),
            ..Default::default()
        });
        ChromaCollection::new(client, collection, dense_embedding_function)
    }

    fn body_json(req: &HttpMockRequest) -> serde_json::Value {
        serde_json::from_slice(req.body_ref()).unwrap()
    }

    fn bm25_config() -> EmbeddingFunctionConfiguration {
        EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
            name: "chroma_bm25".to_string(),
            config: serde_json::json!({}),
        })
    }

    fn sparse_schema(source_key: &str) -> chroma_types::Schema {
        chroma_types::Schema::default()
            .create_index(
                Some("sparse"),
                SparseVectorIndexConfig {
                    embedding_function: Some(bm25_config()),
                    source_key: Some(source_key.to_string()),
                    bm25: Some(true),
                    algorithm: SparseIndexAlgorithm::default(),
                }
                .into(),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn add_records_embeds_documents() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            None,
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/add",
            collection.id()
        );
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    body["ids"] == serde_json::json!(["id1", "id2"])
                        && body["embeddings"] == serde_json::json!([[5.0], [6.0]])
                        && body["documents"] == serde_json::json!(["alpha", "rocket"])
                });
                then.status(200).json_body(serde_json::json!({}));
            })
            .await;

        collection
            .add_records(AddRecordsOptions {
                ids: vec!["id1".to_string(), "id2".to_string()],
                documents: vec!["alpha".to_string(), "rocket".to_string()],
                uris: None,
                metadatas: None,
            })
            .await
            .unwrap();

        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn query_records_uses_query_embedding_method() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            None,
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/query",
            collection.id()
        );
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    body["query_embeddings"] == serde_json::json!([[103.0]])
                        && body["n_results"] == serde_json::json!(3)
                });
                then.status(200).json_body(serde_json::json!({
                    "ids": [[]],
                    "embeddings": null,
                    "documents": null,
                    "uris": null,
                    "metadatas": null,
                    "distances": null,
                    "include": []
                }));
            })
            .await;

        collection
            .query_records(QueryRecordsOptions {
                query_texts: vec!["abc".to_string()],
                n_results: Some(3),
                r#where: None,
                ids: None,
                include: None,
            })
            .await
            .unwrap();

        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn search_records_embeds_dense_and_sparse_queries() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            Some(sparse_schema(DOCUMENT_KEY)),
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/search",
            collection.id()
        );
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    let rank_terms = body["searches"][0]["rank"]["$sum"]
                        .as_array()
                        .expect("rank should be a sum of dense and sparse KNNs");
                    let dense_knn = &rank_terms[0]["$knn"];
                    let sparse_knn = &rank_terms[1]["$knn"];

                    dense_knn["key"] == serde_json::json!("#embedding")
                        && dense_knn["query"] == serde_json::json!([106.0])
                        && sparse_knn["key"] == serde_json::json!("sparse")
                        && sparse_knn["query"]["#type"] == serde_json::json!("sparse_vector")
                        && sparse_knn["query"]["indices"]
                            .as_array()
                            .is_some_and(|indices| !indices.is_empty())
                        && body["searches"][0]["limit"]["limit"] == serde_json::json!(5)
                });
                then.status(200).json_body(serde_json::json!({
                    "ids": [[]],
                    "documents": [null],
                    "embeddings": [null],
                    "metadatas": [null],
                    "scores": [null],
                    "select": [[]]
                }));
            })
            .await;

        let response = collection
            .search_records(SearchRecordsOptions {
                query_texts: vec!["hybrid".to_string()],
                sparse_key: None,
                r#where: None,
                ids: None,
                limit: Some(5),
                offset: 0,
                rank_limit: None,
                select: Some(vec![Key::Document, Key::Score]),
                dense_weight: None,
                sparse_weight: None,
                read_level: ReadLevel::default(),
            })
            .await
            .unwrap();

        assert_eq!(response.ids.len(), 1);
        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn update_records_embeds_present_documents() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            None,
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/update",
            collection.id()
        );
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    body["embeddings"] == serde_json::json!([null, [4.0]])
                        && body["documents"] == serde_json::json!([null, "four"])
                });
                then.status(200).json_body(serde_json::json!({}));
            })
            .await;

        collection
            .update_records(UpdateRecordsOptions {
                ids: vec!["id1".to_string(), "id2".to_string()],
                documents: Some(vec![None, Some("four".to_string())]),
                uris: None,
                metadatas: None,
            })
            .await
            .unwrap();

        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn sparse_metadata_embeds_document_source() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            Some(sparse_schema(DOCUMENT_KEY)),
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/add",
            collection.id()
        );
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    let sparse = &body["metadatas"][0]["sparse"];
                    body["metadatas"][0].is_object()
                        && sparse["#type"] == serde_json::json!("sparse_vector")
                        && sparse["indices"]
                            .as_array()
                            .is_some_and(|indices| !indices.is_empty())
                });
                then.status(200).json_body(serde_json::json!({}));
            })
            .await;

        collection
            .add_records(AddRecordsOptions {
                ids: vec!["id1".to_string()],
                documents: vec!["space text".to_string()],
                uris: None,
                metadatas: None,
            })
            .await
            .unwrap();

        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn sparse_metadata_uses_metadata_source_and_skips_existing_target() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            Some(sparse_schema("source")),
            Some(Arc::new(MockDenseEmbeddingFunction {
                wrong_length: false,
            })),
        );
        let path = format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/upsert",
            collection.id()
        );
        let existing_sparse = SparseVector::new(vec![99], vec![1.0]).unwrap();
        let mut first = UpdateMetadata::new();
        first.insert("source".to_string(), "alpha".into());
        let mut second = UpdateMetadata::new();
        second.insert("source".to_string(), "beta".into());
        second.insert("sparse".to_string(), existing_sparse.into());
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path(path).is_true(|req| {
                    let body = body_json(req);
                    let first_sparse = &body["metadatas"][0]["sparse"];
                    let second_sparse = &body["metadatas"][1]["sparse"];
                    first_sparse["#type"] == serde_json::json!("sparse_vector")
                        && first_sparse["indices"]
                            .as_array()
                            .is_some_and(|indices| !indices.is_empty())
                        && second_sparse["indices"] == serde_json::json!([99])
                        && second_sparse["values"] == serde_json::json!([1.0])
                });
                then.status(200).json_body(serde_json::json!({}));
            })
            .await;

        collection
            .upsert_records(UpsertRecordsOptions {
                ids: vec!["id1".to_string(), "id2".to_string()],
                documents: vec!["alpha doc".to_string(), "beta doc".to_string()],
                uris: None,
                metadatas: Some(vec![Some(first), Some(second)]),
            })
            .await
            .unwrap();

        assert_eq!(mock.calls_async().await, 1);
    }

    #[tokio::test]
    async fn embedding_length_mismatch_is_reported() {
        let server = MockServer::start_async().await;
        let collection = test_collection(
            &server,
            None,
            Some(Arc::new(MockDenseEmbeddingFunction { wrong_length: true })),
        );

        let err = collection
            .add_records(AddRecordsOptions {
                ids: vec!["id1".to_string()],
                documents: vec!["alpha".to_string()],
                uris: None,
                metadatas: None,
            })
            .await
            .err()
            .unwrap();

        match err {
            ChromaHttpClientError::EmbeddingError(EmbeddingError::LengthMismatch {
                expected,
                actual,
            }) => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            other => panic!("expected length mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_accessor_methods() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_accessors").await;
            assert!(!collection.database().is_empty());
            assert_eq!(collection.metadata(), &None);
            assert!(collection.schema().is_some());
            assert!(!collection.tenant().is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_count_empty_collection() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_count_empty").await;

            let count = collection.count().await.unwrap();
            println!("Empty collection count: {}", count);
            assert_eq!(count, 0);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_indexing_status() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_indexing_status").await;

            // Test on empty collection
            let status = collection.get_indexing_status().await.unwrap();
            println!("Indexing status: {:?}", status);
            assert_eq!(status.total_ops, 0);
            assert_eq!(status.num_indexed_ops, 0);
            assert_eq!(status.num_unindexed_ops, 0);

            // Add some records
            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            // Check status after adding records
            let status = collection.get_indexing_status().await.unwrap();
            println!("Indexing status after add: {:?}", status);
            assert_eq!(status.total_ops, 2);
            assert!(status.op_indexing_progress >= 0.0 && status.op_indexing_progress <= 1.0);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_add_single_record() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_add_single").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document1".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Collection count after add: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_add_multiple_records() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_add_multiple").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    Some(vec![
                        Some("first document".to_string()),
                        Some("second document".to_string()),
                        Some("third document".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Collection count after adding multiple: {}", count);
            assert_eq!(count, 3);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_add_with_metadata() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_add_metadata").await;

            let mut metadata = Metadata::new();
            metadata.insert("category".to_string(), "test".into());
            metadata.insert("version".to_string(), 1.into());

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document with metadata".to_string())]),
                    None,
                    Some(vec![Some(metadata)]),
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_add_with_uris() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_add_uris").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("document with uri".to_string())]),
                    Some(vec![Some("https://example.com/doc1".to_string())]),
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_all_records() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_get_all").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("first".to_string()), Some("second".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection.get(None, None, None, None, None).await.unwrap();
            println!("Get all response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(response.ids.contains(&"id1".to_string()));
            assert!(response.ids.contains(&"id2".to_string()));
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_by_ids() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_get_by_ids").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .get(
                    Some(vec!["id1".to_string(), "id3".to_string()]),
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Get by ids response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(response.ids.contains(&"id1".to_string()));
            assert!(response.ids.contains(&"id3".to_string()));
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_with_limit_and_offset() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_get_limit_offset").await;

            collection
                .add(
                    vec![
                        "id1".to_string(),
                        "id2".to_string(),
                        "id3".to_string(),
                        "id4".to_string(),
                    ],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                        vec![10.0, 11.0, 12.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .get(None, None, Some(2), Some(1), None)
                .await
                .unwrap();
            println!("Get with limit and offset response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert!(!response.ids.is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_with_where_clause() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_get_where").await;

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            let response = collection
                .get(None, Some(where_clause), None, None, None)
                .await
                .unwrap();
            println!("Get with where clause response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_get_with_include_list() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_get_include").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("test document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let include = IncludeList(vec![
                Include::Document,
                Include::Embedding,
                Include::Metadata,
            ]);
            let response = collection
                .get(None, None, None, None, Some(include))
                .await
                .unwrap();
            println!("Get with include list response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0], "id1");
            assert!(response.documents.is_some());
            assert_eq!(
                response.documents.as_ref().unwrap()[0],
                Some("test document".to_string())
            );
            assert!(response.embeddings.is_some());
            assert_eq!(
                response.embeddings.as_ref().unwrap()[0],
                vec![1.0, 2.0, 3.0]
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_query_basic() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_query_basic").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![10.0, 20.0, 30.0],
                    ],
                    Some(vec![
                        Some("first".to_string()),
                        Some("second".to_string()),
                        Some("third".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(vec![vec![1.0, 2.0, 3.0]], None, None, None, None)
                .await
                .unwrap();
            println!("Query basic response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert!(!response.ids[0].is_empty());
            assert!(response.ids[0].contains(&"id1".to_string()));
            assert!(response.distances.is_some());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_query_with_n_results() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_query_n_results").await;

            collection
                .add(
                    vec![
                        "id1".to_string(),
                        "id2".to_string(),
                        "id3".to_string(),
                        "id4".to_string(),
                        "id5".to_string(),
                    ],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![1.2, 2.2, 3.2],
                        vec![1.3, 2.3, 3.3],
                        vec![1.4, 2.4, 3.4],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(vec![vec![1.0, 2.0, 3.0]], Some(3), None, None, None)
                .await
                .unwrap();
            println!("Query with n_results response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0].len(), 3);
            assert!(response.distances.is_some());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_query_with_where_clause() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_query_where").await;

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![1.1, 2.1, 3.1]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            let response = collection
                .query(
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    Some(where_clause),
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Query with where clause response: {:?}", response);
            assert_eq!(response.ids.len(), 1);
            assert_eq!(response.ids[0].len(), 1);
            assert_eq!(response.ids[0][0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_query_multiple_embeddings() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_query_multiple").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let response = collection
                .query(
                    vec![vec![1.0, 2.0, 3.0], vec![7.0, 8.0, 9.0]],
                    Some(1),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            println!("Query multiple embeddings response: {:?}", response);
            assert_eq!(response.ids.len(), 2);
            assert_eq!(response.ids[0].len(), 1);
            assert_eq!(response.ids[1].len(), 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_search_with_read_levels() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_search_read_level").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![0.9, 1.9, 2.9],
                    ],
                    Some(vec![
                        Some("first".to_string()),
                        Some("second".to_string()),
                        Some("third".to_string()),
                    ]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let search = SearchPayload::default()
                .rank(RankExpr::Knn {
                    query: QueryVector::Dense(vec![1.0, 2.0, 3.0]),
                    key: Key::Embedding,
                    limit: 10,
                    default: None,
                    return_rank: false,
                })
                .limit(Some(5), 0)
                .select([Key::Document, Key::Score]);

            let index_and_wal = collection
                .search_with_options(vec![search.clone()], ReadLevel::IndexAndWal)
                .await
                .unwrap();
            assert_eq!(index_and_wal.ids.len(), 1);
            assert!(!index_and_wal.ids[0].is_empty());
            assert_eq!(index_and_wal.ids[0].len(), 3);
            assert!(index_and_wal.documents[0].is_some());
            assert!(index_and_wal.scores[0].is_some());

            // IndexOnly may omit recent WAL writes; just ensure the call succeeds
            // and the response structure matches the requested payload.
            let index_only = collection
                .search_with_options(vec![search.clone()], ReadLevel::IndexOnly)
                .await
                .unwrap();
            assert_eq!(index_only.ids.len(), 1);
            assert!(index_only.documents[0].is_some());
            assert!(index_only.scores[0].is_some());

            // IndexAndBoundedWal reads up to a server-configured number of WAL entries;
            // just ensure the call succeeds and the response structure is valid.
            let bounded = collection
                .search_with_options(vec![search], ReadLevel::IndexAndBoundedWal)
                .await
                .unwrap();
            assert_eq!(bounded.ids.len(), 1);
            assert!(bounded.documents[0].is_some());
            assert!(bounded.scores[0].is_some());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_count_with_read_levels() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_count_read_level").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![1.1, 2.1, 3.1],
                        vec![0.9, 1.9, 2.9],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            // INDEX_AND_WAL should see all committed writes
            let count = collection
                .count_with_options(ReadLevel::IndexAndWal)
                .await
                .unwrap();
            assert_eq!(count, 3);

            // INDEX_ONLY may omit recent WAL writes; just ensure the call succeeds
            let count = collection
                .count_with_options(ReadLevel::IndexOnly)
                .await
                .unwrap();
            assert!(count <= 3);

            // INDEX_AND_BOUNDED_WAL reads up to a configured limit; just ensure the call succeeds
            let count = collection
                .count_with_options(ReadLevel::IndexAndBoundedWal)
                .await
                .unwrap();
            assert!(count <= 3);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_update_embeddings() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_update_embeddings").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .update(
                    vec!["id1".to_string()],
                    Some(vec![Some(vec![4.0, 5.0, 6.0])]),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Embedding])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.embeddings.is_some());
            assert_eq!(
                get_response.embeddings.as_ref().unwrap()[0],
                vec![4.0, 5.0, 6.0]
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_update_documents() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_update_documents").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .update(
                    vec!["id1".to_string()],
                    None,
                    Some(vec![Some("updated document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Document])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.documents.is_some());
            assert_eq!(
                get_response.documents.as_ref().unwrap()[0],
                Some("updated document".to_string())
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_update_metadata() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_update_metadata").await;

            let mut original_metadata = Metadata::new();
            original_metadata.insert("version".to_string(), 1.into());

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    None,
                    Some(vec![Some(original_metadata)]),
                )
                .await
                .unwrap();

            let mut updated_metadata = UpdateMetadata::new();
            updated_metadata.insert("version".to_string(), UpdateMetadataValue::Int(2));
            updated_metadata.insert(
                "new_field".to_string(),
                UpdateMetadataValue::Str("test".to_string()),
            );

            collection
                .update(
                    vec!["id1".to_string()],
                    None,
                    None,
                    None,
                    Some(vec![Some(updated_metadata)]),
                )
                .await
                .unwrap();

            let get_response = collection
                .get(
                    Some(vec!["id1".to_string()]),
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Metadata])),
                )
                .await
                .unwrap();
            println!("Get after update response: {:?}", get_response);
            assert!(get_response.metadatas.is_some());
            let metadata = get_response.metadatas.as_ref().unwrap()[0]
                .as_ref()
                .unwrap();
            assert_eq!(metadata.get("version"), Some(&MetadataValue::Int(2)));
            assert_eq!(
                metadata.get("new_field"),
                Some(&MetadataValue::Str("test".to_string()))
            );
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_upsert_insert_new() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_upsert_insert").await;

            collection
                .upsert(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("new document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert insert: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_upsert_update_existing() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_upsert_update").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("original".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .upsert(
                    vec!["id1".to_string()],
                    vec![vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("updated via upsert".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert update: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_upsert_mixed() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_upsert_mixed").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("existing".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .upsert(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![4.0, 5.0, 6.0], vec![7.0, 8.0, 9.0]],
                    Some(vec![Some("updated".to_string()), Some("new".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after upsert mixed: {}", count);
            assert_eq!(count, 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_delete_by_ids() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_delete_by_ids").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                    ],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            collection
                .delete(Some(vec!["id1".to_string(), "id3".to_string()]), None, None)
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after delete: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_delete_by_where() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_delete_by_where").await;

            let mut metadata1 = Metadata::new();
            metadata1.insert("category".to_string(), "a".into());

            let mut metadata2 = Metadata::new();
            metadata2.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    Some(vec![Some(metadata1), Some(metadata2)]),
                )
                .await
                .unwrap();

            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });
            collection
                .delete(None, Some(where_clause), None)
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after delete: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_delete_with_limit() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_delete_with_limit").await;

            let mut metadata_a = Metadata::new();
            metadata_a.insert("category".to_string(), "a".into());

            let mut metadata_b = Metadata::new();
            metadata_b.insert("category".to_string(), "b".into());

            collection
                .add(
                    vec![
                        "id1".to_string(),
                        "id2".to_string(),
                        "id3".to_string(),
                        "id4".to_string(),
                        "id5".to_string(),
                    ],
                    vec![
                        vec![1.0, 2.0, 3.0],
                        vec![4.0, 5.0, 6.0],
                        vec![7.0, 8.0, 9.0],
                        vec![10.0, 11.0, 12.0],
                        vec![13.0, 14.0, 15.0],
                    ],
                    None,
                    None,
                    Some(vec![
                        Some(metadata_a.clone()),
                        Some(metadata_a.clone()),
                        Some(metadata_a),
                        Some(metadata_b.clone()),
                        Some(metadata_b),
                    ]),
                )
                .await
                .unwrap();

            // Where matches 3 records (category == "a"), but limit is 2.
            let where_clause = Where::Metadata(MetadataExpression {
                key: "category".to_string(),
                comparison: MetadataComparison::Primitive(
                    PrimitiveOperator::Equal,
                    MetadataValue::Str("a".to_string()),
                ),
            });

            let response = collection
                .delete(None, Some(where_clause), Some(2))
                .await
                .unwrap();

            assert_eq!(response.deleted, 2);

            let count = collection.count().await.unwrap();
            assert_eq!(count, 3);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_fork_basic() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_fork_source").await;

            collection
                .add(
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
                    Some(vec![Some("first".to_string()), Some("second".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let target_name = unique_collection_name("test_fork_target");
            let forked = collection.fork(target_name.clone()).await.unwrap();
            client.track(&forked);
            println!("Forked collection: {:?}", forked);

            assert_eq!(forked.collection.name, target_name);
            assert_ne!(
                forked.collection.collection_id,
                collection.collection.collection_id
            );

            let forked_count = forked.count().await.unwrap();
            println!("Forked collection count: {}", forked_count);
            assert_eq!(forked_count, 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_fork_preserves_data() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_fork_preserves_source").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    Some(vec![Some("test document".to_string())]),
                    None,
                    None,
                )
                .await
                .unwrap();

            let target_name = unique_collection_name("test_fork_preserves_target");
            let forked = collection.fork(target_name).await.unwrap();
            client.track(&forked);

            let forked_get_response = forked
                .get(
                    None,
                    None,
                    None,
                    None,
                    Some(IncludeList(vec![Include::Document])),
                )
                .await
                .unwrap();
            println!("Forked collection get response: {:?}", forked_get_response);
            assert_eq!(forked_get_response.ids.len(), 1);
            assert_eq!(forked_get_response.ids[0], "id1");
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_fork_independence() {
        with_client(|mut client| async move {
            let collection = client.new_collection("test_fork_independence_source").await;

            collection
                .add(
                    vec!["id1".to_string()],
                    vec![vec![1.0, 2.0, 3.0]],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let target_name = unique_collection_name("test_fork_independence_target");
            let forked = collection.fork(target_name).await.unwrap();
            client.track(&forked);

            forked
                .add(
                    vec!["id2".to_string()],
                    vec![vec![4.0, 5.0, 6.0]],
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

            let original_count = collection.count().await.unwrap();
            let forked_count = forked.count().await.unwrap();
            println!(
                "Original count: {}, Forked count: {}",
                original_count, forked_count
            );
            assert_eq!(original_count, 1);
            assert_eq!(forked_count, 2);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_k8s_integration_modify() {
        with_client(|mut client| async move {
            let mut collection = client.new_collection("test_modify").await;

            let mut new_metadata = Metadata::new();
            new_metadata.insert("foo".into(), "bar".into());

            collection
                .modify(None::<String>, Some(new_metadata))
                .await
                .unwrap();
            assert_eq!(
                collection.metadata().as_ref().unwrap().get("foo"),
                Some(&"bar".into())
            );

            let collection = client.get_collection(collection.name()).await.unwrap();
            assert_eq!(
                collection.metadata().as_ref().unwrap().get("foo"),
                Some(&"bar".into())
            );
        })
        .await;
    }
}

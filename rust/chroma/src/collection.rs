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
//! - **Read operations**: [`count()`](ChromaCollection::count), [`get()`](ChromaCollection::get), [`query()`](ChromaCollection::query), [`search()`](ChromaCollection::search)
//! - **Write operations**: [`add()`](ChromaCollection::add), [`update()`](ChromaCollection::update), [`upsert()`](ChromaCollection::upsert), [`delete()`](ChromaCollection::delete), [`modify()`](ChromaCollection::modify)

use std::sync::Arc;

use chroma_api_types::ForkCollectionPayload;
use chroma_types::{
    plan::SearchPayload, AddCollectionRecordsRequest, AddCollectionRecordsResponse, Collection,
    CollectionUuid, DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, GetRequest,
    GetResponse, IncludeList, Metadata, QueryRequest, QueryResponse, Schema, SearchRequest,
    SearchResponse, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateMetadata, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse, Where,
};
use reqwest::Method;
use serde::{de::DeserializeOwned, Serialize};

use crate::{client::ChromaHttpClientError, ChromaHttpClient};

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
/// # use chroma::collection::ChromaCollection;
/// # use chroma::client::ChromaClientError;
/// # async fn example(collection: ChromaCollection) -> Result<(), ChromaClientError> {
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

impl ChromaCollection {
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
    /// # use chroma::collection::ChromaCollection;
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
    /// # use chroma::collection::ChromaCollection;
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
    /// # use chroma::collection::ChromaCollection;
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
    /// # use chroma::collection::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = collection.count().await?;
    /// assert!(count >= 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count(&self) -> Result<u32, ChromaHttpClientError> {
        self.send::<(), u32>("count", Method::GET, None).await
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
    /// # use chroma::collection::ChromaCollection;
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
        self.send::<_, ()>(
            "modify",
            Method::PUT,
            Some(serde_json::json!({
                "new_name": new_name.as_ref().map(|s| s.as_ref()),
                "new_metadata": new_metadata,
            })),
        )
        .await?;

        let mut updated_collection = (*self.collection).clone();
        if let Some(name) = new_name {
            updated_collection.name = name.as_ref().to_string();
        }
        if let Some(metadata) = new_metadata {
            updated_collection.metadata = Some(metadata);
        }

        self.collection = Arc::new(updated_collection);

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
    /// # use chroma::collection::ChromaCollection;
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
        self.send("get", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
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
        self.send("query", Method::POST, Some(request)).await
    }

    /// Executes advanced search with multiple search payloads in a single request.
    ///
    /// Each [`SearchPayload`] can specify distinct query vectors, filters, and result counts,
    /// enabling efficient batch similarity search with heterogeneous parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any search payload fails validation
    /// - Network communication fails
    ///
    /// # Examples
    ///
    /// Basic search with default parameters:
    /// ```
    /// # use chroma::collection::ChromaCollection;
    /// # use chroma_types::plan::SearchPayload;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let search1 = SearchPayload {
    ///     filter: Default::default(),
    ///     rank: Default::default(),
    ///     limit: Default::default(),
    ///     select: Default::default(),
    /// };
    /// let search2 = SearchPayload {
    ///     filter: Default::default(),
    ///     rank: Default::default(),
    ///     limit: Default::default(),
    ///     select: Default::default(),
    /// };
    ///
    /// let response = collection.search(vec![search1, search2]).await?;
    /// println!("Executed {} searches", response.ids.len());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Advanced search with all fields configured:
    /// ```
    /// # use chroma::collection::ChromaCollection;
    /// # use chroma_types::plan::SearchPayload;
    /// # use chroma_types::{Filter, Rank, RankExpr, QueryVector, Key, Limit, Select};
    /// # use chroma_types::{Where, MetadataExpression, MetadataComparison, PrimitiveOperator, MetadataValue};
    /// # use std::collections::HashSet;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let search = SearchPayload {
    ///     filter: Filter {
    ///         query_ids: Some(vec!["doc1".to_string(), "doc2".to_string()]),
    ///         where_clause: Some(Where::Metadata(MetadataExpression {
    ///             key: "category".to_string(),
    ///             comparison: MetadataComparison::Primitive(
    ///                 PrimitiveOperator::Equal,
    ///                 MetadataValue::Str("research".to_string()),
    ///             ),
    ///         })),
    ///     },
    ///     rank: Rank {
    ///         expr: Some(RankExpr::Knn {
    ///             query: QueryVector::Dense(vec![0.1, 0.2, 0.3, 0.4]),
    ///             key: Key::Embedding,
    ///             limit: 50,
    ///             default: None,
    ///             return_rank: false,
    ///         }),
    ///     },
    ///     limit: Limit {
    ///         offset: 0,
    ///         limit: Some(10),
    ///     },
    ///     select: Select {
    ///         keys: HashSet::from([
    ///             Key::Document,
    ///             Key::Metadata,
    ///             Key::Embedding,
    ///             Key::Score,
    ///         ]),
    ///     },
    /// };
    ///
    /// let response = collection.search(vec![search]).await?;
    /// println!("Found {} results", response.ids[0].len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(
        &self,
        searches: Vec<SearchPayload>,
    ) -> Result<SearchResponse, ChromaHttpClientError> {
        let request = SearchRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            searches,
        )?;
        let request = request.into_payload();
        self.send("search", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
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
        self.send("add", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
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
        self.send("update", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
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
        self.send("upsert", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
    /// # async fn example(collection: ChromaCollection) -> Result<(), Box<dyn std::error::Error>> {
    /// let response = collection.delete(
    ///     Some(vec!["doc1".to_string(), "doc2".to_string()]),
    ///     None
    /// ).await?;
    /// println!("Deleted records successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(
        &self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
    ) -> Result<DeleteCollectionRecordsResponse, ChromaHttpClientError> {
        let request = DeleteCollectionRecordsRequest::try_new(
            self.collection.tenant.clone(),
            self.collection.database.clone(),
            self.collection.collection_id,
            ids,
            r#where,
        )?;
        let request = request.into_payload()?;
        self.send("delete", Method::POST, Some(request)).await
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
    /// # use chroma::collection::ChromaCollection;
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
        let collection: Collection = self.send("fork", Method::POST, Some(request)).await?;
        Ok(ChromaCollection {
            client: self.client.clone(),
            collection: Arc::new(collection),
        })
    }

    /// Internal transport method that constructs collection-specific API paths and delegates to the client.
    async fn send<Body: Serialize, Response: DeserializeOwned>(
        &self,
        operation: &str,
        method: Method,
        body: Option<Body>,
    ) -> Result<Response, ChromaHttpClientError> {
        let operation_name = format!("collection_{operation}");
        let path = format!(
            "/api/v2/tenants/{}/databases/{}/collections/{}/{}",
            self.collection.tenant,
            self.collection.database,
            self.collection.collection_id,
            operation
        );
        self.client
            .send(&operation_name, method, path, body, None::<()>)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::with_client;
    use chroma_types::{
        Include, IncludeList, Metadata, MetadataComparison, MetadataExpression, MetadataValue,
        PrimitiveOperator, UpdateMetadata, UpdateMetadataValue, Where,
    };

    async fn create_test_collection(
        client: &ChromaHttpClient,
        name: &str,
    ) -> Result<ChromaCollection, ChromaHttpClientError> {
        client.create_collection(name, None, None).await
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_accessor_methods() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_accessors")
                .await
                .unwrap();

            assert!(!collection.database().is_empty());
            assert_eq!(collection.metadata(), &None);
            assert!(collection.schema().is_some());
            assert!(!collection.tenant().is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_count_empty_collection() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_count_empty")
                .await
                .unwrap();

            let count = collection.count().await.unwrap();
            println!("Empty collection count: {}", count);
            assert_eq!(count, 0);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_add_single_record() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_single")
                .await
                .unwrap();

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
    async fn test_live_cloud_add_multiple_records() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_multiple")
                .await
                .unwrap();

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
    async fn test_live_cloud_add_with_metadata() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_metadata")
                .await
                .unwrap();

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
    async fn test_live_cloud_add_with_uris() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_add_uris")
                .await
                .unwrap();

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
    async fn test_live_cloud_get_all_records() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_all")
                .await
                .unwrap();

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
    async fn test_live_cloud_get_by_ids() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_by_ids")
                .await
                .unwrap();

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
    async fn test_live_cloud_get_with_limit_and_offset() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_limit_offset")
                .await
                .unwrap();

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
    async fn test_live_cloud_get_with_where_clause() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_where")
                .await
                .unwrap();

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
    async fn test_live_cloud_get_with_include_list() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_get_include")
                .await
                .unwrap();

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
    async fn test_live_cloud_query_basic() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_basic")
                .await
                .unwrap();

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
    async fn test_live_cloud_query_with_n_results() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_n_results")
                .await
                .unwrap();

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
    async fn test_live_cloud_query_with_where_clause() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_where")
                .await
                .unwrap();

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
    async fn test_live_cloud_query_multiple_embeddings() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_query_multiple")
                .await
                .unwrap();

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
    async fn test_live_cloud_update_embeddings() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_embeddings")
                .await
                .unwrap();

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
    async fn test_live_cloud_update_documents() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_documents")
                .await
                .unwrap();

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
    async fn test_live_cloud_update_metadata() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_update_metadata")
                .await
                .unwrap();

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
    async fn test_live_cloud_upsert_insert_new() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_insert")
                .await
                .unwrap();

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
    async fn test_live_cloud_upsert_update_existing() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_update")
                .await
                .unwrap();

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
    async fn test_live_cloud_upsert_mixed() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_upsert_mixed")
                .await
                .unwrap();

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
    async fn test_live_cloud_delete_by_ids() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_delete_by_ids")
                .await
                .unwrap();

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
                .delete(Some(vec!["id1".to_string(), "id3".to_string()]), None)
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
    async fn test_live_cloud_delete_by_where() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_delete_by_where")
                .await
                .unwrap();

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
            collection.delete(None, Some(where_clause)).await.unwrap();

            let count = collection.count().await.unwrap();
            println!("Count after delete: {}", count);
            assert_eq!(count, 1);
        })
        .await;
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_live_cloud_fork_basic() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_source")
                .await
                .unwrap();

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

            let forked = collection.fork("test_fork_target").await.unwrap();
            println!("Forked collection: {:?}", forked);

            assert_eq!(forked.collection.name, "test_fork_target");
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
    async fn test_live_cloud_fork_preserves_data() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_preserves_source")
                .await
                .unwrap();

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

            let forked = collection.fork("test_fork_preserves_target").await.unwrap();

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
    async fn test_live_cloud_fork_independence() {
        with_client(|client| async move {
            let collection = create_test_collection(&client, "test_fork_independence_source")
                .await
                .unwrap();

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

            let forked = collection
                .fork("test_fork_independence_target")
                .await
                .unwrap();

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
}

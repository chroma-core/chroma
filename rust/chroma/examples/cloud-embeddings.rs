//! End-to-end Chroma Cloud embedding example.
//!
//! This example uses `ChromaHttpClientOptions::from_cloud_env()` so credentials
//! and routing come from:
//!
//! - `CHROMA_API_KEY` (required)
//! - `CHROMA_TENANT` (optional if the API key resolves to one tenant)
//! - `CHROMA_DATABASE` (optional if the API key resolves to one database)
//! - `CHROMA_ENDPOINT` (optional, defaults to Chroma Cloud)
//! - `CHROMA_HOST` (optional URL or bare host fallback when `CHROMA_ENDPOINT` is unset)
//! - `CHROMA_EMBED_URL` (optional, defaults to Chroma Cloud embeddings)
//!
//! Run with:
//!
//! ```bash
//! cargo run -p chroma --example cloud-embeddings
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use chroma::client::{ChromaHttpClientError, ChromaHttpClientOptions, CreateCollectionOptions};
use chroma::embed::chroma_cloud::{
    ChromaCloudQwenEmbeddingFunction, ChromaCloudQwenOptions, ChromaCloudSpladeEmbeddingFunction,
    ChromaCloudSpladeOptions,
};
use chroma::embed::{DenseEmbeddingFunction, SparseEmbeddingFunction};
use chroma::types::{
    Include, IncludeList, Key, Metadata, ReadLevel, Schema, SparseVectorIndexConfig,
    UpdateMetadata, VectorIndexConfig,
};
use chroma::{
    AddRecordsOptions, ChromaHttpClient, QueryRecordsOptions, SearchRecordsOptions,
    UpdateRecordsOptions, UpsertRecordsOptions,
};

const SPARSE_KEY: &str = "sparse_embedding";

type ExampleResult<T> = Result<T, ExampleError>;

#[derive(Debug)]
struct ExampleError {
    context: String,
    cause: Box<dyn Error + Send + Sync>,
}

impl ExampleError {
    fn new(context: impl Into<String>, cause: impl Error + Send + Sync + 'static) -> Self {
        Self {
            context: context.into(),
            cause: Box::new(cause),
        }
    }

    fn contains_unauthorized_api_error(&self) -> bool {
        let mut source: Option<&(dyn Error + 'static)> = Some(self.cause.as_ref());
        while let Some(error) = source {
            if let Some(ChromaHttpClientError::ApiError(_, status)) =
                error.downcast_ref::<ChromaHttpClientError>()
            {
                if status.as_u16() == 401 {
                    return true;
                }
            }
            source = error.source();
        }
        false
    }
}

impl Display for ExampleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.context, self.cause)
    }
}

impl Error for ExampleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

trait ResultContext<T> {
    fn with_context(self, context: impl Into<String>) -> ExampleResult<T>;
}

impl<T, E> ResultContext<T> for Result<T, E>
where
    E: Error + Send + Sync + 'static,
{
    fn with_context(self, context: impl Into<String>) -> ExampleResult<T> {
        self.map_err(|error| ExampleError::new(context, error))
    }
}

fn log_error(error: &ExampleError) {
    eprintln!("\ncloud-embeddings failed while {}", error.context);
    eprintln!("  error: {}", error.cause);

    let mut source = error.cause.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }

    if error.contains_unauthorized_api_error() {
        eprintln!(
            "  hint: verify CHROMA_API_KEY, CHROMA_TENANT, CHROMA_DATABASE, and API key permissions"
        );
    }
}

fn metadata(topic: &str, operation: &str, priority: i64) -> Metadata {
    Metadata::from([
        ("topic".to_string(), topic.into()),
        ("operation".to_string(), operation.into()),
        ("priority".to_string(), priority.into()),
    ])
}

fn update_metadata(topic: &str, operation: &str, priority: i64) -> UpdateMetadata {
    UpdateMetadata::from([
        ("topic".to_string(), topic.into()),
        ("operation".to_string(), operation.into()),
        ("priority".to_string(), priority.into()),
    ])
}

fn print_get(label: &str, response: &chroma::types::GetResponse) {
    println!("\n{label}");
    for (index, id) in response.ids.iter().enumerate() {
        let document = response
            .documents
            .as_ref()
            .and_then(|documents| documents.get(index))
            .and_then(|document| document.as_ref())
            .map(String::as_str)
            .unwrap_or("<no document>");
        let topic = response
            .metadatas
            .as_ref()
            .and_then(|metadatas| metadatas.get(index))
            .and_then(|metadata| metadata.as_ref())
            .and_then(|metadata| metadata.get("topic"));

        println!("  {id}: topic={topic:?}; {document}");
    }
}

fn print_query(label: &str, response: &chroma::types::QueryResponse) {
    println!("\n{label}");
    for (query_index, ids) in response.ids.iter().enumerate() {
        println!("  query {query_index}");
        for (rank, id) in ids.iter().enumerate() {
            let distance = response
                .distances
                .as_ref()
                .and_then(|distances| distances.get(query_index))
                .and_then(|distances| distances.get(rank))
                .and_then(|distance| *distance);
            let document = response
                .documents
                .as_ref()
                .and_then(|documents| documents.get(query_index))
                .and_then(|documents| documents.get(rank))
                .and_then(|document| document.as_ref())
                .map(String::as_str)
                .unwrap_or("<no document>");

            println!("    {}. {id} distance={distance:?}; {document}", rank + 1);
        }
    }
}

fn print_search(label: &str, response: &chroma::types::SearchResponse) {
    println!("\n{label}");
    for (payload_index, ids) in response.ids.iter().enumerate() {
        println!("  payload {payload_index}");
        for (rank, id) in ids.iter().enumerate() {
            let score = response
                .scores
                .get(payload_index)
                .and_then(|scores| scores.as_ref())
                .and_then(|scores| scores.get(rank))
                .and_then(|score| *score);
            let topic = response
                .metadatas
                .get(payload_index)
                .and_then(|metadatas| metadatas.as_ref())
                .and_then(|metadatas| metadatas.get(rank))
                .and_then(|metadata| metadata.as_ref())
                .and_then(|metadata| metadata.get("topic"));

            println!("    {}. {id} score={score:?} topic={topic:?}", rank + 1);
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log_error(&error);
            ExitCode::FAILURE
        }
    }
}

async fn run() -> ExampleResult<()> {
    let client = ChromaHttpClient::new(
        ChromaHttpClientOptions::from_cloud_env()
            .with_context("reading Chroma Cloud options from environment")?,
    );
    let tenant = client
        .get_tenant_id()
        .await
        .with_context("resolving tenant")?;
    let database = client
        .get_database_name()
        .await
        .with_context("resolving database")?;
    println!("Connected to Chroma Cloud tenant={tenant} database={database}");

    let dense_embedding_function =
        ChromaCloudQwenEmbeddingFunction::new(ChromaCloudQwenOptions::default());
    let sparse_embedding_function =
        ChromaCloudSpladeEmbeddingFunction::new(ChromaCloudSpladeOptions::default());

    let schema = Schema::default()
        .create_index(
            None,
            VectorIndexConfig {
                space: Some(dense_embedding_function.default_space()),
                embedding_function: Some(dense_embedding_function.configuration()),
                source_key: None,
                hnsw: None,
                spann: None,
            }
            .into(),
        )
        .with_context("configuring dense vector index")?
        .create_index(
            Some(SPARSE_KEY),
            SparseVectorIndexConfig {
                embedding_function: Some(sparse_embedding_function.configuration()),
                source_key: Some(Key::Document.to_string()),
                bm25: Some(false),
                algorithm: Default::default(),
            }
            .into(),
        )
        .with_context("configuring sparse vector index")?;

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .with_context("building collection name")?
        .as_nanos();
    let collection_name = format!("rust_cloud_embeddings_{suffix}");
    let collection = client
        .create_collection_with_options(
            &collection_name,
            CreateCollectionOptions {
                schema: Some(schema),
                metadata: Some(metadata("example", "create_collection", 0)),
                configuration: None,
                embedding_function: None,
            },
        )
        .await
        .with_context(format!("creating collection {collection_name}"))?;
    println!("Created collection {}", collection.name());

    collection
        .add_records(AddRecordsOptions {
            ids: vec![
                "rust-client".to_string(),
                "hybrid-search".to_string(),
                "schema-guide".to_string(),
            ],
            documents: vec![
                "The Rust client can embed documents before adding them.".to_string(),
                "Hybrid search combines dense semantic retrieval with sparse lexical retrieval."
                    .to_string(),
                "Collection schemas configure vector and sparse vector indexes.".to_string(),
            ],
            metadatas: Some(vec![
                Some(metadata("rust", "add", 1)),
                Some(metadata("search", "add", 2)),
                Some(metadata("schema", "add", 3)),
            ]),
            uris: None,
        })
        .await
        .with_context("adding records")?;
    println!("Added records with schema-defined dense and sparse embeddings");

    collection
        .update_records(UpdateRecordsOptions {
            ids: vec!["schema-guide".to_string()],
            documents: Some(vec![Some(
                "Collection schemas can persist embedding functions for later queries.".to_string(),
            )]),
            metadatas: Some(vec![Some(update_metadata("schema", "update", 4))]),
            uris: None,
        })
        .await
        .with_context("updating records")?;
    println!("Updated one record and regenerated its embeddings from the new document");

    collection
        .upsert_records(UpsertRecordsOptions {
            ids: vec!["hybrid-search".to_string(), "operations-guide".to_string()],
            documents: vec![
                "Hybrid search can rank by dense and sparse vectors in one payload.".to_string(),
                "Get, query, and search calls can reuse persisted cloud embedding configuration."
                    .to_string(),
            ],
            metadatas: Some(vec![
                Some(update_metadata("search", "upsert", 5)),
                Some(update_metadata("operations", "upsert", 6)),
            ]),
            uris: None,
        })
        .await
        .with_context("upserting records")?;
    println!("Upserted existing and new records with embedding generation");

    let collection = client
        .get_collection(&collection_name)
        .await
        .with_context(format!("fetching collection {collection_name}"))?;
    println!("Fetched collection again; embedding config is now loaded from Cloud");

    let get_response = collection
        .get(
            Some(vec![
                "rust-client".to_string(),
                "hybrid-search".to_string(),
                "operations-guide".to_string(),
            ]),
            None,
            Some(10),
            Some(0),
            Some(IncludeList(vec![Include::Document, Include::Metadata])),
        )
        .await
        .with_context("getting records")?;
    print_get("Get records", &get_response);

    let query_response = collection
        .query_records(QueryRecordsOptions {
            query_texts: vec!["How do I use Chroma embeddings from Rust?".to_string()],
            n_results: Some(3),
            include: Some(IncludeList(vec![
                Include::Document,
                Include::Metadata,
                Include::Distance,
            ])),
            r#where: None,
            ids: None,
        })
        .await
        .with_context("querying records")?;
    print_query("Dense query_records from text", &query_response);

    let search_response = collection
        .search_records(SearchRecordsOptions {
            query_texts: vec!["dense sparse hybrid search in Rust".to_string()],
            sparse_key: Some(SPARSE_KEY.to_string()),
            r#where: None,
            ids: None,
            limit: Some(3),
            offset: 0,
            rank_limit: Some(16),
            dense_weight: Some(0.7),
            sparse_weight: Some(0.3),
            select: Some(vec![
                Key::Score,
                Key::Document,
                Key::field("topic"),
                Key::field("operation"),
            ]),
            read_level: ReadLevel::IndexAndWal,
        })
        .await
        .with_context("searching records")?;
    print_search("Hybrid search_records from text", &search_response);

    client
        .delete_collection(&collection_name)
        .await
        .with_context(format!("deleting collection {collection_name}"))?;
    println!("\nDeleted example collection {collection_name}");

    Ok(())
}

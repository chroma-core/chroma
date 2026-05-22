//! Chroma Cloud embedding collection example.
//!
//! This example demonstrates the default Chroma Cloud Qwen dense embedding
//! function and Splade sparse embedding function by writing embedded records
//! to a Chroma collection and searching them back.
//!
//! # Running
//!
//! Source your environment first, then run the example:
//!
//! ```bash
//! source .env
//! cargo run -p chroma --example embeddings
//! ```
//!
//! Required environment:
//!
//! ```text
//! CHROMA_API_KEY=...
//! ```
//!
//! Optional environment:
//!
//! ```text
//! CHROMA_EMBED_URL=...
//! ```

use std::error::Error;

use chroma::{
    embed::{
        chroma_cloud::{ChromaCloudQwenEmbeddingFunction, ChromaCloudSpladeEmbeddingFunction},
        EmbeddingFunction,
    },
    types::{
        EmbeddingFunctionConfiguration, IncludeList, Key, Metadata, QueryVector, RankExpr, Schema,
        SearchPayload, SearchResponse, SparseVectorIndexConfig,
    },
    ChromaCollection, ChromaHttpClient,
};
use serde_json::{to_string_pretty, Error as JsonError};

const COLLECTION_NAME: &str = "rust_chroma_cloud_embeddings_example";
const DENSE_KEY: &str = "#embedding";
const SPARSE_KEY: &str = "sparse_embedding";
const QUERY: &str = "How do I create embeddings with the Rust client?";

struct ExampleRecord {
    id: &'static str,
    topic: &'static str,
    document: &'static str,
}

const RECORDS: [ExampleRecord; 4] = [
    ExampleRecord {
        id: "rust-client",
        topic: "rust",
        document: "The Rust client can use Chroma Cloud Qwen embeddings when records are added.",
    },
    ExampleRecord {
        id: "sparse-search",
        topic: "search",
        document: "Splade sparse embeddings help lexical matching and hybrid retrieval.",
    },
    ExampleRecord {
        id: "collection-schema",
        topic: "schema",
        document: "Collection schemas can describe dense and sparse vector indexes.",
    },
    ExampleRecord {
        id: "query-flow",
        topic: "query",
        document: "Query embeddings let applications retrieve similar documents from Chroma.",
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = ChromaHttpClient::cloud()?;

    let qwen = ChromaCloudQwenEmbeddingFunction::builder()
        .task("nl_to_code")
        .build()?;
    let splade = ChromaCloudSpladeEmbeddingFunction::builder()
        .include_tokens(true)
        .build()?;

    let qwen_config = ChromaCloudQwenEmbeddingFunction::configuration()
        .task("nl_to_code")
        .build();
    let splade_config = ChromaCloudSpladeEmbeddingFunction::configuration()
        .include_tokens(true)
        .build();

    let schema = Schema::default_with_embedding_function(qwen_config).create_index(
        Some(SPARSE_KEY),
        SparseVectorIndexConfig {
            embedding_function: Some(splade_config),
            source_key: None,
            bm25: Some(false),
            algorithm: Default::default(),
        }
        .into(),
    )?;

    let _ = client.delete_collection(COLLECTION_NAME).await;
    client
        .create_collection(COLLECTION_NAME, Some(schema), None)
        .await?;
    let collection = client.get_collection(COLLECTION_NAME).await?;
    print_saved_embedding_functions(&collection)?;

    let documents = RECORDS
        .iter()
        .map(|record| record.document)
        .collect::<Vec<_>>();
    let sparse_embeddings = splade.embed_strs(&documents).await?;
    let metadatas = RECORDS
        .iter()
        .zip(sparse_embeddings)
        .map(|(record, sparse_embedding)| {
            let mut metadata = Metadata::new();
            metadata.insert("topic".into(), record.topic.into());
            metadata.insert(SPARSE_KEY.into(), sparse_embedding.into());
            Some(metadata)
        })
        .collect::<Vec<_>>();

    let ids = RECORDS
        .iter()
        .map(|record| record.id.to_string())
        .collect::<Vec<_>>();
    let document_values = RECORDS
        .iter()
        .map(|record| Some(record.document.to_string()))
        .collect::<Vec<_>>();

    collection
        .add(
            ids.clone(),
            None::<Vec<Vec<f32>>>,
            Some(document_values),
            None,
            Some(metadatas),
        )
        .await?;

    let count = collection.count().await?;
    println!("Inserted {count} records into '{}'.", collection.name());

    let retrieved = collection
        .get(
            Some(ids.clone()),
            None,
            Some(ids.len() as u32),
            Some(0),
            Some(IncludeList::default_get()),
        )
        .await?;
    println!("Round-tripped {} records by ID.", retrieved.ids.len());

    let dense_query = qwen.embed_query_strs(&[QUERY]).await?.remove(0);
    let sparse_query = splade.embed_query_strs(&[QUERY]).await?.remove(0);

    let dense_search = SearchPayload::default()
        .rank(RankExpr::Knn {
            query: QueryVector::Dense(dense_query),
            key: Key::Embedding,
            limit: 10,
            default: None,
            return_rank: false,
        })
        .limit(Some(3), 0)
        .select([Key::Document, Key::Score, Key::field("topic")]);

    let sparse_search = SearchPayload::default()
        .rank(RankExpr::Knn {
            query: QueryVector::Sparse(sparse_query),
            key: Key::field(SPARSE_KEY),
            limit: 10,
            default: None,
            return_rank: false,
        })
        .limit(Some(3), 0)
        .select([Key::Document, Key::Score, Key::field("topic")]);

    let results = collection.search(vec![dense_search, sparse_search]).await?;
    print_results("Qwen dense search", &results, 0);
    print_results("Splade sparse search", &results, 1);

    client.delete_collection(COLLECTION_NAME).await?;
    println!("Deleted example collection '{}'.", COLLECTION_NAME);

    Ok(())
}

fn print_saved_embedding_functions(collection: &ChromaCollection) -> Result<(), JsonError> {
    let schema = collection.schema().as_ref();
    let dense_config = schema
        .and_then(|schema| schema.keys.get(DENSE_KEY))
        .and_then(|value_types| value_types.float_list.as_ref())
        .and_then(|float_list| float_list.vector_index.as_ref())
        .and_then(|vector_index| vector_index.config.embedding_function.as_ref())
        .or_else(|| {
            schema
                .and_then(|schema| schema.defaults.float_list.as_ref())
                .and_then(|float_list| float_list.vector_index.as_ref())
                .and_then(|vector_index| vector_index.config.embedding_function.as_ref())
        });
    let sparse_config = schema
        .and_then(|schema| schema.keys.get(SPARSE_KEY))
        .and_then(|value_types| value_types.sparse_vector.as_ref())
        .and_then(|sparse_vector| sparse_vector.sparse_vector_index.as_ref())
        .and_then(|sparse_index| sparse_index.config.embedding_function.as_ref());

    print_embedding_function_config("Saved dense embedding function", dense_config)?;
    print_embedding_function_config("Saved sparse embedding function", sparse_config)?;

    Ok(())
}

fn print_embedding_function_config(
    label: &str,
    config: Option<&EmbeddingFunctionConfiguration>,
) -> Result<(), JsonError> {
    match config {
        Some(config) => {
            println!("{label}:");
            println!("{}", to_string_pretty(config)?);
        }
        None => println!("{label}: <missing>"),
    }
    println!();
    Ok(())
}

fn print_results(label: &str, response: &SearchResponse, search_index: usize) {
    println!("\n{label}");
    for (rank, id) in response.ids[search_index].iter().enumerate() {
        let score = response.scores[search_index]
            .as_ref()
            .and_then(|scores| scores.get(rank))
            .and_then(|score| *score)
            .map(|score| format!("{score:.4}"))
            .unwrap_or_else(|| "N/A".to_string());
        let document = response.documents[search_index]
            .as_ref()
            .and_then(|documents| documents.get(rank))
            .and_then(|document| document.as_deref())
            .unwrap_or("<no document>");
        let topic = response.metadatas[search_index]
            .as_ref()
            .and_then(|metadatas| metadatas.get(rank))
            .and_then(|metadata| metadata.as_ref())
            .and_then(|metadata| metadata.get("topic"))
            .map(|topic| format!("{topic:?}"))
            .unwrap_or_else(|| "N/A".to_string());
        println!(
            "  {}. {} score={} topic={} document={}",
            rank + 1,
            id,
            score,
            topic,
            document
        );
    }
}

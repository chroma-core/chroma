// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use anyhow::Context;                      // for .context(...)
use chromadb::client::ChromaClient;
use chromadb::collection::{ChromaCollection, CollectionEntries};

// ----------------------------------------------------------------------
//  UniFFI scaffolding
// ----------------------------------------------------------------------
uniffi::setup_scaffolding!();

// ----------------------------------------------------------------------
//  FFI-safe error wrapper  (named fields only!)
// ----------------------------------------------------------------------
#[derive(Debug, Error, uniffi::Error)]
pub enum ChromaError {
    #[error("{message}")]
    Generic { message: String },
}

impl From<anyhow::Error> for ChromaError {
    fn from(e: anyhow::Error) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

type FfiResult<T> = Result<T, ChromaError>;


// ----------------------------------------------------------------------
//  Chroma helpers (Tokio async)
// ----------------------------------------------------------------------
#[uniffi::export(async_runtime = "tokio")]
pub async fn create_or_open_hello_collection() -> FfiResult<String> {
    let client = ChromaClient::new(Default::default())
        .await
        .context("connect to Chroma")?;

    let coll: ChromaCollection = client
        .get_or_create_collection("hello_world", None)
        .await
        .context("open/create collection")?;

    Ok(coll.id().to_string())
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn insert_hello_doc() -> FfiResult<u32> {
    let client = ChromaClient::new(Default::default()).await?;
    let coll   = client.get_or_create_collection("hello_world", None).await?;

    let entries = CollectionEntries {
        ids:        vec!["doc-1"],
        embeddings: Some(vec![vec![0.0_f32; 768]]),
        documents:  Some(vec!["Hello, Chroma!"]),
        metadatas:  None,
    };
    coll.upsert(entries, None).await?;
    Ok(coll.count().await? as u32)          // ← count() takes no args
}

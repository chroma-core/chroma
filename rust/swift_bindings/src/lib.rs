// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use anyhow::Context;           

//published crate
use chromadb::client::ChromaClient;
use chromadb::collection::{ChromaCollection, CollectionEntries};


// for local development...doesn't work yet
// running `./build_swift_framework.sh` results in this error: 

/* 
error[E0034]: multiple applicable items in scope
   --> /Users/nicholasarner/.cargo/registry/src/index.crates.io-6f17d22bba15001f/arrow-arith-52.2.0/src/temporal.rs:90:36
    |
90  |         DatePart::Quarter => |d| d.quarter() as i32,
    |                                    ^^^^^^^ multiple `quarter` found
    |
note: candidate #1 is defined in the trait `ChronoDateExt`
   --> /Users/nicholasarner/.cargo/registry/src/index.crates.io-6f17d22bba15001f/arrow-arith-52.2.0/src/temporal.rs:401:5
    |
401 |     fn quarter(&self) -> u32;
    |     ^^^^^^^^^^^^^^^^^^^^^^^^^
note: candidate #2 is defined in the trait `Datelike`
   --> /Users/nicholasarner/.cargo/registry/src/index.crates.io-6f17d22bba15001f/chrono-0.4.41/src/traits.rs:47:5
    |
47  |     fn quarter(&self) -> u32 {
    |     ^^^^^^^^^^^^^^^^^^^^^^^^
help: disambiguate the method for candidate #1
    |
90  |         DatePart::Quarter => |d| ChronoDateExt::quarter(&d) as i32,
    |                                  ~~~~~~~~~~~~~~~~~~~~~~~~~~
help: disambiguate the method for candidate #2
    |
90  |         DatePart::Quarter => |d| Datelike::quarter(&d) as i32,
    |                                  ~~~~~~~~~~~~~~~~~~~~~
    
For more information about this error, try `rustc --explain E0034`.
error: could not compile `arrow-arith` (lib) due to 1 previous error
warning: build failed, waiting for other jobs to finish...
*/

//use chroma::client::ChromaClient;
//use chroma::collection::{ChromaCollection, CollectionEntries};



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

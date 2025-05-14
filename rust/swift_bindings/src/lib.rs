// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use chroma_types::{
    CreateCollectionRequest, 
    AddCollectionRecordsRequest, 
    QueryRequest,
    CreateCollectionError,
    AddCollectionRecordsError,
    QueryError,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;
use tracing;
use tracing_subscriber;
use chroma_frontend::impls::in_memory_frontend::InMemoryFrontend;

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

impl From<CreateCollectionError> for ChromaError {
    fn from(e: CreateCollectionError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

impl From<AddCollectionRecordsError> for ChromaError {
    fn from(e: AddCollectionRecordsError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

impl From<QueryError> for ChromaError {
    fn from(e: QueryError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

type FfiResult<T> = Result<T, ChromaError>;

// ----------------------------------------------------------------------
//  Chroma API Functions (in-memory only)
// ----------------------------------------------------------------------

static FRONTEND: Lazy<Mutex<Option<InMemoryFrontend>>> = Lazy::new(|| Mutex::new(None));

#[uniffi::export]
pub fn initialize() -> FfiResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .try_init();
    let frontend = InMemoryFrontend::new();
    let mut frontend_lock = FRONTEND.lock().unwrap();
    *frontend_lock = Some(frontend);
    Ok(())
}

#[uniffi::export]
pub fn create_collection(name: String) -> FfiResult<String> {
    let mut frontend_lock = FRONTEND.lock().unwrap();
    let frontend = frontend_lock.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    let request = CreateCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        name.clone(),
        None,
        None,
        true,
    ).map_err(|e| ChromaError::Generic { message: format!("request: {e}") })?;
    let coll = frontend.create_collection(request)
        .map_err(|e| ChromaError::Generic { message: format!("create: {e}") })?;
    Ok(coll.collection_id.to_string())
}

#[uniffi::export]
pub fn add_documents(collection_name: String, ids: Vec<String>, embeddings: Vec<Vec<f32>>, documents: Vec<String>) -> FfiResult<u32> {
    let mut frontend_lock = FRONTEND.lock().unwrap();
    let frontend = frontend_lock.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let coll = frontend.get_collection(get_request)
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    // Prepare documents as Option<Vec<Option<String>>>
    let documents_opt = Some(documents.into_iter().map(Some).collect());
    let embeddings_opt = Some(embeddings);
    let request = AddCollectionRecordsRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        coll.collection_id,
        ids,
        embeddings_opt,
        documents_opt,
        None,
        None,
    ).map_err(|e| ChromaError::Generic { message: format!("add req: {e}") })?;
    frontend.add(request)
        .map_err(|e| ChromaError::Generic { message: format!("add: {e}") })?;
    Ok(1)
}

// Add a new struct for query results
#[derive(uniffi::Record)]
pub struct QueryResult {
    pub ids: Vec<String>,
    pub documents: Vec<Option<String>>,
}

#[uniffi::export]
pub fn query_collection(collection_name: String, query_embedding: Vec<f32>, n_results: u32) -> FfiResult<QueryResult> {
    let mut frontend_lock = FRONTEND.lock().unwrap();
    let frontend = frontend_lock.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let coll = frontend.get_collection(get_request)
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    let request = QueryRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        coll.collection_id,
        None, // ids
        None, // where
        vec![query_embedding], // embeddings
        n_results,
        chroma_types::IncludeList(vec![chroma_types::Include::Document]),
    ).map_err(|e| ChromaError::Generic { message: format!("query req: {e}") })?;
    let response = frontend.query(request)
        .map_err(|e| ChromaError::Generic { message: format!("query: {e}") })?;
    let ids = response.ids.into_iter().flatten().collect();
    let documents = response.documents.unwrap_or_default().into_iter().flatten().collect();
    Ok(QueryResult { ids, documents })
}


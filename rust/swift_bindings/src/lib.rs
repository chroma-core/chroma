// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use chroma_config::registry::Registry;
use chroma_frontend::config::FrontendConfig;
use chroma_frontend::impls::service_based_frontend::ServiceBasedFrontend;
use chroma_types::{
    CreateCollectionRequest, 
    AddCollectionRecordsRequest, 
    GetCollectionRequest, 
    QueryRequest,
    AddCollectionRecordsError,
    GetCollectionError,
    CreateTenantRequest,
    CreateDatabaseRequest
};
use chroma_frontend::get_collection_with_segments_provider::CollectionsWithSegmentsProviderConfig;
use chroma_log::config::LogConfig;
use chroma_frontend::executor::config::ExecutorConfig;
use chroma_sysdb::config::SysDbConfig;
use chroma_system::System;
use chroma_config::Configurable;
use std::path::Path;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use tokio::sync::Mutex as TokioMutex;
use serde_json;
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

impl From<AddCollectionRecordsError> for ChromaError {
    fn from(e: AddCollectionRecordsError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

impl From<GetCollectionError> for ChromaError {
    fn from(e: GetCollectionError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

type FfiResult<T> = Result<T, ChromaError>;

// ----------------------------------------------------------------------
//  Chroma API Functions
// ----------------------------------------------------------------------

static FRONTEND: Lazy<TokioMutex<Option<ServiceBasedFrontend>>> = Lazy::new(|| TokioMutex::new(None));

#[uniffi::export(async_runtime = "tokio")]
pub async fn initialize() -> FfiResult<()> {
    tracing::info!("Initializing local Chroma instance...");
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .try_init();
    let frontend = InMemoryFrontend::default();
    *FRONTEND.lock().await = Some(frontend);
    Ok(())
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn create_tenant(name: String) -> FfiResult<()> {
    let mut frontend = FRONTEND.lock().await;
    let frontend = frontend.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    let tenant_req = CreateTenantRequest::try_new(name)
        .map_err(|e| ChromaError::Generic { message: format!("tenant req: {e}") })?;
    frontend.create_tenant(tenant_req).await
        .map_err(|e| ChromaError::Generic { message: format!("create tenant: {e}") })?;
    Ok(())
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn create_database(tenant_id: String, database_name: String) -> FfiResult<()> {
    let mut frontend = FRONTEND.lock().await;
    let frontend = frontend.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    let db_req = CreateDatabaseRequest::try_new(tenant_id, database_name)
        .map_err(|e| ChromaError::Generic { message: format!("db req: {e}") })?;
    frontend.create_database(db_req).await
        .map_err(|e| ChromaError::Generic { message: format!("create database: {e}") })?;
    Ok(())
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn create_collection(name: String) -> FfiResult<String> {
    let mut frontend = FRONTEND.lock().await;
    let frontend = frontend.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    
    let request = CreateCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        name,
        None,
        None,
        true,
    ).map_err(|e| ChromaError::Generic { message: format!("request: {e}") })?;
    
    let coll = frontend.create_collection(request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("create: {e}") })?;
    
    Ok(coll.collection_id.to_string())
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn add_document(collection_name: String, doc_id: String, text: String, embedding: Vec<f32>) -> FfiResult<u32> {
    let mut frontend = FRONTEND.lock().await;
    let frontend = frontend.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    
    // Get the collection first
    let get_request = GetCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    
    let coll = frontend.get_collection(get_request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    
    println!("Adding document to collection: {}", coll.collection_id);
    
    // Add the document
    let request = AddCollectionRecordsRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        coll.collection_id,
        vec![doc_id],
        Some(vec![embedding]),
        Some(vec![Some(text)]),
        None,
        None,
    ).map_err(|e| ChromaError::Generic { message: format!("add req: {e}") })?;
    
    println!("Add request: {:?}", request);
    
    frontend.add(request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("add: {e}") })?;
    
    println!("Document added successfully");
    Ok(1)
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn get_document_by_id(collection_name: String, doc_id: String) -> FfiResult<Option<String>> {
    let mut frontend = FRONTEND.lock().await;
    let frontend = frontend.as_mut().ok_or_else(|| ChromaError::Generic { 
        message: "Chroma not initialized. Call initialize() first.".to_string() 
    })?;
    
    // Get the collection first
    let get_request = GetCollectionRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    
    let coll = frontend.get_collection(get_request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    
    println!("Getting document by ID from collection: {}", coll.collection_id);
    
    // Query with ids parameter
    let query_request = QueryRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        coll.collection_id,
        Some(vec![doc_id]), // ids
        None, // where
        Vec::new(), // embeddings
        1, // n_results
        chroma_types::IncludeList(vec![chroma_types::Include::Document]),
    ).map_err(|e| ChromaError::Generic { message: format!("query req: {e}") })?;
    
    println!("Query request: {:?}", query_request);
    
    let response = frontend.query(query_request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("query: {e}") })?;
    
    println!("Query response: {:?}", response);
    
    // response.documents: Option<Vec<Vec<Option<String>>>>
    let document = response.documents
        .and_then(|doc_groups| doc_groups.into_iter().next())
        .and_then(|group| group.into_iter().next())
        .flatten();
    
    println!("Document found: {:?}", document);
    Ok(document)
}

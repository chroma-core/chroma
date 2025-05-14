// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use anyhow::Context;
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
    let registry = Registry::new();
    let system = System::new();
    let frontend_config = FrontendConfig::sqlite_in_memory();
    let frontend = ServiceBasedFrontend::try_from_config(&(frontend_config, system), &registry)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("frontend init: {e}") })?;
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
pub async fn list_documents(collection_name: String) -> FfiResult<Vec<String>> {
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
    
    println!("Listing documents from collection: {}", coll.collection_id);
    
    // Query all documents (no embeddings, just list)
    let query_request = QueryRequest::try_new(
        "default".to_string(),
        "default".to_string(),
        coll.collection_id,
        None, // ids
        None, // where
        vec![], // embeddings
        1000, // n_results
        chroma_types::IncludeList(vec![chroma_types::Include::Document]),
    ).map_err(|e| ChromaError::Generic { message: format!("query req: {e}") })?;
    
    let response = frontend.query(query_request)
        .await
        .map_err(|e| ChromaError::Generic { message: format!("query: {e}") })?;
    
    println!("Query response: {:?}", response);
    
    // response.documents: Option<Vec<Vec<Option<String>>>>
    let mut documents = Vec::new();
    if let Some(doc_groups) = response.documents {
        for group in doc_groups {
            for doc in group {
                if let Some(text) = doc {
                    documents.push(text);
                }
            }
        }
    }
    println!("Documents found: {:?}", documents);
    Ok(documents)
}

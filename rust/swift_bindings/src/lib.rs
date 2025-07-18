// lib.rs ---------------------------------------------------------------
use thiserror::Error;
use chroma_config::Configurable;
use chroma_types::{
    CreateCollectionRequest, 
    AddCollectionRecordsRequest, 
    QueryRequest,
    CreateCollectionError,
    AddCollectionRecordsError,
    QueryError,
    GetRequest,
    GetCollectionRequest,
    ListCollectionsRequest,
    DeleteCollectionRequest,
    DeleteCollectionRecordsRequest,
    DeleteCollectionRecordsError,
    CountRequest,
    HeartbeatResponse,
    CreateDatabaseRequest,
    CreateTenantRequest,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

use chroma_frontend::{Frontend, FrontendConfig};
use chroma_config::registry::Registry;
use chroma_system::System;
use tokio::runtime::Runtime;

// Constants matching Python bindings
const DEFAULT_DATABASE: &str = "default_database";
const DEFAULT_TENANT: &str = "default_tenant";

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

impl From<DeleteCollectionRecordsError> for ChromaError {
    fn from(e: DeleteCollectionRecordsError) -> Self {
        Self::Generic { message: e.to_string() }
    }
}

type FfiResult<T> = Result<T, ChromaError>;

// ----------------------------------------------------------------------
//  Chroma API Functions (service-based frontend)
// ----------------------------------------------------------------------

static FRONTEND: Lazy<Mutex<Option<Frontend>>> = Lazy::new(|| Mutex::new(None));
static RUNTIME: Lazy<Mutex<Option<Runtime>>> = Lazy::new(|| Mutex::new(None));

use std::sync::Once;


#[uniffi::export]
pub fn initialize(allow_reset: bool) -> FfiResult<()> {
    // Pass through the allow_reset parameter for configurability
    initialize_with_path(None, allow_reset)
}

#[uniffi::export]
pub fn initialize_with_path(path: Option<String>, allow_reset: bool) -> FfiResult<()> {
    

    // Ensure runtime isn't already initialized
    {
        let runtime_lock = RUNTIME.lock().unwrap();
        if runtime_lock.is_some() {
            return Ok(());
        }
    }

    // Create the runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| ChromaError::Generic { message: e.to_string() })?;
    
    // Create the frontend with either in-memory or persistent storage
    let system = System::new();
    let registry = Registry::new();
    
    // Configure storage based on the path parameter - exactly like Python bindings
    let mut config = FrontendConfig::sqlite_in_memory();
    config.allow_reset = allow_reset;
    
    // Set persist_path in the segment manager config
    if let Some(segment_manager) = &mut config.segment_manager {
        segment_manager.persist_path = path.clone();
    }
    
    // Also configure SQLite to use the path for persistent storage
    if let Some(sqlitedb) = &mut config.sqlitedb {
        if let Some(p) = &path {
            // Create the directory if it doesn't exist
            let path_obj = std::path::Path::new(p);
            if !path_obj.exists() {
                std::fs::create_dir_all(path_obj).map_err(|e| ChromaError::Generic { 
                    message: format!("Failed to create directory: {}", e) 
                })?;
            }
            
            // Set up the SQLite URL to point to a file in this directory
            let db_path = path_obj.join("chroma.sqlite3");
            // Use absolute path format without protocol prefix to avoid file system display artifacts
            sqlitedb.url = Some(db_path.to_string_lossy().to_string());
        }
    }
    
    let mut frontend = runtime
        .block_on(async { Frontend::try_from_config(&(config, system), &registry).await })
        .map_err(|e| ChromaError::Generic { message: e.to_string() })?;
        
    // Create the default tenant (ignoring if it already exists)
    let create_tenant_request = CreateTenantRequest::try_new(DEFAULT_TENANT.to_string())
        .map_err(|e| ChromaError::Generic { message: format!("Invalid tenant: {e}") })?;
    let tenant_result = runtime
        .block_on(async {
            frontend.create_tenant(create_tenant_request).await
        });
    
    // Ignore already exists errors for tenant
    if let Err(e) = &tenant_result {
        if !e.to_string().contains("already exists") {
            return Err(ChromaError::Generic {
                message: format!("Failed to create tenant: {e}"),
            });
        }

        
    }
        
    // Create the default database (ignoring if it already exists)
    let create_database_request = CreateDatabaseRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string()
    ).map_err(|e| ChromaError::Generic { message: format!("Invalid database: {e}") })?;
    
    let db_result = runtime
        .block_on(async {
            frontend.create_database(create_database_request).await
        });
        
    // Ignore already exists errors for database
    if let Err(e) = &db_result {
        if !e.to_string().contains("already exists") {
            return Err(ChromaError::Generic {
                message: format!("Failed to create database: {e}"),
            });
        }

        
    }
    
    // Store the runtime in the global variable
    let mut runtime_lock = RUNTIME.lock().unwrap();
    *runtime_lock = Some(runtime);
    
    // Store the frontend in the global variable
    let mut frontend_lock = FRONTEND.lock().unwrap();
    *frontend_lock = Some(frontend);
    
    Ok(())
}

#[uniffi::export]
pub fn create_collection(name: String) -> FfiResult<String> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    let request = CreateCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        name.clone(),
        None,
        None,
        true,
    ).map_err(|e| ChromaError::Generic { message: format!("request: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.create_collection(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("create: {e}") })?;
    Ok(coll.collection_id.to_string())
}

#[uniffi::export]
pub fn add_documents(collection_name: String, ids: Vec<String>, embeddings: Vec<Vec<f32>>, documents: Vec<String>) -> FfiResult<u32> {
    
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    // Prepare documents as Option<Vec<Option<String>>>
    let documents_opt = Some(documents.into_iter().map(Some).collect());
    let embeddings_opt = Some(embeddings);
    let request = AddCollectionRecordsRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
        ids,
        embeddings_opt,
        documents_opt,
        None,
        None,
    ).map_err(|e| ChromaError::Generic { message: format!("add req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    runtime
        .block_on(async { frontend_clone.add(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("add: {e}") })?;
    Ok(1)
}

// Add a new struct for query results
#[derive(uniffi::Record)]
pub struct QueryResult {
    pub ids: Vec<Vec<String>>, // batched: one Vec<String> per query embedding
    pub documents: Vec<Vec<Option<String>>>, // batched: one Vec<Option<String>> per query embedding
}

#[uniffi::export]
pub fn query_collection(
    collection_name: String,
    query_embeddings: Vec<Vec<f32>>, // batching
    n_results: u32,
    where_filter: Option<String>,
    ids: Option<Vec<String>>,
    include: Option<Vec<String>>,
) -> FfiResult<QueryResult> {
    
    

    // Acquire frontend
    let mut frontend_lock = FRONTEND.lock().unwrap();
    let mut frontend = frontend_lock
        .as_ref()
        .cloned()
        .ok_or_else(|| ChromaError::Generic {
            message: "Chroma not initialized. Call initialize() first.".to_string(),
        })?;

    // Acquire runtime
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;

    // Get collection ID
    
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let coll = runtime
        .block_on(async { frontend.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;

    // Parse include list
    let include_list = if let Some(ref inc) = include {
        chroma_types::IncludeList(
            inc.iter().filter_map(|s| match s.to_lowercase().as_str() {
                "documents" => Some(chroma_types::Include::Document),
                "embeddings" => Some(chroma_types::Include::Embedding),
                "metadatas" => Some(chroma_types::Include::Metadata),
                "distances" => Some(chroma_types::Include::Distance),
                _ => None,
            }).collect()
        )
    } else {
        chroma_types::IncludeList(vec![chroma_types::Include::Document])
    };

    
    let request = chroma_types::QueryRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
        ids, // Option<Vec<String>>
        None, // Option<chroma_types::Where>
        query_embeddings, // Vec<Vec<f32>>
        n_results,
        include_list,
    ).map_err(|e| ChromaError::Generic { message: format!("query req: {e}") })?;
    

    
    let result = runtime
        .block_on(async { frontend.query(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("query: {e}") })?;
    

    
    let ids = result.ids;
    let documents = result.documents.unwrap_or_default();
    let ffi_result = QueryResult { ids, documents };
    Ok(ffi_result)

}

#[derive(uniffi::Record)]
pub struct GetResult {
    pub ids: Vec<String>,
    pub documents: Vec<Option<String>>,
}

#[uniffi::export]
pub fn get_all_documents(collection_name: String) -> FfiResult<GetResult> {
    
    
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    let request = GetRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
        None, // ids
        None, // where
        None, // limit (None means no limit)
        0, // offset
        chroma_types::IncludeList::default_get(), // Use default get includes
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let response = runtime
        .block_on(async { frontend_clone.get(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    let ids = response.ids;
    let documents = response.documents.unwrap_or_default();
    Ok(GetResult { ids, documents })
}

#[uniffi::export]
pub fn list_collections() -> FfiResult<Vec<String>> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    let request = chroma_types::ListCollectionsRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        None, // limit
        0,    // offset
    ).map_err(|e| ChromaError::Generic { message: format!("list req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let collections = runtime
        .block_on(async { frontend_clone.list_collections(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("list: {e}") })?;
    let names = collections.into_iter().map(|c| c.name).collect();
    Ok(names)
}

#[uniffi::export]
pub fn delete_collection(collection_name: String) -> FfiResult<()> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    let request = DeleteCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name,
    ).map_err(|e| ChromaError::Generic { message: format!("delete req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    runtime
        .block_on(async { frontend_clone.delete_collection(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("delete: {e}") })?;
    Ok(())
}

#[derive(uniffi::Record)]
pub struct CollectionInfo {
    pub name: String,
    pub collection_id: String,
    pub num_documents: u32,
}

#[uniffi::export]
pub fn get_collection_info(collection_name: String) -> FfiResult<CollectionInfo> {
    
    let mut frontend_lock = FRONTEND.lock().unwrap();
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    // Count documents in the collection
    let count_request = chroma_types::CountRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
    ).map_err(|e| ChromaError::Generic { message: format!("count req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let num_documents = runtime
        .block_on(async { frontend_clone.count(count_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("count: {e}") })?;
    Ok(CollectionInfo {
        name: coll.name,
        collection_id: coll.collection_id.to_string(),
        num_documents,
    })
}

#[uniffi::export]
pub fn reset() -> FfiResult<()> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    let mut frontend_clone = frontend.clone();
    runtime
        .block_on(async { frontend_clone.reset().await })
        .map_err(|e| ChromaError::Generic { message: format!("reset: {e}") })?;
    Ok(())
}

#[uniffi::export]
pub fn get_version() -> FfiResult<String> {
    
    // For now, return a hardcoded version since InMemoryFrontend doesn't have a version method
    Ok("0.1.0".to_string())
}

#[uniffi::export]
pub fn get_max_batch_size() -> FfiResult<u32> {
    
    let mut frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    Ok(frontend.get_max_batch_size())
}

#[uniffi::export]
pub fn heartbeat() -> FfiResult<i64> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    
    let mut frontend_clone = frontend.clone();
    let heartbeat_response = runtime
        .block_on(async { frontend_clone.heartbeat().await })
        .map_err(|e| ChromaError::Generic { message: format!("heartbeat: {e}") })?;
    
    // Convert u128 to i64 (nanoseconds since Unix epoch)
    // If the timestamp is too large for i64, we'll truncate it
    let timestamp = heartbeat_response.nanosecond_heartbeat as i64;
    
    Ok(timestamp)
}

#[uniffi::export]
pub fn count_documents(collection_name: String) -> FfiResult<u32> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    
    // Count documents in the collection
    let count_request = chroma_types::CountRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
    ).map_err(|e| ChromaError::Generic { message: format!("count req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let count = runtime
        .block_on(async { frontend_clone.count(count_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("count: {e}") })?;
    
    Ok(count)
}

#[uniffi::export]
pub fn delete_documents(collection_name: String, ids: Option<Vec<String>>) -> FfiResult<()> {
    
    let frontend = {
        let frontend_lock = FRONTEND.lock().unwrap();
        frontend_lock
            .as_ref()
            .cloned()
            .ok_or_else(|| ChromaError::Generic {
                message: "Chroma not initialized. Call initialize() first.".to_string(),
            })?
    };
    let runtime_lock = RUNTIME.lock().unwrap();
    let runtime = runtime_lock.as_ref().ok_or_else(|| ChromaError::Generic {
        message: "Chroma not initialized. Call initialize() first.".to_string(),
    })?;
    
    // Get collection id
    let get_request = chroma_types::GetCollectionRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        collection_name.clone(),
    ).map_err(|e| ChromaError::Generic { message: format!("get req: {e}") })?;
    let mut frontend_clone = frontend.clone();
    let coll = runtime
        .block_on(async { frontend_clone.get_collection(get_request).await })
        .map_err(|e| ChromaError::Generic { message: format!("get: {e}") })?;
    
    // Create delete request
    let request = DeleteCollectionRecordsRequest::try_new(
        DEFAULT_TENANT.to_string(),
        DEFAULT_DATABASE.to_string(),
        coll.collection_id,
        ids,
        None, // where clause (not supported yet)
    ).map_err(|e| ChromaError::Generic { message: format!("delete req: {e}") })?;
    
    let mut frontend_clone = frontend.clone();
    runtime
        .block_on(async { frontend_clone.delete(request).await })
        .map_err(|e| ChromaError::Generic { message: format!("delete: {e}") })?;
    
    Ok(())
}

        
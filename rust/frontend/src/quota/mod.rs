use std::{
    future::{ready, Future},
    pin::Pin,
};

use chroma_error::ChromaError;
use chroma_types::{CollectionUuid, Metadata, UpdateMetadata, Where};
use thiserror::Error;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Action {
    CreateDatabase,
    CreateCollection,
    ListCollections,
    UpdateCollection,
    Add,
    Get,
    Delete,
    Update,
    Upsert,
    Query,
}

impl TryFrom<&str> for Action {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "create_database" => Ok(Action::CreateDatabase),
            "create_collection" => Ok(Action::CreateCollection),
            "list_collections" => Ok(Action::ListCollections),
            "update_collection" => Ok(Action::UpdateCollection),
            "add" => Ok(Action::Add),
            "get" => Ok(Action::Get),
            "delete" => Ok(Action::Delete),
            "update" => Ok(Action::Update),
            "upsert" => Ok(Action::Upsert),
            "query" => Ok(Action::Query),
            _ => Err(format!("Invalid Action: {}", value)),
        }
    }
}

pub struct QuotaPayload<'other> {
    #[allow(dead_code)]
    pub action: Action,
    #[allow(dead_code)]
    pub tenant: String,
    #[allow(dead_code)]
    pub api_token: Option<String>,
    pub create_collection_metadata: Option<&'other Metadata>,
    pub update_collection_metadata: Option<&'other UpdateMetadata>,
    pub ids: Option<&'other [String]>,
    pub add_embeddings: Option<&'other [Vec<f32>]>,
    pub update_embeddings: Option<&'other [Option<Vec<f32>>]>,
    pub documents: Option<&'other [Option<String>]>,
    pub uris: Option<&'other [Option<String>]>,
    pub metadatas: Option<&'other [Option<Metadata>]>,
    pub update_metadatas: Option<&'other [Option<UpdateMetadata>]>,
    pub r#where: Option<&'other Where>,
    pub collection_name: Option<&'other str>,
    pub collection_new_name: Option<&'other str>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub n_results: Option<u32>,
    pub query_embeddings: Option<&'other [Vec<f32>]>,
    pub collection_uuid: Option<CollectionUuid>,
}

impl<'other> QuotaPayload<'other> {
    pub fn new(action: Action, tenant: String, api_token: Option<String>) -> Self {
        Self {
            action,
            tenant,
            api_token,
            create_collection_metadata: None,
            update_collection_metadata: None,
            ids: None,
            add_embeddings: None,
            update_embeddings: None,
            documents: None,
            uris: None,
            metadatas: None,
            update_metadatas: None,
            r#where: None,
            collection_name: None,
            collection_new_name: None,
            limit: None,
            offset: None,
            n_results: None,
            query_embeddings: None,
            collection_uuid: None,
        }
    }

    // create builder methods for each field except tenant and action
    // Name the method starting with_*
    // Return self
    pub fn with_create_collection_metadata(
        mut self,
        create_collection_metadata: &'other Metadata,
    ) -> Self {
        self.create_collection_metadata = Some(create_collection_metadata);
        self
    }

    pub fn with_update_collection_metadata(
        mut self,
        update_collection_metadata: &'other UpdateMetadata,
    ) -> Self {
        self.update_collection_metadata = Some(update_collection_metadata);
        self
    }

    pub fn with_ids(mut self, ids: &'other [String]) -> Self {
        self.ids = Some(ids);
        self
    }

    pub fn with_add_embeddings(mut self, add_embeddings: &'other [Vec<f32>]) -> Self {
        self.add_embeddings = Some(add_embeddings);
        self
    }

    pub fn with_update_embeddings(mut self, update_embeddings: &'other [Option<Vec<f32>>]) -> Self {
        self.update_embeddings = Some(update_embeddings);
        self
    }

    pub fn with_documents(mut self, documents: &'other [Option<String>]) -> Self {
        self.documents = Some(documents);
        self
    }

    pub fn with_uris(mut self, uris: &'other [Option<String>]) -> Self {
        self.uris = Some(uris);
        self
    }

    pub fn with_metadatas(mut self, metadatas: &'other [Option<Metadata>]) -> Self {
        self.metadatas = Some(metadatas);
        self
    }

    pub fn with_update_metadatas(
        mut self,
        update_metadatas: &'other [Option<UpdateMetadata>],
    ) -> Self {
        self.update_metadatas = Some(update_metadatas);
        self
    }

    pub fn with_where(mut self, r#where: &'other Where) -> Self {
        self.r#where = Some(r#where);
        self
    }

    pub fn with_collection_name(mut self, collection_name: &'other str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn with_collection_new_name(mut self, collection_new_name: &'other str) -> Self {
        self.collection_new_name = Some(collection_new_name);
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_n_results(mut self, n_results: u32) -> Self {
        self.n_results = Some(n_results);
        self
    }

    pub fn with_query_embeddings(mut self, query_embeddings: &'other [Vec<f32>]) -> Self {
        self.query_embeddings = Some(query_embeddings);
        self
    }

    pub fn with_collection_uuid(mut self, collection_uuid: CollectionUuid) -> Self {
        self.collection_uuid = Some(collection_uuid);
        self
    }
}

#[derive(Error, Debug)]
pub enum QuotaEnforcerError {
    #[error("Quota exceeded")]
    QuotaExceeded,
    #[error("Missing API key in the request header")]
    ApiKeyMissing,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Initialization failed")]
    InitializationFailed,
}

impl ChromaError for QuotaEnforcerError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            QuotaEnforcerError::QuotaExceeded => chroma_error::ErrorCodes::ResourceExhausted,
            QuotaEnforcerError::ApiKeyMissing => chroma_error::ErrorCodes::InvalidArgument,
            QuotaEnforcerError::Unauthorized => chroma_error::ErrorCodes::PermissionDenied,
            QuotaEnforcerError::InitializationFailed => chroma_error::ErrorCodes::Internal,
        }
    }
}

pub trait QuotaEnforcer: Send + Sync {
    fn enforce<'other>(
        &'other self,
        payload: &'other QuotaPayload<'other>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QuotaEnforcerError>> + Send + 'other>>;
}

impl QuotaEnforcer for () {
    fn enforce(
        &self,
        _: &QuotaPayload<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QuotaEnforcerError>> + Send>> {
        Box::pin(ready(Ok(())))
    }
}

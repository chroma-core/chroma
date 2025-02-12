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
    action: Action,
    #[allow(dead_code)]
    tenant: String,
    #[allow(dead_code)]
    api_token: String,
    create_collection_metadata: Option<&'other Metadata>,
    update_collection_metadata: Option<&'other UpdateMetadata>,
    ids: Option<&'other [String]>,
    add_embeddings: Option<&'other [Vec<f32>]>,
    update_embeddings: Option<&'other [Option<Vec<f32>>]>,
    documents: Option<&'other [Option<String>]>,
    uris: Option<&'other [Option<String>]>,
    metadatas: Option<&'other [Option<Metadata>]>,
    update_metadatas: Option<&'other [Option<UpdateMetadata>]>,
    r#where: Option<&'other Where>,
    collection_name: Option<&'other String>,
    collection_new_name: Option<&'other String>,
    limit: Option<u32>,
    offset: Option<u32>,
    n_results: Option<u32>,
    query_embeddings: Option<&'other [Vec<f32>]>,
    collection_uuid: Option<CollectionUuid>,
}

impl<'other> QuotaPayload<'other> {
    pub fn new(action: Action, tenant: String, api_token: String) -> Self {
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

    pub fn with_collection_name(mut self, collection_name: &'other String) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn with_collection_new_name(mut self, collection_new_name: &'other String) -> Self {
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
}

impl ChromaError for QuotaEnforcerError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            QuotaEnforcerError::QuotaExceeded => chroma_error::ErrorCodes::ResourceExhausted,
            QuotaEnforcerError::ApiKeyMissing => chroma_error::ErrorCodes::InvalidArgument,
            QuotaEnforcerError::Unauthorized => chroma_error::ErrorCodes::PermissionDenied,
        }
    }
}

pub trait QuotaEnforcer: Send + Sync {
    fn enforce(
        &self,
        payload: &QuotaPayload<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QuotaEnforcerError>> + Send + Sync>>;
}

impl QuotaEnforcer for () {
    fn enforce(
        &self,
        _: &QuotaPayload<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QuotaEnforcerError>> + Send + Sync>> {
        Box::pin(ready(Ok(())))
    }
}

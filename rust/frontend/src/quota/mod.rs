use std::{
    fmt,
    future::{ready, Future},
    pin::Pin,
};

use chroma_error::ChromaError;
use chroma_types::{CollectionUuid, Metadata, UpdateMetadata, Where};
use thiserror::Error;
use validator::Validate;

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
    ForkCollection,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::CreateDatabase => write!(f, "Create Database"),
            Action::CreateCollection => write!(f, "Create Collection"),
            Action::ListCollections => write!(f, "List Collections"),
            Action::UpdateCollection => write!(f, "Update Collection"),
            Action::Add => write!(f, "Add"),
            Action::Get => write!(f, "Get"),
            Action::Delete => write!(f, "Delete"),
            Action::Update => write!(f, "Update"),
            Action::Upsert => write!(f, "Upsert"),
            Action::Query => write!(f, "Query"),
            Action::ForkCollection => write!(f, "Fork Collection"),
        }
    }
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
            "fork_collection" => Ok(Action::ForkCollection),
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
    pub query_ids: Option<&'other [String]>,
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
            query_ids: None,
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

    pub fn with_query_ids(mut self, query_ids: &'other [String]) -> Self {
        self.query_ids = Some(query_ids);
        self
    }

    pub fn with_collection_uuid(mut self, collection_uuid: CollectionUuid) -> Self {
        self.collection_uuid = Some(collection_uuid);
        self
    }
}

use std::collections::HashMap;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum UsageType {
    MetadataKeySizeBytes,       // Max metadata key size in bytes
    MetadataValueSizeBytes,     // Max metadata value size in bytes
    NumMetadataKeys,            // Number of keys in the metadata
    NumWherePredicates,         // Number of predicates in the where
    WhereValueSizeBytes,        // Max where clause value size in bytes
    NumWhereDocumentPredicates, // Number of predicates in the where_document
    WhereDocumentValueLength,   // Max where_document value length
    NumRecords,                 // Number of records
    EmbeddingDimensions,        // Number of ints/floats in the embedding
    DocumentSizeBytes,          // Max document size in bytes
    UriSizeBytes,               // Max uri size in bytes
    IdSizeBytes,                // Max id size in bytes
    NameSizeBytes,              // Max name size in bytes (e.g. collection, database)
    LimitValue,
    NumResults,
    NumQueryEmbeddings,    // Number of query embeddings
    CollectionSizeRecords, // Number of records in the collection
    NumCollections,        // Total number of collections for a tenant
    NumDatabases,          // Total number of databases for a tenant
    NumQueryIDs,           // Number of IDs to filter by in a query
    RegexPatternLength,    // Length of regex pattern specified in filter
    NumForks,              // Number of forks a root collection may have
}

impl fmt::Display for UsageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UsageType::MetadataKeySizeBytes => write!(f, "Size of metadata dictionary key (bytes)"),
            UsageType::MetadataValueSizeBytes => {
                write!(f, "Size of metadata dictionary value (bytes)")
            }
            UsageType::NumMetadataKeys => write!(f, "Number of metadata dictionary keys"),
            UsageType::NumWherePredicates => write!(f, "Number of where clause predicates"),
            UsageType::WhereValueSizeBytes => write!(f, "Size of where clause value (bytes)"),
            UsageType::NumWhereDocumentPredicates => {
                write!(f, "Number of where document predicates")
            }
            UsageType::WhereDocumentValueLength => write!(f, "Length of where document value"),
            UsageType::NumRecords => write!(f, "Number of records"),
            UsageType::EmbeddingDimensions => write!(f, "Embedding dimension"),
            UsageType::DocumentSizeBytes => write!(f, "Document size (bytes)"),
            UsageType::UriSizeBytes => write!(f, "URI size (bytes)"),
            UsageType::IdSizeBytes => write!(f, "ID size (bytes)"),
            UsageType::NameSizeBytes => write!(f, "Name size (bytes)"),
            UsageType::LimitValue => write!(f, "Limit value"),
            UsageType::NumResults => write!(f, "Number of results"),
            UsageType::NumQueryEmbeddings => write!(f, "Number of query embeddings"),
            UsageType::CollectionSizeRecords => write!(f, "Collection size (records)"),
            UsageType::NumCollections => write!(f, "Number of collections"),
            UsageType::NumDatabases => write!(f, "Number of databases"),
            UsageType::NumQueryIDs => write!(f, "Number of IDs to filter by in a query"),
            UsageType::RegexPatternLength => write!(f, "Length of regex pattern"),
            UsageType::NumForks => write!(f, "Number of forks"),
        }
    }
}

impl TryFrom<&str> for UsageType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "metadata_key_size_bytes" => Ok(UsageType::MetadataKeySizeBytes),
            "metadata_value_size_bytes" => Ok(UsageType::MetadataValueSizeBytes),
            "num_metadata_keys" => Ok(UsageType::NumMetadataKeys),
            "num_where_predicates" => Ok(UsageType::NumWherePredicates),
            "where_value_size_bytes" => Ok(UsageType::WhereValueSizeBytes),
            "num_where_document_predicates" => Ok(UsageType::NumWhereDocumentPredicates),
            "where_document_value_length" => Ok(UsageType::WhereDocumentValueLength),
            "num_records" => Ok(UsageType::NumRecords),
            "embedding_dimensions" => Ok(UsageType::EmbeddingDimensions),
            "document_size_bytes" => Ok(UsageType::DocumentSizeBytes),
            "uri_size_bytes" => Ok(UsageType::UriSizeBytes),
            "id_size_bytes" => Ok(UsageType::IdSizeBytes),
            "name_size_bytes" => Ok(UsageType::NameSizeBytes),
            "limit_value" => Ok(UsageType::LimitValue),
            "num_results" => Ok(UsageType::NumResults),
            "num_query_embeddings" => Ok(UsageType::NumQueryEmbeddings),
            "collection_size_records" => Ok(UsageType::CollectionSizeRecords),
            "num_collections" => Ok(UsageType::NumCollections),
            "num_databases" => Ok(UsageType::NumDatabases),
            "num_query_ids" => Ok(UsageType::NumQueryIDs),
            "regex_pattern_length" => Ok(UsageType::RegexPatternLength),
            "num_forks" => Ok(UsageType::NumForks),
            _ => Err(format!("Invalid UsageType: {}", value)),
        }
    }
}

lazy_static::lazy_static! {
    pub static ref DEFAULT_QUOTAS: HashMap<UsageType, usize> = {
        let mut m = HashMap::new();
        m.insert(UsageType::MetadataKeySizeBytes, 36);
        m.insert(UsageType::MetadataValueSizeBytes, 36);
        m.insert(UsageType::NumMetadataKeys, 16);
        m.insert(UsageType::NumWherePredicates, 8);
        m.insert(UsageType::WhereValueSizeBytes, 36); // Same as METADATA_VALUE_SIZE
        m.insert(UsageType::NumWhereDocumentPredicates, 8);
        m.insert(UsageType::WhereDocumentValueLength, 130);
        m.insert(UsageType::NumRecords, 100);
        m.insert(UsageType::EmbeddingDimensions, 3072);
        m.insert(UsageType::DocumentSizeBytes, 5000);
        m.insert(UsageType::UriSizeBytes, 32);
        m.insert(UsageType::IdSizeBytes, 128);
        m.insert(UsageType::NameSizeBytes, 128);
        m.insert(UsageType::LimitValue, 1000);
        m.insert(UsageType::NumResults, 100);
        m.insert(UsageType::NumQueryEmbeddings, 100);
        m.insert(UsageType::CollectionSizeRecords, 1_000_000);
        m.insert(UsageType::NumCollections, 1_000_000);
        m.insert(UsageType::NumDatabases, 10);
        m.insert(UsageType::NumQueryIDs, 1000);
        m.insert(UsageType::RegexPatternLength, 0);
        m.insert(UsageType::NumForks, 256);
        m
    };
}

#[derive(Debug, Validate)]
pub struct QuotaExceededError {
    pub usage_type: UsageType,
    pub action: Action,
    pub usage: usize,
    pub limit: usize,
    #[validate(length(min = 1))]
    pub message: Option<String>,
}

impl fmt::Display for QuotaExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "'{}' exceeded quota limit for action '{}': current usage of {} exceeds limit of {}",
            self.usage_type, self.action, self.usage, self.limit
        )?;
        if let Some(msg) = self.message.as_ref() {
            write!(f, ". {}", msg)?;
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum QuotaEnforcerError {
    #[error("Quota exceeded: {0}")]
    QuotaExceeded(QuotaExceededError),
    #[error("Missing API key in the request header")]
    ApiKeyMissing,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Initialization failed")]
    InitializationFailed,
    #[error("{0}")]
    GenericQuotaError(String),
}

impl ChromaError for QuotaEnforcerError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            QuotaEnforcerError::QuotaExceeded(_) => chroma_error::ErrorCodes::UnprocessableEntity,
            QuotaEnforcerError::ApiKeyMissing => chroma_error::ErrorCodes::InvalidArgument,
            QuotaEnforcerError::Unauthorized => chroma_error::ErrorCodes::PermissionDenied,
            QuotaEnforcerError::InitializationFailed => chroma_error::ErrorCodes::Internal,
            QuotaEnforcerError::GenericQuotaError(_) => chroma_error::ErrorCodes::ResourceExhausted,
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

#[cfg(test)]
mod tests {
    use super::{Action, QuotaExceededError, UsageType};
    use validator::Validate;

    #[test]
    fn test_quota_exceeded_error_message_none() {
        let error = QuotaExceededError {
            usage_type: UsageType::NumRecords,
            action: Action::Add,
            usage: 100,
            limit: 50,
            message: None,
        };
        assert!(error.validate().is_ok());
    }

    #[test]
    fn test_quota_exceeded_error_message_empty() {
        let error = QuotaExceededError {
            usage_type: UsageType::NumRecords,
            action: Action::Add,
            usage: 100,
            limit: 50,
            message: Some("".to_string()),
        };
        assert!(error.validate().is_err());
    }

    #[test]
    fn test_quota_exceeded_error_message_valid() {
        let custom_message = "This is a valid message.";
        let error = QuotaExceededError {
            usage_type: UsageType::NumRecords,
            action: Action::Add,
            usage: 100,
            limit: 50,
            message: Some(custom_message.to_string()),
        };
        assert!(error.validate().is_ok());
        let error_string = format!("{}", error);
        let expected_error_string = "'Number of records' exceeded quota limit for action 'Add': current usage of 100 exceeds limit of 50. This is a valid message.";
        assert_eq!(error_string, expected_error_string);
    }

    #[test]
    fn test_quota_exceeded_error_display_no_message() {
        let error = QuotaExceededError {
            usage_type: UsageType::NumRecords,
            action: Action::Add,
            usage: 100,
            limit: 50,
            message: None,
        };
        assert!(error.validate().is_ok());
        let error_string = format!("{}", error);
        assert_eq!(error_string, "'Number of records' exceeded quota limit for action 'Add': current usage of 100 exceeds limit of 50");
    }
}

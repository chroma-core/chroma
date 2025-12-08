use chroma_error::{ChromaError, ErrorCodes};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use validator::Validate;

use crate::collection_configuration::{
    EmbeddingFunctionConfiguration, InternalCollectionConfiguration,
    UpdateVectorIndexConfiguration, VectorIndexConfiguration,
};
use crate::hnsw_configuration::Space;
use crate::metadata::{MetadataComparison, MetadataValueType, Where};
use crate::operator::QueryVector;
use crate::{
    default_batch_size, default_construction_ef, default_construction_ef_spann,
    default_initial_lambda, default_m, default_m_spann, default_merge_threshold,
    default_nreplica_count, default_num_centers_to_merge_to, default_num_samples_kmeans,
    default_num_threads, default_reassign_neighbor_count, default_resize_factor, default_search_ef,
    default_search_ef_spann, default_search_nprobe, default_search_rng_epsilon,
    default_search_rng_factor, default_space, default_split_threshold, default_sync_threshold,
    default_write_nprobe, default_write_rng_epsilon, default_write_rng_factor,
    HnswParametersFromSegmentError, InternalHnswConfiguration, InternalSpannConfiguration,
    InternalUpdateCollectionConfiguration, KnnIndex, Segment, CHROMA_KEY,
};

impl ChromaError for SchemaError {
    fn code(&self) -> ErrorCodes {
        match self {
            // Internal errors (500)
            // These indicate system/internal issues during schema operations
            SchemaError::MissingIndexConfiguration { .. } => ErrorCodes::Internal,
            SchemaError::InvalidSchema { .. } => ErrorCodes::Internal,
            // DefaultsMismatch and ConfigurationConflict only occur during schema merge()
            // which happens internally during compaction, not from user input
            SchemaError::DefaultsMismatch => ErrorCodes::Internal,
            SchemaError::ConfigurationConflict { .. } => ErrorCodes::Internal,

            // User/External errors (400)
            // These indicate user-provided invalid input
            SchemaError::InvalidUserInput { .. } => ErrorCodes::InvalidArgument,
            SchemaError::ConfigAndSchemaConflict => ErrorCodes::InvalidArgument,
            SchemaError::InvalidHnswConfig(_) => ErrorCodes::InvalidArgument,
            SchemaError::InvalidSpannConfig(_) => ErrorCodes::InvalidArgument,
            SchemaError::Builder(e) => e.code(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Schema is malformed: missing index configuration for metadata key '{key}' with type '{value_type}'")]
    MissingIndexConfiguration { key: String, value_type: String },
    #[error("Schema reconciliation failed: {reason}")]
    InvalidSchema { reason: String },
    #[error("Cannot set both collection config and schema simultaneously")]
    ConfigAndSchemaConflict,
    #[error("Cannot merge schemas with differing defaults")]
    DefaultsMismatch,
    #[error("Conflicting configuration for {context}")]
    ConfigurationConflict { context: String },
    #[error("Invalid HNSW configuration: {0}")]
    InvalidHnswConfig(validator::ValidationErrors),
    #[error("Invalid SPANN configuration: {0}")]
    InvalidSpannConfig(validator::ValidationErrors),
    #[error("Invalid schema input: {reason}")]
    InvalidUserInput { reason: String },
    #[error(transparent)]
    Builder(#[from] SchemaBuilderError),
}

#[derive(Debug, Error)]
pub enum SchemaBuilderError {
    #[error("Vector index must be configured globally using create_index(None, config), not on specific key '{key}'")]
    VectorIndexMustBeGlobal { key: String },
    #[error("FTS index must be configured globally using create_index(None, config), not on specific key '{key}'")]
    FtsIndexMustBeGlobal { key: String },
    #[error("Cannot modify special key '{key}' - it is managed automatically by the system. To customize vector search, modify the global vector config instead.")]
    SpecialKeyModificationNotAllowed { key: String },
    #[error("Sparse vector index requires a specific key. Use create_index(Some(\"key_name\"), config) instead of create_index(None, config)")]
    SparseVectorRequiresKey,
    #[error("Only one sparse vector index allowed per collection. Key '{existing_key}' already has a sparse vector index. Remove it first or use that key.")]
    MultipleSparseVectorIndexes { existing_key: String },
    #[error("Vector index deletion not supported. The vector index is always enabled on #embedding. To disable vector search, disable the collection instead.")]
    VectorIndexDeletionNotSupported,
    #[error("FTS index deletion not supported. The FTS index is always enabled on #document. To disable full-text search, use a different collection without FTS.")]
    FtsIndexDeletionNotSupported,
    #[error("Sparse vector index deletion not supported yet. Sparse vector indexes cannot be removed once created.")]
    SparseVectorIndexDeletionNotSupported,
}

#[derive(Debug, Error)]
pub enum FilterValidationError {
    #[error(
        "Cannot filter using metadata key '{key}' with type '{value_type:?}' because indexing is disabled"
    )]
    IndexingDisabled {
        key: String,
        value_type: MetadataValueType,
    },
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

impl ChromaError for SchemaBuilderError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl ChromaError for FilterValidationError {
    fn code(&self) -> ErrorCodes {
        match self {
            FilterValidationError::IndexingDisabled { .. } => ErrorCodes::InvalidArgument,
            FilterValidationError::Schema(_) => ErrorCodes::Internal,
        }
    }
}

// ============================================================================
// SCHEMA CONSTANTS
// ============================================================================
// These constants must match the Python constants in chromadb/api/types.py

// Value type name constants
pub const STRING_VALUE_NAME: &str = "string";
pub const INT_VALUE_NAME: &str = "int";
pub const BOOL_VALUE_NAME: &str = "bool";
pub const FLOAT_VALUE_NAME: &str = "float";
pub const FLOAT_LIST_VALUE_NAME: &str = "float_list";
pub const SPARSE_VECTOR_VALUE_NAME: &str = "sparse_vector";

// Index type name constants
pub const FTS_INDEX_NAME: &str = "fts_index";
pub const VECTOR_INDEX_NAME: &str = "vector_index";
pub const SPARSE_VECTOR_INDEX_NAME: &str = "sparse_vector_index";
pub const STRING_INVERTED_INDEX_NAME: &str = "string_inverted_index";
pub const INT_INVERTED_INDEX_NAME: &str = "int_inverted_index";
pub const FLOAT_INVERTED_INDEX_NAME: &str = "float_inverted_index";
pub const BOOL_INVERTED_INDEX_NAME: &str = "bool_inverted_index";

// Special metadata keys - must match Python constants in chromadb/api/types.py
pub const DOCUMENT_KEY: &str = "#document";
pub const EMBEDDING_KEY: &str = "#embedding";

// Static regex pattern to validate CMEK for GCP
static CMEK_GCP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"projects/\w+/locations/\w+/keyRings/\w+/cryptoKeys/\w+")
        .expect("The CMEK pattern for GCP should be valid")
});

/// Customer-managed encryption key
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Cmek {
    Gcp(Arc<String>),
}

impl Cmek {
    /// Create a GCP CMEK from a KMS resource name
    pub fn gcp(resource: String) -> Self {
        Cmek::Gcp(Arc::new(resource))
    }

    pub fn validate_pattern(&self) -> bool {
        match self {
            Cmek::Gcp(resource) => CMEK_GCP_RE.is_match(resource),
        }
    }
}

// ============================================================================
// SCHEMA STRUCTURES
// ============================================================================

/// Schema representation for collection index configurations
///
/// This represents the server-side schema structure used for index management

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct Schema {
    /// Default index configurations for each value type
    pub defaults: ValueTypes,
    /// Key-specific index overrides
    /// TODO(Sanket): Needed for backwards compatibility. Should remove after deploy.
    #[serde(rename = "keys", alias = "key_overrides")]
    pub keys: HashMap<String, ValueTypes>,
}

impl Schema {
    pub fn update(&mut self, configuration: &InternalUpdateCollectionConfiguration) {
        if let Some(vector_update) = &configuration.vector_index {
            if let Some(default_vector_index) = self.defaults_vector_index_mut() {
                Self::apply_vector_index_update(default_vector_index, vector_update);
            }
            if let Some(embedding_vector_index) = self.embedding_vector_index_mut() {
                Self::apply_vector_index_update(embedding_vector_index, vector_update);
            }
        }

        if let Some(embedding_function) = configuration.embedding_function.as_ref() {
            if let Some(default_vector_index) = self.defaults_vector_index_mut() {
                default_vector_index.config.embedding_function = Some(embedding_function.clone());
            }
            if let Some(embedding_vector_index) = self.embedding_vector_index_mut() {
                embedding_vector_index.config.embedding_function = Some(embedding_function.clone());
            }
        }
    }

    fn defaults_vector_index_mut(&mut self) -> Option<&mut VectorIndexType> {
        self.defaults
            .float_list
            .as_mut()
            .and_then(|float_list| float_list.vector_index.as_mut())
    }

    fn embedding_vector_index_mut(&mut self) -> Option<&mut VectorIndexType> {
        self.keys
            .get_mut(EMBEDDING_KEY)
            .and_then(|value_types| value_types.float_list.as_mut())
            .and_then(|float_list| float_list.vector_index.as_mut())
    }

    fn apply_vector_index_update(
        vector_index: &mut VectorIndexType,
        update: &UpdateVectorIndexConfiguration,
    ) {
        match update {
            UpdateVectorIndexConfiguration::Hnsw(Some(hnsw_update)) => {
                if let Some(hnsw_config) = vector_index.config.hnsw.as_mut() {
                    if let Some(ef_search) = hnsw_update.ef_search {
                        hnsw_config.ef_search = Some(ef_search);
                    }
                    if let Some(max_neighbors) = hnsw_update.max_neighbors {
                        hnsw_config.max_neighbors = Some(max_neighbors);
                    }
                    if let Some(num_threads) = hnsw_update.num_threads {
                        hnsw_config.num_threads = Some(num_threads);
                    }
                    if let Some(resize_factor) = hnsw_update.resize_factor {
                        hnsw_config.resize_factor = Some(resize_factor);
                    }
                    if let Some(sync_threshold) = hnsw_update.sync_threshold {
                        hnsw_config.sync_threshold = Some(sync_threshold);
                    }
                    if let Some(batch_size) = hnsw_update.batch_size {
                        hnsw_config.batch_size = Some(batch_size);
                    }
                }
            }
            UpdateVectorIndexConfiguration::Hnsw(None) => {}
            UpdateVectorIndexConfiguration::Spann(Some(spann_update)) => {
                if let Some(spann_config) = vector_index.config.spann.as_mut() {
                    if let Some(search_nprobe) = spann_update.search_nprobe {
                        spann_config.search_nprobe = Some(search_nprobe);
                    }
                    if let Some(ef_search) = spann_update.ef_search {
                        spann_config.ef_search = Some(ef_search);
                    }
                }
            }
            UpdateVectorIndexConfiguration::Spann(None) => {}
        }
    }

    pub fn is_sparse_index_enabled(&self) -> bool {
        let defaults_enabled = self
            .defaults
            .sparse_vector
            .as_ref()
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .is_some_and(|idx| idx.enabled);
        let key_enabled = self.keys.values().any(|value_types| {
            value_types
                .sparse_vector
                .as_ref()
                .and_then(|sv| sv.sparse_vector_index.as_ref())
                .is_some_and(|idx| idx.enabled)
        });
        defaults_enabled || key_enabled
    }
}

impl Default for Schema {
    /// Create a default Schema that matches Python's behavior exactly.
    ///
    /// Python creates a Schema with:
    /// - All inverted indexes enabled by default (string, int, float, bool)
    /// - Vector and FTS indexes disabled in defaults
    /// - Special keys configured: #document (FTS enabled) and #embedding (vector enabled)
    /// - Vector config has space=None, hnsw=None, spann=None (deferred to backend)
    ///
    /// # Examples
    /// ```
    /// use chroma_types::Schema;
    ///
    /// let schema = Schema::default();
    /// assert!(schema.keys.contains_key("#document"));
    /// assert!(schema.keys.contains_key("#embedding"));
    /// ```
    fn default() -> Self {
        // Initialize defaults - match Python's _initialize_defaults()
        let defaults = ValueTypes {
            string: Some(StringValueType {
                fts_index: Some(FtsIndexType {
                    enabled: false,
                    config: FtsIndexConfig {},
                }),
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: true,
                    config: StringInvertedIndexConfig {},
                }),
            }),
            float_list: Some(FloatListValueType {
                vector_index: Some(VectorIndexType {
                    enabled: false,
                    config: VectorIndexConfig {
                        space: None, // Python leaves as None (resolved on serialization)
                        embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                        source_key: None,
                        hnsw: None,  // Python doesn't specify
                        spann: None, // Python doesn't specify
                    },
                }),
            }),
            sparse_vector: Some(SparseVectorValueType {
                sparse_vector_index: Some(SparseVectorIndexType {
                    enabled: false,
                    config: SparseVectorIndexConfig {
                        embedding_function: None,
                        source_key: None,
                        bm25: None,
                    },
                }),
            }),
            int: Some(IntValueType {
                int_inverted_index: Some(IntInvertedIndexType {
                    enabled: true,
                    config: IntInvertedIndexConfig {},
                }),
            }),
            float: Some(FloatValueType {
                float_inverted_index: Some(FloatInvertedIndexType {
                    enabled: true,
                    config: FloatInvertedIndexConfig {},
                }),
            }),
            boolean: Some(BoolValueType {
                bool_inverted_index: Some(BoolInvertedIndexType {
                    enabled: true,
                    config: BoolInvertedIndexConfig {},
                }),
            }),
        };

        // Initialize key-specific overrides - match Python's _initialize_keys()
        let mut keys = HashMap::new();

        // #document: FTS enabled, string inverted disabled
        keys.insert(
            DOCUMENT_KEY.to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: false,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        // #embedding: Vector index enabled with source_key=#document
        keys.insert(
            EMBEDDING_KEY.to_string(),
            ValueTypes {
                float_list: Some(FloatListValueType {
                    vector_index: Some(VectorIndexType {
                        enabled: true,
                        config: VectorIndexConfig {
                            space: None, // Python leaves as None (resolved on serialization)
                            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                            source_key: Some(DOCUMENT_KEY.to_string()),
                            hnsw: None,  // Python doesn't specify
                            spann: None, // Python doesn't specify
                        },
                    }),
                }),
                ..Default::default()
            },
        );

        Schema { defaults, keys }
    }
}

pub fn is_embedding_function_default(
    embedding_function: &Option<EmbeddingFunctionConfiguration>,
) -> bool {
    match embedding_function {
        None => true,
        Some(embedding_function) => embedding_function.is_default(),
    }
}

/// Check if space is default (None means default, or if present, should be default space)
pub fn is_space_default(space: &Option<Space>) -> bool {
    match space {
        None => true,                     // None means default
        Some(s) => *s == default_space(), // If present, check if it's the default space
    }
}

/// Check if HNSW config is default
pub fn is_hnsw_config_default(hnsw_config: &HnswIndexConfig) -> bool {
    hnsw_config.ef_construction == Some(default_construction_ef())
        && hnsw_config.ef_search == Some(default_search_ef())
        && hnsw_config.max_neighbors == Some(default_m())
        && hnsw_config.num_threads == Some(default_num_threads())
        && hnsw_config.batch_size == Some(default_batch_size())
        && hnsw_config.sync_threshold == Some(default_sync_threshold())
        && hnsw_config.resize_factor == Some(default_resize_factor())
}

// ============================================================================
// NEW STRONGLY-TYPED SCHEMA STRUCTURES
// ============================================================================

/// Strongly-typed value type configurations
/// Contains optional configurations for each supported value type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ValueTypes {
    #[serde(
        rename = "string",
        alias = "#string",
        skip_serializing_if = "Option::is_none"
    )] // STRING_VALUE_NAME
    pub string: Option<StringValueType>,

    #[serde(
        rename = "float_list",
        alias = "#float_list",
        skip_serializing_if = "Option::is_none"
    )]
    // FLOAT_LIST_VALUE_NAME
    pub float_list: Option<FloatListValueType>,

    #[serde(
        rename = "sparse_vector",
        alias = "#sparse_vector",
        skip_serializing_if = "Option::is_none"
    )]
    // SPARSE_VECTOR_VALUE_NAME
    pub sparse_vector: Option<SparseVectorValueType>,

    #[serde(
        rename = "int",
        alias = "#int",
        skip_serializing_if = "Option::is_none"
    )] // INT_VALUE_NAME
    pub int: Option<IntValueType>,

    #[serde(
        rename = "float",
        alias = "#float",
        skip_serializing_if = "Option::is_none"
    )] // FLOAT_VALUE_NAME
    pub float: Option<FloatValueType>,

    #[serde(
        rename = "bool",
        alias = "#bool",
        skip_serializing_if = "Option::is_none"
    )] // BOOL_VALUE_NAME
    pub boolean: Option<BoolValueType>,
}

/// String value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct StringValueType {
    #[serde(
        rename = "fts_index",
        alias = "$fts_index",
        skip_serializing_if = "Option::is_none"
    )] // FTS_INDEX_NAME
    pub fts_index: Option<FtsIndexType>,

    #[serde(
        rename = "string_inverted_index", // STRING_INVERTED_INDEX_NAME
        alias = "$string_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub string_inverted_index: Option<StringInvertedIndexType>,
}

/// Float list value type index configurations (for vectors)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FloatListValueType {
    #[serde(
        rename = "vector_index",
        alias = "$vector_index",
        skip_serializing_if = "Option::is_none"
    )] // VECTOR_INDEX_NAME
    pub vector_index: Option<VectorIndexType>,
}

/// Sparse vector value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SparseVectorValueType {
    #[serde(
        rename = "sparse_vector_index", // SPARSE_VECTOR_INDEX_NAME
        alias = "$sparse_vector_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub sparse_vector_index: Option<SparseVectorIndexType>,
}

/// Integer value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct IntValueType {
    #[serde(
        rename = "int_inverted_index",
        alias = "$int_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    // INT_INVERTED_INDEX_NAME
    pub int_inverted_index: Option<IntInvertedIndexType>,
}

/// Float value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FloatValueType {
    #[serde(
        rename = "float_inverted_index", // FLOAT_INVERTED_INDEX_NAME
        alias = "$float_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub float_inverted_index: Option<FloatInvertedIndexType>,
}

/// Boolean value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BoolValueType {
    #[serde(
        rename = "bool_inverted_index", // BOOL_INVERTED_INDEX_NAME
        alias = "$bool_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub bool_inverted_index: Option<BoolInvertedIndexType>,
}

// Individual index type structs with enabled status and config
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FtsIndexType {
    pub enabled: bool,
    pub config: FtsIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct VectorIndexType {
    pub enabled: bool,
    pub config: VectorIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SparseVectorIndexType {
    pub enabled: bool,
    pub config: SparseVectorIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct StringInvertedIndexType {
    pub enabled: bool,
    pub config: StringInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct IntInvertedIndexType {
    pub enabled: bool,
    pub config: IntInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FloatInvertedIndexType {
    pub enabled: bool,
    pub config: FloatInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct BoolInvertedIndexType {
    pub enabled: bool,
    pub config: BoolInvertedIndexConfig,
}

impl Schema {
    /// Create a new Schema with strongly-typed default configurations
    pub fn new_default(default_knn_index: KnnIndex) -> Self {
        // Vector index disabled on all keys except #embedding.
        let vector_config = VectorIndexType {
            enabled: false,
            config: VectorIndexConfig {
                space: Some(default_space()),
                embedding_function: None,
                source_key: None,
                hnsw: match default_knn_index {
                    KnnIndex::Hnsw => Some(HnswIndexConfig {
                        ef_construction: Some(default_construction_ef()),
                        max_neighbors: Some(default_m()),
                        ef_search: Some(default_search_ef()),
                        num_threads: Some(default_num_threads()),
                        batch_size: Some(default_batch_size()),
                        sync_threshold: Some(default_sync_threshold()),
                        resize_factor: Some(default_resize_factor()),
                    }),
                    KnnIndex::Spann => None,
                },
                spann: match default_knn_index {
                    KnnIndex::Hnsw => None,
                    KnnIndex::Spann => Some(SpannIndexConfig {
                        search_nprobe: Some(default_search_nprobe()),
                        search_rng_factor: Some(default_search_rng_factor()),
                        search_rng_epsilon: Some(default_search_rng_epsilon()),
                        nreplica_count: Some(default_nreplica_count()),
                        write_rng_factor: Some(default_write_rng_factor()),
                        write_rng_epsilon: Some(default_write_rng_epsilon()),
                        split_threshold: Some(default_split_threshold()),
                        num_samples_kmeans: Some(default_num_samples_kmeans()),
                        initial_lambda: Some(default_initial_lambda()),
                        reassign_neighbor_count: Some(default_reassign_neighbor_count()),
                        merge_threshold: Some(default_merge_threshold()),
                        num_centers_to_merge_to: Some(default_num_centers_to_merge_to()),
                        write_nprobe: Some(default_write_nprobe()),
                        ef_construction: Some(default_construction_ef_spann()),
                        ef_search: Some(default_search_ef_spann()),
                        max_neighbors: Some(default_m_spann()),
                    }),
                },
            },
        };

        // Initialize defaults struct directly instead of using Default::default() + field assignments
        let defaults = ValueTypes {
            string: Some(StringValueType {
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: true,
                    config: StringInvertedIndexConfig {},
                }),
                fts_index: Some(FtsIndexType {
                    enabled: false,
                    config: FtsIndexConfig {},
                }),
            }),
            float: Some(FloatValueType {
                float_inverted_index: Some(FloatInvertedIndexType {
                    enabled: true,
                    config: FloatInvertedIndexConfig {},
                }),
            }),
            int: Some(IntValueType {
                int_inverted_index: Some(IntInvertedIndexType {
                    enabled: true,
                    config: IntInvertedIndexConfig {},
                }),
            }),
            boolean: Some(BoolValueType {
                bool_inverted_index: Some(BoolInvertedIndexType {
                    enabled: true,
                    config: BoolInvertedIndexConfig {},
                }),
            }),
            float_list: Some(FloatListValueType {
                vector_index: Some(vector_config),
            }),
            sparse_vector: Some(SparseVectorValueType {
                sparse_vector_index: Some(SparseVectorIndexType {
                    enabled: false,
                    config: SparseVectorIndexConfig {
                        embedding_function: Some(EmbeddingFunctionConfiguration::Unknown),
                        source_key: None,
                        bm25: Some(false),
                    },
                }),
            }),
        };

        // Set up key overrides
        let mut keys = HashMap::new();

        // Enable vector index for #embedding.
        let embedding_defaults = ValueTypes {
            float_list: Some(FloatListValueType {
                vector_index: Some(VectorIndexType {
                    enabled: true,
                    config: VectorIndexConfig {
                        space: Some(default_space()),
                        embedding_function: None,
                        source_key: Some(DOCUMENT_KEY.to_string()),
                        hnsw: match default_knn_index {
                            KnnIndex::Hnsw => Some(HnswIndexConfig {
                                ef_construction: Some(default_construction_ef()),
                                max_neighbors: Some(default_m()),
                                ef_search: Some(default_search_ef()),
                                num_threads: Some(default_num_threads()),
                                batch_size: Some(default_batch_size()),
                                sync_threshold: Some(default_sync_threshold()),
                                resize_factor: Some(default_resize_factor()),
                            }),
                            KnnIndex::Spann => None,
                        },
                        spann: match default_knn_index {
                            KnnIndex::Hnsw => None,
                            KnnIndex::Spann => Some(SpannIndexConfig {
                                search_nprobe: Some(default_search_nprobe()),
                                search_rng_factor: Some(default_search_rng_factor()),
                                search_rng_epsilon: Some(default_search_rng_epsilon()),
                                nreplica_count: Some(default_nreplica_count()),
                                write_rng_factor: Some(default_write_rng_factor()),
                                write_rng_epsilon: Some(default_write_rng_epsilon()),
                                split_threshold: Some(default_split_threshold()),
                                num_samples_kmeans: Some(default_num_samples_kmeans()),
                                initial_lambda: Some(default_initial_lambda()),
                                reassign_neighbor_count: Some(default_reassign_neighbor_count()),
                                merge_threshold: Some(default_merge_threshold()),
                                num_centers_to_merge_to: Some(default_num_centers_to_merge_to()),
                                write_nprobe: Some(default_write_nprobe()),
                                ef_construction: Some(default_construction_ef_spann()),
                                ef_search: Some(default_search_ef_spann()),
                                max_neighbors: Some(default_m_spann()),
                            }),
                        },
                    },
                }),
            }),
            ..Default::default()
        };
        keys.insert(EMBEDDING_KEY.to_string(), embedding_defaults);

        // Document defaults - initialize directly instead of Default::default() + field assignment
        let document_defaults = ValueTypes {
            string: Some(StringValueType {
                fts_index: Some(FtsIndexType {
                    enabled: true,
                    config: FtsIndexConfig {},
                }),
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: false,
                    config: StringInvertedIndexConfig {},
                }),
            }),
            ..Default::default()
        };
        keys.insert(DOCUMENT_KEY.to_string(), document_defaults);

        Schema { defaults, keys }
    }

    pub fn get_internal_spann_config(&self) -> Option<InternalSpannConfiguration> {
        let to_internal = |vector_index: &VectorIndexType| {
            let space = vector_index.config.space.clone();
            vector_index
                .config
                .spann
                .clone()
                .map(|config| (space.as_ref(), &config).into())
        };

        self.keys
            .get(EMBEDDING_KEY)
            .and_then(|value_types| value_types.float_list.as_ref())
            .and_then(|float_list| float_list.vector_index.as_ref())
            .and_then(to_internal)
            .or_else(|| {
                self.defaults
                    .float_list
                    .as_ref()
                    .and_then(|float_list| float_list.vector_index.as_ref())
                    .and_then(to_internal)
            })
    }

    pub fn get_internal_hnsw_config(&self) -> Option<InternalHnswConfiguration> {
        let to_internal = |vector_index: &VectorIndexType| {
            if vector_index.config.spann.is_some() {
                return None;
            }
            let space = vector_index.config.space.as_ref();
            let hnsw_config = vector_index.config.hnsw.as_ref();
            Some((space, hnsw_config).into())
        };

        self.keys
            .get(EMBEDDING_KEY)
            .and_then(|value_types| value_types.float_list.as_ref())
            .and_then(|float_list| float_list.vector_index.as_ref())
            .and_then(to_internal)
            .or_else(|| {
                self.defaults
                    .float_list
                    .as_ref()
                    .and_then(|float_list| float_list.vector_index.as_ref())
                    .and_then(to_internal)
            })
    }

    pub fn get_internal_hnsw_config_with_legacy_fallback(
        &self,
        segment: &Segment,
    ) -> Result<Option<InternalHnswConfiguration>, HnswParametersFromSegmentError> {
        if let Some(config) = self.get_internal_hnsw_config() {
            let config_from_metadata =
                InternalHnswConfiguration::from_legacy_segment_metadata(&segment.metadata)?;

            if config == InternalHnswConfiguration::default() && config != config_from_metadata {
                return Ok(Some(config_from_metadata));
            }

            return Ok(Some(config));
        }

        Ok(None)
    }

    /// Reconcile user-provided schema with system defaults
    ///
    /// This method merges user configurations with system defaults, ensuring that:
    /// - User overrides take precedence over defaults
    /// - Missing user configurations fall back to system defaults
    /// - Field-level merging for complex configurations (Vector, HNSW, SPANN, etc.)
    pub fn reconcile_with_defaults(
        user_schema: Option<&Schema>,
        knn_index: KnnIndex,
    ) -> Result<Self, SchemaError> {
        let default_schema = Schema::new_default(knn_index);

        match user_schema {
            Some(user) => {
                // Merge defaults with user overrides
                let merged_defaults =
                    Self::merge_value_types(&default_schema.defaults, &user.defaults, knn_index)?;

                // Merge key overrides
                let mut merged_keys = default_schema.keys.clone();
                for (key, user_value_types) in &user.keys {
                    if let Some(default_value_types) = merged_keys.get(key) {
                        // Merge with existing default key override
                        let merged_value_types = Self::merge_value_types(
                            default_value_types,
                            user_value_types,
                            knn_index,
                        )?;
                        merged_keys.insert(key.clone(), merged_value_types);
                    } else {
                        // New key override from user
                        merged_keys.insert(key.clone(), user_value_types.clone());
                    }
                }

                Ok(Schema {
                    defaults: merged_defaults,
                    keys: merged_keys,
                })
            }
            None => Ok(default_schema),
        }
    }

    /// Merge two schemas together, combining key overrides when possible.
    pub fn merge(&self, other: &Schema) -> Result<Schema, SchemaError> {
        if self.defaults != other.defaults {
            return Err(SchemaError::DefaultsMismatch);
        }

        let mut keys = self.keys.clone();

        for (key, other_value_types) in &other.keys {
            if let Some(existing) = keys.get(key).cloned() {
                let merged = Self::merge_override_value_types(key, &existing, other_value_types)?;
                keys.insert(key.clone(), merged);
            } else {
                keys.insert(key.clone(), other_value_types.clone());
            }
        }

        Ok(Schema {
            defaults: self.defaults.clone(),
            keys,
        })
    }

    fn merge_override_value_types(
        key: &str,
        left: &ValueTypes,
        right: &ValueTypes,
    ) -> Result<ValueTypes, SchemaError> {
        Ok(ValueTypes {
            string: Self::merge_string_override(key, left.string.as_ref(), right.string.as_ref())?,
            float: Self::merge_float_override(key, left.float.as_ref(), right.float.as_ref())?,
            int: Self::merge_int_override(key, left.int.as_ref(), right.int.as_ref())?,
            boolean: Self::merge_bool_override(key, left.boolean.as_ref(), right.boolean.as_ref())?,
            float_list: Self::merge_float_list_override(
                key,
                left.float_list.as_ref(),
                right.float_list.as_ref(),
            )?,
            sparse_vector: Self::merge_sparse_vector_override(
                key,
                left.sparse_vector.as_ref(),
                right.sparse_vector.as_ref(),
            )?,
        })
    }

    fn merge_string_override(
        key: &str,
        left: Option<&StringValueType>,
        right: Option<&StringValueType>,
    ) -> Result<Option<StringValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(StringValueType {
                string_inverted_index: Self::merge_index_or_error(
                    l.string_inverted_index.as_ref(),
                    r.string_inverted_index.as_ref(),
                    &format!("key '{key}' string.string_inverted_index"),
                )?,
                fts_index: Self::merge_index_or_error(
                    l.fts_index.as_ref(),
                    r.fts_index.as_ref(),
                    &format!("key '{key}' string.fts_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_float_override(
        key: &str,
        left: Option<&FloatValueType>,
        right: Option<&FloatValueType>,
    ) -> Result<Option<FloatValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(FloatValueType {
                float_inverted_index: Self::merge_index_or_error(
                    l.float_inverted_index.as_ref(),
                    r.float_inverted_index.as_ref(),
                    &format!("key '{key}' float.float_inverted_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_int_override(
        key: &str,
        left: Option<&IntValueType>,
        right: Option<&IntValueType>,
    ) -> Result<Option<IntValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(IntValueType {
                int_inverted_index: Self::merge_index_or_error(
                    l.int_inverted_index.as_ref(),
                    r.int_inverted_index.as_ref(),
                    &format!("key '{key}' int.int_inverted_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_bool_override(
        key: &str,
        left: Option<&BoolValueType>,
        right: Option<&BoolValueType>,
    ) -> Result<Option<BoolValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(BoolValueType {
                bool_inverted_index: Self::merge_index_or_error(
                    l.bool_inverted_index.as_ref(),
                    r.bool_inverted_index.as_ref(),
                    &format!("key '{key}' bool.bool_inverted_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_float_list_override(
        key: &str,
        left: Option<&FloatListValueType>,
        right: Option<&FloatListValueType>,
    ) -> Result<Option<FloatListValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(FloatListValueType {
                vector_index: Self::merge_index_or_error(
                    l.vector_index.as_ref(),
                    r.vector_index.as_ref(),
                    &format!("key '{key}' float_list.vector_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_sparse_vector_override(
        key: &str,
        left: Option<&SparseVectorValueType>,
        right: Option<&SparseVectorValueType>,
    ) -> Result<Option<SparseVectorValueType>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => Ok(Some(SparseVectorValueType {
                sparse_vector_index: Self::merge_index_or_error(
                    l.sparse_vector_index.as_ref(),
                    r.sparse_vector_index.as_ref(),
                    &format!("key '{key}' sparse_vector.sparse_vector_index"),
                )?,
            })),
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_index_or_error<T: Clone + PartialEq>(
        left: Option<&T>,
        right: Option<&T>,
        context: &str,
    ) -> Result<Option<T>, SchemaError> {
        match (left, right) {
            (Some(l), Some(r)) => {
                if l == r {
                    Ok(Some(l.clone()))
                } else {
                    Err(SchemaError::ConfigurationConflict {
                        context: context.to_string(),
                    })
                }
            }
            (Some(l), None) => Ok(Some(l.clone())),
            (None, Some(r)) => Ok(Some(r.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge two ValueTypes with field-level merging
    /// User values take precedence over default values
    fn merge_value_types(
        default: &ValueTypes,
        user: &ValueTypes,
        knn_index: KnnIndex,
    ) -> Result<ValueTypes, SchemaError> {
        // Merge float_list first
        let float_list = Self::merge_float_list_type(
            default.float_list.as_ref(),
            user.float_list.as_ref(),
            knn_index,
        );

        // Validate the merged float_list (covers all merge cases)
        if let Some(ref fl) = float_list {
            Self::validate_float_list_value_type(fl)?;
        }

        Ok(ValueTypes {
            string: Self::merge_string_type(default.string.as_ref(), user.string.as_ref())?,
            float: Self::merge_float_type(default.float.as_ref(), user.float.as_ref())?,
            int: Self::merge_int_type(default.int.as_ref(), user.int.as_ref())?,
            boolean: Self::merge_bool_type(default.boolean.as_ref(), user.boolean.as_ref())?,
            float_list,
            sparse_vector: Self::merge_sparse_vector_type(
                default.sparse_vector.as_ref(),
                user.sparse_vector.as_ref(),
            )?,
        })
    }

    /// Merge StringValueType configurations
    fn merge_string_type(
        default: Option<&StringValueType>,
        user: Option<&StringValueType>,
    ) -> Result<Option<StringValueType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(StringValueType {
                string_inverted_index: Self::merge_string_inverted_index_type(
                    default.string_inverted_index.as_ref(),
                    user.string_inverted_index.as_ref(),
                )?,
                fts_index: Self::merge_fts_index_type(
                    default.fts_index.as_ref(),
                    user.fts_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge FloatValueType configurations
    fn merge_float_type(
        default: Option<&FloatValueType>,
        user: Option<&FloatValueType>,
    ) -> Result<Option<FloatValueType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(FloatValueType {
                float_inverted_index: Self::merge_float_inverted_index_type(
                    default.float_inverted_index.as_ref(),
                    user.float_inverted_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge IntValueType configurations
    fn merge_int_type(
        default: Option<&IntValueType>,
        user: Option<&IntValueType>,
    ) -> Result<Option<IntValueType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(IntValueType {
                int_inverted_index: Self::merge_int_inverted_index_type(
                    default.int_inverted_index.as_ref(),
                    user.int_inverted_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge BoolValueType configurations
    fn merge_bool_type(
        default: Option<&BoolValueType>,
        user: Option<&BoolValueType>,
    ) -> Result<Option<BoolValueType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(BoolValueType {
                bool_inverted_index: Self::merge_bool_inverted_index_type(
                    default.bool_inverted_index.as_ref(),
                    user.bool_inverted_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge FloatListValueType configurations
    fn merge_float_list_type(
        default: Option<&FloatListValueType>,
        user: Option<&FloatListValueType>,
        knn_index: KnnIndex,
    ) -> Option<FloatListValueType> {
        match (default, user) {
            (Some(default), Some(user)) => Some(FloatListValueType {
                vector_index: Self::merge_vector_index_type(
                    default.vector_index.as_ref(),
                    user.vector_index.as_ref(),
                    knn_index,
                ),
            }),
            (Some(default), None) => Some(default.clone()),
            (None, Some(user)) => Some(user.clone()),
            (None, None) => None,
        }
    }

    /// Merge SparseVectorValueType configurations
    fn merge_sparse_vector_type(
        default: Option<&SparseVectorValueType>,
        user: Option<&SparseVectorValueType>,
    ) -> Result<Option<SparseVectorValueType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(SparseVectorValueType {
                sparse_vector_index: Self::merge_sparse_vector_index_type(
                    default.sparse_vector_index.as_ref(),
                    user.sparse_vector_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge individual index type configurations
    fn merge_string_inverted_index_type(
        default: Option<&StringInvertedIndexType>,
        user: Option<&StringInvertedIndexType>,
    ) -> Result<Option<StringInvertedIndexType>, SchemaError> {
        match (default, user) {
            (Some(_default), Some(user)) => {
                Ok(Some(StringInvertedIndexType {
                    enabled: user.enabled,       // User enabled state takes precedence
                    config: user.config.clone(), // User config takes precedence
                }))
            }
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_fts_index_type(
        default: Option<&FtsIndexType>,
        user: Option<&FtsIndexType>,
    ) -> Result<Option<FtsIndexType>, SchemaError> {
        match (default, user) {
            (Some(_default), Some(user)) => Ok(Some(FtsIndexType {
                enabled: user.enabled,
                config: user.config.clone(),
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_float_inverted_index_type(
        default: Option<&FloatInvertedIndexType>,
        user: Option<&FloatInvertedIndexType>,
    ) -> Result<Option<FloatInvertedIndexType>, SchemaError> {
        match (default, user) {
            (Some(_default), Some(user)) => Ok(Some(FloatInvertedIndexType {
                enabled: user.enabled,
                config: user.config.clone(),
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_int_inverted_index_type(
        default: Option<&IntInvertedIndexType>,
        user: Option<&IntInvertedIndexType>,
    ) -> Result<Option<IntInvertedIndexType>, SchemaError> {
        match (default, user) {
            (Some(_default), Some(user)) => Ok(Some(IntInvertedIndexType {
                enabled: user.enabled,
                config: user.config.clone(),
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_bool_inverted_index_type(
        default: Option<&BoolInvertedIndexType>,
        user: Option<&BoolInvertedIndexType>,
    ) -> Result<Option<BoolInvertedIndexType>, SchemaError> {
        match (default, user) {
            (Some(_default), Some(user)) => Ok(Some(BoolInvertedIndexType {
                enabled: user.enabled,
                config: user.config.clone(),
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_vector_index_type(
        default: Option<&VectorIndexType>,
        user: Option<&VectorIndexType>,
        knn_index: KnnIndex,
    ) -> Option<VectorIndexType> {
        match (default, user) {
            (Some(default), Some(user)) => Some(VectorIndexType {
                enabled: user.enabled,
                config: Self::merge_vector_index_config(&default.config, &user.config, knn_index),
            }),
            (Some(default), None) => Some(default.clone()),
            (None, Some(user)) => Some(user.clone()),
            (None, None) => None,
        }
    }

    fn merge_sparse_vector_index_type(
        default: Option<&SparseVectorIndexType>,
        user: Option<&SparseVectorIndexType>,
    ) -> Result<Option<SparseVectorIndexType>, SchemaError> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(SparseVectorIndexType {
                enabled: user.enabled,
                config: Self::merge_sparse_vector_index_config(&default.config, &user.config),
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Validate FloatListValueType vector index configurations
    /// This validates HNSW and SPANN configs within the merged float_list
    fn validate_float_list_value_type(float_list: &FloatListValueType) -> Result<(), SchemaError> {
        if let Some(vector_index) = &float_list.vector_index {
            if let Some(hnsw) = &vector_index.config.hnsw {
                hnsw.validate().map_err(SchemaError::InvalidHnswConfig)?;
            }
            if let Some(spann) = &vector_index.config.spann {
                spann.validate().map_err(SchemaError::InvalidSpannConfig)?;
            }
        }
        Ok(())
    }

    /// Merge VectorIndexConfig with field-level merging
    fn merge_vector_index_config(
        default: &VectorIndexConfig,
        user: &VectorIndexConfig,
        knn_index: KnnIndex,
    ) -> VectorIndexConfig {
        match knn_index {
            KnnIndex::Hnsw => VectorIndexConfig {
                space: user.space.clone().or(default.space.clone()),
                embedding_function: user
                    .embedding_function
                    .clone()
                    .or(default.embedding_function.clone()),
                source_key: user.source_key.clone().or(default.source_key.clone()),
                hnsw: Self::merge_hnsw_configs(default.hnsw.as_ref(), user.hnsw.as_ref()),
                spann: None,
            },
            KnnIndex::Spann => VectorIndexConfig {
                space: user.space.clone().or(default.space.clone()),
                embedding_function: user
                    .embedding_function
                    .clone()
                    .or(default.embedding_function.clone()),
                source_key: user.source_key.clone().or(default.source_key.clone()),
                hnsw: None,
                spann: Self::merge_spann_configs(default.spann.as_ref(), user.spann.as_ref()),
            },
        }
    }

    /// Merge SparseVectorIndexConfig with field-level merging
    fn merge_sparse_vector_index_config(
        default: &SparseVectorIndexConfig,
        user: &SparseVectorIndexConfig,
    ) -> SparseVectorIndexConfig {
        SparseVectorIndexConfig {
            embedding_function: user
                .embedding_function
                .clone()
                .or(default.embedding_function.clone()),
            source_key: user.source_key.clone().or(default.source_key.clone()),
            bm25: user.bm25.or(default.bm25),
        }
    }

    /// Merge HNSW configurations with field-level merging
    fn merge_hnsw_configs(
        default_hnsw: Option<&HnswIndexConfig>,
        user_hnsw: Option<&HnswIndexConfig>,
    ) -> Option<HnswIndexConfig> {
        match (default_hnsw, user_hnsw) {
            (Some(default), Some(user)) => Some(HnswIndexConfig {
                ef_construction: user.ef_construction.or(default.ef_construction),
                max_neighbors: user.max_neighbors.or(default.max_neighbors),
                ef_search: user.ef_search.or(default.ef_search),
                num_threads: user.num_threads.or(default.num_threads),
                batch_size: user.batch_size.or(default.batch_size),
                sync_threshold: user.sync_threshold.or(default.sync_threshold),
                resize_factor: user.resize_factor.or(default.resize_factor),
            }),
            (Some(default), None) => Some(default.clone()),
            (None, Some(user)) => Some(user.clone()),
            (None, None) => None,
        }
    }

    /// Merge SPANN configurations with field-level merging
    fn merge_spann_configs(
        default_spann: Option<&SpannIndexConfig>,
        user_spann: Option<&SpannIndexConfig>,
    ) -> Option<SpannIndexConfig> {
        match (default_spann, user_spann) {
            (Some(default), Some(user)) => Some(SpannIndexConfig {
                search_nprobe: user.search_nprobe.or(default.search_nprobe),
                search_rng_factor: user.search_rng_factor.or(default.search_rng_factor),
                search_rng_epsilon: user.search_rng_epsilon.or(default.search_rng_epsilon),
                nreplica_count: user.nreplica_count.or(default.nreplica_count),
                write_rng_factor: user.write_rng_factor.or(default.write_rng_factor),
                write_rng_epsilon: user.write_rng_epsilon.or(default.write_rng_epsilon),
                split_threshold: user.split_threshold.or(default.split_threshold),
                num_samples_kmeans: user.num_samples_kmeans.or(default.num_samples_kmeans),
                initial_lambda: user.initial_lambda.or(default.initial_lambda),
                reassign_neighbor_count: user
                    .reassign_neighbor_count
                    .or(default.reassign_neighbor_count),
                merge_threshold: user.merge_threshold.or(default.merge_threshold),
                num_centers_to_merge_to: user
                    .num_centers_to_merge_to
                    .or(default.num_centers_to_merge_to),
                write_nprobe: user.write_nprobe.or(default.write_nprobe),
                ef_construction: user.ef_construction.or(default.ef_construction),
                ef_search: user.ef_search.or(default.ef_search),
                max_neighbors: user.max_neighbors.or(default.max_neighbors),
            }),
            (Some(default), None) => Some(default.clone()),
            (None, Some(user)) => Some(user.clone()),
            (None, None) => None,
        }
    }

    /// Reconcile Schema with InternalCollectionConfiguration
    ///
    /// Simple reconciliation logic:
    /// 1. If collection config is default → return schema (schema is source of truth)
    /// 2. If collection config is non-default and schema is default → override schema with collection config
    ///
    /// Note: The case where both are non-default is validated earlier in reconcile_schema_and_config
    pub fn reconcile_with_collection_config(
        schema: &Schema,
        collection_config: &InternalCollectionConfiguration,
        default_knn_index: KnnIndex,
    ) -> Result<Schema, SchemaError> {
        // 1. Check if collection config is default
        if collection_config.is_default() {
            if schema.is_default() {
                // if both are default, use the schema, and apply the ef from config if available
                // for both defaults and #embedding key
                let mut new_schema = Schema::new_default(default_knn_index);

                if collection_config.embedding_function.is_some() {
                    if let Some(float_list) = &mut new_schema.defaults.float_list {
                        if let Some(vector_index) = &mut float_list.vector_index {
                            vector_index.config.embedding_function =
                                collection_config.embedding_function.clone();
                        }
                    }
                    if let Some(embedding_types) = new_schema.keys.get_mut(EMBEDDING_KEY) {
                        if let Some(float_list) = &mut embedding_types.float_list {
                            if let Some(vector_index) = &mut float_list.vector_index {
                                vector_index.config.embedding_function =
                                    collection_config.embedding_function.clone();
                            }
                        }
                    }
                }
                return Ok(new_schema);
            } else {
                // Collection config is default and schema is non-default → schema is source of truth
                return Ok(schema.clone());
            }
        }

        // 2. Collection config is non-default, schema must be default (already validated earlier)
        // Convert collection config to schema
        Self::try_from(collection_config)
    }

    pub fn reconcile_schema_and_config(
        schema: Option<&Schema>,
        configuration: Option<&InternalCollectionConfiguration>,
        knn_index: KnnIndex,
    ) -> Result<Schema, SchemaError> {
        // Early validation: check if both user-provided schema and config are non-default
        if let (Some(user_schema), Some(config)) = (schema, configuration) {
            if !user_schema.is_default() && !config.is_default() {
                return Err(SchemaError::ConfigAndSchemaConflict);
            }
        }

        let reconciled_schema = Self::reconcile_with_defaults(schema, knn_index)?;
        if let Some(config) = configuration {
            Self::reconcile_with_collection_config(&reconciled_schema, config, knn_index)
        } else {
            Ok(reconciled_schema)
        }
    }

    pub fn default_with_embedding_function(
        embedding_function: EmbeddingFunctionConfiguration,
    ) -> Schema {
        let mut schema = Schema::new_default(KnnIndex::Spann);
        if let Some(float_list) = &mut schema.defaults.float_list {
            if let Some(vector_index) = &mut float_list.vector_index {
                vector_index.config.embedding_function = Some(embedding_function.clone());
            }
        }
        if let Some(embedding_types) = schema.keys.get_mut(EMBEDDING_KEY) {
            if let Some(float_list) = &mut embedding_types.float_list {
                if let Some(vector_index) = &mut float_list.vector_index {
                    vector_index.config.embedding_function = Some(embedding_function);
                }
            }
        }
        schema
    }

    /// Check if schema is default by checking each field individually
    pub fn is_default(&self) -> bool {
        // Check if defaults are default (field by field)
        if !Self::is_value_types_default(&self.defaults) {
            return false;
        }

        for key in self.keys.keys() {
            if key != EMBEDDING_KEY && key != DOCUMENT_KEY {
                return false;
            }
        }

        // Check #embedding key
        if let Some(embedding_value) = self.keys.get(EMBEDDING_KEY) {
            if !Self::is_embedding_value_types_default(embedding_value) {
                return false;
            }
        }

        // Check #document key
        if let Some(document_value) = self.keys.get(DOCUMENT_KEY) {
            if !Self::is_document_value_types_default(document_value) {
                return false;
            }
        }

        true
    }

    /// Check if ValueTypes (defaults) are in default state
    fn is_value_types_default(value_types: &ValueTypes) -> bool {
        // Check string field
        if let Some(string) = &value_types.string {
            if let Some(string_inverted) = &string.string_inverted_index {
                if !string_inverted.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
            if let Some(fts) = &string.fts_index {
                if fts.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
        }

        // Check float field
        if let Some(float) = &value_types.float {
            if let Some(float_inverted) = &float.float_inverted_index {
                if !float_inverted.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
        }

        // Check int field
        if let Some(int) = &value_types.int {
            if let Some(int_inverted) = &int.int_inverted_index {
                if !int_inverted.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
        }

        // Check boolean field
        if let Some(boolean) = &value_types.boolean {
            if let Some(bool_inverted) = &boolean.bool_inverted_index {
                if !bool_inverted.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
        }

        // Check float_list field (vector index should be disabled)
        if let Some(float_list) = &value_types.float_list {
            if let Some(vector_index) = &float_list.vector_index {
                if vector_index.enabled {
                    return false;
                }
                if !is_embedding_function_default(&vector_index.config.embedding_function) {
                    return false;
                }
                if !is_space_default(&vector_index.config.space) {
                    return false;
                }
                // Check that the config has default structure
                if vector_index.config.source_key.is_some() {
                    return false;
                }
                // Check that either hnsw or spann config is present (not both, not neither)
                // and that the config values are default
                match (&vector_index.config.hnsw, &vector_index.config.spann) {
                    (Some(hnsw_config), None) => {
                        if !hnsw_config.is_default() {
                            return false;
                        }
                    }
                    (None, Some(spann_config)) => {
                        if !spann_config.is_default() {
                            return false;
                        }
                    }
                    (Some(_), Some(_)) => return false, // Both present
                    (None, None) => {}
                }
            }
        }

        // Check sparse_vector field (should be disabled)
        if let Some(sparse_vector) = &value_types.sparse_vector {
            if let Some(sparse_index) = &sparse_vector.sparse_vector_index {
                if sparse_index.enabled {
                    return false;
                }
                // Check config structure
                if !is_embedding_function_default(&sparse_index.config.embedding_function) {
                    return false;
                }
                if sparse_index.config.source_key.is_some() {
                    return false;
                }
                if let Some(bm25) = &sparse_index.config.bm25 {
                    if bm25 != &false {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Check if ValueTypes for #embedding key are in default state
    fn is_embedding_value_types_default(value_types: &ValueTypes) -> bool {
        // For #embedding, only float_list should be set
        if value_types.string.is_some()
            || value_types.float.is_some()
            || value_types.int.is_some()
            || value_types.boolean.is_some()
            || value_types.sparse_vector.is_some()
        {
            return false;
        }

        // Check float_list field (vector index should be enabled)
        if let Some(float_list) = &value_types.float_list {
            if let Some(vector_index) = &float_list.vector_index {
                if !vector_index.enabled {
                    return false;
                }
                if !is_space_default(&vector_index.config.space) {
                    return false;
                }
                // Check that embedding_function is default
                if !is_embedding_function_default(&vector_index.config.embedding_function) {
                    return false;
                }
                // Check that source_key is #document
                if vector_index.config.source_key.as_deref() != Some(DOCUMENT_KEY) {
                    return false;
                }
                // Check that either hnsw or spann config is present (not both, not neither)
                // and that the config values are default
                match (&vector_index.config.hnsw, &vector_index.config.spann) {
                    (Some(hnsw_config), None) => {
                        if !hnsw_config.is_default() {
                            return false;
                        }
                    }
                    (None, Some(spann_config)) => {
                        if !spann_config.is_default() {
                            return false;
                        }
                    }
                    (Some(_), Some(_)) => return false, // Both present
                    (None, None) => {}
                }
            }
        }

        true
    }

    /// Check if ValueTypes for #document key are in default state
    fn is_document_value_types_default(value_types: &ValueTypes) -> bool {
        // For #document, only string should be set
        if value_types.float_list.is_some()
            || value_types.float.is_some()
            || value_types.int.is_some()
            || value_types.boolean.is_some()
            || value_types.sparse_vector.is_some()
        {
            return false;
        }

        // Check string field
        if let Some(string) = &value_types.string {
            if let Some(fts) = &string.fts_index {
                if !fts.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
            if let Some(string_inverted) = &string.string_inverted_index {
                if string_inverted.enabled {
                    return false;
                }
                // Config is an empty struct, so no need to check it
            }
        }

        true
    }

    /// Check if a specific metadata key-value should be indexed based on schema configuration
    pub fn is_metadata_type_index_enabled(
        &self,
        key: &str,
        value_type: MetadataValueType,
    ) -> Result<bool, SchemaError> {
        let v_type = self.keys.get(key).unwrap_or(&self.defaults);

        match value_type {
            MetadataValueType::Bool => match &v_type.boolean {
                Some(bool_type) => match &bool_type.bool_inverted_index {
                    Some(bool_inverted_index) => Ok(bool_inverted_index.enabled),
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "bool".to_string(),
                    }),
                },
                None => match &self.defaults.boolean {
                    Some(bool_type) => match &bool_type.bool_inverted_index {
                        Some(bool_inverted_index) => Ok(bool_inverted_index.enabled),
                        None => Err(SchemaError::MissingIndexConfiguration {
                            key: key.to_string(),
                            value_type: "bool".to_string(),
                        }),
                    },
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "bool".to_string(),
                    }),
                },
            },
            MetadataValueType::Int => match &v_type.int {
                Some(int_type) => match &int_type.int_inverted_index {
                    Some(int_inverted_index) => Ok(int_inverted_index.enabled),
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "int".to_string(),
                    }),
                },
                None => match &self.defaults.int {
                    Some(int_type) => match &int_type.int_inverted_index {
                        Some(int_inverted_index) => Ok(int_inverted_index.enabled),
                        None => Err(SchemaError::MissingIndexConfiguration {
                            key: key.to_string(),
                            value_type: "int".to_string(),
                        }),
                    },
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "int".to_string(),
                    }),
                },
            },
            MetadataValueType::Float => match &v_type.float {
                Some(float_type) => match &float_type.float_inverted_index {
                    Some(float_inverted_index) => Ok(float_inverted_index.enabled),
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "float".to_string(),
                    }),
                },
                None => match &self.defaults.float {
                    Some(float_type) => match &float_type.float_inverted_index {
                        Some(float_inverted_index) => Ok(float_inverted_index.enabled),
                        None => Err(SchemaError::MissingIndexConfiguration {
                            key: key.to_string(),
                            value_type: "float".to_string(),
                        }),
                    },
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "float".to_string(),
                    }),
                },
            },
            MetadataValueType::Str => match &v_type.string {
                Some(string_type) => match &string_type.string_inverted_index {
                    Some(string_inverted_index) => Ok(string_inverted_index.enabled),
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "string".to_string(),
                    }),
                },
                None => match &self.defaults.string {
                    Some(string_type) => match &string_type.string_inverted_index {
                        Some(string_inverted_index) => Ok(string_inverted_index.enabled),
                        None => Err(SchemaError::MissingIndexConfiguration {
                            key: key.to_string(),
                            value_type: "string".to_string(),
                        }),
                    },
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "string".to_string(),
                    }),
                },
            },
            MetadataValueType::SparseVector => match &v_type.sparse_vector {
                Some(sparse_vector_type) => match &sparse_vector_type.sparse_vector_index {
                    Some(sparse_vector_index) => Ok(sparse_vector_index.enabled),
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "sparse_vector".to_string(),
                    }),
                },
                None => match &self.defaults.sparse_vector {
                    Some(sparse_vector_type) => match &sparse_vector_type.sparse_vector_index {
                        Some(sparse_vector_index) => Ok(sparse_vector_index.enabled),
                        None => Err(SchemaError::MissingIndexConfiguration {
                            key: key.to_string(),
                            value_type: "sparse_vector".to_string(),
                        }),
                    },
                    None => Err(SchemaError::MissingIndexConfiguration {
                        key: key.to_string(),
                        value_type: "sparse_vector".to_string(),
                    }),
                },
            },
        }
    }

    pub fn is_metadata_where_indexing_enabled(
        &self,
        where_clause: &Where,
    ) -> Result<(), FilterValidationError> {
        match where_clause {
            Where::Composite(composite) => {
                for child in &composite.children {
                    self.is_metadata_where_indexing_enabled(child)?;
                }
                Ok(())
            }
            Where::Document(_) => Ok(()),
            Where::Metadata(expression) => {
                let value_type = match &expression.comparison {
                    MetadataComparison::Primitive(_, value) => value.value_type(),
                    MetadataComparison::Set(_, set_value) => set_value.value_type(),
                };
                let is_enabled = self
                    .is_metadata_type_index_enabled(expression.key.as_str(), value_type)
                    .map_err(FilterValidationError::Schema)?;
                if !is_enabled {
                    return Err(FilterValidationError::IndexingDisabled {
                        key: expression.key.clone(),
                        value_type,
                    });
                }
                Ok(())
            }
        }
    }

    pub fn is_knn_key_indexing_enabled(
        &self,
        key: &str,
        query: &QueryVector,
    ) -> Result<(), FilterValidationError> {
        match query {
            QueryVector::Sparse(_) => {
                let is_enabled = self
                    .is_metadata_type_index_enabled(key, MetadataValueType::SparseVector)
                    .map_err(FilterValidationError::Schema)?;
                if !is_enabled {
                    return Err(FilterValidationError::IndexingDisabled {
                        key: key.to_string(),
                        value_type: MetadataValueType::SparseVector,
                    });
                }
                Ok(())
            }
            QueryVector::Dense(_) => {
                // TODO: once we allow turning off dense vector indexing, we need to check if the key is enabled
                // Dense vectors are always indexed
                Ok(())
            }
        }
    }

    pub fn ensure_key_from_metadata(&mut self, key: &str, value_type: MetadataValueType) -> bool {
        if key.starts_with(CHROMA_KEY) {
            return false;
        }
        let value_types = self.keys.entry(key.to_string()).or_default();
        match value_type {
            MetadataValueType::Bool => {
                if value_types.boolean.is_none() {
                    value_types.boolean = self.defaults.boolean.clone();
                    return true;
                }
            }
            MetadataValueType::Int => {
                if value_types.int.is_none() {
                    value_types.int = self.defaults.int.clone();
                    return true;
                }
            }
            MetadataValueType::Float => {
                if value_types.float.is_none() {
                    value_types.float = self.defaults.float.clone();
                    return true;
                }
            }
            MetadataValueType::Str => {
                if value_types.string.is_none() {
                    value_types.string = self.defaults.string.clone();
                    return true;
                }
            }
            MetadataValueType::SparseVector => {
                if value_types.sparse_vector.is_none() {
                    value_types.sparse_vector = self.defaults.sparse_vector.clone();
                    return true;
                }
            }
        }
        false
    }

    // ========================================================================
    // BUILDER PATTERN METHODS
    // ========================================================================

    /// Create an index configuration (builder pattern)
    ///
    /// This method allows fluent, chainable configuration of indexes on a schema.
    /// It matches the Python API's `.create_index()` method.
    ///
    /// # Arguments
    /// * `key` - Optional key name for per-key index. `None` applies to defaults/special keys
    /// * `config` - Index configuration to create
    ///
    /// # Returns
    /// `Self` for method chaining
    ///
    /// # Errors
    /// Returns error if:
    /// - Attempting to create index on special keys (`#document`, `#embedding`)
    /// - Invalid configuration (e.g., vector index on non-embedding key)
    /// - Conflicting with existing indexes (e.g., multiple sparse vector indexes)
    ///
    /// # Examples
    /// ```
    /// use chroma_types::{Schema, VectorIndexConfig, StringInvertedIndexConfig, Space, SchemaBuilderError};
    ///
    /// # fn main() -> Result<(), SchemaBuilderError> {
    /// let schema = Schema::default()
    ///     .create_index(None, VectorIndexConfig {
    ///         space: Some(Space::Cosine),
    ///         embedding_function: None,
    ///         source_key: None,
    ///         hnsw: None,
    ///         spann: None,
    ///     }.into())?
    ///     .create_index(Some("category"), StringInvertedIndexConfig {}.into())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_index(
        mut self,
        key: Option<&str>,
        config: IndexConfig,
    ) -> Result<Self, SchemaBuilderError> {
        // Handle special cases: Vector and FTS (global configs only)
        match (&key, &config) {
            (None, IndexConfig::Vector(cfg)) => {
                self._set_vector_index_config_builder(cfg.clone());
                return Ok(self);
            }
            (None, IndexConfig::Fts(cfg)) => {
                self._set_fts_index_config_builder(cfg.clone());
                return Ok(self);
            }
            (Some(k), IndexConfig::Vector(_)) => {
                return Err(SchemaBuilderError::VectorIndexMustBeGlobal { key: k.to_string() });
            }
            (Some(k), IndexConfig::Fts(_)) => {
                return Err(SchemaBuilderError::FtsIndexMustBeGlobal { key: k.to_string() });
            }
            _ => {}
        }

        // Validate special keys
        if let Some(k) = key {
            if k == DOCUMENT_KEY || k == EMBEDDING_KEY {
                return Err(SchemaBuilderError::SpecialKeyModificationNotAllowed {
                    key: k.to_string(),
                });
            }
        }

        // Validate sparse vector requires key
        if key.is_none() && matches!(config, IndexConfig::SparseVector(_)) {
            return Err(SchemaBuilderError::SparseVectorRequiresKey);
        }

        // Dispatch to appropriate helper
        match key {
            Some(k) => self._set_index_for_key_builder(k, config, true)?,
            None => self._set_index_in_defaults_builder(config, true)?,
        }

        Ok(self)
    }

    /// Delete/disable an index configuration (builder pattern)
    ///
    /// This method allows disabling indexes on a schema.
    /// It matches the Python API's `.delete_index()` method.
    ///
    /// # Arguments
    /// * `key` - Optional key name for per-key index. `None` applies to defaults
    /// * `config` - Index configuration to disable
    ///
    /// # Returns
    /// `Self` for method chaining
    ///
    /// # Errors
    /// Returns error if:
    /// - Attempting to delete index on special keys (`#document`, `#embedding`)
    /// - Attempting to delete vector, FTS, or sparse vector indexes (not currently supported)
    ///
    /// # Examples
    /// ```
    /// use chroma_types::{Schema, StringInvertedIndexConfig, SchemaBuilderError};
    ///
    /// # fn main() -> Result<(), SchemaBuilderError> {
    /// let schema = Schema::default()
    ///     .delete_index(Some("category"), StringInvertedIndexConfig {}.into())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_index(
        mut self,
        key: Option<&str>,
        config: IndexConfig,
    ) -> Result<Self, SchemaBuilderError> {
        // Validate special keys
        if let Some(k) = key {
            if k == DOCUMENT_KEY || k == EMBEDDING_KEY {
                return Err(SchemaBuilderError::SpecialKeyModificationNotAllowed {
                    key: k.to_string(),
                });
            }
        }

        // Disallow deleting vector, FTS, and sparse vector indexes (match Python restrictions)
        match &config {
            IndexConfig::Vector(_) => {
                return Err(SchemaBuilderError::VectorIndexDeletionNotSupported);
            }
            IndexConfig::Fts(_) => {
                return Err(SchemaBuilderError::FtsIndexDeletionNotSupported);
            }
            IndexConfig::SparseVector(_) => {
                return Err(SchemaBuilderError::SparseVectorIndexDeletionNotSupported);
            }
            _ => {}
        }

        // Dispatch to appropriate helper (enabled=false)
        match key {
            Some(k) => self._set_index_for_key_builder(k, config, false)?,
            None => self._set_index_in_defaults_builder(config, false)?,
        }

        Ok(self)
    }

    /// Set vector index config globally (applies to #embedding)
    fn _set_vector_index_config_builder(&mut self, config: VectorIndexConfig) {
        // Update defaults (disabled, just config update)
        if let Some(float_list) = &mut self.defaults.float_list {
            if let Some(vector_index) = &mut float_list.vector_index {
                vector_index.config = config.clone();
            }
        }

        // Update #embedding key (enabled, config update, preserve source_key=#document)
        if let Some(embedding_types) = self.keys.get_mut(EMBEDDING_KEY) {
            if let Some(float_list) = &mut embedding_types.float_list {
                if let Some(vector_index) = &mut float_list.vector_index {
                    let mut updated_config = config;
                    // Preserve source_key as #document
                    updated_config.source_key = Some(DOCUMENT_KEY.to_string());
                    vector_index.config = updated_config;
                }
            }
        }
    }

    /// Set FTS index config globally (applies to #document)
    fn _set_fts_index_config_builder(&mut self, config: FtsIndexConfig) {
        // Update defaults (disabled, just config update)
        if let Some(string) = &mut self.defaults.string {
            if let Some(fts_index) = &mut string.fts_index {
                fts_index.config = config.clone();
            }
        }

        // Update #document key (enabled, config update)
        if let Some(document_types) = self.keys.get_mut(DOCUMENT_KEY) {
            if let Some(string) = &mut document_types.string {
                if let Some(fts_index) = &mut string.fts_index {
                    fts_index.config = config;
                }
            }
        }
    }

    /// Set index configuration for a specific key
    fn _set_index_for_key_builder(
        &mut self,
        key: &str,
        config: IndexConfig,
        enabled: bool,
    ) -> Result<(), SchemaBuilderError> {
        // Check for multiple sparse vector indexes BEFORE getting mutable reference
        if enabled && matches!(config, IndexConfig::SparseVector(_)) {
            // Find existing sparse vector index
            let existing_key = self
                .keys
                .iter()
                .find(|(k, v)| {
                    k.as_str() != key
                        && v.sparse_vector
                            .as_ref()
                            .and_then(|sv| sv.sparse_vector_index.as_ref())
                            .map(|idx| idx.enabled)
                            .unwrap_or(false)
                })
                .map(|(k, _)| k.clone());

            if let Some(existing_key) = existing_key {
                return Err(SchemaBuilderError::MultipleSparseVectorIndexes { existing_key });
            }
        }

        // Get or create ValueTypes for this key
        let value_types = self.keys.entry(key.to_string()).or_default();

        // Set the appropriate index based on config type
        match config {
            IndexConfig::Vector(_) => {
                return Err(SchemaBuilderError::VectorIndexMustBeGlobal {
                    key: key.to_string(),
                });
            }
            IndexConfig::Fts(_) => {
                return Err(SchemaBuilderError::FtsIndexMustBeGlobal {
                    key: key.to_string(),
                });
            }
            IndexConfig::SparseVector(cfg) => {
                value_types.sparse_vector = Some(SparseVectorValueType {
                    sparse_vector_index: Some(SparseVectorIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::StringInverted(cfg) => {
                if value_types.string.is_none() {
                    value_types.string = Some(StringValueType {
                        fts_index: None,
                        string_inverted_index: None,
                    });
                }
                if let Some(string) = &mut value_types.string {
                    string.string_inverted_index = Some(StringInvertedIndexType {
                        enabled,
                        config: cfg,
                    });
                }
            }
            IndexConfig::IntInverted(cfg) => {
                value_types.int = Some(IntValueType {
                    int_inverted_index: Some(IntInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::FloatInverted(cfg) => {
                value_types.float = Some(FloatValueType {
                    float_inverted_index: Some(FloatInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::BoolInverted(cfg) => {
                value_types.boolean = Some(BoolValueType {
                    bool_inverted_index: Some(BoolInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
        }

        Ok(())
    }

    /// Set index configuration in defaults
    fn _set_index_in_defaults_builder(
        &mut self,
        config: IndexConfig,
        enabled: bool,
    ) -> Result<(), SchemaBuilderError> {
        match config {
            IndexConfig::Vector(_) => {
                return Err(SchemaBuilderError::VectorIndexMustBeGlobal {
                    key: "defaults".to_string(),
                });
            }
            IndexConfig::Fts(_) => {
                return Err(SchemaBuilderError::FtsIndexMustBeGlobal {
                    key: "defaults".to_string(),
                });
            }
            IndexConfig::SparseVector(cfg) => {
                self.defaults.sparse_vector = Some(SparseVectorValueType {
                    sparse_vector_index: Some(SparseVectorIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::StringInverted(cfg) => {
                if self.defaults.string.is_none() {
                    self.defaults.string = Some(StringValueType {
                        fts_index: None,
                        string_inverted_index: None,
                    });
                }
                if let Some(string) = &mut self.defaults.string {
                    string.string_inverted_index = Some(StringInvertedIndexType {
                        enabled,
                        config: cfg,
                    });
                }
            }
            IndexConfig::IntInverted(cfg) => {
                self.defaults.int = Some(IntValueType {
                    int_inverted_index: Some(IntInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::FloatInverted(cfg) => {
                self.defaults.float = Some(FloatValueType {
                    float_inverted_index: Some(FloatInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
            IndexConfig::BoolInverted(cfg) => {
                self.defaults.boolean = Some(BoolValueType {
                    bool_inverted_index: Some(BoolInvertedIndexType {
                        enabled,
                        config: cfg,
                    }),
                });
            }
        }

        Ok(())
    }
}

// ============================================================================
// INDEX CONFIGURATION STRUCTURES
// ============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct VectorIndexConfig {
    /// Vector space for similarity calculation (cosine, l2, ip)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub space: Option<Space>,
    /// Embedding function configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
    /// Key to source the vector from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_key: Option<String>,
    /// HNSW algorithm configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw: Option<HnswIndexConfig>,
    /// SPANN algorithm configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spann: Option<SpannIndexConfig>,
}

/// Configuration for HNSW vector index algorithm parameters
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, Default)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct HnswIndexConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_neighbors: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_threads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 2))]
    pub batch_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 2))]
    pub sync_threshold: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resize_factor: Option<f64>,
}

impl HnswIndexConfig {
    /// Check if this config has default values
    /// None values are considered default (not set by user)
    /// Note: We skip num_threads as it's variable based on available_parallelism
    pub fn is_default(&self) -> bool {
        if let Some(ef_construction) = self.ef_construction {
            if ef_construction != default_construction_ef() {
                return false;
            }
        }
        if let Some(max_neighbors) = self.max_neighbors {
            if max_neighbors != default_m() {
                return false;
            }
        }
        if let Some(ef_search) = self.ef_search {
            if ef_search != default_search_ef() {
                return false;
            }
        }
        if let Some(batch_size) = self.batch_size {
            if batch_size != default_batch_size() {
                return false;
            }
        }
        if let Some(sync_threshold) = self.sync_threshold {
            if sync_threshold != default_sync_threshold() {
                return false;
            }
        }
        if let Some(resize_factor) = self.resize_factor {
            if resize_factor != default_resize_factor() {
                return false;
            }
        }
        // Skip num_threads check as it's system-dependent
        true
    }
}

/// Configuration for SPANN vector index algorithm parameters
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate, Default)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SpannIndexConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 128))]
    pub search_nprobe: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1.0, max = 1.0))]
    pub search_rng_factor: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 5.0, max = 10.0))]
    pub search_rng_epsilon: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 8))]
    pub nreplica_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1.0, max = 1.0))]
    pub write_rng_factor: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 5.0, max = 10.0))]
    pub write_rng_epsilon: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 50, max = 200))]
    pub split_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 1000))]
    pub num_samples_kmeans: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 100.0, max = 100.0))]
    pub initial_lambda: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 64))]
    pub reassign_neighbor_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 25, max = 100))]
    pub merge_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 8))]
    pub num_centers_to_merge_to: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 64))]
    pub write_nprobe: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 200))]
    pub ef_construction: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 200))]
    pub ef_search: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 64))]
    pub max_neighbors: Option<usize>,
}

impl SpannIndexConfig {
    /// Check if this config has default values
    /// None values are considered default (not set by user)
    pub fn is_default(&self) -> bool {
        if let Some(search_nprobe) = self.search_nprobe {
            if search_nprobe != default_search_nprobe() {
                return false;
            }
        }
        if let Some(search_rng_factor) = self.search_rng_factor {
            if search_rng_factor != default_search_rng_factor() {
                return false;
            }
        }
        if let Some(search_rng_epsilon) = self.search_rng_epsilon {
            if search_rng_epsilon != default_search_rng_epsilon() {
                return false;
            }
        }
        if let Some(nreplica_count) = self.nreplica_count {
            if nreplica_count != default_nreplica_count() {
                return false;
            }
        }
        if let Some(write_rng_factor) = self.write_rng_factor {
            if write_rng_factor != default_write_rng_factor() {
                return false;
            }
        }
        if let Some(write_rng_epsilon) = self.write_rng_epsilon {
            if write_rng_epsilon != default_write_rng_epsilon() {
                return false;
            }
        }
        if let Some(split_threshold) = self.split_threshold {
            if split_threshold != default_split_threshold() {
                return false;
            }
        }
        if let Some(num_samples_kmeans) = self.num_samples_kmeans {
            if num_samples_kmeans != default_num_samples_kmeans() {
                return false;
            }
        }
        if let Some(initial_lambda) = self.initial_lambda {
            if initial_lambda != default_initial_lambda() {
                return false;
            }
        }
        if let Some(reassign_neighbor_count) = self.reassign_neighbor_count {
            if reassign_neighbor_count != default_reassign_neighbor_count() {
                return false;
            }
        }
        if let Some(merge_threshold) = self.merge_threshold {
            if merge_threshold != default_merge_threshold() {
                return false;
            }
        }
        if let Some(num_centers_to_merge_to) = self.num_centers_to_merge_to {
            if num_centers_to_merge_to != default_num_centers_to_merge_to() {
                return false;
            }
        }
        if let Some(write_nprobe) = self.write_nprobe {
            if write_nprobe != default_write_nprobe() {
                return false;
            }
        }
        if let Some(ef_construction) = self.ef_construction {
            if ef_construction != default_construction_ef_spann() {
                return false;
            }
        }
        if let Some(ef_search) = self.ef_search {
            if ef_search != default_search_ef_spann() {
                return false;
            }
        }
        if let Some(max_neighbors) = self.max_neighbors {
            if max_neighbors != default_m_spann() {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SparseVectorIndexConfig {
    /// Embedding function configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
    /// Key to source the sparse vector from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_key: Option<String>,
    /// Whether this embedding is BM25
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bm25: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct FtsIndexConfig {
    // FTS index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StringInvertedIndexConfig {
    // String inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct IntInvertedIndexConfig {
    // Integer inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct FloatInvertedIndexConfig {
    // Float inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct BoolInvertedIndexConfig {
    // Boolean inverted index typically has no additional parameters
}

// ============================================================================
// BUILDER PATTERN SUPPORT
// ============================================================================

/// Union type for all index configurations (used by builder pattern)
#[derive(Clone, Debug)]
pub enum IndexConfig {
    Vector(VectorIndexConfig),
    SparseVector(SparseVectorIndexConfig),
    Fts(FtsIndexConfig),
    StringInverted(StringInvertedIndexConfig),
    IntInverted(IntInvertedIndexConfig),
    FloatInverted(FloatInvertedIndexConfig),
    BoolInverted(BoolInvertedIndexConfig),
}

// Convenience From implementations for ergonomic usage
impl From<VectorIndexConfig> for IndexConfig {
    fn from(config: VectorIndexConfig) -> Self {
        IndexConfig::Vector(config)
    }
}

impl From<SparseVectorIndexConfig> for IndexConfig {
    fn from(config: SparseVectorIndexConfig) -> Self {
        IndexConfig::SparseVector(config)
    }
}

impl From<FtsIndexConfig> for IndexConfig {
    fn from(config: FtsIndexConfig) -> Self {
        IndexConfig::Fts(config)
    }
}

impl From<StringInvertedIndexConfig> for IndexConfig {
    fn from(config: StringInvertedIndexConfig) -> Self {
        IndexConfig::StringInverted(config)
    }
}

impl From<IntInvertedIndexConfig> for IndexConfig {
    fn from(config: IntInvertedIndexConfig) -> Self {
        IndexConfig::IntInverted(config)
    }
}

impl From<FloatInvertedIndexConfig> for IndexConfig {
    fn from(config: FloatInvertedIndexConfig) -> Self {
        IndexConfig::FloatInverted(config)
    }
}

impl From<BoolInvertedIndexConfig> for IndexConfig {
    fn from(config: BoolInvertedIndexConfig) -> Self {
        IndexConfig::BoolInverted(config)
    }
}

impl TryFrom<&InternalCollectionConfiguration> for Schema {
    type Error = SchemaError;

    fn try_from(config: &InternalCollectionConfiguration) -> Result<Self, Self::Error> {
        // Start with a default schema structure
        let mut schema = match &config.vector_index {
            VectorIndexConfiguration::Hnsw(_) => Schema::new_default(KnnIndex::Hnsw),
            VectorIndexConfiguration::Spann(_) => Schema::new_default(KnnIndex::Spann),
        };
        // Convert vector index configuration
        let vector_config = match &config.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw_config) => VectorIndexConfig {
                space: Some(hnsw_config.space.clone()),
                embedding_function: config.embedding_function.clone(),
                source_key: None,
                hnsw: Some(HnswIndexConfig {
                    ef_construction: Some(hnsw_config.ef_construction),
                    max_neighbors: Some(hnsw_config.max_neighbors),
                    ef_search: Some(hnsw_config.ef_search),
                    num_threads: Some(hnsw_config.num_threads),
                    batch_size: Some(hnsw_config.batch_size),
                    sync_threshold: Some(hnsw_config.sync_threshold),
                    resize_factor: Some(hnsw_config.resize_factor),
                }),
                spann: None,
            },
            VectorIndexConfiguration::Spann(spann_config) => VectorIndexConfig {
                space: Some(spann_config.space.clone()),
                embedding_function: config.embedding_function.clone(),
                source_key: None,
                hnsw: None,
                spann: Some(SpannIndexConfig {
                    search_nprobe: Some(spann_config.search_nprobe),
                    search_rng_factor: Some(spann_config.search_rng_factor),
                    search_rng_epsilon: Some(spann_config.search_rng_epsilon),
                    nreplica_count: Some(spann_config.nreplica_count),
                    write_rng_factor: Some(spann_config.write_rng_factor),
                    write_rng_epsilon: Some(spann_config.write_rng_epsilon),
                    split_threshold: Some(spann_config.split_threshold),
                    num_samples_kmeans: Some(spann_config.num_samples_kmeans),
                    initial_lambda: Some(spann_config.initial_lambda),
                    reassign_neighbor_count: Some(spann_config.reassign_neighbor_count),
                    merge_threshold: Some(spann_config.merge_threshold),
                    num_centers_to_merge_to: Some(spann_config.num_centers_to_merge_to),
                    write_nprobe: Some(spann_config.write_nprobe),
                    ef_construction: Some(spann_config.ef_construction),
                    ef_search: Some(spann_config.ef_search),
                    max_neighbors: Some(spann_config.max_neighbors),
                }),
            },
        };

        // Update defaults (keep enabled=false, just update the config)
        // This serves as the template for any new float_list fields
        if let Some(float_list) = &mut schema.defaults.float_list {
            if let Some(vector_index) = &mut float_list.vector_index {
                vector_index.config = vector_config.clone();
            }
        }

        // Update the vector_index in the existing #embedding key override
        // Keep enabled=true (already set by new_default) and update the config
        // Set source_key to DOCUMENT_KEY for the embedding key
        if let Some(embedding_types) = schema.keys.get_mut(EMBEDDING_KEY) {
            if let Some(float_list) = &mut embedding_types.float_list {
                if let Some(vector_index) = &mut float_list.vector_index {
                    let mut vector_config = vector_config;
                    vector_config.source_key = Some(DOCUMENT_KEY.to_string());
                    vector_index.config = vector_config;
                }
            }
        }

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw_configuration::Space;
    use crate::metadata::SparseVector;
    use crate::{
        EmbeddingFunctionNewConfiguration, InternalHnswConfiguration, InternalSpannConfiguration,
    };
    use serde_json::json;

    #[test]
    fn test_reconcile_with_defaults_none_user_schema() {
        // Test that when no user schema is provided, we get the default schema
        let result = Schema::reconcile_with_defaults(None, KnnIndex::Spann).unwrap();
        let expected = Schema::new_default(KnnIndex::Spann);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_reconcile_with_defaults_empty_user_schema() {
        // Test merging with an empty user schema
        let user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        let result = Schema::reconcile_with_defaults(Some(&user_schema), KnnIndex::Spann).unwrap();
        let expected = Schema::new_default(KnnIndex::Spann);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_reconcile_with_defaults_user_overrides_string_enabled() {
        // Test that user can override string inverted index enabled state
        let mut user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        user_schema.defaults.string = Some(StringValueType {
            string_inverted_index: Some(StringInvertedIndexType {
                enabled: false, // Override default (true) to false
                config: StringInvertedIndexConfig {},
            }),
            fts_index: None,
        });

        let result = Schema::reconcile_with_defaults(Some(&user_schema), KnnIndex::Spann).unwrap();

        // Check that the user override took precedence
        assert!(
            !result
                .defaults
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );
        // Check that other defaults are still present
        assert!(result.defaults.float.is_some());
        assert!(result.defaults.int.is_some());
    }

    #[test]
    fn test_reconcile_with_defaults_user_overrides_vector_config() {
        // Test field-level merging for vector configurations
        let mut user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        user_schema.defaults.float_list = Some(FloatListValueType {
            vector_index: Some(VectorIndexType {
                enabled: true, // Enable vector index (default is false)
                config: VectorIndexConfig {
                    space: Some(Space::L2),                     // Override default space
                    embedding_function: None,                   // Will use default
                    source_key: Some("custom_key".to_string()), // Override default
                    hnsw: Some(HnswIndexConfig {
                        ef_construction: Some(500), // Override default
                        max_neighbors: None,        // Will use default
                        ef_search: None,            // Will use default
                        num_threads: None,
                        batch_size: None,
                        sync_threshold: None,
                        resize_factor: None,
                    }),
                    spann: None,
                },
            }),
        });

        // Use HNSW defaults for this test so we have HNSW config to merge with
        let result = {
            let default_schema = Schema::new_default(KnnIndex::Hnsw);
            let merged_defaults = Schema::merge_value_types(
                &default_schema.defaults,
                &user_schema.defaults,
                KnnIndex::Hnsw,
            )
            .unwrap();
            let mut merged_keys = default_schema.keys.clone();
            for (key, user_value_types) in user_schema.keys {
                if let Some(default_value_types) = merged_keys.get(&key) {
                    let merged_value_types = Schema::merge_value_types(
                        default_value_types,
                        &user_value_types,
                        KnnIndex::Hnsw,
                    )
                    .unwrap();
                    merged_keys.insert(key, merged_value_types);
                } else {
                    merged_keys.insert(key, user_value_types);
                }
            }
            Schema {
                defaults: merged_defaults,
                keys: merged_keys,
            }
        };

        let vector_config = &result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config;

        // Check user overrides took precedence
        assert_eq!(vector_config.space, Some(Space::L2));
        assert_eq!(vector_config.source_key, Some("custom_key".to_string()));
        assert_eq!(
            vector_config.hnsw.as_ref().unwrap().ef_construction,
            Some(500)
        );

        // Check defaults were preserved for unspecified fields
        assert_eq!(vector_config.embedding_function, None);
        // Since user provided HNSW config, the default max_neighbors should be merged in
        assert_eq!(
            vector_config.hnsw.as_ref().unwrap().max_neighbors,
            Some(default_m())
        );
    }

    #[test]
    fn test_reconcile_with_defaults_keys() {
        // Test that key overrides are properly merged
        let mut user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        // Add a custom key override
        let custom_key_types = ValueTypes {
            string: Some(StringValueType {
                fts_index: Some(FtsIndexType {
                    enabled: true,
                    config: FtsIndexConfig {},
                }),
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: false,
                    config: StringInvertedIndexConfig {},
                }),
            }),
            ..Default::default()
        };
        user_schema
            .keys
            .insert("custom_key".to_string(), custom_key_types);

        let result = Schema::reconcile_with_defaults(Some(&user_schema), KnnIndex::Spann).unwrap();

        // Check that default key overrides are preserved
        assert!(result.keys.contains_key(EMBEDDING_KEY));
        assert!(result.keys.contains_key(DOCUMENT_KEY));

        // Check that user key override was added
        assert!(result.keys.contains_key("custom_key"));
        let custom_override = result.keys.get("custom_key").unwrap();
        assert!(
            custom_override
                .string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn test_reconcile_with_defaults_override_existing_key() {
        // Test overriding an existing key override (like #embedding)
        let mut user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        // Override the #embedding key with custom settings
        let embedding_override = ValueTypes {
            float_list: Some(FloatListValueType {
                vector_index: Some(VectorIndexType {
                    enabled: false, // Override default enabled=true to false
                    config: VectorIndexConfig {
                        space: Some(Space::Ip), // Override default space
                        embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                        source_key: Some("custom_embedding_key".to_string()),
                        hnsw: None,
                        spann: None,
                    },
                }),
            }),
            ..Default::default()
        };
        user_schema
            .keys
            .insert(EMBEDDING_KEY.to_string(), embedding_override);

        let result = Schema::reconcile_with_defaults(Some(&user_schema), KnnIndex::Spann).unwrap();

        let embedding_config = result.keys.get(EMBEDDING_KEY).unwrap();
        let vector_config = &embedding_config
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();

        // Check user overrides took precedence
        assert!(!vector_config.enabled);
        assert_eq!(vector_config.config.space, Some(Space::Ip));
        assert_eq!(
            vector_config.config.source_key,
            Some("custom_embedding_key".to_string())
        );
    }

    #[test]
    fn test_convert_schema_to_collection_config_hnsw_roundtrip() {
        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Cosine,
                ef_construction: 128,
                ef_search: 96,
                max_neighbors: 42,
                num_threads: 8,
                resize_factor: 1.5,
                sync_threshold: 2_000,
                batch_size: 256,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "custom".to_string(),
                    config: json!({"alpha": 1}),
                },
            )),
        };

        let schema = Schema::try_from(&collection_config).unwrap();
        let reconstructed = InternalCollectionConfiguration::try_from(&schema).unwrap();

        assert_eq!(reconstructed, collection_config);
    }

    #[test]
    fn test_convert_schema_to_collection_config_spann_roundtrip() {
        let spann_config = InternalSpannConfiguration {
            space: Space::Cosine,
            search_nprobe: 11,
            search_rng_factor: 1.7,
            write_nprobe: 5,
            nreplica_count: 3,
            split_threshold: 150,
            merge_threshold: 80,
            ef_construction: 120,
            ef_search: 90,
            max_neighbors: 40,
            ..Default::default()
        };

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(spann_config.clone()),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "custom".to_string(),
                    config: json!({"beta": true}),
                },
            )),
        };

        let schema = Schema::try_from(&collection_config).unwrap();
        let reconstructed = InternalCollectionConfiguration::try_from(&schema).unwrap();

        assert_eq!(reconstructed, collection_config);
    }

    #[test]
    fn test_convert_schema_to_collection_config_rejects_mixed_index() {
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        if let Some(embedding) = schema.keys.get_mut(EMBEDDING_KEY) {
            if let Some(float_list) = &mut embedding.float_list {
                if let Some(vector_index) = &mut float_list.vector_index {
                    vector_index.config.spann = Some(SpannIndexConfig {
                        search_nprobe: Some(1),
                        search_rng_factor: Some(1.0),
                        search_rng_epsilon: Some(0.1),
                        nreplica_count: Some(1),
                        write_rng_factor: Some(1.0),
                        write_rng_epsilon: Some(0.1),
                        split_threshold: Some(100),
                        num_samples_kmeans: Some(10),
                        initial_lambda: Some(0.5),
                        reassign_neighbor_count: Some(10),
                        merge_threshold: Some(50),
                        num_centers_to_merge_to: Some(3),
                        write_nprobe: Some(1),
                        ef_construction: Some(50),
                        ef_search: Some(40),
                        max_neighbors: Some(20),
                    });
                }
            }
        }

        let result = InternalCollectionConfiguration::try_from(&schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_ensure_key_from_metadata_no_changes_for_existing_key() {
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        let before = schema.clone();
        let modified = schema.ensure_key_from_metadata(DOCUMENT_KEY, MetadataValueType::Str);
        assert!(!modified);
        assert_eq!(schema, before);
    }

    #[test]
    fn test_ensure_key_from_metadata_populates_new_key_with_default_value_type() {
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        assert!(!schema.keys.contains_key("custom_field"));

        let modified = schema.ensure_key_from_metadata("custom_field", MetadataValueType::Bool);

        assert!(modified);
        let entry = schema
            .keys
            .get("custom_field")
            .expect("expected new key override to be inserted");
        assert_eq!(entry.boolean, schema.defaults.boolean);
        assert!(entry.string.is_none());
        assert!(entry.int.is_none());
        assert!(entry.float.is_none());
        assert!(entry.float_list.is_none());
        assert!(entry.sparse_vector.is_none());
    }

    #[test]
    fn test_ensure_key_from_metadata_adds_missing_value_type_to_existing_key() {
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        let initial_len = schema.keys.len();
        schema.keys.insert(
            "custom_field".to_string(),
            ValueTypes {
                string: schema.defaults.string.clone(),
                ..Default::default()
            },
        );

        let modified = schema.ensure_key_from_metadata("custom_field", MetadataValueType::Bool);

        assert!(modified);
        assert_eq!(schema.keys.len(), initial_len + 1);
        let entry = schema
            .keys
            .get("custom_field")
            .expect("expected key override to exist after ensure call");
        assert!(entry.string.is_some());
        assert_eq!(entry.boolean, schema.defaults.boolean);
    }

    #[test]
    fn test_is_knn_key_indexing_enabled_sparse_disabled_errors() {
        let schema = Schema::new_default(KnnIndex::Spann);
        let result = schema.is_knn_key_indexing_enabled(
            "custom_sparse",
            &QueryVector::Sparse(SparseVector::new(vec![0_u32], vec![1.0_f32]).unwrap()),
        );

        let err = result.expect_err("expected indexing disabled error");
        match err {
            FilterValidationError::IndexingDisabled { key, value_type } => {
                assert_eq!(key, "custom_sparse");
                assert_eq!(value_type, crate::metadata::MetadataValueType::SparseVector);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn test_is_knn_key_indexing_enabled_sparse_enabled_succeeds() {
        let mut schema = Schema::new_default(KnnIndex::Spann);
        schema.keys.insert(
            "sparse_enabled".to_string(),
            ValueTypes {
                sparse_vector: Some(SparseVectorValueType {
                    sparse_vector_index: Some(SparseVectorIndexType {
                        enabled: true,
                        config: SparseVectorIndexConfig {
                            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                            source_key: None,
                            bm25: None,
                        },
                    }),
                }),
                ..Default::default()
            },
        );

        let result = schema.is_knn_key_indexing_enabled(
            "sparse_enabled",
            &QueryVector::Sparse(SparseVector::new(vec![0_u32], vec![1.0_f32]).unwrap()),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_is_knn_key_indexing_enabled_dense_succeeds() {
        let schema = Schema::new_default(KnnIndex::Spann);
        let result = schema.is_knn_key_indexing_enabled(
            EMBEDDING_KEY,
            &QueryVector::Dense(vec![0.1_f32, 0.2_f32]),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_merge_hnsw_configs_field_level() {
        // Test field-level merging for HNSW configurations
        let default_hnsw = HnswIndexConfig {
            ef_construction: Some(200),
            max_neighbors: Some(16),
            ef_search: Some(10),
            num_threads: Some(4),
            batch_size: Some(100),
            sync_threshold: Some(1000),
            resize_factor: Some(1.2),
        };

        let user_hnsw = HnswIndexConfig {
            ef_construction: Some(300), // Override
            max_neighbors: None,        // Will use default
            ef_search: Some(20),        // Override
            num_threads: None,          // Will use default
            batch_size: None,           // Will use default
            sync_threshold: Some(2000), // Override
            resize_factor: None,        // Will use default
        };

        let result = Schema::merge_hnsw_configs(Some(&default_hnsw), Some(&user_hnsw)).unwrap();

        // Check user overrides
        assert_eq!(result.ef_construction, Some(300));
        assert_eq!(result.ef_search, Some(20));
        assert_eq!(result.sync_threshold, Some(2000));

        // Check defaults preserved
        assert_eq!(result.max_neighbors, Some(16));
        assert_eq!(result.num_threads, Some(4));
        assert_eq!(result.batch_size, Some(100));
        assert_eq!(result.resize_factor, Some(1.2));
    }

    #[test]
    fn test_merge_spann_configs_field_level() {
        // Test field-level merging for SPANN configurations
        let default_spann = SpannIndexConfig {
            search_nprobe: Some(10),
            search_rng_factor: Some(1.0),  // Must be exactly 1.0
            search_rng_epsilon: Some(7.0), // Must be 5.0-10.0
            nreplica_count: Some(3),
            write_rng_factor: Some(1.0),  // Must be exactly 1.0
            write_rng_epsilon: Some(6.0), // Must be 5.0-10.0
            split_threshold: Some(100),   // Must be 50-200
            num_samples_kmeans: Some(100),
            initial_lambda: Some(100.0), // Must be exactly 100.0
            reassign_neighbor_count: Some(50),
            merge_threshold: Some(50),        // Must be 25-100
            num_centers_to_merge_to: Some(4), // Max is 8
            write_nprobe: Some(5),
            ef_construction: Some(100),
            ef_search: Some(10),
            max_neighbors: Some(16),
        };

        let user_spann = SpannIndexConfig {
            search_nprobe: Some(20),       // Override
            search_rng_factor: None,       // Will use default
            search_rng_epsilon: Some(8.0), // Override (valid: 5.0-10.0)
            nreplica_count: None,          // Will use default
            write_rng_factor: None,
            write_rng_epsilon: None,
            split_threshold: Some(150), // Override (valid: 50-200)
            num_samples_kmeans: None,
            initial_lambda: None,
            reassign_neighbor_count: None,
            merge_threshold: None,
            num_centers_to_merge_to: None,
            write_nprobe: None,
            ef_construction: None,
            ef_search: None,
            max_neighbors: None,
        };

        let result = Schema::merge_spann_configs(Some(&default_spann), Some(&user_spann)).unwrap();

        // Check user overrides
        assert_eq!(result.search_nprobe, Some(20));
        assert_eq!(result.search_rng_epsilon, Some(8.0));
        assert_eq!(result.split_threshold, Some(150));

        // Check defaults preserved
        assert_eq!(result.search_rng_factor, Some(1.0));
        assert_eq!(result.nreplica_count, Some(3));
        assert_eq!(result.initial_lambda, Some(100.0));
    }

    #[test]
    fn test_spann_index_config_into_internal_configuration() {
        let config = SpannIndexConfig {
            search_nprobe: Some(33),
            search_rng_factor: Some(1.2),
            search_rng_epsilon: None,
            nreplica_count: None,
            write_rng_factor: Some(1.5),
            write_rng_epsilon: None,
            split_threshold: Some(75),
            num_samples_kmeans: None,
            initial_lambda: Some(0.9),
            reassign_neighbor_count: Some(40),
            merge_threshold: None,
            num_centers_to_merge_to: Some(4),
            write_nprobe: Some(60),
            ef_construction: Some(180),
            ef_search: Some(170),
            max_neighbors: Some(32),
        };

        let with_space: InternalSpannConfiguration = (Some(&Space::Cosine), &config).into();
        assert_eq!(with_space.space, Space::Cosine);
        assert_eq!(with_space.search_nprobe, 33);
        assert_eq!(with_space.search_rng_factor, 1.2);
        assert_eq!(with_space.search_rng_epsilon, default_search_rng_epsilon());
        assert_eq!(with_space.write_rng_factor, 1.5);
        assert_eq!(with_space.write_nprobe, 60);
        assert_eq!(with_space.ef_construction, 180);
        assert_eq!(with_space.ef_search, 170);
        assert_eq!(with_space.max_neighbors, 32);
        assert_eq!(with_space.merge_threshold, default_merge_threshold());

        let default_space_config: InternalSpannConfiguration = (None, &config).into();
        assert_eq!(default_space_config.space, default_space());
    }

    #[test]
    fn test_merge_string_type_combinations() {
        // Test all combinations of default and user StringValueType

        // Both Some - should merge
        let default = StringValueType {
            string_inverted_index: Some(StringInvertedIndexType {
                enabled: true,
                config: StringInvertedIndexConfig {},
            }),
            fts_index: Some(FtsIndexType {
                enabled: false,
                config: FtsIndexConfig {},
            }),
        };

        let user = StringValueType {
            string_inverted_index: Some(StringInvertedIndexType {
                enabled: false, // Override
                config: StringInvertedIndexConfig {},
            }),
            fts_index: None, // Will use default
        };

        let result = Schema::merge_string_type(Some(&default), Some(&user))
            .unwrap()
            .unwrap();
        assert!(!result.string_inverted_index.as_ref().unwrap().enabled); // User override
        assert!(!result.fts_index.as_ref().unwrap().enabled); // Default preserved

        // Default Some, User None - should return default
        let result = Schema::merge_string_type(Some(&default), None)
            .unwrap()
            .unwrap();
        assert!(result.string_inverted_index.as_ref().unwrap().enabled);

        // Default None, User Some - should return user
        let result = Schema::merge_string_type(None, Some(&user))
            .unwrap()
            .unwrap();
        assert!(!result.string_inverted_index.as_ref().unwrap().enabled);

        // Both None - should return None
        let result = Schema::merge_string_type(None, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_vector_index_config_comprehensive() {
        // Test comprehensive vector index config merging
        let default_config = VectorIndexConfig {
            space: Some(Space::Cosine),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
            source_key: Some("default_key".to_string()),
            hnsw: Some(HnswIndexConfig {
                ef_construction: Some(200),
                max_neighbors: Some(16),
                ef_search: Some(10),
                num_threads: Some(4),
                batch_size: Some(100),
                sync_threshold: Some(1000),
                resize_factor: Some(1.2),
            }),
            spann: None,
        };

        let user_config = VectorIndexConfig {
            space: Some(Space::L2),                   // Override
            embedding_function: None,                 // Will use default
            source_key: Some("user_key".to_string()), // Override
            hnsw: Some(HnswIndexConfig {
                ef_construction: Some(300), // Override
                max_neighbors: None,        // Will use default
                ef_search: None,            // Will use default
                num_threads: None,
                batch_size: None,
                sync_threshold: None,
                resize_factor: None,
            }),
            spann: Some(SpannIndexConfig {
                search_nprobe: Some(15),
                search_rng_factor: None,
                search_rng_epsilon: None,
                nreplica_count: None,
                write_rng_factor: None,
                write_rng_epsilon: None,
                split_threshold: None,
                num_samples_kmeans: None,
                initial_lambda: None,
                reassign_neighbor_count: None,
                merge_threshold: None,
                num_centers_to_merge_to: None,
                write_nprobe: None,
                ef_construction: None,
                ef_search: None,
                max_neighbors: None,
            }), // Add SPANN config
        };

        let result =
            Schema::merge_vector_index_config(&default_config, &user_config, KnnIndex::Hnsw);

        // Check field-level merging
        assert_eq!(result.space, Some(Space::L2)); // User override
        assert_eq!(
            result.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        ); // Default preserved
        assert_eq!(result.source_key, Some("user_key".to_string())); // User override

        // Check HNSW merging
        assert_eq!(result.hnsw.as_ref().unwrap().ef_construction, Some(300)); // User override
        assert_eq!(result.hnsw.as_ref().unwrap().max_neighbors, Some(16)); // Default preserved

        // Check SPANN is not present, since merging in the context of HNSW
        assert!(result.spann.is_none());
    }

    #[test]
    fn test_merge_sparse_vector_index_config() {
        // Test sparse vector index config merging
        let default_config = SparseVectorIndexConfig {
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
            source_key: Some("default_sparse_key".to_string()),
            bm25: None,
        };

        let user_config = SparseVectorIndexConfig {
            embedding_function: None,                        // Will use default
            source_key: Some("user_sparse_key".to_string()), // Override
            bm25: None,
        };

        let result = Schema::merge_sparse_vector_index_config(&default_config, &user_config);

        // Check user override
        assert_eq!(result.source_key, Some("user_sparse_key".to_string()));
        // Check default preserved
        assert_eq!(
            result.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );
    }

    #[test]
    fn test_complex_nested_merging_scenario() {
        // Test a complex scenario with multiple levels of merging
        let mut user_schema = Schema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        // Set up complex user defaults
        user_schema.defaults.string = Some(StringValueType {
            string_inverted_index: Some(StringInvertedIndexType {
                enabled: false,
                config: StringInvertedIndexConfig {},
            }),
            fts_index: Some(FtsIndexType {
                enabled: true,
                config: FtsIndexConfig {},
            }),
        });

        user_schema.defaults.float_list = Some(FloatListValueType {
            vector_index: Some(VectorIndexType {
                enabled: true,
                config: VectorIndexConfig {
                    space: Some(Space::Ip),
                    embedding_function: None, // Will use default
                    source_key: Some("custom_vector_key".to_string()),
                    hnsw: Some(HnswIndexConfig {
                        ef_construction: Some(400),
                        max_neighbors: Some(32),
                        ef_search: None, // Will use default
                        num_threads: None,
                        batch_size: None,
                        sync_threshold: None,
                        resize_factor: None,
                    }),
                    spann: None,
                },
            }),
        });

        // Set up key overrides
        let custom_key_override = ValueTypes {
            string: Some(StringValueType {
                fts_index: Some(FtsIndexType {
                    enabled: true,
                    config: FtsIndexConfig {},
                }),
                string_inverted_index: None,
            }),
            ..Default::default()
        };
        user_schema
            .keys
            .insert("custom_field".to_string(), custom_key_override);

        // Use HNSW defaults for this test so we have HNSW config to merge with
        let result = {
            let default_schema = Schema::new_default(KnnIndex::Hnsw);
            let merged_defaults = Schema::merge_value_types(
                &default_schema.defaults,
                &user_schema.defaults,
                KnnIndex::Hnsw,
            )
            .unwrap();
            let mut merged_keys = default_schema.keys.clone();
            for (key, user_value_types) in user_schema.keys {
                if let Some(default_value_types) = merged_keys.get(&key) {
                    let merged_value_types = Schema::merge_value_types(
                        default_value_types,
                        &user_value_types,
                        KnnIndex::Hnsw,
                    )
                    .unwrap();
                    merged_keys.insert(key, merged_value_types);
                } else {
                    merged_keys.insert(key, user_value_types);
                }
            }
            Schema {
                defaults: merged_defaults,
                keys: merged_keys,
            }
        };

        // Verify complex merging worked correctly

        // Check defaults merging
        assert!(
            !result
                .defaults
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );
        assert!(
            result
                .defaults
                .string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );

        let vector_config = &result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config;
        assert_eq!(vector_config.space, Some(Space::Ip));
        assert_eq!(vector_config.embedding_function, None); // Default preserved
        assert_eq!(
            vector_config.source_key,
            Some("custom_vector_key".to_string())
        );
        assert_eq!(
            vector_config.hnsw.as_ref().unwrap().ef_construction,
            Some(400)
        );
        assert_eq!(vector_config.hnsw.as_ref().unwrap().max_neighbors, Some(32));
        assert_eq!(
            vector_config.hnsw.as_ref().unwrap().ef_search,
            Some(default_search_ef())
        ); // Default preserved

        // Check key overrides
        assert!(result.keys.contains_key(EMBEDDING_KEY)); // Default preserved
        assert!(result.keys.contains_key(DOCUMENT_KEY)); // Default preserved
        assert!(result.keys.contains_key("custom_field")); // User added

        let custom_override = result.keys.get("custom_field").unwrap();
        assert!(
            custom_override
                .string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );
        assert!(custom_override
            .string
            .as_ref()
            .unwrap()
            .string_inverted_index
            .is_none());
    }

    #[test]
    fn test_reconcile_with_collection_config_default_config() {
        // Test that when collection config is default, schema is returned as-is
        let collection_config = InternalCollectionConfiguration::default_hnsw();
        let schema = Schema::try_from(&collection_config).unwrap();

        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();
        assert_eq!(result, schema);
    }

    // Test all 8 cases of double default scenarios
    #[test]
    fn test_reconcile_double_default_hnsw_config_hnsw_schema_default_knn_hnsw() {
        let collection_config = InternalCollectionConfiguration::default_hnsw();
        let schema = Schema::new_default(KnnIndex::Hnsw);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Should create new schema with default_knn_index (Hnsw)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_hnsw_config_hnsw_schema_default_knn_spann() {
        let collection_config = InternalCollectionConfiguration::default_hnsw();
        let schema = Schema::new_default(KnnIndex::Hnsw);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Should create new schema with default_knn_index (Spann)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_hnsw_config_spann_schema_default_knn_hnsw() {
        let collection_config = InternalCollectionConfiguration::default_hnsw();
        let schema = Schema::new_default(KnnIndex::Spann);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Should create new schema with default_knn_index (Hnsw)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_hnsw_config_spann_schema_default_knn_spann() {
        let collection_config = InternalCollectionConfiguration::default_hnsw();
        let schema = Schema::new_default(KnnIndex::Spann);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Should create new schema with default_knn_index (Spann)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_spann_config_spann_schema_default_knn_hnsw() {
        let collection_config = InternalCollectionConfiguration::default_spann();
        let schema = Schema::new_default(KnnIndex::Spann);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Should create new schema with default_knn_index (Hnsw)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_spann_config_spann_schema_default_knn_spann() {
        let collection_config = InternalCollectionConfiguration::default_spann();
        let schema = Schema::new_default(KnnIndex::Spann);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Should create new schema with default_knn_index (Spann)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_none());
        // Defaults should have source_key=None
        assert_eq!(
            result
                .defaults
                .float_list
                .as_ref()
                .unwrap()
                .vector_index
                .as_ref()
                .unwrap()
                .config
                .source_key,
            None
        );
    }

    #[test]
    fn test_reconcile_double_default_spann_config_hnsw_schema_default_knn_hnsw() {
        let collection_config = InternalCollectionConfiguration::default_spann();
        let schema = Schema::new_default(KnnIndex::Hnsw);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Should create new schema with default_knn_index (Hnsw)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_none());
    }

    #[test]
    fn test_reconcile_double_default_spann_config_hnsw_schema_default_knn_spann() {
        let collection_config = InternalCollectionConfiguration::default_spann();
        let schema = Schema::new_default(KnnIndex::Hnsw);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Should create new schema with default_knn_index (Spann)
        assert!(result.defaults.float_list.is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .spann
            .is_some());
        assert!(result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap()
            .config
            .hnsw
            .is_none());
    }

    #[test]
    fn test_defaults_source_key_not_document() {
        // Test that defaults.float_list.vector_index.config.source_key is None, not DOCUMENT_KEY
        let schema_hnsw = Schema::new_default(KnnIndex::Hnsw);
        let schema_spann = Schema::new_default(KnnIndex::Spann);

        // Check HNSW default schema
        let defaults_hnsw = schema_hnsw
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(defaults_hnsw.config.source_key, None);

        // Check Spann default schema
        let defaults_spann = schema_spann
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(defaults_spann.config.source_key, None);

        // Test after reconcile with NON-default collection config
        // This path calls try_from where our fix is
        let collection_config_hnsw = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                ef_construction: 300,
                max_neighbors: 32,
                ef_search: 50,
                num_threads: 8,
                batch_size: 200,
                sync_threshold: 2000,
                resize_factor: 1.5,
                space: Space::L2,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };
        let result_hnsw = Schema::reconcile_with_collection_config(
            &schema_hnsw,
            &collection_config_hnsw,
            KnnIndex::Hnsw,
        )
        .unwrap();
        let reconciled_defaults_hnsw = result_hnsw
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(reconciled_defaults_hnsw.config.source_key, None);

        let collection_config_spann = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                search_nprobe: 20,
                search_rng_factor: 3.0,
                search_rng_epsilon: 0.2,
                nreplica_count: 5,
                write_rng_factor: 2.0,
                write_rng_epsilon: 0.1,
                split_threshold: 2000,
                num_samples_kmeans: 200,
                initial_lambda: 0.8,
                reassign_neighbor_count: 100,
                merge_threshold: 800,
                num_centers_to_merge_to: 20,
                write_nprobe: 10,
                ef_construction: 400,
                ef_search: 60,
                max_neighbors: 24,
                space: Space::Cosine,
            }),
            embedding_function: None,
        };
        let result_spann = Schema::reconcile_with_collection_config(
            &schema_spann,
            &collection_config_spann,
            KnnIndex::Spann,
        )
        .unwrap();
        let reconciled_defaults_spann = result_spann
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(reconciled_defaults_spann.config.source_key, None);

        // Verify that #embedding key DOES have source_key set to DOCUMENT_KEY
        let embedding_hnsw = result_hnsw.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index_hnsw = embedding_hnsw
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(
            embedding_vector_index_hnsw.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );

        let embedding_spann = result_spann.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index_spann = embedding_spann
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(
            embedding_vector_index_spann.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );
    }

    #[test]
    fn test_try_from_source_key() {
        // Direct test of try_from to verify source_key behavior
        // Defaults should have source_key=None, #embedding should have source_key=DOCUMENT_KEY

        // Test with HNSW config
        let collection_config_hnsw = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                ef_construction: 300,
                max_neighbors: 32,
                ef_search: 50,
                num_threads: 8,
                batch_size: 200,
                sync_threshold: 2000,
                resize_factor: 1.5,
                space: Space::L2,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };
        let schema_hnsw = Schema::try_from(&collection_config_hnsw).unwrap();

        // Check defaults have source_key=None
        let defaults_hnsw = schema_hnsw
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(defaults_hnsw.config.source_key, None);

        // Check #embedding has source_key=DOCUMENT_KEY
        let embedding_hnsw = schema_hnsw.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index_hnsw = embedding_hnsw
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(
            embedding_vector_index_hnsw.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );

        // Test with Spann config
        let collection_config_spann = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                search_nprobe: 20,
                search_rng_factor: 3.0,
                search_rng_epsilon: 0.2,
                nreplica_count: 5,
                write_rng_factor: 2.0,
                write_rng_epsilon: 0.1,
                split_threshold: 2000,
                num_samples_kmeans: 200,
                initial_lambda: 0.8,
                reassign_neighbor_count: 100,
                merge_threshold: 800,
                num_centers_to_merge_to: 20,
                write_nprobe: 10,
                ef_construction: 400,
                ef_search: 60,
                max_neighbors: 24,
                space: Space::Cosine,
            }),
            embedding_function: None,
        };
        let schema_spann = Schema::try_from(&collection_config_spann).unwrap();

        // Check defaults have source_key=None
        let defaults_spann = schema_spann
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(defaults_spann.config.source_key, None);

        // Check #embedding has source_key=DOCUMENT_KEY
        let embedding_spann = schema_spann.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index_spann = embedding_spann
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(
            embedding_vector_index_spann.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );
    }

    #[test]
    fn test_default_hnsw_with_default_embedding_function() {
        // Test that when InternalCollectionConfiguration is default HNSW but has
        // an embedding function with name "default" and config as {}, it still
        // goes through the double default path and preserves source_key behavior
        use crate::collection_configuration::EmbeddingFunctionNewConfiguration;

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default()),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "default".to_string(),
                    config: serde_json::json!({}),
                },
            )),
        };

        // Verify it's still considered default
        assert!(collection_config.is_default());

        let schema = Schema::new_default(KnnIndex::Hnsw);
        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Check that defaults have source_key=None
        let defaults = result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(defaults.config.source_key, None);

        // Check that #embedding has source_key=DOCUMENT_KEY
        let embedding = result.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index = embedding
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert_eq!(
            embedding_vector_index.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );

        // verify vector index config is set to spann
        let vector_index_config = defaults.config.clone();
        assert!(vector_index_config.spann.is_some());
        assert!(vector_index_config.hnsw.is_none());

        // Verify embedding function was set correctly
        assert_eq!(
            embedding_vector_index.config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "default".to_string(),
                    config: serde_json::json!({}),
                },
            ))
        );
        assert_eq!(
            defaults.config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "default".to_string(),
                    config: serde_json::json!({}),
                },
            ))
        );
    }

    #[test]
    fn test_reconcile_with_collection_config_both_non_default() {
        // Test that when both schema and collection config are non-default, it returns an error
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        schema.defaults.string = Some(StringValueType {
            fts_index: Some(FtsIndexType {
                enabled: true,
                config: FtsIndexConfig {},
            }),
            string_inverted_index: None,
        });

        let mut collection_config = InternalCollectionConfiguration::default_hnsw();
        // Make collection config non-default by changing a parameter
        if let VectorIndexConfiguration::Hnsw(ref mut hnsw_config) = collection_config.vector_index
        {
            hnsw_config.ef_construction = 500; // Non-default value
        }

        // Use reconcile_schema_and_config which has the early validation
        let result = Schema::reconcile_schema_and_config(
            Some(&schema),
            Some(&collection_config),
            KnnIndex::Spann,
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::ConfigAndSchemaConflict
        ));
    }

    #[test]
    fn test_reconcile_with_collection_config_hnsw_override() {
        // Test that non-default HNSW collection config overrides default schema
        let schema = Schema::new_default(KnnIndex::Hnsw); // Use actual default schema

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                ef_construction: 300,
                max_neighbors: 32,
                ef_search: 50,
                num_threads: 8,
                batch_size: 200,
                sync_threshold: 2000,
                resize_factor: 1.5,
                space: Space::L2,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };

        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Check that #embedding key override was created with the collection config settings
        let embedding_override = result.keys.get(EMBEDDING_KEY).unwrap();
        let vector_index = embedding_override
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();

        assert!(vector_index.enabled);
        assert_eq!(vector_index.config.space, Some(Space::L2));
        assert_eq!(
            vector_index.config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );
        assert_eq!(
            vector_index.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );

        let hnsw_config = vector_index.config.hnsw.as_ref().unwrap();
        assert_eq!(hnsw_config.ef_construction, Some(300));
        assert_eq!(hnsw_config.max_neighbors, Some(32));
        assert_eq!(hnsw_config.ef_search, Some(50));
        assert_eq!(hnsw_config.num_threads, Some(8));
        assert_eq!(hnsw_config.batch_size, Some(200));
        assert_eq!(hnsw_config.sync_threshold, Some(2000));
        assert_eq!(hnsw_config.resize_factor, Some(1.5));

        assert!(vector_index.config.spann.is_none());
    }

    #[test]
    fn test_reconcile_with_collection_config_spann_override() {
        // Test that non-default SPANN collection config overrides default schema
        let schema = Schema::new_default(KnnIndex::Spann); // Use actual default schema

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                search_nprobe: 20,
                search_rng_factor: 3.0,
                search_rng_epsilon: 0.2,
                nreplica_count: 5,
                write_rng_factor: 2.0,
                write_rng_epsilon: 0.1,
                split_threshold: 2000,
                num_samples_kmeans: 200,
                initial_lambda: 0.8,
                reassign_neighbor_count: 100,
                merge_threshold: 800,
                num_centers_to_merge_to: 20,
                write_nprobe: 10,
                ef_construction: 400,
                ef_search: 60,
                max_neighbors: 24,
                space: Space::Cosine,
            }),
            embedding_function: None,
        };

        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Spann)
                .unwrap();

        // Check that #embedding key override was created with the collection config settings
        let embedding_override = result.keys.get(EMBEDDING_KEY).unwrap();
        let vector_index = embedding_override
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();

        assert!(vector_index.enabled);
        assert_eq!(vector_index.config.space, Some(Space::Cosine));
        assert_eq!(vector_index.config.embedding_function, None);
        assert_eq!(
            vector_index.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );

        assert!(vector_index.config.hnsw.is_none());

        let spann_config = vector_index.config.spann.as_ref().unwrap();
        assert_eq!(spann_config.search_nprobe, Some(20));
        assert_eq!(spann_config.search_rng_factor, Some(3.0));
        assert_eq!(spann_config.search_rng_epsilon, Some(0.2));
        assert_eq!(spann_config.nreplica_count, Some(5));
        assert_eq!(spann_config.write_rng_factor, Some(2.0));
        assert_eq!(spann_config.write_rng_epsilon, Some(0.1));
        assert_eq!(spann_config.split_threshold, Some(2000));
        assert_eq!(spann_config.num_samples_kmeans, Some(200));
        assert_eq!(spann_config.initial_lambda, Some(0.8));
        assert_eq!(spann_config.reassign_neighbor_count, Some(100));
        assert_eq!(spann_config.merge_threshold, Some(800));
        assert_eq!(spann_config.num_centers_to_merge_to, Some(20));
        assert_eq!(spann_config.write_nprobe, Some(10));
        assert_eq!(spann_config.ef_construction, Some(400));
        assert_eq!(spann_config.ef_search, Some(60));
        assert_eq!(spann_config.max_neighbors, Some(24));
    }

    #[test]
    fn test_reconcile_with_collection_config_updates_both_defaults_and_embedding() {
        // Test that collection config updates BOTH defaults.float_list.vector_index
        // AND keys["embedding"].float_list.vector_index
        let schema = Schema::new_default(KnnIndex::Hnsw);

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                ef_construction: 300,
                max_neighbors: 32,
                ef_search: 50,
                num_threads: 8,
                batch_size: 200,
                sync_threshold: 2000,
                resize_factor: 1.5,
                space: Space::L2,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };

        let result =
            Schema::reconcile_with_collection_config(&schema, &collection_config, KnnIndex::Hnsw)
                .unwrap();

        // Check that defaults.float_list.vector_index was updated
        let defaults_vector_index = result
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();

        // Should be disabled in defaults (template for new keys)
        assert!(!defaults_vector_index.enabled);
        // But config should be updated
        assert_eq!(defaults_vector_index.config.space, Some(Space::L2));
        assert_eq!(
            defaults_vector_index.config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );
        assert_eq!(defaults_vector_index.config.source_key, None);
        let defaults_hnsw = defaults_vector_index.config.hnsw.as_ref().unwrap();
        assert_eq!(defaults_hnsw.ef_construction, Some(300));
        assert_eq!(defaults_hnsw.max_neighbors, Some(32));

        // Check that #embedding key override was also updated
        let embedding_override = result.keys.get(EMBEDDING_KEY).unwrap();
        let embedding_vector_index = embedding_override
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();

        // Should be enabled on #embedding
        assert!(embedding_vector_index.enabled);
        // Config should match defaults
        assert_eq!(embedding_vector_index.config.space, Some(Space::L2));
        assert_eq!(
            embedding_vector_index.config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );
        assert_eq!(
            embedding_vector_index.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );
        let embedding_hnsw = embedding_vector_index.config.hnsw.as_ref().unwrap();
        assert_eq!(embedding_hnsw.ef_construction, Some(300));
        assert_eq!(embedding_hnsw.max_neighbors, Some(32));
    }

    #[test]
    fn test_is_schema_default() {
        // Test that actual default schemas are correctly identified
        let default_hnsw_schema = Schema::new_default(KnnIndex::Hnsw);
        assert!(default_hnsw_schema.is_default());

        let default_spann_schema = Schema::new_default(KnnIndex::Spann);
        assert!(default_spann_schema.is_default());

        // Test that a modified default schema is not considered default
        let mut modified_schema = Schema::new_default(KnnIndex::Hnsw);
        // Make a clear modification - change the string inverted index enabled state
        if let Some(ref mut string_type) = modified_schema.defaults.string {
            if let Some(ref mut string_inverted) = string_type.string_inverted_index {
                string_inverted.enabled = false; // Default is true, so this should make it non-default
            }
        }
        assert!(!modified_schema.is_default());

        // Test that schema with additional key overrides is not default
        let mut schema_with_extra_overrides = Schema::new_default(KnnIndex::Hnsw);
        schema_with_extra_overrides
            .keys
            .insert("custom_key".to_string(), ValueTypes::default());
        assert!(!schema_with_extra_overrides.is_default());
    }

    #[test]
    fn test_is_schema_default_with_space() {
        let schema = Schema::new_default(KnnIndex::Hnsw);
        assert!(schema.is_default());

        let mut schema_with_space = Schema::new_default(KnnIndex::Hnsw);
        if let Some(ref mut float_list) = schema_with_space.defaults.float_list {
            if let Some(ref mut vector_index) = float_list.vector_index {
                vector_index.config.space = Some(Space::Cosine);
            }
        }
        assert!(!schema_with_space.is_default());

        let mut schema_with_space_in_embedding_key = Schema::new_default(KnnIndex::Spann);
        if let Some(ref mut embedding_key) = schema_with_space_in_embedding_key
            .keys
            .get_mut(EMBEDDING_KEY)
        {
            if let Some(ref mut float_list) = embedding_key.float_list {
                if let Some(ref mut vector_index) = float_list.vector_index {
                    vector_index.config.space = Some(Space::Cosine);
                }
            }
        }
        assert!(!schema_with_space_in_embedding_key.is_default());
    }

    #[test]
    fn test_is_schema_default_with_embedding_function() {
        let schema = Schema::new_default(KnnIndex::Hnsw);
        assert!(schema.is_default());

        let mut schema_with_embedding_function = Schema::new_default(KnnIndex::Hnsw);
        if let Some(ref mut float_list) = schema_with_embedding_function.defaults.float_list {
            if let Some(ref mut vector_index) = float_list.vector_index {
                vector_index.config.embedding_function =
                    Some(EmbeddingFunctionConfiguration::Legacy);
            }
        }
        assert!(!schema_with_embedding_function.is_default());

        let mut schema_with_embedding_function_in_embedding_key =
            Schema::new_default(KnnIndex::Spann);
        if let Some(ref mut embedding_key) = schema_with_embedding_function_in_embedding_key
            .keys
            .get_mut(EMBEDDING_KEY)
        {
            if let Some(ref mut float_list) = embedding_key.float_list {
                if let Some(ref mut vector_index) = float_list.vector_index {
                    vector_index.config.embedding_function =
                        Some(EmbeddingFunctionConfiguration::Legacy);
                }
            }
        }
        assert!(!schema_with_embedding_function_in_embedding_key.is_default());
    }

    #[test]
    fn test_add_merges_keys_by_value_type() {
        let mut schema_a = Schema::new_default(KnnIndex::Hnsw);
        let mut schema_b = Schema::new_default(KnnIndex::Hnsw);

        let string_override = ValueTypes {
            string: Some(StringValueType {
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: true,
                    config: StringInvertedIndexConfig {},
                }),
                fts_index: None,
            }),
            ..Default::default()
        };
        schema_a
            .keys
            .insert("custom_field".to_string(), string_override);

        let float_override = ValueTypes {
            float: Some(FloatValueType {
                float_inverted_index: Some(FloatInvertedIndexType {
                    enabled: true,
                    config: FloatInvertedIndexConfig {},
                }),
            }),
            ..Default::default()
        };
        schema_b
            .keys
            .insert("custom_field".to_string(), float_override);

        let merged = schema_a.merge(&schema_b).unwrap();
        let merged_override = merged.keys.get("custom_field").unwrap();

        assert!(merged_override.string.is_some());
        assert!(merged_override.float.is_some());
        assert!(
            merged_override
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );
        assert!(
            merged_override
                .float
                .as_ref()
                .unwrap()
                .float_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn test_add_rejects_different_defaults() {
        let schema_a = Schema::new_default(KnnIndex::Hnsw);
        let mut schema_b = Schema::new_default(KnnIndex::Hnsw);

        if let Some(string_type) = schema_b.defaults.string.as_mut() {
            if let Some(string_index) = string_type.string_inverted_index.as_mut() {
                string_index.enabled = false;
            }
        }

        let err = schema_a.merge(&schema_b).unwrap_err();
        assert!(matches!(err, SchemaError::DefaultsMismatch));
    }

    #[test]
    fn test_add_detects_conflicting_value_type_configuration() {
        let mut schema_a = Schema::new_default(KnnIndex::Hnsw);
        let mut schema_b = Schema::new_default(KnnIndex::Hnsw);

        let string_override_enabled = ValueTypes {
            string: Some(StringValueType {
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: true,
                    config: StringInvertedIndexConfig {},
                }),
                fts_index: None,
            }),
            ..Default::default()
        };
        schema_a
            .keys
            .insert("custom_field".to_string(), string_override_enabled);

        let string_override_disabled = ValueTypes {
            string: Some(StringValueType {
                string_inverted_index: Some(StringInvertedIndexType {
                    enabled: false,
                    config: StringInvertedIndexConfig {},
                }),
                fts_index: None,
            }),
            ..Default::default()
        };
        schema_b
            .keys
            .insert("custom_field".to_string(), string_override_disabled);

        let err = schema_a.merge(&schema_b).unwrap_err();
        assert!(matches!(err, SchemaError::ConfigurationConflict { .. }));
    }

    // TODO(Sanket): Remove this test once deployed
    #[test]
    fn test_backward_compatibility_aliases() {
        // Test that old format with # and $ prefixes and key_overrides can be deserialized
        let old_format_json = r###"{
            "defaults": {
                "#string": {
                    "$fts_index": {
                        "enabled": true,
                        "config": {}
                    }
                },
                "#int": {
                    "$int_inverted_index": {
                        "enabled": true,
                        "config": {}
                    }
                },
                "#float_list": {
                    "$vector_index": {
                        "enabled": true,
                        "config": {
                            "spann": {
                                "search_nprobe": 10
                            }
                        }
                    }
                }
            },
            "key_overrides": {
                "#document": {
                    "#string": {
                        "$fts_index": {
                            "enabled": false,
                            "config": {}
                        }
                    }
                }
            }
        }"###;

        let schema_from_old: Schema = serde_json::from_str(old_format_json).unwrap();

        // Test that new format without prefixes and keys can be deserialized
        let new_format_json = r###"{
            "defaults": {
                "string": {
                    "fts_index": {
                        "enabled": true,
                        "config": {}
                    }
                },
                "int": {
                    "int_inverted_index": {
                        "enabled": true,
                        "config": {}
                    }
                },
                "float_list": {
                    "vector_index": {
                        "enabled": true,
                        "config": {
                            "spann": {
                                "search_nprobe": 10
                            }
                        }
                    }
                }
            },
            "keys": {
                "#document": {
                    "string": {
                        "fts_index": {
                            "enabled": false,
                            "config": {}
                        }
                    }
                }
            }
        }"###;

        let schema_from_new: Schema = serde_json::from_str(new_format_json).unwrap();

        // Both should deserialize to the same structure
        assert_eq!(schema_from_old, schema_from_new);

        // Verify the deserialized content is correct
        assert!(schema_from_old.defaults.string.is_some());
        assert!(schema_from_old
            .defaults
            .string
            .as_ref()
            .unwrap()
            .fts_index
            .is_some());
        assert!(
            schema_from_old
                .defaults
                .string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );

        assert!(schema_from_old.defaults.int.is_some());
        assert!(schema_from_old
            .defaults
            .int
            .as_ref()
            .unwrap()
            .int_inverted_index
            .is_some());

        assert!(schema_from_old.defaults.float_list.is_some());
        assert!(schema_from_old
            .defaults
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .is_some());

        assert!(schema_from_old.keys.contains_key(DOCUMENT_KEY));
        let doc_override = schema_from_old.keys.get(DOCUMENT_KEY).unwrap();
        assert!(doc_override.string.is_some());
        assert!(
            !doc_override
                .string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Test that serialization always outputs the new format (without prefixes)
        let serialized = serde_json::to_string(&schema_from_old).unwrap();

        // Should contain new format keys
        assert!(serialized.contains(r#""keys":"#));
        assert!(serialized.contains(r#""string":"#));
        assert!(serialized.contains(r#""fts_index":"#));
        assert!(serialized.contains(r#""int_inverted_index":"#));
        assert!(serialized.contains(r#""vector_index":"#));

        // Should NOT contain old format keys
        assert!(!serialized.contains(r#""key_overrides":"#));
        assert!(!serialized.contains(r###""#string":"###));
        assert!(!serialized.contains(r###""$fts_index":"###));
        assert!(!serialized.contains(r###""$int_inverted_index":"###));
        assert!(!serialized.contains(r###""$vector_index":"###));
    }

    #[test]
    fn test_hnsw_index_config_validation() {
        use validator::Validate;

        // Valid configuration - should pass
        let valid_config = HnswIndexConfig {
            batch_size: Some(10),
            sync_threshold: Some(100),
            ef_construction: Some(100),
            max_neighbors: Some(16),
            ..Default::default()
        };
        assert!(valid_config.validate().is_ok());

        // Invalid: batch_size too small (min 2)
        let invalid_batch_size = HnswIndexConfig {
            batch_size: Some(1),
            ..Default::default()
        };
        assert!(invalid_batch_size.validate().is_err());

        // Invalid: sync_threshold too small (min 2)
        let invalid_sync_threshold = HnswIndexConfig {
            sync_threshold: Some(1),
            ..Default::default()
        };
        assert!(invalid_sync_threshold.validate().is_err());

        // Valid: boundary values (exactly 2) should pass
        let boundary_config = HnswIndexConfig {
            batch_size: Some(2),
            sync_threshold: Some(2),
            ..Default::default()
        };
        assert!(boundary_config.validate().is_ok());

        // Valid: None values should pass validation
        let all_none_config = HnswIndexConfig {
            ..Default::default()
        };
        assert!(all_none_config.validate().is_ok());

        // Valid: fields without validation can be any value
        let other_fields_config = HnswIndexConfig {
            ef_construction: Some(1),
            max_neighbors: Some(1),
            ef_search: Some(1),
            num_threads: Some(1),
            resize_factor: Some(0.1),
            ..Default::default()
        };
        assert!(other_fields_config.validate().is_ok());
    }

    #[test]
    fn test_spann_index_config_validation() {
        use validator::Validate;

        // Valid configuration - should pass
        let valid_config = SpannIndexConfig {
            write_nprobe: Some(32),
            nreplica_count: Some(4),
            split_threshold: Some(100),
            merge_threshold: Some(50),
            reassign_neighbor_count: Some(32),
            num_centers_to_merge_to: Some(4),
            ef_construction: Some(100),
            ef_search: Some(100),
            max_neighbors: Some(32),
            search_rng_factor: Some(1.0),
            write_rng_factor: Some(1.0),
            search_rng_epsilon: Some(7.5),
            write_rng_epsilon: Some(7.5),
            ..Default::default()
        };
        assert!(valid_config.validate().is_ok());

        // Invalid: write_nprobe too large (max 64)
        let invalid_write_nprobe = SpannIndexConfig {
            write_nprobe: Some(200),
            ..Default::default()
        };
        assert!(invalid_write_nprobe.validate().is_err());

        // Invalid: split_threshold too small (min 50)
        let invalid_split_threshold = SpannIndexConfig {
            split_threshold: Some(10),
            ..Default::default()
        };
        assert!(invalid_split_threshold.validate().is_err());

        // Invalid: split_threshold too large (max 200)
        let invalid_split_threshold_high = SpannIndexConfig {
            split_threshold: Some(250),
            ..Default::default()
        };
        assert!(invalid_split_threshold_high.validate().is_err());

        // Invalid: nreplica_count too large (max 8)
        let invalid_nreplica = SpannIndexConfig {
            nreplica_count: Some(10),
            ..Default::default()
        };
        assert!(invalid_nreplica.validate().is_err());

        // Invalid: reassign_neighbor_count too large (max 64)
        let invalid_reassign = SpannIndexConfig {
            reassign_neighbor_count: Some(100),
            ..Default::default()
        };
        assert!(invalid_reassign.validate().is_err());

        // Invalid: merge_threshold out of range (min 25, max 100)
        let invalid_merge_threshold_low = SpannIndexConfig {
            merge_threshold: Some(5),
            ..Default::default()
        };
        assert!(invalid_merge_threshold_low.validate().is_err());

        let invalid_merge_threshold_high = SpannIndexConfig {
            merge_threshold: Some(150),
            ..Default::default()
        };
        assert!(invalid_merge_threshold_high.validate().is_err());

        // Invalid: num_centers_to_merge_to too large (max 8)
        let invalid_num_centers = SpannIndexConfig {
            num_centers_to_merge_to: Some(10),
            ..Default::default()
        };
        assert!(invalid_num_centers.validate().is_err());

        // Invalid: ef_construction too large (max 200)
        let invalid_ef_construction = SpannIndexConfig {
            ef_construction: Some(300),
            ..Default::default()
        };
        assert!(invalid_ef_construction.validate().is_err());

        // Invalid: ef_search too large (max 200)
        let invalid_ef_search = SpannIndexConfig {
            ef_search: Some(300),
            ..Default::default()
        };
        assert!(invalid_ef_search.validate().is_err());

        // Invalid: max_neighbors too large (max 64)
        let invalid_max_neighbors = SpannIndexConfig {
            max_neighbors: Some(100),
            ..Default::default()
        };
        assert!(invalid_max_neighbors.validate().is_err());

        // Invalid: search_nprobe too large (max 128)
        let invalid_search_nprobe = SpannIndexConfig {
            search_nprobe: Some(200),
            ..Default::default()
        };
        assert!(invalid_search_nprobe.validate().is_err());

        // Invalid: search_rng_factor not exactly 1.0 (min 1.0, max 1.0)
        let invalid_search_rng_factor_low = SpannIndexConfig {
            search_rng_factor: Some(0.9),
            ..Default::default()
        };
        assert!(invalid_search_rng_factor_low.validate().is_err());

        let invalid_search_rng_factor_high = SpannIndexConfig {
            search_rng_factor: Some(1.1),
            ..Default::default()
        };
        assert!(invalid_search_rng_factor_high.validate().is_err());

        // Valid: search_rng_factor exactly 1.0
        let valid_search_rng_factor = SpannIndexConfig {
            search_rng_factor: Some(1.0),
            ..Default::default()
        };
        assert!(valid_search_rng_factor.validate().is_ok());

        // Invalid: search_rng_epsilon out of range (min 5.0, max 10.0)
        let invalid_search_rng_epsilon_low = SpannIndexConfig {
            search_rng_epsilon: Some(4.0),
            ..Default::default()
        };
        assert!(invalid_search_rng_epsilon_low.validate().is_err());

        let invalid_search_rng_epsilon_high = SpannIndexConfig {
            search_rng_epsilon: Some(11.0),
            ..Default::default()
        };
        assert!(invalid_search_rng_epsilon_high.validate().is_err());

        // Valid: search_rng_epsilon within range
        let valid_search_rng_epsilon = SpannIndexConfig {
            search_rng_epsilon: Some(7.5),
            ..Default::default()
        };
        assert!(valid_search_rng_epsilon.validate().is_ok());

        // Invalid: write_rng_factor not exactly 1.0 (min 1.0, max 1.0)
        let invalid_write_rng_factor_low = SpannIndexConfig {
            write_rng_factor: Some(0.9),
            ..Default::default()
        };
        assert!(invalid_write_rng_factor_low.validate().is_err());

        let invalid_write_rng_factor_high = SpannIndexConfig {
            write_rng_factor: Some(1.1),
            ..Default::default()
        };
        assert!(invalid_write_rng_factor_high.validate().is_err());

        // Valid: write_rng_factor exactly 1.0
        let valid_write_rng_factor = SpannIndexConfig {
            write_rng_factor: Some(1.0),
            ..Default::default()
        };
        assert!(valid_write_rng_factor.validate().is_ok());

        // Invalid: write_rng_epsilon out of range (min 5.0, max 10.0)
        let invalid_write_rng_epsilon_low = SpannIndexConfig {
            write_rng_epsilon: Some(4.0),
            ..Default::default()
        };
        assert!(invalid_write_rng_epsilon_low.validate().is_err());

        let invalid_write_rng_epsilon_high = SpannIndexConfig {
            write_rng_epsilon: Some(11.0),
            ..Default::default()
        };
        assert!(invalid_write_rng_epsilon_high.validate().is_err());

        // Valid: write_rng_epsilon within range
        let valid_write_rng_epsilon = SpannIndexConfig {
            write_rng_epsilon: Some(7.5),
            ..Default::default()
        };
        assert!(valid_write_rng_epsilon.validate().is_ok());

        // Invalid: num_samples_kmeans too large (max 1000)
        let invalid_num_samples_kmeans = SpannIndexConfig {
            num_samples_kmeans: Some(1500),
            ..Default::default()
        };
        assert!(invalid_num_samples_kmeans.validate().is_err());

        // Valid: num_samples_kmeans within range
        let valid_num_samples_kmeans = SpannIndexConfig {
            num_samples_kmeans: Some(500),
            ..Default::default()
        };
        assert!(valid_num_samples_kmeans.validate().is_ok());

        // Invalid: initial_lambda not exactly 100.0 (min 100.0, max 100.0)
        let invalid_initial_lambda_high = SpannIndexConfig {
            initial_lambda: Some(150.0),
            ..Default::default()
        };
        assert!(invalid_initial_lambda_high.validate().is_err());

        let invalid_initial_lambda_low = SpannIndexConfig {
            initial_lambda: Some(50.0),
            ..Default::default()
        };
        assert!(invalid_initial_lambda_low.validate().is_err());

        // Valid: initial_lambda exactly 100.0
        let valid_initial_lambda = SpannIndexConfig {
            initial_lambda: Some(100.0),
            ..Default::default()
        };
        assert!(valid_initial_lambda.validate().is_ok());

        // Valid: None values should pass validation
        let all_none_config = SpannIndexConfig {
            ..Default::default()
        };
        assert!(all_none_config.validate().is_ok());
    }

    #[test]
    fn test_builder_pattern_crud_workflow() {
        // Test comprehensive CRUD workflow using the builder pattern

        // CREATE: Build a schema with multiple indexes
        let schema = Schema::new_default(KnnIndex::Hnsw)
            .create_index(
                None,
                IndexConfig::Vector(VectorIndexConfig {
                    space: Some(Space::Cosine),
                    embedding_function: None,
                    source_key: None,
                    hnsw: Some(HnswIndexConfig {
                        ef_construction: Some(200),
                        max_neighbors: Some(32),
                        ef_search: Some(50),
                        num_threads: None,
                        batch_size: None,
                        sync_threshold: None,
                        resize_factor: None,
                    }),
                    spann: None,
                }),
            )
            .expect("vector config should succeed")
            .create_index(
                Some("category"),
                IndexConfig::StringInverted(StringInvertedIndexConfig {}),
            )
            .expect("string inverted on key should succeed")
            .create_index(
                Some("year"),
                IndexConfig::IntInverted(IntInvertedIndexConfig {}),
            )
            .expect("int inverted on key should succeed")
            .create_index(
                Some("rating"),
                IndexConfig::FloatInverted(FloatInvertedIndexConfig {}),
            )
            .expect("float inverted on key should succeed")
            .create_index(
                Some("is_active"),
                IndexConfig::BoolInverted(BoolInvertedIndexConfig {}),
            )
            .expect("bool inverted on key should succeed");

        // READ: Verify the schema was built correctly
        // Check vector config
        assert!(schema.keys.contains_key(EMBEDDING_KEY));
        let embedding = schema.keys.get(EMBEDDING_KEY).unwrap();
        assert!(embedding.float_list.is_some());
        let vector_index = embedding
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert!(vector_index.enabled);
        assert_eq!(vector_index.config.space, Some(Space::Cosine));
        assert_eq!(
            vector_index.config.hnsw.as_ref().unwrap().ef_construction,
            Some(200)
        );

        // Check per-key indexes
        assert!(schema.keys.contains_key("category"));
        assert!(schema.keys.contains_key("year"));
        assert!(schema.keys.contains_key("rating"));
        assert!(schema.keys.contains_key("is_active"));

        // Verify category string inverted index
        let category = schema.keys.get("category").unwrap();
        assert!(category.string.is_some());
        let string_idx = category
            .string
            .as_ref()
            .unwrap()
            .string_inverted_index
            .as_ref()
            .unwrap();
        assert!(string_idx.enabled);

        // Verify year int inverted index
        let year = schema.keys.get("year").unwrap();
        assert!(year.int.is_some());
        let int_idx = year
            .int
            .as_ref()
            .unwrap()
            .int_inverted_index
            .as_ref()
            .unwrap();
        assert!(int_idx.enabled);

        // UPDATE/DELETE: Disable some indexes
        let schema = schema
            .delete_index(
                Some("category"),
                IndexConfig::StringInverted(StringInvertedIndexConfig {}),
            )
            .expect("delete string inverted should succeed")
            .delete_index(
                Some("year"),
                IndexConfig::IntInverted(IntInvertedIndexConfig {}),
            )
            .expect("delete int inverted should succeed");

        // VERIFY DELETE: Check that indexes were disabled
        let category = schema.keys.get("category").unwrap();
        let string_idx = category
            .string
            .as_ref()
            .unwrap()
            .string_inverted_index
            .as_ref()
            .unwrap();
        assert!(!string_idx.enabled); // Should be disabled now

        let year = schema.keys.get("year").unwrap();
        let int_idx = year
            .int
            .as_ref()
            .unwrap()
            .int_inverted_index
            .as_ref()
            .unwrap();
        assert!(!int_idx.enabled); // Should be disabled now

        // Verify other indexes still enabled
        let rating = schema.keys.get("rating").unwrap();
        let float_idx = rating
            .float
            .as_ref()
            .unwrap()
            .float_inverted_index
            .as_ref()
            .unwrap();
        assert!(float_idx.enabled); // Should still be enabled

        let is_active = schema.keys.get("is_active").unwrap();
        let bool_idx = is_active
            .boolean
            .as_ref()
            .unwrap()
            .bool_inverted_index
            .as_ref()
            .unwrap();
        assert!(bool_idx.enabled); // Should still be enabled
    }

    #[test]
    fn test_builder_create_index_validation_errors() {
        // Test all validation errors for create_index() as documented in the docstring:
        // - Attempting to create index on special keys (#document, #embedding)
        // - Invalid configuration (e.g., vector index on non-embedding key)
        // - Conflicting with existing indexes (e.g., multiple sparse vector indexes)

        // Error: Vector index on specific key (must be global)
        let result = Schema::new_default(KnnIndex::Hnsw).create_index(
            Some("my_vectors"),
            IndexConfig::Vector(VectorIndexConfig {
                space: Some(Space::L2),
                embedding_function: None,
                source_key: None,
                hnsw: None,
                spann: None,
            }),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::VectorIndexMustBeGlobal { key } if key == "my_vectors"
        ));

        // Error: FTS index on specific key (must be global)
        let result = Schema::new_default(KnnIndex::Hnsw)
            .create_index(Some("my_text"), IndexConfig::Fts(FtsIndexConfig {}));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::FtsIndexMustBeGlobal { key } if key == "my_text"
        ));

        // Error: Cannot create index on special key #document
        let result = Schema::new_default(KnnIndex::Hnsw).create_index(
            Some(DOCUMENT_KEY),
            IndexConfig::StringInverted(StringInvertedIndexConfig {}),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SpecialKeyModificationNotAllowed { .. }
        ));

        // Error: Cannot create index on special key #embedding
        let result = Schema::new_default(KnnIndex::Hnsw).create_index(
            Some(EMBEDDING_KEY),
            IndexConfig::IntInverted(IntInvertedIndexConfig {}),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SpecialKeyModificationNotAllowed { .. }
        ));

        // Error: Sparse vector without key (must specify key)
        let result = Schema::new_default(KnnIndex::Hnsw).create_index(
            None,
            IndexConfig::SparseVector(SparseVectorIndexConfig {
                embedding_function: None,
                source_key: None,
                bm25: None,
            }),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SparseVectorRequiresKey
        ));

        // Error: Multiple sparse vector indexes (only one allowed per collection)
        let result = Schema::new_default(KnnIndex::Hnsw)
            .create_index(
                Some("sparse1"),
                IndexConfig::SparseVector(SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                }),
            )
            .expect("first sparse should succeed")
            .create_index(
                Some("sparse2"),
                IndexConfig::SparseVector(SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                }),
            );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::MultipleSparseVectorIndexes { existing_key } if existing_key == "sparse1"
        ));
    }

    #[test]
    fn test_builder_delete_index_validation_errors() {
        // Test all validation errors for delete_index() as documented in the docstring:
        // - Attempting to delete index on special keys (#document, #embedding)
        // - Attempting to delete vector, FTS, or sparse vector indexes (not currently supported)

        // Error: Delete on special key #embedding
        let result = Schema::new_default(KnnIndex::Hnsw).delete_index(
            Some(EMBEDDING_KEY),
            IndexConfig::StringInverted(StringInvertedIndexConfig {}),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SpecialKeyModificationNotAllowed { .. }
        ));

        // Error: Delete on special key #document
        let result = Schema::new_default(KnnIndex::Hnsw).delete_index(
            Some(DOCUMENT_KEY),
            IndexConfig::IntInverted(IntInvertedIndexConfig {}),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SpecialKeyModificationNotAllowed { .. }
        ));

        // Error: Delete vector index (not currently supported)
        let result = Schema::new_default(KnnIndex::Hnsw).delete_index(
            None,
            IndexConfig::Vector(VectorIndexConfig {
                space: None,
                embedding_function: None,
                source_key: None,
                hnsw: None,
                spann: None,
            }),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::VectorIndexDeletionNotSupported
        ));

        // Error: Delete FTS index (not currently supported)
        let result = Schema::new_default(KnnIndex::Hnsw)
            .delete_index(None, IndexConfig::Fts(FtsIndexConfig {}));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::FtsIndexDeletionNotSupported
        ));

        // Error: Delete sparse vector index (not currently supported)
        let result = Schema::new_default(KnnIndex::Hnsw)
            .create_index(
                Some("sparse"),
                IndexConfig::SparseVector(SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                }),
            )
            .expect("create should succeed")
            .delete_index(
                Some("sparse"),
                IndexConfig::SparseVector(SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                }),
            );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaBuilderError::SparseVectorIndexDeletionNotSupported
        ));
    }

    #[test]
    fn test_builder_pattern_chaining() {
        // Test complex chaining scenario
        let schema = Schema::new_default(KnnIndex::Hnsw)
            .create_index(Some("tag1"), StringInvertedIndexConfig {}.into())
            .unwrap()
            .create_index(Some("tag2"), StringInvertedIndexConfig {}.into())
            .unwrap()
            .create_index(Some("tag3"), StringInvertedIndexConfig {}.into())
            .unwrap()
            .create_index(Some("count"), IntInvertedIndexConfig {}.into())
            .unwrap()
            .delete_index(Some("tag2"), StringInvertedIndexConfig {}.into())
            .unwrap()
            .create_index(Some("score"), FloatInvertedIndexConfig {}.into())
            .unwrap();

        // Verify tag1 is enabled
        assert!(
            schema
                .keys
                .get("tag1")
                .unwrap()
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Verify tag2 is disabled
        assert!(
            !schema
                .keys
                .get("tag2")
                .unwrap()
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Verify tag3 is enabled
        assert!(
            schema
                .keys
                .get("tag3")
                .unwrap()
                .string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Verify count is enabled
        assert!(
            schema
                .keys
                .get("count")
                .unwrap()
                .int
                .as_ref()
                .unwrap()
                .int_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Verify score is enabled
        assert!(
            schema
                .keys
                .get("score")
                .unwrap()
                .float
                .as_ref()
                .unwrap()
                .float_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );
    }

    #[test]
    fn test_schema_default_matches_python() {
        // Test that Schema::default() matches Python's Schema() behavior exactly
        let schema = Schema::default();

        // ============================================================================
        // VERIFY DEFAULTS (match Python's _initialize_defaults)
        // ============================================================================

        // String defaults: FTS disabled, string inverted enabled
        assert!(schema.defaults.string.is_some());
        let string = schema.defaults.string.as_ref().unwrap();
        assert!(!string.fts_index.as_ref().unwrap().enabled);
        assert!(string.string_inverted_index.as_ref().unwrap().enabled);

        // Float list defaults: vector index disabled
        assert!(schema.defaults.float_list.is_some());
        let float_list = schema.defaults.float_list.as_ref().unwrap();
        assert!(!float_list.vector_index.as_ref().unwrap().enabled);
        let vector_config = &float_list.vector_index.as_ref().unwrap().config;
        assert_eq!(vector_config.space, None); // Python leaves as None
        assert_eq!(vector_config.hnsw, None); // Python doesn't specify
        assert_eq!(vector_config.spann, None); // Python doesn't specify
        assert_eq!(vector_config.source_key, None);

        // Sparse vector defaults: disabled
        assert!(schema.defaults.sparse_vector.is_some());
        let sparse = schema.defaults.sparse_vector.as_ref().unwrap();
        assert!(!sparse.sparse_vector_index.as_ref().unwrap().enabled);

        // Int defaults: inverted index enabled
        assert!(schema.defaults.int.is_some());
        assert!(
            schema
                .defaults
                .int
                .as_ref()
                .unwrap()
                .int_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Float defaults: inverted index enabled
        assert!(schema.defaults.float.is_some());
        assert!(
            schema
                .defaults
                .float
                .as_ref()
                .unwrap()
                .float_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // Bool defaults: inverted index enabled
        assert!(schema.defaults.boolean.is_some());
        assert!(
            schema
                .defaults
                .boolean
                .as_ref()
                .unwrap()
                .bool_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // ============================================================================
        // VERIFY SPECIAL KEYS (match Python's _initialize_keys)
        // ============================================================================

        // #document: FTS enabled, string inverted disabled
        assert!(schema.keys.contains_key(DOCUMENT_KEY));
        let doc = schema.keys.get(DOCUMENT_KEY).unwrap();
        assert!(doc.string.is_some());
        assert!(
            doc.string
                .as_ref()
                .unwrap()
                .fts_index
                .as_ref()
                .unwrap()
                .enabled
        );
        assert!(
            !doc.string
                .as_ref()
                .unwrap()
                .string_inverted_index
                .as_ref()
                .unwrap()
                .enabled
        );

        // #embedding: vector index enabled with source_key=#document
        assert!(schema.keys.contains_key(EMBEDDING_KEY));
        let embedding = schema.keys.get(EMBEDDING_KEY).unwrap();
        assert!(embedding.float_list.is_some());
        let vec_idx = embedding
            .float_list
            .as_ref()
            .unwrap()
            .vector_index
            .as_ref()
            .unwrap();
        assert!(vec_idx.enabled);
        assert_eq!(vec_idx.config.source_key, Some(DOCUMENT_KEY.to_string()));
        assert_eq!(vec_idx.config.space, None); // Python leaves as None
        assert_eq!(vec_idx.config.hnsw, None); // Python doesn't specify
        assert_eq!(vec_idx.config.spann, None); // Python doesn't specify

        // Verify only these two special keys exist
        assert_eq!(schema.keys.len(), 2);
    }

    #[test]
    fn test_schema_default_works_with_builder() {
        // Test that Schema::default() can be used with builder pattern
        let schema = Schema::default()
            .create_index(Some("category"), StringInvertedIndexConfig {}.into())
            .expect("should succeed");

        // Verify the new index was added
        assert!(schema.keys.contains_key("category"));
        assert!(schema.keys.contains_key(DOCUMENT_KEY));
        assert!(schema.keys.contains_key(EMBEDDING_KEY));
        assert_eq!(schema.keys.len(), 3);
    }
}

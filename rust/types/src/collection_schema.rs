use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use utoipa::ToSchema;

use crate::collection_configuration::{
    EmbeddingFunctionConfiguration, InternalCollectionConfiguration, VectorIndexConfiguration,
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
    InternalSpannConfiguration, KnnIndex,
};

impl ChromaError for SchemaError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Schema is malformed: missing index configuration for metadata key '{key}' with type '{value_type}'")]
    MissingIndexConfiguration { key: String, value_type: String },
    #[error("Schema reconciliation failed: {reason}")]
    InvalidSchema { reason: String },
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

// ============================================================================
// SCHEMA STRUCTURES
// ============================================================================

/// Internal schema representation for collection index configurations
/// This represents the server-side schema structure used for index management

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InternalSchema {
    /// Default index configurations for each value type
    pub defaults: ValueTypes,
    /// Key-specific index overrides
    /// TODO(Sanket): Needed for backwards compatibility. Should remove after deploy.
    #[serde(rename = "keys", alias = "key_overrides")]
    pub keys: HashMap<String, ValueTypes>,
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema, Default)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FloatListValueType {
    #[serde(
        rename = "vector_index",
        alias = "$vector_index",
        skip_serializing_if = "Option::is_none"
    )] // VECTOR_INDEX_NAME
    pub vector_index: Option<VectorIndexType>,
}

/// Sparse vector value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SparseVectorValueType {
    #[serde(
        rename = "sparse_vector_index", // SPARSE_VECTOR_INDEX_NAME
        alias = "$sparse_vector_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub sparse_vector_index: Option<SparseVectorIndexType>,
}

/// Integer value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FloatValueType {
    #[serde(
        rename = "float_inverted_index", // FLOAT_INVERTED_INDEX_NAME
        alias = "$float_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub float_inverted_index: Option<FloatInvertedIndexType>,
}

/// Boolean value type index configurations
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BoolValueType {
    #[serde(
        rename = "bool_inverted_index", // BOOL_INVERTED_INDEX_NAME
        alias = "$bool_inverted_index",
        skip_serializing_if = "Option::is_none"
    )]
    pub bool_inverted_index: Option<BoolInvertedIndexType>,
}

// Individual index type structs with enabled status and config
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FtsIndexType {
    pub enabled: bool,
    pub config: FtsIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VectorIndexType {
    pub enabled: bool,
    pub config: VectorIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SparseVectorIndexType {
    pub enabled: bool,
    pub config: SparseVectorIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StringInvertedIndexType {
    pub enabled: bool,
    pub config: StringInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct IntInvertedIndexType {
    pub enabled: bool,
    pub config: IntInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FloatInvertedIndexType {
    pub enabled: bool,
    pub config: FloatInvertedIndexConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BoolInvertedIndexType {
    pub enabled: bool,
    pub config: BoolInvertedIndexConfig,
}

impl InternalSchema {
    /// Create a new InternalSchema with strongly-typed default configurations
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

        InternalSchema { defaults, keys }
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

    /// Reconcile user-provided schema with system defaults
    ///
    /// This method merges user configurations with system defaults, ensuring that:
    /// - User overrides take precedence over defaults
    /// - Missing user configurations fall back to system defaults
    /// - Field-level merging for complex configurations (Vector, HNSW, SPANN, etc.)
    pub fn reconcile_with_defaults(user_schema: Option<&InternalSchema>) -> Result<Self, String> {
        let default_schema = InternalSchema::new_default(KnnIndex::Spann);

        match user_schema {
            Some(user) => {
                // Merge defaults with user overrides
                let merged_defaults =
                    Self::merge_value_types(&default_schema.defaults, &user.defaults)?;

                // Merge key overrides
                let mut merged_keys = default_schema.keys.clone();
                for (key, user_value_types) in &user.keys {
                    if let Some(default_value_types) = merged_keys.get(key) {
                        // Merge with existing default key override
                        let merged_value_types =
                            Self::merge_value_types(default_value_types, user_value_types)?;
                        merged_keys.insert(key.clone(), merged_value_types);
                    } else {
                        // New key override from user
                        merged_keys.insert(key.clone(), user_value_types.clone());
                    }
                }

                Ok(InternalSchema {
                    defaults: merged_defaults,
                    keys: merged_keys,
                })
            }
            None => Ok(default_schema),
        }
    }

    /// Merge two schemas together, combining key overrides when possible.
    pub fn merge(&self, other: &InternalSchema) -> Result<InternalSchema, SchemaError> {
        if self.defaults != other.defaults {
            return Err(SchemaError::InvalidSchema {
                reason: "Cannot merge schemas with differing defaults".to_string(),
            });
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

        Ok(InternalSchema {
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
                    Err(SchemaError::InvalidSchema {
                        reason: format!("Conflicting configuration for {context}"),
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
    fn merge_value_types(default: &ValueTypes, user: &ValueTypes) -> Result<ValueTypes, String> {
        Ok(ValueTypes {
            string: Self::merge_string_type(default.string.as_ref(), user.string.as_ref())?,
            float: Self::merge_float_type(default.float.as_ref(), user.float.as_ref())?,
            int: Self::merge_int_type(default.int.as_ref(), user.int.as_ref())?,
            boolean: Self::merge_bool_type(default.boolean.as_ref(), user.boolean.as_ref())?,
            float_list: Self::merge_float_list_type(
                default.float_list.as_ref(),
                user.float_list.as_ref(),
            )?,
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
    ) -> Result<Option<StringValueType>, String> {
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
    ) -> Result<Option<FloatValueType>, String> {
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
    ) -> Result<Option<IntValueType>, String> {
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
    ) -> Result<Option<BoolValueType>, String> {
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
    ) -> Result<Option<FloatListValueType>, String> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(FloatListValueType {
                vector_index: Self::merge_vector_index_type(
                    default.vector_index.as_ref(),
                    user.vector_index.as_ref(),
                )?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge SparseVectorValueType configurations
    fn merge_sparse_vector_type(
        default: Option<&SparseVectorValueType>,
        user: Option<&SparseVectorValueType>,
    ) -> Result<Option<SparseVectorValueType>, String> {
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
    ) -> Result<Option<StringInvertedIndexType>, String> {
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
    ) -> Result<Option<FtsIndexType>, String> {
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
    ) -> Result<Option<FloatInvertedIndexType>, String> {
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
    ) -> Result<Option<IntInvertedIndexType>, String> {
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
    ) -> Result<Option<BoolInvertedIndexType>, String> {
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
    ) -> Result<Option<VectorIndexType>, String> {
        match (default, user) {
            (Some(default), Some(user)) => {
                Ok(Some(VectorIndexType {
                    enabled: user.enabled, // User enabled state takes precedence
                    config: Self::merge_vector_index_config(&default.config, &user.config)?,
                }))
            }
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    fn merge_sparse_vector_index_type(
        default: Option<&SparseVectorIndexType>,
        user: Option<&SparseVectorIndexType>,
    ) -> Result<Option<SparseVectorIndexType>, String> {
        match (default, user) {
            (Some(default), Some(user)) => Ok(Some(SparseVectorIndexType {
                enabled: user.enabled,
                config: Self::merge_sparse_vector_index_config(&default.config, &user.config)?,
            })),
            (Some(default), None) => Ok(Some(default.clone())),
            (None, Some(user)) => Ok(Some(user.clone())),
            (None, None) => Ok(None),
        }
    }

    /// Merge VectorIndexConfig with field-level merging
    fn merge_vector_index_config(
        default: &VectorIndexConfig,
        user: &VectorIndexConfig,
    ) -> Result<VectorIndexConfig, String> {
        Ok(VectorIndexConfig {
            space: user.space.clone().or(default.space.clone()),
            embedding_function: user
                .embedding_function
                .clone()
                .or(default.embedding_function.clone()),
            source_key: user.source_key.clone().or(default.source_key.clone()),
            hnsw: Self::merge_hnsw_configs(default.hnsw.as_ref(), user.hnsw.as_ref()),
            spann: Self::merge_spann_configs(default.spann.as_ref(), user.spann.as_ref()),
        })
    }

    /// Merge SparseVectorIndexConfig with field-level merging
    fn merge_sparse_vector_index_config(
        default: &SparseVectorIndexConfig,
        user: &SparseVectorIndexConfig,
    ) -> Result<SparseVectorIndexConfig, String> {
        Ok(SparseVectorIndexConfig {
            embedding_function: user
                .embedding_function
                .clone()
                .or(default.embedding_function.clone()),
            source_key: user.source_key.clone().or(default.source_key.clone()),
            bm25: user.bm25.or(default.bm25),
        })
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

    /// Reconcile InternalSchema with InternalCollectionConfiguration
    ///
    /// Simple reconciliation logic:
    /// 1. If collection config is default → return schema (schema is source of truth)
    /// 2. If collection config is non-default and schema is default → override schema with collection config
    ///
    /// Note: The case where both are non-default is validated earlier in reconcile_schema_and_config
    pub fn reconcile_with_collection_config(
        schema: &InternalSchema,
        collection_config: &InternalCollectionConfiguration,
    ) -> Result<InternalSchema, String> {
        // 1. Check if collection config is default
        if collection_config.is_default() {
            // Collection config is default → schema is source of truth
            return Ok(schema.clone());
        }

        // 2. Collection config is non-default, schema must be default (already validated earlier)
        // Convert collection config to schema
        Self::convert_collection_config_to_schema(collection_config)
    }

    pub fn reconcile_schema_and_config(
        schema: Option<&InternalSchema>,
        configuration: Option<&InternalCollectionConfiguration>,
    ) -> Result<InternalSchema, String> {
        // Early validation: check if both user-provided schema and config are non-default
        if let (Some(user_schema), Some(config)) = (schema, configuration) {
            if !user_schema.is_default() && !config.is_default() {
                let error_message =
                    "Schema and configuration are both provided but conflict".to_string();
                return Err(error_message);
            }
        }
        let reconciled_schema = Self::reconcile_with_defaults(schema)?;
        if let Some(config) = configuration {
            Self::reconcile_with_collection_config(&reconciled_schema, config)
        } else {
            Ok(reconciled_schema)
        }
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
                // Check that the config has default structure
                // We allow space and embedding_function to vary, but check structure
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

    /// Convert InternalCollectionConfiguration to InternalSchema
    fn convert_collection_config_to_schema(
        collection_config: &InternalCollectionConfiguration,
    ) -> Result<InternalSchema, String> {
        // Start with a default schema structure
        let mut schema = InternalSchema::new_default(KnnIndex::Spann); // Default to HNSW, will be overridden

        // Convert vector index configuration
        let vector_config = match &collection_config.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw_config) => VectorIndexConfig {
                space: Some(hnsw_config.space.clone()),
                embedding_function: collection_config.embedding_function.clone(),
                source_key: Some(DOCUMENT_KEY.to_string()), // Default source key
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
                embedding_function: collection_config.embedding_function.clone(),
                source_key: Some(DOCUMENT_KEY.to_string()), // Default source key
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
        if let Some(embedding_types) = schema.keys.get_mut(EMBEDDING_KEY) {
            if let Some(float_list) = &mut embedding_types.float_list {
                if let Some(vector_index) = &mut float_list.vector_index {
                    vector_index.config = vector_config;
                }
            }
        }

        Ok(schema)
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
}

// ============================================================================
// INDEX CONFIGURATION STRUCTURES
// ============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
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
    pub batch_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SpannIndexConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_nprobe: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_rng_factor: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_rng_epsilon: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nreplica_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_rng_factor: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_rng_epsilon: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_samples_kmeans: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_lambda: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reassign_neighbor_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_threshold: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_centers_to_merge_to: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_nprobe: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construction: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_search: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FtsIndexConfig {
    // FTS index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StringInvertedIndexConfig {
    // String inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IntInvertedIndexConfig {
    // Integer inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FloatInvertedIndexConfig {
    // Float inverted index typically has no additional parameters
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BoolInvertedIndexConfig {
    // Boolean inverted index typically has no additional parameters
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
        let result = InternalSchema::reconcile_with_defaults(None).unwrap();
        let expected = InternalSchema::new_default(KnnIndex::Spann);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_reconcile_with_defaults_empty_user_schema() {
        // Test merging with an empty user schema
        let user_schema = InternalSchema {
            defaults: ValueTypes::default(),
            keys: HashMap::new(),
        };

        let result = InternalSchema::reconcile_with_defaults(Some(&user_schema)).unwrap();
        let expected = InternalSchema::new_default(KnnIndex::Spann);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_reconcile_with_defaults_user_overrides_string_enabled() {
        // Test that user can override string inverted index enabled state
        let mut user_schema = InternalSchema {
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

        let result = InternalSchema::reconcile_with_defaults(Some(&user_schema)).unwrap();

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
        let mut user_schema = InternalSchema {
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
            let default_schema = InternalSchema::new_default(KnnIndex::Hnsw);
            let merged_defaults =
                InternalSchema::merge_value_types(&default_schema.defaults, &user_schema.defaults)
                    .unwrap();
            let mut merged_keys = default_schema.keys.clone();
            for (key, user_value_types) in user_schema.keys {
                if let Some(default_value_types) = merged_keys.get(&key) {
                    let merged_value_types =
                        InternalSchema::merge_value_types(default_value_types, &user_value_types)
                            .unwrap();
                    merged_keys.insert(key, merged_value_types);
                } else {
                    merged_keys.insert(key, user_value_types);
                }
            }
            InternalSchema {
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
        let mut user_schema = InternalSchema {
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

        let result = InternalSchema::reconcile_with_defaults(Some(&user_schema)).unwrap();

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
        let mut user_schema = InternalSchema {
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

        let result = InternalSchema::reconcile_with_defaults(Some(&user_schema)).unwrap();

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

        let schema =
            InternalSchema::convert_collection_config_to_schema(&collection_config).unwrap();
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

        let schema =
            InternalSchema::convert_collection_config_to_schema(&collection_config).unwrap();
        let reconstructed = InternalCollectionConfiguration::try_from(&schema).unwrap();

        assert_eq!(reconstructed, collection_config);
    }

    #[test]
    fn test_convert_schema_to_collection_config_rejects_mixed_index() {
        let mut schema = InternalSchema::new_default(KnnIndex::Hnsw);
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
        let mut schema = InternalSchema::new_default(KnnIndex::Hnsw);
        let before = schema.clone();
        let modified = schema.ensure_key_from_metadata(DOCUMENT_KEY, MetadataValueType::Str);
        assert!(!modified);
        assert_eq!(schema, before);
    }

    #[test]
    fn test_ensure_key_from_metadata_populates_new_key_with_default_value_type() {
        let mut schema = InternalSchema::new_default(KnnIndex::Hnsw);
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
        let mut schema = InternalSchema::new_default(KnnIndex::Hnsw);
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
        let schema = InternalSchema::new_default(KnnIndex::Spann);
        let result = schema.is_knn_key_indexing_enabled(
            "custom_sparse",
            &QueryVector::Sparse(SparseVector::new(vec![0_u32], vec![1.0_f32])),
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
        let mut schema = InternalSchema::new_default(KnnIndex::Spann);
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
            &QueryVector::Sparse(SparseVector::new(vec![0_u32], vec![1.0_f32])),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_is_knn_key_indexing_enabled_dense_succeeds() {
        let schema = InternalSchema::new_default(KnnIndex::Spann);
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

        let result =
            InternalSchema::merge_hnsw_configs(Some(&default_hnsw), Some(&user_hnsw)).unwrap();

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
            search_rng_factor: Some(2.0),
            search_rng_epsilon: Some(0.1),
            nreplica_count: Some(3),
            write_rng_factor: Some(1.5),
            write_rng_epsilon: Some(0.05),
            split_threshold: Some(1000),
            num_samples_kmeans: Some(100),
            initial_lambda: Some(0.5),
            reassign_neighbor_count: Some(50),
            merge_threshold: Some(500),
            num_centers_to_merge_to: Some(10),
            write_nprobe: Some(5),
            ef_construction: Some(200),
            ef_search: Some(10),
            max_neighbors: Some(16),
        };

        let user_spann = SpannIndexConfig {
            search_nprobe: Some(20),       // Override
            search_rng_factor: None,       // Will use default
            search_rng_epsilon: Some(0.2), // Override
            nreplica_count: None,          // Will use default
            write_rng_factor: None,
            write_rng_epsilon: None,
            split_threshold: Some(2000), // Override
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

        let result =
            InternalSchema::merge_spann_configs(Some(&default_spann), Some(&user_spann)).unwrap();

        // Check user overrides
        assert_eq!(result.search_nprobe, Some(20));
        assert_eq!(result.search_rng_epsilon, Some(0.2));
        assert_eq!(result.split_threshold, Some(2000));

        // Check defaults preserved
        assert_eq!(result.search_rng_factor, Some(2.0));
        assert_eq!(result.nreplica_count, Some(3));
        assert_eq!(result.initial_lambda, Some(0.5));
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

        let result = InternalSchema::merge_string_type(Some(&default), Some(&user))
            .unwrap()
            .unwrap();
        assert!(!result.string_inverted_index.as_ref().unwrap().enabled); // User override
        assert!(!result.fts_index.as_ref().unwrap().enabled); // Default preserved

        // Default Some, User None - should return default
        let result = InternalSchema::merge_string_type(Some(&default), None)
            .unwrap()
            .unwrap();
        assert!(result.string_inverted_index.as_ref().unwrap().enabled);

        // Default None, User Some - should return user
        let result = InternalSchema::merge_string_type(None, Some(&user))
            .unwrap()
            .unwrap();
        assert!(!result.string_inverted_index.as_ref().unwrap().enabled);

        // Both None - should return None
        let result = InternalSchema::merge_string_type(None, None).unwrap();
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
            InternalSchema::merge_vector_index_config(&default_config, &user_config).unwrap();

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

        // Check SPANN was added from user
        assert!(result.spann.is_some());
        assert_eq!(result.spann.as_ref().unwrap().search_nprobe, Some(15));
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

        let result =
            InternalSchema::merge_sparse_vector_index_config(&default_config, &user_config)
                .unwrap();

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
        let mut user_schema = InternalSchema {
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
            let default_schema = InternalSchema::new_default(KnnIndex::Hnsw);
            let merged_defaults =
                InternalSchema::merge_value_types(&default_schema.defaults, &user_schema.defaults)
                    .unwrap();
            let mut merged_keys = default_schema.keys.clone();
            for (key, user_value_types) in user_schema.keys {
                if let Some(default_value_types) = merged_keys.get(&key) {
                    let merged_value_types =
                        InternalSchema::merge_value_types(default_value_types, &user_value_types)
                            .unwrap();
                    merged_keys.insert(key, merged_value_types);
                } else {
                    merged_keys.insert(key, user_value_types);
                }
            }
            InternalSchema {
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
        let schema = InternalSchema::new_default(KnnIndex::Hnsw);
        let collection_config = InternalCollectionConfiguration::default_hnsw();

        let result =
            InternalSchema::reconcile_with_collection_config(&schema, &collection_config).unwrap();
        assert_eq!(result, schema);
    }

    #[test]
    fn test_reconcile_with_collection_config_both_non_default() {
        // Test that when both schema and collection config are non-default, it returns an error
        let mut schema = InternalSchema::new_default(KnnIndex::Hnsw);
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
        let result =
            InternalSchema::reconcile_schema_and_config(Some(&schema), Some(&collection_config));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Schema and configuration are both provided but conflict"
        );
    }

    #[test]
    fn test_reconcile_with_collection_config_hnsw_override() {
        // Test that non-default HNSW collection config overrides default schema
        let schema = InternalSchema::new_default(KnnIndex::Hnsw); // Use actual default schema

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
            InternalSchema::reconcile_with_collection_config(&schema, &collection_config).unwrap();

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
        let schema = InternalSchema::new_default(KnnIndex::Spann); // Use actual default schema

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
            InternalSchema::reconcile_with_collection_config(&schema, &collection_config).unwrap();

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
        let schema = InternalSchema::new_default(KnnIndex::Hnsw);

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
            InternalSchema::reconcile_with_collection_config(&schema, &collection_config).unwrap();

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
        assert_eq!(
            defaults_vector_index.config.source_key,
            Some(DOCUMENT_KEY.to_string())
        );
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
        let default_hnsw_schema = InternalSchema::new_default(KnnIndex::Hnsw);
        assert!(default_hnsw_schema.is_default());

        let default_spann_schema = InternalSchema::new_default(KnnIndex::Spann);
        assert!(default_spann_schema.is_default());

        // Test that a modified default schema is not considered default
        let mut modified_schema = InternalSchema::new_default(KnnIndex::Hnsw);
        // Make a clear modification - change the string inverted index enabled state
        if let Some(ref mut string_type) = modified_schema.defaults.string {
            if let Some(ref mut string_inverted) = string_type.string_inverted_index {
                string_inverted.enabled = false; // Default is true, so this should make it non-default
            }
        }
        assert!(!modified_schema.is_default());

        // Test that schema with additional key overrides is not default
        let mut schema_with_extra_overrides = InternalSchema::new_default(KnnIndex::Hnsw);
        schema_with_extra_overrides
            .keys
            .insert("custom_key".to_string(), ValueTypes::default());
        assert!(!schema_with_extra_overrides.is_default());
    }

    #[test]
    fn test_add_merges_keys_by_value_type() {
        let mut schema_a = InternalSchema::new_default(KnnIndex::Hnsw);
        let mut schema_b = InternalSchema::new_default(KnnIndex::Hnsw);

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
        let schema_a = InternalSchema::new_default(KnnIndex::Hnsw);
        let mut schema_b = InternalSchema::new_default(KnnIndex::Hnsw);

        if let Some(string_type) = schema_b.defaults.string.as_mut() {
            if let Some(string_index) = string_type.string_inverted_index.as_mut() {
                string_index.enabled = false;
            }
        }

        let err = schema_a.merge(&schema_b).unwrap_err();
        match err {
            SchemaError::InvalidSchema { reason } => {
                assert_eq!(reason, "Cannot merge schemas with differing defaults")
            }
            _ => panic!("Expected InvalidSchema error"),
        }
    }

    #[test]
    fn test_add_detects_conflicting_value_type_configuration() {
        let mut schema_a = InternalSchema::new_default(KnnIndex::Hnsw);
        let mut schema_b = InternalSchema::new_default(KnnIndex::Hnsw);

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
        match err {
            SchemaError::InvalidSchema { reason } => {
                assert!(reason.contains("Conflicting configuration"));
            }
            _ => panic!("Expected InvalidSchema error"),
        }
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

        let schema_from_old: InternalSchema = serde_json::from_str(old_format_json).unwrap();

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

        let schema_from_new: InternalSchema = serde_json::from_str(new_format_json).unwrap();

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
}

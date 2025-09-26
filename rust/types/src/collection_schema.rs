use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

use crate::collection_configuration::{
    EmbeddingFunctionConfiguration, InternalCollectionConfiguration, VectorIndexConfiguration,
};
use crate::hnsw_configuration::Space;
use crate::{
    default_batch_size, default_construction_ef, default_construction_ef_spann,
    default_initial_lambda, default_m, default_m_spann, default_merge_threshold,
    default_nreplica_count, default_num_centers_to_merge_to, default_num_samples_kmeans,
    default_num_threads, default_reassign_neighbor_count, default_resize_factor, default_search_ef,
    default_search_ef_spann, default_search_nprobe, default_search_rng_epsilon,
    default_search_rng_factor, default_space, default_split_threshold, default_sync_threshold,
    default_write_nprobe, default_write_rng_epsilon, default_write_rng_factor, KnnIndex,
};

// Value type name constants for data type identification
pub const STRING_VALUE_NAME: &str = "#string";
pub const INT_VALUE_NAME: &str = "#int";
pub const BOOL_VALUE_NAME: &str = "#bool";
pub const FLOAT_VALUE_NAME: &str = "#float";
pub const FLOAT_LIST_VALUE_NAME: &str = "#float_list";
pub const SPARSE_VECTOR_VALUE_NAME: &str = "#sparse_vector";

// Index type name constants for index identification
pub const FTS_INDEX_NAME: &str = "$fts_index";
pub const VECTOR_INDEX_NAME: &str = "$vector_index";
pub const SPARSE_VECTOR_INDEX_NAME: &str = "$sparse_vector_index";
pub const STRING_INVERTED_INDEX_NAME: &str = "$string_inverted_index";
pub const INT_INVERTED_INDEX_NAME: &str = "$int_inverted_index";
pub const FLOAT_INVERTED_INDEX_NAME: &str = "$float_inverted_index";
pub const BOOL_INVERTED_INDEX_NAME: &str = "$bool_inverted_index";

// Special key constants for predefined field names
pub const DOCUMENT_KEY: &str = "$document";
pub const EMBEDDING_KEY: &str = "$embedding";

/// Strong type for value type names (like "#string", "#float_list", etc.)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, ToSchema)]
pub struct ValueTypeName(String);

impl ValueTypeName {
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    // Convenience constructors for common value types
    pub fn string() -> Self {
        Self(STRING_VALUE_NAME.to_string())
    }
    pub fn int() -> Self {
        Self(INT_VALUE_NAME.to_string())
    }
    pub fn bool() -> Self {
        Self(BOOL_VALUE_NAME.to_string())
    }
    pub fn float() -> Self {
        Self(FLOAT_VALUE_NAME.to_string())
    }
    pub fn float_list() -> Self {
        Self(FLOAT_LIST_VALUE_NAME.to_string())
    }
    pub fn sparse_vector() -> Self {
        Self(SPARSE_VECTOR_VALUE_NAME.to_string())
    }
}

impl From<&str> for ValueTypeName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for ValueTypeName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl<'de> serde::Deserialize<'de> for ValueTypeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            STRING_VALUE_NAME
            | INT_VALUE_NAME
            | BOOL_VALUE_NAME
            | FLOAT_VALUE_NAME
            | FLOAT_LIST_VALUE_NAME
            | SPARSE_VECTOR_VALUE_NAME => Ok(ValueTypeName(s)),
            _ => Err(serde::de::Error::custom(format!(
                "unknown value type: '{}'. Valid types are: {}, {}, {}, {}, {}, {}",
                s,
                STRING_VALUE_NAME,
                INT_VALUE_NAME,
                BOOL_VALUE_NAME,
                FLOAT_VALUE_NAME,
                FLOAT_LIST_VALUE_NAME,
                SPARSE_VECTOR_VALUE_NAME
            ))),
        }
    }
}

/// Strong type for index type names (like "$fts_index", "$vector_index", etc.)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, ToSchema)]
pub struct IndexTypeName(String);

impl IndexTypeName {
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    // Convenience constructors for common index types
    pub fn fts() -> Self {
        Self(FTS_INDEX_NAME.to_string())
    }
    pub fn vector() -> Self {
        Self(VECTOR_INDEX_NAME.to_string())
    }
    pub fn sparse_vector() -> Self {
        Self(SPARSE_VECTOR_INDEX_NAME.to_string())
    }
    pub fn string_inverted() -> Self {
        Self(STRING_INVERTED_INDEX_NAME.to_string())
    }
    pub fn int_inverted() -> Self {
        Self(INT_INVERTED_INDEX_NAME.to_string())
    }
    pub fn float_inverted() -> Self {
        Self(FLOAT_INVERTED_INDEX_NAME.to_string())
    }
    pub fn bool_inverted() -> Self {
        Self(BOOL_INVERTED_INDEX_NAME.to_string())
    }
}

impl From<&str> for IndexTypeName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for IndexTypeName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl<'de> serde::Deserialize<'de> for IndexTypeName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            FTS_INDEX_NAME
            | VECTOR_INDEX_NAME
            | SPARSE_VECTOR_INDEX_NAME
            | STRING_INVERTED_INDEX_NAME
            | INT_INVERTED_INDEX_NAME
            | FLOAT_INVERTED_INDEX_NAME
            | BOOL_INVERTED_INDEX_NAME => Ok(IndexTypeName(s)),
            _ => Err(serde::de::Error::custom(format!(
                "unknown index type: '{}'. Valid types are: {}, {}, {}, {}, {}, {}, {}",
                s,
                FTS_INDEX_NAME,
                VECTOR_INDEX_NAME,
                SPARSE_VECTOR_INDEX_NAME,
                STRING_INVERTED_INDEX_NAME,
                INT_INVERTED_INDEX_NAME,
                FLOAT_INVERTED_INDEX_NAME,
                BOOL_INVERTED_INDEX_NAME
            ))),
        }
    }
}

/// Internal schema representation for collection index configurations
/// This represents the server-side schema structure used for index management

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InternalSchema {
    /// Default index configurations for each value type
    pub defaults: HashMap<ValueTypeName, ValueTypeIndexes>,
    /// Key-specific index overrides
    pub key_overrides: HashMap<String, HashMap<ValueTypeName, ValueTypeIndexes>>,
}

impl InternalSchema {
    fn new_default(default_knn_index: KnnIndex) -> Self {
        let mut defaults = HashMap::new();

        // String value type defaults
        let mut string_indexes = HashMap::new();
        string_indexes.insert(IndexTypeName::string_inverted(), IndexValue::Boolean(true));
        string_indexes.insert(IndexTypeName::fts(), IndexValue::Boolean(false));
        defaults.insert(ValueTypeName::string(), string_indexes);

        // Float value type defaults
        let mut float_indexes = HashMap::new();
        float_indexes.insert(IndexTypeName::float_inverted(), IndexValue::Boolean(true));
        defaults.insert(ValueTypeName::float(), float_indexes);

        // Float list value type defaults
        let mut float_list_indexes = HashMap::new();
        float_list_indexes.insert(IndexTypeName::vector(), IndexValue::Boolean(false));
        defaults.insert(ValueTypeName::float_list(), float_list_indexes);

        // Sparse vector value type defaults
        let mut sparse_vector_indexes = HashMap::new();
        sparse_vector_indexes.insert(IndexTypeName::sparse_vector(), IndexValue::Boolean(false));
        defaults.insert(ValueTypeName::sparse_vector(), sparse_vector_indexes);

        // Bool value type defaults
        let mut bool_indexes = HashMap::new();
        bool_indexes.insert(IndexTypeName::bool_inverted(), IndexValue::Boolean(true));
        defaults.insert(ValueTypeName::bool(), bool_indexes);

        // Int value type defaults
        let mut int_indexes = HashMap::new();
        int_indexes.insert(IndexTypeName::int_inverted(), IndexValue::Boolean(true));
        defaults.insert(ValueTypeName::int(), int_indexes);

        // Default key overrides
        let mut key_overrides = HashMap::new();

        // $document key overrides - enable FTS, disable string inverted index
        let mut document_overrides = HashMap::new();
        let mut document_string_indexes = HashMap::new();
        document_string_indexes.insert(IndexTypeName::fts(), IndexValue::Boolean(true));
        document_string_indexes
            .insert(IndexTypeName::string_inverted(), IndexValue::Boolean(false));
        document_overrides.insert(ValueTypeName::string(), document_string_indexes);
        key_overrides.insert(DOCUMENT_KEY.to_string(), document_overrides);

        // $embedding key overrides - enable vector index with document source
        let mut embedding_overrides = HashMap::new();
        let mut embedding_float_list_indexes = HashMap::new();
        let (hnsw, spann) = match default_knn_index {
            KnnIndex::Hnsw => (
                Some(HnswIndexConfig {
                    ef_construction: Some(default_construction_ef()),
                    ef_search: Some(default_search_ef()),
                    max_neighbors: Some(default_m()),
                    num_threads: Some(default_num_threads()),
                    resize_factor: Some(default_resize_factor()),
                    sync_threshold: Some(default_sync_threshold()),
                    batch_size: Some(default_batch_size()),
                }),
                None,
            ),
            KnnIndex::Spann => (
                None,
                Some(SpannIndexConfig {
                    search_nprobe: Some(default_search_nprobe()),
                    search_rng_factor: Some(default_search_rng_factor()),
                    search_rng_epsilon: Some(default_search_rng_epsilon()),
                    write_nprobe: Some(default_write_nprobe()),
                    nreplica_count: Some(default_nreplica_count()),
                    write_rng_factor: Some(default_write_rng_factor()),
                    write_rng_epsilon: Some(default_write_rng_epsilon()),
                    split_threshold: Some(default_split_threshold()),
                    num_samples_kmeans: Some(default_num_samples_kmeans()),
                    initial_lambda: Some(default_initial_lambda()),
                    reassign_neighbor_count: Some(default_reassign_neighbor_count()),
                    merge_threshold: Some(default_merge_threshold()),
                    num_centers_to_merge_to: Some(default_num_centers_to_merge_to()),
                    ef_construction: Some(default_construction_ef_spann()),
                    ef_search: Some(default_search_ef_spann()),
                    max_neighbors: Some(default_m_spann()),
                }),
            ),
        };
        embedding_float_list_indexes.insert(
            IndexTypeName::vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::Vector(VectorIndexConfig {
                    space: Some(default_space()),
                    embedding_function: None,
                    source_key: Some(DOCUMENT_KEY.to_string()),
                    hnsw,
                    spann,
                }),
            }),
        );
        embedding_overrides.insert(ValueTypeName::float_list(), embedding_float_list_indexes);
        key_overrides.insert(EMBEDDING_KEY.to_string(), embedding_overrides);

        InternalSchema {
            defaults,
            key_overrides,
        }
    }
    /// Reconcile user-provided schema with system defaults
    ///
    /// This method merges user configurations with system defaults, ensuring that:
    /// - User overrides take precedence over defaults
    /// - Index config fields are merged at the field level (user fields override default fields)
    /// - Missing user configurations fall back to system defaults
    pub fn reconcile_with_defaults(user_schema: Option<InternalSchema>) -> Result<Self, String> {
        let default_schema = InternalSchema::new_default(KnnIndex::Spann);

        match user_schema {
            Some(user) => {
                let mut reconciled = default_schema;

                // Reconcile defaults with field-level merging
                for (value_type, user_indexes) in user.defaults {
                    if let Some(default_indexes) = reconciled.defaults.get(&value_type) {
                        let merged_indexes =
                            Self::reconcile_value_type_indexes(default_indexes, &user_indexes)?;
                        reconciled.defaults.insert(value_type, merged_indexes);
                    } else {
                        reconciled.defaults.insert(value_type, user_indexes);
                    }
                }

                // Reconcile key overrides with field-level merging
                for (key, user_value_types) in user.key_overrides {
                    if let Some(default_value_types) = reconciled.key_overrides.get(&key) {
                        let mut merged_value_types = default_value_types.clone();
                        for (value_type, user_indexes) in user_value_types {
                            if let Some(default_indexes) = merged_value_types.get(&value_type) {
                                let merged_indexes = Self::reconcile_value_type_indexes(
                                    default_indexes,
                                    &user_indexes,
                                )?;
                                merged_value_types.insert(value_type, merged_indexes);
                            } else {
                                merged_value_types.insert(value_type, user_indexes);
                            }
                        }
                        reconciled.key_overrides.insert(key, merged_value_types);
                    } else {
                        reconciled.key_overrides.insert(key, user_value_types);
                    }
                }

                Ok(reconciled)
            }
            None => Ok(default_schema),
        }
    }

    /// Check if InternalSchema is default (checks vector index config in $embedding key)
    fn is_schema_default(schema: &InternalSchema) -> bool {
        // Get the vector index config from $embedding key
        if let Some(embedding_overrides) = schema.key_overrides.get("$embedding") {
            if let Some(float_list_indexes) = embedding_overrides.get(&ValueTypeName::float_list())
            {
                if let Some(IndexValue::IndexConfig(config)) =
                    float_list_indexes.get(&IndexTypeName::vector())
                {
                    if let TypedIndexConfig::Vector(vector_config) = &config.config {
                        // Check if all fields are default
                        return is_embedding_function_default(&vector_config.embedding_function)
                            && is_space_default(&vector_config.space)
                            && vector_config
                                .hnsw
                                .as_ref()
                                .map(is_hnsw_config_default)
                                .unwrap_or(true)
                            && vector_config
                                .spann
                                .as_ref()
                                .map(is_spann_config_default)
                                .unwrap_or(true);
                    }
                }
            }
        }
        true // No vector index config means default
    }

    /// Override schema vector index config with collection config values
    fn override_schema_with_collection_config(
        schema: &mut InternalSchema,
        collection_config: &InternalCollectionConfiguration,
    ) {
        // Get existing source_key and enabled status from schema if it exists
        let (existing_source_key, existing_enabled) = schema
            .key_overrides
            .get("$embedding")
            .and_then(|overrides| overrides.get(&ValueTypeName::float_list()))
            .and_then(|indexes| indexes.get(&IndexTypeName::vector()))
            .map(|index_value| {
                if let IndexValue::IndexConfig(config) = index_value {
                    if let TypedIndexConfig::Vector(vector_config) = &config.config {
                        return (vector_config.source_key.clone(), config.enabled);
                    }
                }
                (None, true) // Default fallback
            })
            .unwrap_or((None, true)); // Default if no existing config

        // Get or create the embedding overrides
        let embedding_overrides = schema
            .key_overrides
            .entry("$embedding".to_string())
            .or_default();
        let float_list_indexes = embedding_overrides
            .entry(ValueTypeName::float_list())
            .or_default();

        // Create vector index config from collection config
        let vector_config = match &collection_config.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw_config) => VectorIndexConfig {
                embedding_function: collection_config.embedding_function.clone(),
                space: Some(hnsw_config.space.clone()),
                source_key: existing_source_key,
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
                embedding_function: collection_config.embedding_function.clone(),
                space: Some(spann_config.space.clone()),
                source_key: existing_source_key,
                hnsw: None,
                spann: Some(SpannIndexConfig {
                    search_nprobe: Some(spann_config.search_nprobe),
                    search_rng_factor: Some(spann_config.search_rng_factor),
                    search_rng_epsilon: Some(spann_config.search_rng_epsilon),
                    write_nprobe: Some(spann_config.write_nprobe),
                    nreplica_count: Some(spann_config.nreplica_count),
                    write_rng_factor: Some(spann_config.write_rng_factor),
                    write_rng_epsilon: Some(spann_config.write_rng_epsilon),
                    split_threshold: Some(spann_config.split_threshold),
                    num_samples_kmeans: Some(spann_config.num_samples_kmeans),
                    initial_lambda: Some(spann_config.initial_lambda),
                    reassign_neighbor_count: Some(spann_config.reassign_neighbor_count),
                    merge_threshold: Some(spann_config.merge_threshold),
                    num_centers_to_merge_to: Some(spann_config.num_centers_to_merge_to),
                    ef_construction: Some(spann_config.ef_construction),
                    ef_search: Some(spann_config.ef_search),
                    max_neighbors: Some(spann_config.max_neighbors),
                }),
            },
        };

        // Insert the vector index config
        float_list_indexes.insert(
            IndexTypeName::vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: existing_enabled,
                config: TypedIndexConfig::Vector(vector_config),
            }),
        );
    }

    /// Reconcile InternalSchema with InternalCollectionConfiguration
    ///
    /// Simple reconciliation logic:
    /// 1. If collection config is default → return schema (schema is source of truth)
    /// 2. If collection config is non-default and schema is non-default → error (both set)
    /// 3. If collection config is non-default and schema is default → override schema with collection config
    pub fn reconcile_with_collection_config(
        schema: InternalSchema,
        collection_config: InternalCollectionConfiguration,
    ) -> Result<InternalSchema, String> {
        // 1. Check if collection config is default
        if collection_config.is_default() {
            // Collection config is default → schema is source of truth
            return Ok(schema);
        }

        // 2. Collection config is non-default, check if schema is also non-default
        if !Self::is_schema_default(&schema) {
            // Both are non-default → error
            return Err(
                "Cannot set both collection config and schema at the same time".to_string(),
            );
        }

        // 3. Collection config is non-default, schema is default → override schema with collection config
        let mut schema = schema;
        Self::override_schema_with_collection_config(&mut schema, &collection_config);
        Ok(schema)
    }

    /// Validate that vector index configuration doesn't have both HNSW and SPANN set
    fn validate_vector_index_config(config: &VectorIndexConfig) -> Result<(), String> {
        match (config.hnsw.as_ref(), config.spann.as_ref()) {
            (Some(_), Some(_)) => Err(
                "Cannot specify both HNSW and SPANN configurations for the same vector index"
                    .to_string(),
            ),
            _ => Ok(()),
        }
    }

    /// Reconcile index configurations at the field level
    ///
    /// This method merges two index configurations, with user config taking precedence
    /// for fields that are set, while preserving default values for unset fields.
    fn reconcile_index_configs(
        default_config: &TypedIndexConfig,
        user_config: &TypedIndexConfig,
    ) -> Result<TypedIndexConfig, String> {
        match (default_config, user_config) {
            (TypedIndexConfig::Vector(default_vector), TypedIndexConfig::Vector(user_vector)) => {
                // Validate that user doesn't specify both HNSW and SPANN
                Self::validate_vector_index_config(user_vector)?;

                // Handle mutual exclusivity: if user specifies one index type, don't merge with default's other type
                let (hnsw, spann) = match (user_vector.hnsw.as_ref(), user_vector.spann.as_ref()) {
                    // User specified HNSW → use user's HNSW, ignore default SPANN
                    (Some(_), None) => (
                        Self::reconcile_hnsw_configs(
                            default_vector.hnsw.as_ref(),
                            user_vector.hnsw.as_ref(),
                        ),
                        None,
                    ),
                    // User specified SPANN → use user's SPANN, ignore default HNSW
                    (None, Some(_)) => (
                        None,
                        Self::reconcile_spann_configs(
                            default_vector.spann.as_ref(),
                            user_vector.spann.as_ref(),
                        ),
                    ),
                    // User specified both → this should never happen due to validation above
                    (Some(_), Some(_)) => {
                        unreachable!(
                            "Both HNSW and SPANN specified - should be caught by validation"
                        )
                    }
                    // User specified neither → use default
                    (None, None) => (
                        Self::reconcile_hnsw_configs(
                            default_vector.hnsw.as_ref(),
                            user_vector.hnsw.as_ref(),
                        ),
                        Self::reconcile_spann_configs(
                            default_vector.spann.as_ref(),
                            user_vector.spann.as_ref(),
                        ),
                    ),
                };

                Ok(TypedIndexConfig::Vector(VectorIndexConfig {
                    space: user_vector.space.clone().or(default_vector.space.clone()),
                    embedding_function: user_vector
                        .embedding_function
                        .clone()
                        .or(default_vector.embedding_function.clone()),
                    source_key: user_vector
                        .source_key
                        .clone()
                        .or(default_vector.source_key.clone()),
                    hnsw,
                    spann,
                }))
            }
            (
                TypedIndexConfig::SparseVector(default_sparse),
                TypedIndexConfig::SparseVector(user_sparse),
            ) => Ok(TypedIndexConfig::SparseVector(SparseVectorIndexConfig {
                embedding_function: user_sparse
                    .embedding_function
                    .clone()
                    .or(default_sparse.embedding_function.clone()),
                source_key: user_sparse
                    .source_key
                    .clone()
                    .or(default_sparse.source_key.clone()),
            })),
            // For other index types, user config takes precedence entirely
            // TODO: When adding new index types with complex configurations,
            // implement field-level merging similar to Vector and SparseVector above
            _ => Ok(user_config.clone()),
        }
    }

    /// Reconcile HNSW configurations with field-level merging
    fn reconcile_hnsw_configs(
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

    /// Reconcile SPANN configurations with field-level merging
    fn reconcile_spann_configs(
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

    /// Reconcile index values with proper field-level merging
    ///
    /// This method handles the complex case where we need to merge:
    /// - Boolean values (simple enabled/disabled)
    /// - Full index configurations with field-level merging
    fn reconcile_index_values(
        default_value: &IndexValue,
        user_value: &IndexValue,
    ) -> Result<IndexValue, String> {
        match (default_value, user_value) {
            // If user has a boolean, use the enabled status with default config
            (IndexValue::IndexConfig(default_config), IndexValue::Boolean(user_enabled)) => {
                Ok(IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                    enabled: *user_enabled,
                    config: default_config.config.clone(),
                }))
            }

            // If user has a boolean and default is also boolean, use user boolean
            (IndexValue::Boolean(_), IndexValue::Boolean(user_enabled)) => {
                Ok(IndexValue::Boolean(*user_enabled))
            }

            // If user has a full config, merge with default config
            (IndexValue::IndexConfig(default_config), IndexValue::IndexConfig(user_config)) => {
                let reconciled_config =
                    Self::reconcile_index_configs(&default_config.config, &user_config.config)?;
                Ok(IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                    enabled: user_config.enabled,
                    config: reconciled_config,
                }))
            }

            // If user has a config but default has a boolean, use user config
            (IndexValue::Boolean(_), IndexValue::IndexConfig(user_config)) => {
                // Validate the user config before accepting it
                if let TypedIndexConfig::Vector(vector_config) = &user_config.config {
                    Self::validate_vector_index_config(vector_config)?;
                }
                Ok(IndexValue::IndexConfig(user_config.clone()))
            }
        }
    }

    /// Reconcile value type indexes with field-level merging
    ///
    /// This method merges user and default value type configurations,
    /// ensuring that user overrides are preserved while defaults fill in gaps.
    fn reconcile_value_type_indexes(
        default_indexes: &ValueTypeIndexes,
        user_indexes: &ValueTypeIndexes,
    ) -> Result<ValueTypeIndexes, String> {
        let mut reconciled = default_indexes.clone();

        for (index_name, user_value) in user_indexes {
            if let Some(default_value) = reconciled.get(index_name) {
                // Merge user and default values
                reconciled.insert(
                    index_name.clone(),
                    Self::reconcile_index_values(default_value, user_value)?,
                );
            } else {
                // User has a new index type not in defaults - validate if it's a vector config
                if let IndexValue::IndexConfig(user_config) = user_value {
                    if let TypedIndexConfig::Vector(vector_config) = &user_config.config {
                        Self::validate_vector_index_config(vector_config)?;
                    }
                }
                reconciled.insert(index_name.clone(), user_value.clone());
            }
        }

        Ok(reconciled)
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

/// Check if SPANN config is default
fn is_spann_config_default(spann_config: &SpannIndexConfig) -> bool {
    spann_config.search_nprobe == Some(default_search_nprobe())
        && spann_config.search_rng_factor == Some(default_search_rng_factor())
        && spann_config.search_rng_epsilon == Some(default_search_rng_epsilon())
        && spann_config.write_nprobe == Some(default_write_nprobe())
        && spann_config.nreplica_count == Some(default_nreplica_count())
        && spann_config.write_rng_factor == Some(default_write_rng_factor())
        && spann_config.write_rng_epsilon == Some(default_write_rng_epsilon())
        && spann_config.split_threshold == Some(default_split_threshold())
        && spann_config.num_samples_kmeans == Some(default_num_samples_kmeans())
        && spann_config.initial_lambda == Some(default_initial_lambda())
        && spann_config.reassign_neighbor_count == Some(default_reassign_neighbor_count())
        && spann_config.merge_threshold == Some(default_merge_threshold())
        && spann_config.num_centers_to_merge_to == Some(default_num_centers_to_merge_to())
        && spann_config.ef_construction == Some(default_construction_ef_spann())
        && spann_config.ef_search == Some(default_search_ef_spann())
        && spann_config.max_neighbors == Some(default_m_spann())
}

/// Type alias for value type index configurations
/// Maps index names to either boolean (simple enabled/disabled) or full index configuration
pub type ValueTypeIndexes = HashMap<IndexTypeName, IndexValue>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum IndexValue {
    /// Simple boolean for enabled/disabled state
    Boolean(bool),
    /// Full index configuration with enabled state and config parameters
    IndexConfig(TypedIndexConfigWithEnabled),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TypedIndexConfigWithEnabled {
    /// Whether the index is enabled
    pub enabled: bool,
    /// The specific index configuration
    #[serde(flatten)]
    pub config: TypedIndexConfig,
}

// Strong config types for when you need them

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SparseVectorIndexConfig {
    /// Embedding function configuration (flexible JSON for dynamic configurations)
    /// TODO(Sanket): Strongly type ef.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_function: Option<serde_json::Value>,
    /// Key to source the sparse vector from
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_key: Option<String>,
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

/// Strong-typed index configurations based on index type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TypedIndexConfig {
    Vector(VectorIndexConfig),
    SparseVector(SparseVectorIndexConfig),
    Fts(FtsIndexConfig),
    StringInverted(StringInvertedIndexConfig),
    IntInverted(IntInvertedIndexConfig),
    FloatInverted(FloatInvertedIndexConfig),
    BoolInverted(BoolInvertedIndexConfig),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InternalHnswConfiguration, InternalSpannConfiguration};

    // Test 1: Deserialize hardcoded JSON string (default schema structure)
    #[test]
    fn test_default_schema_deserialization() {
        // Test deserializing the standard default schema JSON structure
        let default_schema_json = serde_json::json!({
            "defaults": {
                "#string": { "$fts_index": false, "$string_inverted_index": true },
                "#int": { "$int_inverted_index": true },
                "#bool": { "$bool_inverted_index": true },
                "#float": { "$float_inverted_index": true },
                "#float_list": { "$vector_index": false },
                "#sparse_vector": { "$sparse_vector_index": false }
            },
            "key_overrides": {
                "$document": {
                    "#string": { "$fts_index": true, "$string_inverted_index": false }
                },
                "$embedding": {
                    "#float_list": {
                        "$vector_index": {
                            "enabled": true,
                            "Vector": { "source_key": "$document" }
                        }
                    }
                }
            }
        });

        let schema: InternalSchema =
            serde_json::from_value(default_schema_json).expect("Should deserialize default schema");

        // Validate defaults field by field
        assert_eq!(schema.defaults.len(), 6);

        // Validate string defaults
        let string_defaults = schema.defaults.get(&ValueTypeName::string()).unwrap();
        assert_eq!(string_defaults.len(), 2);
        assert_eq!(
            string_defaults.get(&IndexTypeName::fts()),
            Some(&IndexValue::Boolean(false))
        );
        assert_eq!(
            string_defaults.get(&IndexTypeName::string_inverted()),
            Some(&IndexValue::Boolean(true))
        );

        // Validate int defaults
        let int_defaults = schema.defaults.get(&ValueTypeName::int()).unwrap();
        assert_eq!(int_defaults.len(), 1);
        assert_eq!(
            int_defaults.get(&IndexTypeName::int_inverted()),
            Some(&IndexValue::Boolean(true))
        );

        // Validate bool defaults
        let bool_defaults = schema.defaults.get(&ValueTypeName::bool()).unwrap();
        assert_eq!(bool_defaults.len(), 1);
        assert_eq!(
            bool_defaults.get(&IndexTypeName::bool_inverted()),
            Some(&IndexValue::Boolean(true))
        );

        // Validate float defaults
        let float_defaults = schema.defaults.get(&ValueTypeName::float()).unwrap();
        assert_eq!(float_defaults.len(), 1);
        assert_eq!(
            float_defaults.get(&IndexTypeName::float_inverted()),
            Some(&IndexValue::Boolean(true))
        );

        // Validate float_list defaults
        let float_list_defaults = schema.defaults.get(&ValueTypeName::float_list()).unwrap();
        assert_eq!(float_list_defaults.len(), 1);
        assert_eq!(
            float_list_defaults.get(&IndexTypeName::vector()),
            Some(&IndexValue::Boolean(false))
        );

        // Validate sparse_vector defaults
        let sparse_vector_defaults = schema
            .defaults
            .get(&ValueTypeName::sparse_vector())
            .unwrap();
        assert_eq!(sparse_vector_defaults.len(), 1);
        assert_eq!(
            sparse_vector_defaults.get(&IndexTypeName::sparse_vector()),
            Some(&IndexValue::Boolean(false))
        );

        // Validate key_overrides field by field
        assert_eq!(schema.key_overrides.len(), 2);

        // Validate $document key overrides
        let document_overrides = schema.key_overrides.get("$document").unwrap();
        assert_eq!(document_overrides.len(), 1);
        let document_string_overrides = document_overrides.get(&ValueTypeName::string()).unwrap();
        assert_eq!(document_string_overrides.len(), 2);
        assert_eq!(
            document_string_overrides.get(&IndexTypeName::fts()),
            Some(&IndexValue::Boolean(true))
        );
        assert_eq!(
            document_string_overrides.get(&IndexTypeName::string_inverted()),
            Some(&IndexValue::Boolean(false))
        );

        // Validate $embedding key overrides
        let embedding_overrides = schema.key_overrides.get("$embedding").unwrap();
        assert_eq!(embedding_overrides.len(), 1);
        let embedding_float_list_overrides = embedding_overrides
            .get(&ValueTypeName::float_list())
            .unwrap();
        assert_eq!(embedding_float_list_overrides.len(), 1);

        // Validate the vector index configuration in $embedding
        let vector_index_value = embedding_float_list_overrides
            .get(&IndexTypeName::vector())
            .unwrap();
        match vector_index_value {
            IndexValue::IndexConfig(index_config) => {
                assert!(index_config.enabled);
                // Validate the strongly typed config structure
                match &index_config.config {
                    TypedIndexConfig::Vector(vector_config) => {
                        assert_eq!(vector_config.source_key, Some("$document".to_string()));
                        assert_eq!(vector_config.space, None);
                        assert_eq!(vector_config.embedding_function, None);
                        assert_eq!(vector_config.hnsw, None);
                        assert_eq!(vector_config.spann, None);
                    }
                    _ => panic!("Expected Vector config for vector index"),
                }
            }
            _ => panic!("Expected IndexConfig for vector index"),
        }
    }

    // Test 2: Malformed JSON deserialization fails
    #[test]
    fn test_malformed_json_handling() {
        // Test 1: defaults field is not an object
        let malformed_json = serde_json::json!({
            "defaults": "not_an_object",
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(malformed_json);
        assert!(
            result.is_err(),
            "Should reject when defaults is not an object"
        );

        // Test 2: key_overrides field is not an object
        let malformed_json = serde_json::json!({
            "defaults": {},
            "key_overrides": "not_an_object"
        });
        let result = serde_json::from_value::<InternalSchema>(malformed_json);
        assert!(
            result.is_err(),
            "Should reject when key_overrides is not an object"
        );

        // Test 3: Missing required fields
        let missing_fields_json = serde_json::json!({
            "defaults": {}
        });
        let result = serde_json::from_value::<InternalSchema>(missing_fields_json);
        assert!(result.is_err(), "Should reject missing key_overrides field");

        // Test 4: Missing defaults field
        let missing_defaults_json = serde_json::json!({
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(missing_defaults_json);
        assert!(result.is_err(), "Should reject missing defaults field");

        // Test 5: Invalid index value type (not boolean or object)
        // Let's test what actually happens with untagged enum
        let invalid_index_value_json = serde_json::json!({
            "defaults": {
                "#string": {
                    "$fts_index": "not_a_boolean"
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_index_value_json);
        // The untagged enum will try Boolean first, which fails, then try IndexConfig
        // But IndexConfig requires an object with "enabled" field, so this should fail
        assert!(
            result.is_err(),
            "Should reject non-boolean, non-object index value"
        );

        // Test 6: Invalid index config structure (missing enabled field)
        let invalid_config_json = serde_json::json!({
            "defaults": {
                "#float_list": {
                    "$vector_index": {
                        "config": { "source_key": "$document" }
                        // Missing "enabled" field
                    }
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_config_json);
        assert!(
            result.is_err(),
            "Should reject index config missing enabled field"
        );

        // Test 7: Invalid enabled field type (not boolean)
        let invalid_enabled_type_json = serde_json::json!({
            "defaults": {
                "#float_list": {
                    "$vector_index": {
                        "enabled": "not_a_boolean",
                        "config": { "source_key": "$document" }
                    }
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_enabled_type_json);
        assert!(result.is_err(), "Should reject non-boolean enabled field");

        // Test 8: Invalid config field type (not object)
        // Note: This now fails because config is strongly typed
        let invalid_config_type_json = serde_json::json!({
            "defaults": {
                "#float_list": {
                    "$vector_index": {
                        "enabled": true,
                        "Vector": "not_an_object"
                    }
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_config_type_json);
        assert!(
            result.is_err(),
            "Should reject non-object Vector config with strict validation"
        );

        // Test 9: Invalid key override structure (value type not object)
        let invalid_key_override_json = serde_json::json!({
            "defaults": {},
            "key_overrides": {
                "$document": {
                    "#string": "not_an_object"
                }
            }
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_key_override_json);
        assert!(
            result.is_err(),
            "Should reject invalid key override structure"
        );

        // Test 10: Invalid nested key override structure
        let invalid_nested_override_json = serde_json::json!({
            "defaults": {},
            "key_overrides": {
                "$document": "not_an_object"
            }
        });
        let result = serde_json::from_value::<InternalSchema>(invalid_nested_override_json);
        assert!(
            result.is_err(),
            "Should reject invalid nested key override structure"
        );

        // Test 11: Invalid unknown value type (should fail with strict validation)
        let unknown_value_type_json = serde_json::json!({
            "defaults": {
                "#unknown_value_type": {
                    "$fts_index": true
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(unknown_value_type_json);
        assert!(
            result.is_err(),
            "Should reject unknown value types with strict validation"
        );

        // Test 12: Invalid unknown index type (should fail with strict validation)
        let unknown_index_type_json = serde_json::json!({
            "defaults": {
                "#string": {
                    "$unknown_index_type": true
                }
            },
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(unknown_index_type_json);
        assert!(
            result.is_err(),
            "Should reject unknown index types with strict validation"
        );

        // Test 13: Empty but valid structure (should succeed)
        let empty_valid_json = serde_json::json!({
            "defaults": {},
            "key_overrides": {}
        });
        let result = serde_json::from_value::<InternalSchema>(empty_valid_json);
        assert!(result.is_ok(), "Should accept empty but valid structure");

        // Test 14: Valid structure with extra fields (should succeed - serde ignores unknown fields)
        let extra_fields_json = serde_json::json!({
            "defaults": {},
            "key_overrides": {},
            "extra_field": "should_be_ignored"
        });
        let result = serde_json::from_value::<InternalSchema>(extra_fields_json);
        assert!(
            result.is_ok(),
            "Should accept valid structure with extra fields"
        );
    }

    // Test 3: Serialize InternalSchema to JSON and validate format structure
    #[test]
    fn test_schema_serialization_format() {
        let schema = InternalSchema::new_default(KnnIndex::Spann);
        let json_string = serde_json::to_string_pretty(&schema).expect("Should serialize");

        // Parse the serialized JSON back to verify it's valid
        let parsed_json: serde_json::Value =
            serde_json::from_str(&json_string).expect("Should parse serialized JSON");

        // Verify the structure matches expected schema format
        assert!(parsed_json.get("defaults").is_some());
        assert!(parsed_json.get("key_overrides").is_some());

        // Verify defaults structure and values
        let defaults = parsed_json.get("defaults").unwrap().as_object().unwrap();
        assert_eq!(defaults.len(), 6);

        // Verify all expected value types exist
        assert!(defaults.contains_key("#string"));
        assert!(defaults.contains_key("#int"));
        assert!(defaults.contains_key("#bool"));
        assert!(defaults.contains_key("#float"));
        assert!(defaults.contains_key("#float_list"));
        assert!(defaults.contains_key("#sparse_vector"));

        // Verify string defaults
        let string_defaults = defaults.get("#string").unwrap().as_object().unwrap();
        assert_eq!(string_defaults.len(), 2);
        assert_eq!(
            string_defaults.get("$string_inverted_index"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            string_defaults.get("$fts_index"),
            Some(&serde_json::Value::Bool(false))
        );

        // Verify float_list defaults
        let float_list_defaults = defaults.get("#float_list").unwrap().as_object().unwrap();
        assert_eq!(float_list_defaults.len(), 1);
        assert_eq!(
            float_list_defaults.get("$vector_index"),
            Some(&serde_json::Value::Bool(false))
        );

        // Verify sparse_vector defaults
        let sparse_vector_defaults = defaults.get("#sparse_vector").unwrap().as_object().unwrap();
        assert_eq!(sparse_vector_defaults.len(), 1);
        assert_eq!(
            sparse_vector_defaults.get("$sparse_vector_index"),
            Some(&serde_json::Value::Bool(false))
        );

        // Verify key_overrides structure
        let key_overrides = parsed_json
            .get("key_overrides")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(key_overrides.len(), 2);
        assert!(key_overrides.contains_key("$document"));
        assert!(key_overrides.contains_key("$embedding"));

        // Verify $document overrides
        let document_overrides = key_overrides.get("$document").unwrap().as_object().unwrap();
        assert_eq!(document_overrides.len(), 1);
        let document_string_config = document_overrides
            .get("#string")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(document_string_config.len(), 2);
        assert_eq!(
            document_string_config.get("$fts_index"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            document_string_config.get("$string_inverted_index"),
            Some(&serde_json::Value::Bool(false))
        );

        // Verify $embedding has the correct vector index configuration
        let embedding_overrides = key_overrides
            .get("$embedding")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(embedding_overrides.len(), 1);
        let float_list_config = embedding_overrides
            .get("#float_list")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(float_list_config.len(), 1);
        let vector_index_config = float_list_config
            .get("$vector_index")
            .unwrap()
            .as_object()
            .unwrap();

        assert_eq!(
            vector_index_config.get("enabled"),
            Some(&serde_json::Value::Bool(true))
        );
        let vector_config = vector_index_config
            .get("Vector")
            .unwrap()
            .as_object()
            .unwrap();
        assert_eq!(
            vector_config.get("source_key"),
            Some(&serde_json::Value::String("$document".to_string()))
        );
        assert_eq!(vector_config.len(), 3); // source_key, space, and spann should be present

        // Test round-trip: deserialize the JSON back to InternalSchema
        let deserialized_schema: InternalSchema =
            serde_json::from_value(parsed_json).expect("Should deserialize back to InternalSchema");

        // Verify the deserialized schema matches the original
        assert_eq!(
            schema, deserialized_schema,
            "Round-trip serialization should preserve data"
        );
    }

    // Test 4: Validate that InternalSchema::new_default(KnnIndex::Spann) creates the expected default JSON
    #[test]
    fn test_default_schema_matches_expected() {
        let schema = InternalSchema::new_default(KnnIndex::Spann);
        let schema_json = serde_json::to_value(&schema).expect("Should serialize schema");

        // This is the expected default JSON structure for the schema
        let expected_default_json = serde_json::json!({
            "defaults": {
                "#string": { "$fts_index": false, "$string_inverted_index": true },
                "#int": { "$int_inverted_index": true },
                "#bool": { "$bool_inverted_index": true },
                "#float": { "$float_inverted_index": true },
                "#float_list": { "$vector_index": false },
                "#sparse_vector": { "$sparse_vector_index": false }
            },
            "key_overrides": {
                "$document": {
                    "#string": { "$fts_index": true, "$string_inverted_index": false }
                },
                "$embedding": {
                    "#float_list": {
                        "$vector_index": {
                            "enabled": true,
                            "Vector": {
                                "source_key": "$document",
                                "space": "l2",
                                "spann": {
                                    "search_nprobe": 64,
                                    "search_rng_factor": 1.0,
                                    "search_rng_epsilon": 10.0,
                                    "write_nprobe": 32,
                                    "nreplica_count": 8,
                                    "write_rng_factor": 1.0,
                                    "write_rng_epsilon": 5.0,
                                    "split_threshold": 50,
                                    "num_samples_kmeans": 1000,
                                    "initial_lambda": 100.0,
                                    "reassign_neighbor_count": 64,
                                    "merge_threshold": 25,
                                    "num_centers_to_merge_to": 8,
                                    "ef_construction": 200,
                                    "ef_search": 200,
                                    "max_neighbors": 64
                                }
                            }
                        }
                    }
                }
            }
        });

        // Compare the structures - they should be identical
        assert_eq!(
            schema_json, expected_default_json,
            "InternalSchema::new_default(KnnIndex::Spann) should produce the expected default JSON structure"
        );
    }

    // Test 5: Test embedding function serialization/deserialization - Legacy
    #[test]
    fn test_legacy_embedding_function_serialization() {
        use crate::collection_configuration::EmbeddingFunctionConfiguration;

        // Create a VectorIndexConfig with legacy embedding function
        let vector_config = VectorIndexConfig {
            space: Some(Space::L2),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
            source_key: Some("$document".to_string()),
            hnsw: None,
            spann: None,
        };

        // Serialize to JSON
        let json =
            serde_json::to_value(&vector_config).expect("Should serialize VectorIndexConfig");

        // Verify the structure
        assert_eq!(json["space"], "l2");
        assert_eq!(json["source_key"], "$document");
        assert_eq!(json["embedding_function"]["type"], "legacy");
        assert!(json["hnsw"].is_null());
        assert!(json["spann"].is_null());

        // Deserialize back to VectorIndexConfig
        let deserialized_config: VectorIndexConfig =
            serde_json::from_value(json).expect("Should deserialize VectorIndexConfig");

        // Verify the deserialized config matches the original
        assert_eq!(deserialized_config.space, Some(Space::L2));
        assert_eq!(
            deserialized_config.source_key,
            Some("$document".to_string())
        );
        assert_eq!(
            deserialized_config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );
        assert!(deserialized_config.hnsw.is_none());
        assert!(deserialized_config.spann.is_none());
    }

    // Test 6: Test embedding function serialization/deserialization - Known
    #[test]
    fn test_known_embedding_function_serialization() {
        use crate::collection_configuration::{
            EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration,
        };

        // Create a VectorIndexConfig with known embedding function
        let known_config = EmbeddingFunctionNewConfiguration {
            name: "default".to_string(),
            config: serde_json::json!({"param1": "value1", "param2": 42}),
        };
        let vector_config = VectorIndexConfig {
            space: Some(Space::Cosine),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(known_config)),
            source_key: Some("$document".to_string()),
            hnsw: None,
            spann: None,
        };

        // Serialize to JSON
        let json =
            serde_json::to_value(&vector_config).expect("Should serialize VectorIndexConfig");

        // Verify the structure
        assert_eq!(json["space"], "cosine");
        assert_eq!(json["source_key"], "$document");
        assert_eq!(json["embedding_function"]["type"], "known");
        assert_eq!(json["embedding_function"]["name"], "default");
        assert_eq!(json["embedding_function"]["config"]["param1"], "value1");
        assert_eq!(json["embedding_function"]["config"]["param2"], 42);

        // Deserialize back to VectorIndexConfig
        let deserialized_config: VectorIndexConfig =
            serde_json::from_value(json).expect("Should deserialize VectorIndexConfig");

        // Verify the deserialized config matches the original
        assert_eq!(deserialized_config.space, Some(Space::Cosine));
        assert_eq!(
            deserialized_config.source_key,
            Some("$document".to_string())
        );

        // Verify the embedding function configuration
        match deserialized_config.embedding_function {
            Some(EmbeddingFunctionConfiguration::Known(known)) => {
                assert_eq!(known.name, "default");
                assert_eq!(known.config["param1"], "value1");
                assert_eq!(known.config["param2"], 42);
            }
            _ => panic!("Expected Known embedding function configuration"),
        }
    }

    // Test 7: Test embedding function round-trip serialization
    #[test]
    fn test_embedding_function_round_trip() {
        use crate::collection_configuration::{
            EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration,
        };

        // Test both legacy and known embedding functions
        let test_cases = vec![
            ("legacy", EmbeddingFunctionConfiguration::Legacy),
            (
                "known",
                EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                    name: "test_ef".to_string(),
                    config: serde_json::json!({"test_param": "test_value"}),
                }),
            ),
        ];

        for (case_name, embedding_function) in test_cases {
            let vector_config = VectorIndexConfig {
                space: Some(Space::L2),
                embedding_function: Some(embedding_function),
                source_key: Some("$test".to_string()),
                hnsw: None,
                spann: None,
            };

            // Serialize to JSON
            let json = serde_json::to_value(&vector_config)
                .unwrap_or_else(|_| panic!("Should serialize VectorIndexConfig for {}", case_name));

            // Deserialize back to VectorIndexConfig
            let deserialized_config: VectorIndexConfig = serde_json::from_value(json)
                .unwrap_or_else(|_| {
                    panic!("Should deserialize VectorIndexConfig for {}", case_name)
                });

            // Verify round-trip preservation
            assert_eq!(
                vector_config.embedding_function, deserialized_config.embedding_function,
                "Embedding function should be preserved in round-trip for {}",
                case_name
            );
        }
    }

    // Test 8: Test embedding function validation and error handling
    #[test]
    fn test_embedding_function_validation() {
        use crate::collection_configuration::EmbeddingFunctionConfiguration;

        // Test valid legacy embedding function
        let valid_legacy_json = serde_json::json!({
            "space": "l2",
            "embedding_function": {"type": "legacy"},
            "source_key": "$document"
        });
        let config: VectorIndexConfig = serde_json::from_value(valid_legacy_json)
            .expect("Should deserialize valid legacy embedding function");

        assert_eq!(
            config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Legacy)
        );

        // Test valid known embedding function
        let valid_known_json = serde_json::json!({
            "space": "cosine",
            "embedding_function": {
                "type": "known",
                "name": "test_ef",
                "config": {"param": "value"}
            },
            "source_key": "$document"
        });
        let config: VectorIndexConfig = serde_json::from_value(valid_known_json)
            .expect("Should deserialize valid known embedding function");

        match config.embedding_function {
            Some(EmbeddingFunctionConfiguration::Known(known)) => {
                assert_eq!(known.name, "test_ef");
                assert_eq!(known.config["param"], "value");
            }
            _ => panic!("Expected Known embedding function configuration"),
        }

        // Test invalid embedding function type
        let invalid_json = serde_json::json!({
            "space": "l2",
            "embedding_function": {"type": "invalid_type"},
            "source_key": "$document"
        });
        let result: Result<VectorIndexConfig, _> = serde_json::from_value(invalid_json);
        assert!(
            result.is_err(),
            "Should fail to deserialize invalid embedding function type"
        );
    }

    // Test 9: Test embedding function in full schema context
    #[test]
    fn test_embedding_function_in_schema_context() {
        use crate::collection_configuration::{
            EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration,
        };

        // Create a schema with embedding function configuration
        let vector_config = VectorIndexConfig {
            space: Some(Space::L2),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "default".to_string(),
                    config: serde_json::json!({"model": "test_model"}),
                },
            )),
            source_key: Some("$document".to_string()),
            hnsw: None,
            spann: None,
        };

        // Create an InternalSchema with this configuration using the correct structure
        let mut key_overrides = HashMap::new();
        let mut float_list_config = HashMap::new();
        float_list_config.insert(
            IndexTypeName(VECTOR_INDEX_NAME.to_string()),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::Vector(vector_config),
            }),
        );
        key_overrides.insert(
            ValueTypeName(FLOAT_LIST_VALUE_NAME.to_string()),
            float_list_config,
        );

        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: vec![("$embedding".to_string(), key_overrides)]
                .into_iter()
                .collect(),
        };

        // Serialize the full schema
        let schema_json = serde_json::to_value(&schema).expect("Should serialize InternalSchema");

        // Verify the embedding function is present in the schema
        let embedding_config = schema_json["key_overrides"]["$embedding"]["#float_list"]
            ["$vector_index"]["Vector"]
            .as_object()
            .unwrap();
        assert_eq!(embedding_config["space"], "l2");
        assert_eq!(embedding_config["source_key"], "$document");
        assert_eq!(embedding_config["embedding_function"]["type"], "known");
        assert_eq!(embedding_config["embedding_function"]["name"], "default");
        assert_eq!(
            embedding_config["embedding_function"]["config"]["model"],
            "test_model"
        );

        // Deserialize back to InternalSchema
        let deserialized_schema: InternalSchema = serde_json::from_value(schema_json)
            .expect("Should deserialize InternalSchema with embedding function");

        // Verify the embedding function configuration is preserved
        let deserialized_vector_config = match &deserialized_schema.key_overrides["$embedding"]
            [&ValueTypeName(FLOAT_LIST_VALUE_NAME.to_string())]
            [&IndexTypeName(VECTOR_INDEX_NAME.to_string())]
        {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                ..
            }) => config,
            _ => panic!("Expected Vector index configuration"),
        };

        match &deserialized_vector_config.embedding_function {
            Some(EmbeddingFunctionConfiguration::Known(known)) => {
                assert_eq!(known.name, "default");
                assert_eq!(known.config["model"], "test_model");
            }
            _ => panic!("Expected Known embedding function configuration"),
        }
    }

    // ===== RECONCILE METHOD TESTS =====

    #[test]
    fn test_reconcile_with_none_returns_defaults() {
        let reconciled = InternalSchema::reconcile_with_defaults(None).unwrap();
        let defaults = InternalSchema::new_default(KnnIndex::Spann);

        // Should be identical to defaults
        assert_eq!(reconciled.defaults, defaults.defaults);
        assert_eq!(reconciled.key_overrides, defaults.key_overrides);
    }

    #[test]
    fn test_reconcile_with_empty_user_schema() {
        let empty_user = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: HashMap::new(),
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(empty_user)).unwrap();
        let defaults = InternalSchema::new_default(KnnIndex::Spann);

        // Should be identical to defaults since user schema is empty
        assert_eq!(reconciled.defaults, defaults.defaults);
        assert_eq!(reconciled.key_overrides, defaults.key_overrides);
    }

    #[test]
    fn test_reconcile_user_overrides_defaults() {
        let mut user_defaults = HashMap::new();
        let mut user_string_indexes = HashMap::new();
        user_string_indexes.insert(IndexTypeName::fts(), IndexValue::Boolean(true));
        user_defaults.insert(ValueTypeName::string(), user_string_indexes);

        let user_schema = InternalSchema {
            defaults: user_defaults,
            key_overrides: HashMap::new(),
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // User should override the default FTS setting
        let string_indexes = reconciled.defaults.get(&ValueTypeName::string()).unwrap();
        assert_eq!(
            string_indexes.get(&IndexTypeName::fts()),
            Some(&IndexValue::Boolean(true))
        );

        // Other defaults should still be present
        assert!(reconciled
            .defaults
            .contains_key(&ValueTypeName::float_list()));
        assert!(reconciled
            .defaults
            .contains_key(&ValueTypeName::sparse_vector()));
    }

    #[test]
    fn test_reconcile_key_overrides() {
        let mut user_key_overrides = HashMap::new();
        let mut custom_key_overrides = HashMap::new();
        let mut custom_string_indexes = HashMap::new();
        custom_string_indexes.insert(IndexTypeName::fts(), IndexValue::Boolean(false));
        custom_key_overrides.insert(ValueTypeName::string(), custom_string_indexes);
        user_key_overrides.insert("custom_key".to_string(), custom_key_overrides);

        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // User key override should be present
        assert!(reconciled.key_overrides.contains_key("custom_key"));

        // Default key overrides should still be present
        assert!(reconciled.key_overrides.contains_key("$document"));
        assert!(reconciled.key_overrides.contains_key("$embedding"));
    }

    #[test]
    fn test_reconcile_vector_config_field_merging() {
        // Create user schema with partial vector config
        let mut user_key_overrides = HashMap::new();
        let mut embedding_overrides = HashMap::new();
        let mut float_list_indexes = HashMap::new();

        // User only specifies space, other fields should come from defaults
        let user_vector_config = VectorIndexConfig {
            space: Some(Space::Cosine),
            embedding_function: None, // User doesn't specify
            source_key: None,         // User doesn't specify
            hnsw: None,
            spann: None,
        };

        float_list_indexes.insert(
            IndexTypeName::vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::Vector(user_vector_config),
            }),
        );
        embedding_overrides.insert(ValueTypeName::float_list(), float_list_indexes);
        user_key_overrides.insert("$embedding".to_string(), embedding_overrides);

        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // Check that user space is preserved
        let embedding_config = reconciled
            .key_overrides
            .get("$embedding")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match embedding_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                ..
            }) => {
                assert_eq!(config.space, Some(Space::Cosine));
                // Default source_key should be preserved
                assert_eq!(config.source_key, Some("$document".to_string()));
            }
            _ => panic!("Expected Vector index configuration"),
        }
    }

    #[test]
    fn test_reconcile_boolean_vs_boolean() {
        // Test case where user has boolean and default is also boolean
        let mut user_defaults = HashMap::new();
        let mut user_float_list_indexes = HashMap::new();
        user_float_list_indexes.insert(IndexTypeName::vector(), IndexValue::Boolean(true));
        user_defaults.insert(ValueTypeName::float_list(), user_float_list_indexes);

        let user_schema = InternalSchema {
            defaults: user_defaults,
            key_overrides: HashMap::new(),
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // User boolean should override default boolean
        let float_list_indexes = reconciled
            .defaults
            .get(&ValueTypeName::float_list())
            .unwrap();

        assert_eq!(
            float_list_indexes.get(&IndexTypeName::vector()),
            Some(&IndexValue::Boolean(true))
        );
    }

    #[test]
    fn test_reconcile_boolean_vs_config() {
        // Test case where user has boolean but default has config
        // We'll use the $embedding key override which has a config by default
        let mut user_key_overrides = HashMap::new();
        let mut embedding_overrides = HashMap::new();
        let mut float_list_indexes = HashMap::new();

        // User specifies boolean false for vector index
        float_list_indexes.insert(IndexTypeName::vector(), IndexValue::Boolean(false));
        embedding_overrides.insert(ValueTypeName::float_list(), float_list_indexes);
        user_key_overrides.insert("$embedding".to_string(), embedding_overrides);

        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // User boolean should override default config but preserve config structure
        let embedding_config = reconciled
            .key_overrides
            .get("$embedding")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match embedding_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled,
                config: TypedIndexConfig::Vector(config),
            }) => {
                assert!(!enabled); // User specified false
                                   // Should preserve default config structure
                assert_eq!(config.source_key, Some("$document".to_string()));
            }
            _ => panic!("Expected IndexConfig with Vector config"),
        }
    }

    #[test]
    fn test_reconcile_config_vs_boolean() {
        // Test case where user has config but default has boolean
        let mut user_key_overrides = HashMap::new();
        let mut custom_overrides = HashMap::new();
        let mut custom_float_list_indexes = HashMap::new();

        let user_vector_config = VectorIndexConfig {
            space: Some(Space::L2),
            embedding_function: None,
            source_key: Some("custom_source".to_string()),
            hnsw: None,
            spann: None,
        };

        custom_float_list_indexes.insert(
            IndexTypeName::vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::Vector(user_vector_config),
            }),
        );
        custom_overrides.insert(ValueTypeName::float_list(), custom_float_list_indexes);
        user_key_overrides.insert("custom_key".to_string(), custom_overrides);

        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // User config should be preserved
        let custom_config = reconciled
            .key_overrides
            .get("custom_key")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match custom_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                enabled,
            }) => {
                assert!(enabled);
                assert_eq!(config.space, Some(Space::L2));
                assert_eq!(config.source_key, Some("custom_source".to_string()));
            }
            _ => panic!("Expected Vector index configuration"),
        }
    }

    #[test]
    fn test_reconcile_sparse_vector_config() {
        // Test sparse vector config field merging
        let mut user_key_overrides = HashMap::new();
        let mut sparse_overrides = HashMap::new();
        let mut sparse_vector_indexes = HashMap::new();

        // User only specifies source_key, embedding_function should come from defaults
        let user_sparse_config = SparseVectorIndexConfig {
            embedding_function: None, // User doesn't specify
            source_key: Some("custom_sparse_source".to_string()),
        };

        sparse_vector_indexes.insert(
            IndexTypeName::sparse_vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::SparseVector(user_sparse_config),
            }),
        );
        sparse_overrides.insert(ValueTypeName::sparse_vector(), sparse_vector_indexes);
        user_key_overrides.insert("custom_sparse_key".to_string(), sparse_overrides);

        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // Check that user source_key is preserved
        let sparse_config = reconciled
            .key_overrides
            .get("custom_sparse_key")
            .unwrap()
            .get(&ValueTypeName::sparse_vector())
            .unwrap()
            .get(&IndexTypeName::sparse_vector())
            .unwrap();

        match sparse_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::SparseVector(config),
                ..
            }) => {
                assert_eq!(config.source_key, Some("custom_sparse_source".to_string()));
            }
            _ => panic!("Expected SparseVector index configuration"),
        }
    }

    #[test]
    fn test_reconcile_complex_scenario() {
        // Test a complex scenario with multiple overrides and field merging
        let mut user_defaults = HashMap::new();
        let mut user_key_overrides = HashMap::new();

        // User overrides default FTS setting
        let mut user_string_indexes = HashMap::new();
        user_string_indexes.insert(IndexTypeName::fts(), IndexValue::Boolean(true));
        user_defaults.insert(ValueTypeName::string(), user_string_indexes);

        // User adds custom key with partial vector config
        let mut custom_overrides = HashMap::new();
        let mut custom_float_list_indexes = HashMap::new();

        let user_vector_config = VectorIndexConfig {
            space: Some(Space::Ip),
            embedding_function: None, // Should use default
            source_key: Some("custom_doc".to_string()),
            hnsw: None,
            spann: None,
        };

        custom_float_list_indexes.insert(
            IndexTypeName::vector(),
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                enabled: true,
                config: TypedIndexConfig::Vector(user_vector_config),
            }),
        );
        custom_overrides.insert(ValueTypeName::float_list(), custom_float_list_indexes);
        user_key_overrides.insert("custom_embedding".to_string(), custom_overrides);

        let user_schema = InternalSchema {
            defaults: user_defaults,
            key_overrides: user_key_overrides,
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // Verify user defaults are applied
        let string_indexes = reconciled.defaults.get(&ValueTypeName::string()).unwrap();
        assert_eq!(
            string_indexes.get(&IndexTypeName::fts()),
            Some(&IndexValue::Boolean(true))
        );

        // Verify custom key override is applied with field merging
        let custom_config = reconciled
            .key_overrides
            .get("custom_embedding")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match custom_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                enabled,
            }) => {
                assert!(enabled);
                assert_eq!(config.space, Some(Space::Ip));
                assert_eq!(config.source_key, Some("custom_doc".to_string()));
            }
            _ => panic!("Expected Vector index configuration"),
        }

        // Verify default key overrides are still present
        assert!(reconciled.key_overrides.contains_key("$document"));
        assert!(reconciled.key_overrides.contains_key("$embedding"));
    }

    #[test]
    fn test_reconcile_preserves_default_structure() {
        // Test that reconciliation preserves the complete default structure
        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: HashMap::new(),
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();
        let defaults = InternalSchema::new_default(KnnIndex::Spann);

        // All default value types should be present
        assert!(reconciled.defaults.contains_key(&ValueTypeName::string()));
        assert!(reconciled.defaults.contains_key(&ValueTypeName::float()));
        assert!(reconciled
            .defaults
            .contains_key(&ValueTypeName::float_list()));
        assert!(reconciled
            .defaults
            .contains_key(&ValueTypeName::sparse_vector()));
        assert!(reconciled.defaults.contains_key(&ValueTypeName::bool()));
        assert!(reconciled.defaults.contains_key(&ValueTypeName::int()));

        // All default key overrides should be present
        assert!(reconciled.key_overrides.contains_key("$document"));
        assert!(reconciled.key_overrides.contains_key("$embedding"));

        // Should be identical to defaults
        assert_eq!(reconciled.defaults, defaults.defaults);
        assert_eq!(reconciled.key_overrides, defaults.key_overrides);
    }

    #[test]
    fn test_reconcile_hnsw_field_merging() {
        // Test that HNSW configs are merged field by field, not blanket overwritten

        // Create a user schema with partial HNSW config
        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();

                // User provides partial HNSW config (only ef_construction)
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: true,
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::Cosine),
                            embedding_function: None,
                            source_key: None,
                            hnsw: Some(HnswIndexConfig {
                                ef_construction: Some(200), // User overrides this
                                max_neighbors: None,        // User doesn't specify
                                ef_search: None,            // User doesn't specify
                                num_threads: None,
                                batch_size: None,
                                sync_threshold: None,
                                resize_factor: None,
                            }),
                            spann: None, // Only HNSW, no SPANN
                        }),
                    }),
                );

                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // Verify that the reconciled config has field-level merging
        let vector_config = reconciled
            .key_overrides
            .get("$embedding")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match vector_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                enabled,
            }) => {
                assert!(enabled);

                // Verify HNSW field-level merging
                if let Some(hnsw) = &config.hnsw {
                    // User's ef_construction should be preserved
                    assert_eq!(hnsw.ef_construction, Some(200));
                    // User's None values should be preserved (not overridden by defaults)
                    assert_eq!(hnsw.max_neighbors, None);
                    assert_eq!(hnsw.ef_search, None);
                } else {
                    panic!("HNSW config should be present");
                }

                // Should not have SPANN config
                assert!(config.spann.is_none());

                // Verify that the user's space value is preserved
                assert_eq!(config.space, Some(Space::Cosine));
            }
            _ => panic!("Expected Vector config"),
        }
    }

    #[test]
    fn test_reconcile_spann_field_merging() {
        // Test that SPANN configs are merged field by field, not blanket overwritten

        // Create a user schema with partial SPANN config
        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();

                // User provides partial SPANN config (only search_nprobe)
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: true,
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::L2),
                            embedding_function: None,
                            source_key: None,
                            hnsw: None, // Only SPANN, no HNSW
                            spann: Some(SpannIndexConfig {
                                search_nprobe: Some(100), // User overrides this
                                search_rng_factor: None,  // User doesn't specify
                                search_rng_epsilon: None, // User doesn't specify
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
                            }),
                        }),
                    }),
                );

                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        let reconciled = InternalSchema::reconcile_with_defaults(Some(user_schema)).unwrap();

        // Verify that the reconciled config has field-level merging
        let vector_config = reconciled
            .key_overrides
            .get("$embedding")
            .unwrap()
            .get(&ValueTypeName::float_list())
            .unwrap()
            .get(&IndexTypeName::vector())
            .unwrap();

        match vector_config {
            IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                config: TypedIndexConfig::Vector(config),
                enabled,
            }) => {
                assert!(enabled);

                // Should not have HNSW config
                assert!(config.hnsw.is_none());

                // Verify SPANN field-level merging
                if let Some(spann) = &config.spann {
                    // User's search_nprobe should be preserved
                    assert_eq!(spann.search_nprobe, Some(100));
                    // Default values should be used for fields user didn't specify
                    assert_eq!(spann.search_rng_factor, Some(1.0));
                    assert_eq!(spann.search_rng_epsilon, Some(10.0));
                } else {
                    panic!("SPANN config should be present");
                }

                // Verify that the user's space value is preserved
                assert_eq!(config.space, Some(Space::L2));
            }
            _ => panic!("Expected Vector config"),
        }
    }

    #[test]
    fn test_reconcile_both_hnsw_and_spann_error() {
        // Test that specifying both HNSW and SPANN configurations results in an error
        let user_schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();

                // User incorrectly provides both HNSW and SPANN configs
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: true,
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::Cosine),
                            embedding_function: None,
                            source_key: None,
                            hnsw: Some(HnswIndexConfig {
                                ef_construction: Some(200),
                                max_neighbors: None,
                                ef_search: None,
                                num_threads: None,
                                batch_size: None,
                                sync_threshold: None,
                                resize_factor: None,
                            }),
                            spann: Some(SpannIndexConfig {
                                search_nprobe: Some(100),
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
                            }),
                        }),
                    }),
                );

                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        let result = InternalSchema::reconcile_with_defaults(Some(user_schema));

        // Should return an error
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("Cannot specify both HNSW and SPANN configurations"));
    }

    #[test]
    fn test_reconcile_collection_default_uses_schema() {
        // Collection config is default → schema is source of truth
        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: true,
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::Cosine),
                            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                            source_key: None,
                            hnsw: Some(HnswIndexConfig {
                                ef_construction: Some(200),
                                max_neighbors: Some(32),
                                ef_search: Some(100),
                                num_threads: Some(8),
                                batch_size: Some(2000),
                                sync_threshold: Some(20000),
                                resize_factor: Some(3.0),
                            }),
                            spann: None,
                        }),
                    }),
                );
                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        // Collection config with all default values
        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: default_space(),
                ef_construction: default_construction_ef(),
                ef_search: default_search_ef(),
                max_neighbors: default_m(),
                num_threads: default_num_threads(),
                batch_size: default_batch_size(),
                sync_threshold: default_sync_threshold(),
                resize_factor: default_resize_factor(),
            }),
            embedding_function: None,
        };

        let result =
            InternalSchema::reconcile_with_collection_config(schema.clone(), collection_config);
        assert!(result.is_ok());
        let reconciled = result.unwrap();

        // Should use schema (collection config is default)
        assert_eq!(reconciled.key_overrides, schema.key_overrides);
    }

    #[test]
    fn test_reconcile_both_non_default_error() {
        // Both schema and collection are non-default → error
        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: true,
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::Cosine),
                            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
                            source_key: None,
                            hnsw: Some(HnswIndexConfig {
                                ef_construction: Some(200),
                                max_neighbors: Some(32),
                                ef_search: Some(100),
                                num_threads: Some(8),
                                batch_size: Some(2000),
                                sync_threshold: Some(20000),
                                resize_factor: Some(3.0),
                            }),
                            spann: None,
                        }),
                    }),
                );
                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        // Collection config with non-default values
        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Ip,
                ef_construction: 300,
                ef_search: 150,
                max_neighbors: 64,
                num_threads: 16,
                batch_size: 5000,
                sync_threshold: 50000,
                resize_factor: 5.0,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };

        let result = InternalSchema::reconcile_with_collection_config(schema, collection_config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot set both collection config and schema at the same time"));
    }

    #[test]
    fn test_reconcile_collection_non_default_schema_default_uses_collection() {
        // Collection config is non-default, schema is default → override schema with collection config
        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: HashMap::new(), // Empty schema (default)
        };

        // Collection config with non-default values
        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Cosine,
                ef_construction: 200,
                ef_search: 100,
                max_neighbors: 32,
                num_threads: 8,
                batch_size: 2000,
                sync_threshold: 20000,
                resize_factor: 3.0,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };

        let result = InternalSchema::reconcile_with_collection_config(schema, collection_config);
        assert!(result.is_ok());
        let reconciled = result.unwrap();

        // Should have collection config values in schema
        let embedding_override = reconciled
            .key_overrides
            .get("$embedding")
            .and_then(|overrides| overrides.get(&ValueTypeName::float_list()))
            .and_then(|indexes| indexes.get(&IndexTypeName::vector()));

        match embedding_override {
            Some(IndexValue::IndexConfig(config)) => {
                if let TypedIndexConfig::Vector(vector_config) = &config.config {
                    assert_eq!(vector_config.space, Some(Space::Cosine));
                    assert_eq!(
                        vector_config.embedding_function,
                        Some(EmbeddingFunctionConfiguration::Legacy)
                    );
                    if let Some(hnsw) = &vector_config.hnsw {
                        assert_eq!(hnsw.ef_construction, Some(200));
                        assert_eq!(hnsw.max_neighbors, Some(32));
                    } else {
                        panic!("Expected HNSW config");
                    }
                } else {
                    panic!("Expected Vector config");
                }
            }
            _ => panic!("Expected IndexConfig"),
        }
    }

    #[test]
    fn test_reconcile_preserves_schema_source_key_and_enabled_status() {
        // Test that override_schema_with_collection_config preserves schema's source_key and enabled status
        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: {
                let mut overrides = HashMap::new();
                let mut float_list_indexes = HashMap::new();
                let mut vector_indexes = HashMap::new();
                vector_indexes.insert(
                    IndexTypeName::vector(),
                    IndexValue::IndexConfig(TypedIndexConfigWithEnabled {
                        enabled: false, // Schema has enabled=false
                        config: TypedIndexConfig::Vector(VectorIndexConfig {
                            space: Some(Space::L2),
                            embedding_function: None,
                            source_key: Some("custom_source".to_string()), // Schema has custom source_key
                            hnsw: None,
                            spann: None,
                        }),
                    }),
                );
                float_list_indexes.insert(ValueTypeName::float_list(), vector_indexes);
                overrides.insert("$embedding".to_string(), float_list_indexes);
                overrides
            },
        };

        // Collection config with different values
        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Cosine, // Different space
                ef_construction: 300,
                ef_search: 150,
                max_neighbors: 64,
                num_threads: 16,
                batch_size: 5000,
                sync_threshold: 50000,
                resize_factor: 5.0,
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Legacy),
        };

        let result = InternalSchema::reconcile_with_collection_config(schema, collection_config);
        assert!(result.is_ok());
        let reconciled = result.unwrap();

        // Verify that schema's source_key and enabled status are preserved
        let embedding_override = reconciled
            .key_overrides
            .get("$embedding")
            .and_then(|overrides| overrides.get(&ValueTypeName::float_list()))
            .and_then(|indexes| indexes.get(&IndexTypeName::vector()));

        match embedding_override {
            Some(IndexValue::IndexConfig(config)) => {
                // Should preserve schema's enabled=false
                assert!(!config.enabled, "Should preserve schema's enabled=false");

                if let TypedIndexConfig::Vector(vector_config) = &config.config {
                    // Should preserve schema's custom source_key
                    assert_eq!(
                        vector_config.source_key,
                        Some("custom_source".to_string()),
                        "Should preserve schema's custom source_key"
                    );

                    // Should use collection config's space
                    assert_eq!(
                        vector_config.space,
                        Some(Space::Cosine),
                        "Should use collection config's space"
                    );

                    // Should use collection config's embedding function
                    assert_eq!(
                        vector_config.embedding_function,
                        Some(EmbeddingFunctionConfiguration::Legacy),
                        "Should use collection config's embedding function"
                    );
                } else {
                    panic!("Expected Vector config");
                }
            }
            _ => panic!("Expected IndexConfig"),
        }
    }

    #[test]
    fn test_reconcile_preserves_schema_none_source_key() {
        // Test that when schema is default, collection config is applied correctly
        // This test verifies that the override logic works when schema is empty
        let schema = InternalSchema {
            defaults: HashMap::new(),
            key_overrides: HashMap::new(), // Empty schema (default)
        };

        let collection_config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                space: Space::L2,
                search_nprobe: 32,
                search_rng_factor: 2.0,
                search_rng_epsilon: 5.0,
                write_nprobe: 16,
                nreplica_count: 4,
                write_rng_factor: 1.5,
                write_rng_epsilon: 2.5,
                split_threshold: 25,
                num_samples_kmeans: 500,
                initial_lambda: 50.0,
                reassign_neighbor_count: 32,
                merge_threshold: 12,
                num_centers_to_merge_to: 4,
                ef_construction: 100,
                ef_search: 100,
                max_neighbors: 32,
            }),
            embedding_function: None,
        };

        let result = InternalSchema::reconcile_with_collection_config(schema, collection_config);
        if let Err(e) = &result {
            panic!("Expected Ok but got error: {}", e);
        }
        let reconciled = result.unwrap();

        let embedding_override = reconciled
            .key_overrides
            .get("$embedding")
            .and_then(|overrides| overrides.get(&ValueTypeName::float_list()))
            .and_then(|indexes| indexes.get(&IndexTypeName::vector()));

        match embedding_override {
            Some(IndexValue::IndexConfig(config)) => {
                if let TypedIndexConfig::Vector(vector_config) = &config.config {
                    // Since schema was empty, source_key should be None (no existing value to preserve)
                    assert_eq!(
                        vector_config.source_key, None,
                        "Should have None source_key when schema was empty"
                    );

                    // Should use collection config's space
                    assert_eq!(vector_config.space, Some(Space::L2));

                    // Should have SPANN config from collection config
                    assert!(vector_config.spann.is_some());
                    assert!(vector_config.hnsw.is_none());

                    // Should use collection config's embedding function (None in this case)
                    assert_eq!(vector_config.embedding_function, None);
                } else {
                    panic!("Expected Vector config");
                }
            }
            _ => panic!("Expected IndexConfig"),
        }
    }
}

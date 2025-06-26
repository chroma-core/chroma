use crate::{
    HnswConfiguration, HnswParametersFromSegmentError, InternalHnswConfiguration,
    InternalSpannConfiguration, Metadata, Segment, SpannConfiguration, UpdateHnswConfiguration,
    UpdateSpannConfiguration,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, ToSchema, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    Int,
    Float,
    String,
    Boolean,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Int => write!(f, "int"),
            ValueType::Float => write!(f, "float"),
            ValueType::String => write!(f, "string"),
            ValueType::Boolean => write!(f, "boolean"),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Copy)]
pub enum KnnIndex {
    #[serde(alias = "hnsw")]
    Hnsw,
    #[serde(alias = "spann")]
    Spann,
}

pub fn default_default_knn_index() -> KnnIndex {
    KnnIndex::Hnsw
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, ToSchema)]
pub struct CollectionSchema {
    pub metadata_index: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum EmbeddingFunctionConfiguration {
    #[serde(rename = "legacy")]
    Legacy,
    #[serde(rename = "known")]
    Known(EmbeddingFunctionNewConfiguration),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EmbeddingFunctionNewConfiguration {
    pub name: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VectorIndexConfiguration {
    Hnsw(InternalHnswConfiguration),
    Spann(InternalSpannConfiguration),
}

impl VectorIndexConfiguration {
    pub fn update(&mut self, vector_index: &VectorIndexConfiguration) {
        match (self, vector_index) {
            (VectorIndexConfiguration::Hnsw(hnsw), VectorIndexConfiguration::Hnsw(hnsw_new)) => {
                *hnsw = hnsw_new.clone();
            }
            (
                VectorIndexConfiguration::Spann(spann),
                VectorIndexConfiguration::Spann(spann_new),
            ) => {
                *spann = spann_new.clone();
            }
            (VectorIndexConfiguration::Hnsw(_), VectorIndexConfiguration::Spann(_)) => {
                // For now, we don't support converting between different index types
                // This could be implemented in the future if needed
            }
            (VectorIndexConfiguration::Spann(_), VectorIndexConfiguration::Hnsw(_)) => {
                // For now, we don't support converting between different index types
                // This could be implemented in the future if needed
            }
        }
    }
}
impl From<InternalHnswConfiguration> for VectorIndexConfiguration {
    fn from(config: InternalHnswConfiguration) -> Self {
        VectorIndexConfiguration::Hnsw(config)
    }
}

impl From<InternalSpannConfiguration> for VectorIndexConfiguration {
    fn from(config: InternalSpannConfiguration) -> Self {
        VectorIndexConfiguration::Spann(config)
    }
}

fn default_vector_index_config() -> VectorIndexConfiguration {
    VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct InternalCollectionConfiguration {
    #[serde(default = "default_vector_index_config")]
    pub vector_index: VectorIndexConfiguration,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
    pub schema: Option<HashMap<String, HashMap<ValueType, CollectionSchema>>>,
}

impl InternalCollectionConfiguration {
    pub fn from_legacy_metadata(
        metadata: Metadata,
    ) -> Result<Self, HnswParametersFromSegmentError> {
        let hnsw = InternalHnswConfiguration::from_legacy_segment_metadata(&Some(metadata))?;
        Ok(Self {
            vector_index: VectorIndexConfiguration::Hnsw(hnsw),
            embedding_function: None,
            schema: None,
        })
    }

    pub fn default_hnsw() -> Self {
        Self {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default()),
            embedding_function: None,
            schema: None,
        }
    }

    pub fn default_spann() -> Self {
        Self {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration::default()),
            embedding_function: None,
            schema: None,
        }
    }

    pub fn get_hnsw_config_with_legacy_fallback(
        &self,
        segment: &Segment,
    ) -> Result<Option<InternalHnswConfiguration>, HnswParametersFromSegmentError> {
        self.get_hnsw_config_from_legacy_metadata(&segment.metadata)
    }

    pub fn get_hnsw_config_from_legacy_metadata(
        &self,
        metadata: &Option<Metadata>,
    ) -> Result<Option<InternalHnswConfiguration>, HnswParametersFromSegmentError> {
        if let Some(config) = self.get_hnsw_config() {
            let config_from_metadata =
                InternalHnswConfiguration::from_legacy_segment_metadata(metadata)?;

            if config == InternalHnswConfiguration::default() && config != config_from_metadata {
                return Ok(Some(config_from_metadata));
            }

            return Ok(Some(config));
        }

        Ok(None)
    }

    pub fn get_spann_config(&self) -> Option<InternalSpannConfiguration> {
        match &self.vector_index {
            VectorIndexConfiguration::Spann(config) => Some(config.clone()),
            _ => None,
        }
    }

    fn get_hnsw_config(&self) -> Option<InternalHnswConfiguration> {
        match &self.vector_index {
            VectorIndexConfiguration::Hnsw(config) => Some(config.clone()),
            _ => None,
        }
    }

    pub fn update(
        &mut self,
        update_configuration: &UpdateCollectionConfiguration,
    ) -> Result<(), UpdateCollectionConfigurationToInternalConfigurationError> {
        // Update vector_index if it exists in the update configuration

        if let Some(hnsw_config) = &update_configuration.hnsw {
            if let VectorIndexConfiguration::Hnsw(current_config) = &mut self.vector_index {
                // Update only the non-None fields from the update configuration
                if let Some(ef_search) = hnsw_config.ef_search {
                    current_config.ef_search = ef_search;
                }
                if let Some(max_neighbors) = hnsw_config.max_neighbors {
                    current_config.max_neighbors = max_neighbors;
                }
                if let Some(num_threads) = hnsw_config.num_threads {
                    current_config.num_threads = num_threads;
                }
                if let Some(resize_factor) = hnsw_config.resize_factor {
                    current_config.resize_factor = resize_factor;
                }
                if let Some(sync_threshold) = hnsw_config.sync_threshold {
                    current_config.sync_threshold = sync_threshold;
                }
                if let Some(batch_size) = hnsw_config.batch_size {
                    current_config.batch_size = batch_size;
                }
            }
        }
        if let Some(spann_config) = &update_configuration.spann {
            if let VectorIndexConfiguration::Spann(current_config) = &mut self.vector_index {
                if let Some(search_nprobe) = spann_config.search_nprobe {
                    current_config.search_nprobe = search_nprobe;
                }
                if let Some(ef_search) = spann_config.ef_search {
                    current_config.ef_search = ef_search;
                }
            }
        }
        // Update embedding_function if it exists in the update configuration
        if let Some(embedding_function) = &update_configuration.embedding_function {
            self.embedding_function = Some(embedding_function.clone());
        }
        if let Some(update_schema) = &update_configuration.schema {
            if let Some(current_schema) = &mut self.schema {
                for (update_key, update_value) in update_schema {
                    if let Some(current_value) = current_schema.get_mut(update_key) {
                        for (update_value_type, update_collection_schema) in update_value {
                            current_value.insert(
                                update_value_type.clone(),
                                update_collection_schema.clone(),
                            );
                        }
                    } else {
                        current_schema.insert(update_key.clone(), update_value.clone());
                    }
                }
            } else {
                self.schema = Some(update_schema.clone());
            }
        }

        Ok(())
    }

    pub fn try_from_config(
        value: CollectionConfiguration,
        default_knn_index: KnnIndex,
        metadata: Option<Metadata>,
    ) -> Result<Self, CollectionConfigurationToInternalConfigurationError> {
        let mut hnsw: Option<HnswConfiguration> = value.hnsw;
        let spann: Option<SpannConfiguration> = value.spann;

        // if neither hnsw nor spann is provided, use the collection metadata to build an hnsw configuration
        // the match then handles cases where hnsw is provided, and correctly routes to either spann or hnsw configuration
        // based on the default_knn_index
        if hnsw.is_none() && spann.is_none() {
            let hnsw_config_from_metadata =
            InternalHnswConfiguration::from_legacy_segment_metadata(&metadata).map_err(|e| {
                CollectionConfigurationToInternalConfigurationError::HnswParametersFromSegmentError(
                    e,
                )
            })?;
            hnsw = Some(hnsw_config_from_metadata.into());
        }

        match (hnsw, spann) {
            (Some(_), Some(_)) => Err(CollectionConfigurationToInternalConfigurationError::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => {
                match default_knn_index {
                    // Create a spann index. Only inherit the space if it exists in the hnsw config.
                    // This is for backwards compatibility so that users who migrate to distributed
                    // from local don't break their code.
                    KnnIndex::Spann => {
                        let internal_config = if let Some(space) = hnsw.space {
                            InternalSpannConfiguration {
                                space,
                                ..Default::default()
                            }
                        } else {
                            InternalSpannConfiguration::default()
                        };

                        Ok(InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Spann(internal_config),
                            embedding_function: value.embedding_function,
                            schema: value.schema,
                        })
                    },
                    KnnIndex::Hnsw => {
                        let hnsw: InternalHnswConfiguration = hnsw.into();
                        Ok(InternalCollectionConfiguration {
                            vector_index: hnsw.into(),
                            embedding_function: value.embedding_function,
                            schema: value.schema,
                        })
                    }
                }
            }
            (None, Some(spann)) => {
                match default_knn_index {
                    // Create a hnsw index. Only inherit the space if it exists in the spann config.
                    // This is for backwards compatibility so that users who migrate to local
                    // from distributed don't break their code.
                    KnnIndex::Hnsw => {
                        let internal_config = if let Some(space) = spann.space {
                            InternalHnswConfiguration {
                                space,
                                ..Default::default()
                            }
                        } else {
                            InternalHnswConfiguration::default()
                        };
                        Ok(InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Hnsw(internal_config),
                            embedding_function: value.embedding_function,
                            schema: value.schema,
                        })
                    }
                    KnnIndex::Spann => {
                        let spann: InternalSpannConfiguration = spann.into();
                        Ok(InternalCollectionConfiguration {
                            vector_index: spann.into(),
                            embedding_function: value.embedding_function,
                            schema: value.schema,
                        })
                    }
                }
            }
            (None, None) => {
                let vector_index = match default_knn_index {
                    KnnIndex::Hnsw => InternalHnswConfiguration::default().into(),
                    KnnIndex::Spann => InternalSpannConfiguration::default().into(),
                };
                Ok(InternalCollectionConfiguration {
                    vector_index,
                    embedding_function: value.embedding_function,
                    schema: value.schema,
                })
            }
        }
    }
}

impl TryFrom<CollectionConfiguration> for InternalCollectionConfiguration {
    type Error = CollectionConfigurationToInternalConfigurationError;

    fn try_from(value: CollectionConfiguration) -> Result<Self, Self::Error> {
        // validate the schema
        validate_schema(&value.schema)?;
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(Self::Error::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => {
                let hnsw: InternalHnswConfiguration = hnsw.into();
                Ok(InternalCollectionConfiguration {
                    vector_index: hnsw.into(),
                    embedding_function: value.embedding_function,
                    schema: value.schema,
                })
            }
            (None, Some(spann)) => {
                let spann: InternalSpannConfiguration = spann.into();
                Ok(InternalCollectionConfiguration {
                    vector_index: spann.into(),
                    embedding_function: value.embedding_function,
                    schema: value.schema,
                })
            }
            (None, None) => Ok(InternalCollectionConfiguration {
                vector_index: InternalHnswConfiguration::default().into(),
                embedding_function: value.embedding_function,
                schema: value.schema,
            }),
        }
    }
}

fn validate_schema(
    schema: &Option<HashMap<String, HashMap<ValueType, CollectionSchema>>>,
) -> Result<(), CollectionConfigurationToInternalConfigurationError> {
    // get list of keys, any duplicates are invalid
    if let Some(schema) = schema {
        let keys = schema.keys().collect::<Vec<&String>>();
        if keys.len() != schema.len() {
            return Err(CollectionConfigurationToInternalConfigurationError::SchemaDuplicateKeys);
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum CollectionConfigurationToInternalConfigurationError {
    #[error("Multiple vector index configurations provided")]
    MultipleVectorIndexConfigurations,
    #[error("Failed to parse hnsw parameters from segment metadata")]
    HnswParametersFromSegmentError(#[from] HnswParametersFromSegmentError),
    #[error("Schema duplicate keys")]
    SchemaDuplicateKeys,
}

impl ChromaError for CollectionConfigurationToInternalConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
            Self::HnswParametersFromSegmentError(_) => ErrorCodes::InvalidArgument,
            Self::SchemaDuplicateKeys => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Default, Deserialize, Serialize, ToSchema, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct CollectionConfiguration {
    pub hnsw: Option<HnswConfiguration>,
    pub spann: Option<SpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
    pub schema: Option<HashMap<String, HashMap<ValueType, CollectionSchema>>>,
}

impl From<InternalCollectionConfiguration> for CollectionConfiguration {
    fn from(value: InternalCollectionConfiguration) -> Self {
        Self {
            hnsw: match value.vector_index.clone() {
                VectorIndexConfiguration::Hnsw(config) => Some(config.into()),
                _ => None,
            },
            spann: match value.vector_index {
                VectorIndexConfiguration::Spann(config) => Some(config.into()),
                _ => None,
            },
            embedding_function: value.embedding_function,
            schema: value.schema,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateVectorIndexConfiguration {
    Hnsw(Option<UpdateHnswConfiguration>),
    Spann(Option<UpdateSpannConfiguration>),
}

impl From<UpdateHnswConfiguration> for UpdateVectorIndexConfiguration {
    fn from(config: UpdateHnswConfiguration) -> Self {
        UpdateVectorIndexConfiguration::Hnsw(Some(config))
    }
}

impl From<UpdateSpannConfiguration> for UpdateVectorIndexConfiguration {
    fn from(config: UpdateSpannConfiguration) -> Self {
        UpdateVectorIndexConfiguration::Spann(Some(config))
    }
}

#[derive(Debug, Error)]
pub enum UpdateCollectionConfigurationToInternalConfigurationError {
    #[error("Multiple vector index configurations provided")]
    MultipleVectorIndexConfigurations,
    #[error("Schema value type mismatch: existing: {0}, updated: {1}")]
    SchemaValueTypeMismatch(ValueType, ValueType),
}

impl ChromaError for UpdateCollectionConfigurationToInternalConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
            Self::SchemaValueTypeMismatch(_, _) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateCollectionConfiguration {
    pub hnsw: Option<UpdateHnswConfiguration>,
    pub spann: Option<UpdateSpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
    pub schema: Option<HashMap<String, HashMap<ValueType, CollectionSchema>>>,
}

pub fn diff_metadata_index_enable(
    old_schema: &Option<HashMap<String, HashMap<ValueType, CollectionSchema>>>,
    update_schema: &HashMap<String, HashMap<ValueType, CollectionSchema>>,
) -> Vec<(String, ValueType)> {
    let mut backfill_needed = Vec::new();
    for (update_key, update_value) in update_schema {
        for (update_value_type, update_collection_schema) in update_value {
            let old_metadata_index = old_schema
                .as_ref()
                .and_then(|s| s.get(update_key))
                .and_then(|vt_map| vt_map.get(update_value_type))
                .map(|cs| cs.metadata_index)
                .unwrap_or(true); // default to true if not present
            let new_metadata_index = update_collection_schema.metadata_index;
            if !old_metadata_index && new_metadata_index {
                backfill_needed.push((update_key.clone(), update_value_type.clone()));
            }
        }
    }
    backfill_needed
}

#[cfg(test)]
mod tests {
    use crate::hnsw_configuration::HnswConfiguration;
    use crate::hnsw_configuration::HnswSpace;
    use crate::spann_configuration::SpannConfiguration;
    use crate::{test_segment, CollectionUuid, Metadata};

    use super::*;

    #[test]
    fn metadata_overrides_parameter() {
        let mut metadata = Metadata::new();
        metadata.insert(
            "hnsw:construction_ef".to_string(),
            crate::MetadataValue::Int(1),
        );

        let mut segment = test_segment(CollectionUuid::new(), crate::SegmentScope::VECTOR);
        segment.metadata = Some(metadata);

        let config = InternalCollectionConfiguration::default_hnsw();
        let overridden_config = config
            .get_hnsw_config_with_legacy_fallback(&segment)
            .unwrap()
            .unwrap();

        assert_eq!(overridden_config.ef_construction, 1);
    }

    #[test]
    fn metadata_ignored_when_config_is_not_default() {
        let mut metadata = Metadata::new();
        metadata.insert(
            "hnsw:construction_ef".to_string(),
            crate::MetadataValue::Int(1),
        );

        let mut segment = test_segment(CollectionUuid::new(), crate::SegmentScope::VECTOR);
        segment.metadata = Some(metadata);

        let config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                ef_construction: 2,
                ..Default::default()
            }),
            embedding_function: None,
            schema: None,
        };

        let overridden_config = config
            .get_hnsw_config_with_legacy_fallback(&segment)
            .unwrap()
            .unwrap();

        // Setting from metadata is ignored since the config is not default
        assert_eq!(overridden_config.ef_construction, 2);
    }

    #[test]
    fn test_hnsw_config_with_hnsw_default() {
        let hnsw_config = HnswConfiguration {
            max_neighbors: Some(16),
            ef_construction: Some(100),
            ef_search: Some(10),
            batch_size: Some(100),
            num_threads: Some(4),
            sync_threshold: Some(500),
            resize_factor: Some(1.2),
            space: Some(HnswSpace::Cosine),
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Hnsw,
            None,
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        let expected_vector_index = VectorIndexConfiguration::Hnsw(hnsw_config.into());
        assert_eq!(internal_config.vector_index, expected_vector_index);
    }

    #[test]
    fn test_hnsw_config_with_spann_default() {
        let hnsw_config = HnswConfiguration {
            max_neighbors: Some(16),
            ef_construction: Some(100),
            ef_search: Some(10),
            batch_size: Some(100),
            num_threads: Some(4),
            sync_threshold: Some(500),
            resize_factor: Some(1.2),
            space: Some(HnswSpace::Cosine),
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Spann,
            None,
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        let expected_vector_index = VectorIndexConfiguration::Spann(InternalSpannConfiguration {
            space: hnsw_config.space.unwrap_or(HnswSpace::L2),
            ..Default::default()
        });
        assert_eq!(internal_config.vector_index, expected_vector_index);
    }

    #[test]
    fn test_spann_config_with_spann_default() {
        let spann_config = SpannConfiguration {
            ef_construction: Some(100),
            ef_search: Some(10),
            max_neighbors: Some(16),
            search_nprobe: Some(1),
            write_nprobe: Some(1),
            space: Some(HnswSpace::Cosine),
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Spann,
            None,
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        let expected_vector_index = VectorIndexConfiguration::Spann(spann_config.into());
        assert_eq!(internal_config.vector_index, expected_vector_index);
    }

    #[test]
    fn test_spann_config_with_hnsw_default() {
        let spann_config = SpannConfiguration {
            ef_construction: Some(100),
            ef_search: Some(10),
            max_neighbors: Some(16),
            search_nprobe: Some(1),
            write_nprobe: Some(1),
            space: Some(HnswSpace::Cosine),
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Hnsw,
            None,
        );

        let expected_vector_index = VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
            space: spann_config.space.unwrap_or(HnswSpace::L2),
            ..Default::default()
        });
        assert_eq!(
            internal_config_result.unwrap().vector_index,
            expected_vector_index
        );
    }

    #[test]
    fn test_no_config_with_metadata_default_hnsw() {
        let metadata = Metadata::new();
        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Hnsw,
            Some(metadata),
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        assert_eq!(
            internal_config.vector_index,
            VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default())
        );
    }

    #[test]
    fn test_no_config_with_metadata_default_spann() {
        let metadata = Metadata::new();
        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Spann,
            Some(metadata),
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        assert_eq!(
            internal_config.vector_index,
            VectorIndexConfiguration::Spann(InternalSpannConfiguration::default())
        );
    }

    #[test]
    fn test_legacy_metadata_with_hnsw_config() {
        let mut metadata = Metadata::new();
        metadata.insert(
            "hnsw:space".to_string(),
            crate::MetadataValue::Str("cosine".to_string()),
        );
        metadata.insert(
            "hnsw:construction_ef".to_string(),
            crate::MetadataValue::Int(1),
        );

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Hnsw,
            Some(metadata),
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        assert_eq!(
            internal_config.vector_index,
            VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: HnswSpace::Cosine,
                ef_construction: 1,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_legacy_metadata_with_spann_config() {
        let mut metadata = Metadata::new();
        metadata.insert(
            "hnsw:space".to_string(),
            crate::MetadataValue::Str("cosine".to_string()),
        );
        metadata.insert(
            "hnsw:construction_ef".to_string(),
            crate::MetadataValue::Int(1),
        );

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: None,
            embedding_function: None,
            schema: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Spann,
            Some(metadata),
        );

        assert!(internal_config_result.is_ok());

        let internal_config = internal_config_result.unwrap();

        assert_eq!(
            internal_config.vector_index,
            VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                space: HnswSpace::Cosine,
                ..Default::default()
            })
        );
    }
}

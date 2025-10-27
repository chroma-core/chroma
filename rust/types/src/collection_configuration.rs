use crate::{
    collection_schema::is_embedding_function_default, default_batch_size, default_construction_ef,
    default_construction_ef_spann, default_initial_lambda, default_m, default_m_spann,
    default_merge_threshold, default_nreplica_count, default_num_centers_to_merge_to,
    default_num_samples_kmeans, default_num_threads, default_reassign_neighbor_count,
    default_resize_factor, default_search_ef, default_search_ef_spann, default_search_nprobe,
    default_search_rng_epsilon, default_search_rng_factor, default_space, default_split_threshold,
    default_sync_threshold, default_write_nprobe, default_write_rng_epsilon,
    default_write_rng_factor,
};
use crate::{
    HnswConfiguration, HnswParametersFromSegmentError, InternalHnswConfiguration,
    InternalSpannConfiguration, Metadata, Schema, Segment, SpannConfiguration,
    UpdateHnswConfiguration, UpdateSpannConfiguration, VectorIndexConfig, EMBEDDING_KEY,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[serde(tag = "type")]
pub enum EmbeddingFunctionConfiguration {
    #[serde(rename = "legacy")]
    Legacy,
    #[serde(rename = "known")]
    Known(EmbeddingFunctionNewConfiguration),
    #[serde(rename = "unknown")]
    Unknown,
}

impl EmbeddingFunctionConfiguration {
    pub fn is_default(&self) -> bool {
        match self {
            EmbeddingFunctionConfiguration::Legacy => false,
            EmbeddingFunctionConfiguration::Unknown => true,
            EmbeddingFunctionConfiguration::Known(config) => config.name == "default",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct EmbeddingFunctionNewConfiguration {
    pub name: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct InternalCollectionConfiguration {
    #[serde(default = "default_vector_index_config")]
    pub vector_index: VectorIndexConfiguration,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

impl InternalCollectionConfiguration {
    pub fn from_legacy_metadata(
        metadata: Metadata,
    ) -> Result<Self, HnswParametersFromSegmentError> {
        let hnsw = InternalHnswConfiguration::from_legacy_segment_metadata(&Some(metadata))?;
        Ok(Self {
            vector_index: VectorIndexConfiguration::Hnsw(hnsw),
            embedding_function: None,
        })
    }

    pub fn default_hnsw() -> Self {
        Self {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default()),
            embedding_function: None,
        }
    }

    pub fn default_spann() -> Self {
        Self {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration::default()),
            embedding_function: None,
        }
    }

    /// Check if this collection configuration is default
    pub fn is_default(&self) -> bool {
        if !is_embedding_function_default(&self.embedding_function) {
            return false;
        }

        // Check vector index configuration
        match &self.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw_config) => {
                hnsw_config.ef_construction == default_construction_ef()
                    && hnsw_config.ef_search == default_search_ef()
                    && hnsw_config.max_neighbors == default_m()
                    && hnsw_config.num_threads == default_num_threads()
                    && hnsw_config.batch_size == default_batch_size()
                    && hnsw_config.sync_threshold == default_sync_threshold()
                    && hnsw_config.resize_factor == default_resize_factor()
                    && hnsw_config.space == default_space()
            }
            VectorIndexConfiguration::Spann(spann_config) => {
                spann_config.search_nprobe == default_search_nprobe()
                    && spann_config.search_rng_factor == default_search_rng_factor()
                    && spann_config.search_rng_epsilon == default_search_rng_epsilon()
                    && spann_config.write_nprobe == default_write_nprobe()
                    && spann_config.nreplica_count == default_nreplica_count()
                    && spann_config.write_rng_factor == default_write_rng_factor()
                    && spann_config.write_rng_epsilon == default_write_rng_epsilon()
                    && spann_config.split_threshold == default_split_threshold()
                    && spann_config.num_samples_kmeans == default_num_samples_kmeans()
                    && spann_config.initial_lambda == default_initial_lambda()
                    && spann_config.reassign_neighbor_count == default_reassign_neighbor_count()
                    && spann_config.merge_threshold == default_merge_threshold()
                    && spann_config.num_centers_to_merge_to == default_num_centers_to_merge_to()
                    && spann_config.ef_construction == default_construction_ef_spann()
                    && spann_config.ef_search == default_search_ef_spann()
                    && spann_config.max_neighbors == default_m_spann()
                    && spann_config.space == default_space()
            }
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

    pub fn update(&mut self, configuration: &InternalUpdateCollectionConfiguration) {
        // Update vector_index if it exists in the update configuration

        if let Some(vector_index) = &configuration.vector_index {
            match vector_index {
                UpdateVectorIndexConfiguration::Hnsw(hnsw_config) => {
                    if let VectorIndexConfiguration::Hnsw(current_config) = &mut self.vector_index {
                        if let Some(update_config) = hnsw_config {
                            if let Some(ef_search) = update_config.ef_search {
                                current_config.ef_search = ef_search;
                            }
                            if let Some(max_neighbors) = update_config.max_neighbors {
                                current_config.max_neighbors = max_neighbors;
                            }
                            if let Some(num_threads) = update_config.num_threads {
                                current_config.num_threads = num_threads;
                            }
                            if let Some(resize_factor) = update_config.resize_factor {
                                current_config.resize_factor = resize_factor;
                            }
                            if let Some(sync_threshold) = update_config.sync_threshold {
                                current_config.sync_threshold = sync_threshold;
                            }
                            if let Some(batch_size) = update_config.batch_size {
                                current_config.batch_size = batch_size;
                            }
                        }
                    }
                }
                UpdateVectorIndexConfiguration::Spann(spann_config) => {
                    if let VectorIndexConfiguration::Spann(current_config) = &mut self.vector_index
                    {
                        if let Some(update_config) = spann_config {
                            if let Some(search_nprobe) = update_config.search_nprobe {
                                current_config.search_nprobe = search_nprobe;
                            }
                            if let Some(ef_search) = update_config.ef_search {
                                current_config.ef_search = ef_search;
                            }
                        }
                    }
                }
            }
        }
        // Update embedding_function if it exists in the update configuration
        if let Some(embedding_function) = &configuration.embedding_function {
            self.embedding_function = Some(embedding_function.clone());
        }
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
                    // Create a spann index. Only inherit the space if it exists in the hnsw config or legacy metadata.
                    // This is for backwards compatibility so that users who migrate to distributed
                    // from local don't break their code.
                    KnnIndex::Spann => {
                        let mut hnsw: InternalHnswConfiguration = hnsw.into();
                        let temp_config = InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Hnsw(hnsw.clone()),
                            embedding_function: None,
                        };
                        let hnsw_params = temp_config.get_hnsw_config_from_legacy_metadata(&metadata)?;
                        if let Some(hnsw_params) = hnsw_params {
                            hnsw = hnsw_params;
                        }
                        let spann_config = InternalSpannConfiguration {
                            space: hnsw.space,
                            ..Default::default()
                        };

                        Ok(InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Spann(spann_config),
                            embedding_function: value.embedding_function,
                        })
                    },
                    KnnIndex::Hnsw => {
                        let hnsw: InternalHnswConfiguration = hnsw.into();
                        let mut internal_config = InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Hnsw(hnsw),
                            embedding_function: value.embedding_function,
                        };
                        let hnsw_params = internal_config.get_hnsw_config_from_legacy_metadata(&metadata)?;
                        if let Some(hnsw_params) = hnsw_params {
                            internal_config.vector_index = VectorIndexConfiguration::Hnsw(hnsw_params);
                        }
                        Ok(internal_config)
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
                        })
                    }
                    KnnIndex::Spann => {
                        let spann: InternalSpannConfiguration = spann.into();
                        Ok(InternalCollectionConfiguration {
                            vector_index: spann.into(),
                            embedding_function: value.embedding_function,
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
                })
            }
        }
    }
}

impl TryFrom<CollectionConfiguration> for InternalCollectionConfiguration {
    type Error = CollectionConfigurationToInternalConfigurationError;

    fn try_from(value: CollectionConfiguration) -> Result<Self, Self::Error> {
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(Self::Error::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => {
                let hnsw: InternalHnswConfiguration = hnsw.into();
                Ok(InternalCollectionConfiguration {
                    vector_index: hnsw.into(),
                    embedding_function: value.embedding_function,
                })
            }
            (None, Some(spann)) => {
                let spann: InternalSpannConfiguration = spann.into();
                Ok(InternalCollectionConfiguration {
                    vector_index: spann.into(),
                    embedding_function: value.embedding_function,
                })
            }
            (None, None) => Ok(InternalCollectionConfiguration {
                vector_index: InternalHnswConfiguration::default().into(),
                embedding_function: value.embedding_function,
            }),
        }
    }
}

impl TryFrom<&Schema> for InternalCollectionConfiguration {
    type Error = String;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let vector_config = schema
            .keys
            .get(EMBEDDING_KEY)
            .and_then(|value_types| value_types.float_list.as_ref())
            .and_then(|float_list| float_list.vector_index.as_ref())
            .map(|vector_index| vector_index.config.clone())
            .or_else(|| {
                schema
                    .defaults
                    .float_list
                    .as_ref()
                    .and_then(|float_list| float_list.vector_index.as_ref())
                    .map(|vector_index| vector_index.config.clone())
            })
            .ok_or_else(|| "Missing vector index configuration for #embedding".to_string())?;

        let VectorIndexConfig {
            space,
            embedding_function,
            hnsw,
            spann,
            ..
        } = vector_config;

        match (hnsw, spann) {
            (Some(_), Some(_)) => Err(
                "Vector index configuration must not contain both HNSW and SPANN settings"
                    .to_string(),
            ),
            (Some(hnsw_config), None) => {
                let internal_hnsw = (space.as_ref(), Some(&hnsw_config)).into();
                Ok(InternalCollectionConfiguration {
                    vector_index: VectorIndexConfiguration::Hnsw(internal_hnsw),
                    embedding_function,
                })
            }
            (None, Some(spann_config)) => {
                let internal_spann = (space.as_ref(), &spann_config).into();
                Ok(InternalCollectionConfiguration {
                    vector_index: VectorIndexConfiguration::Spann(internal_spann),
                    embedding_function,
                })
            }
            (None, None) => {
                let internal_hnsw = (space.as_ref(), None).into();
                Ok(InternalCollectionConfiguration {
                    vector_index: VectorIndexConfiguration::Hnsw(internal_hnsw),
                    embedding_function,
                })
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CollectionConfigurationToInternalConfigurationError {
    #[error("Multiple vector index configurations provided")]
    MultipleVectorIndexConfigurations,
    #[error("Failed to parse hnsw parameters from segment metadata")]
    HnswParametersFromSegmentError(#[from] HnswParametersFromSegmentError),
}

impl ChromaError for CollectionConfigurationToInternalConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
            Self::HnswParametersFromSegmentError(_) => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Default, Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct CollectionConfiguration {
    pub hnsw: Option<HnswConfiguration>,
    pub spann: Option<SpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
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
}

impl ChromaError for UpdateCollectionConfigurationToInternalConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateCollectionConfiguration {
    pub hnsw: Option<UpdateHnswConfiguration>,
    pub spann: Option<UpdateSpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct InternalUpdateCollectionConfiguration {
    pub vector_index: Option<UpdateVectorIndexConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

#[derive(Debug, Error)]
pub enum UpdateCollectionConfigurationToInternalUpdateConfigurationError {
    #[error("Multiple vector index configurations provided")]
    MultipleVectorIndexConfigurations,
}

impl ChromaError for UpdateCollectionConfigurationToInternalUpdateConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
        }
    }
}

impl TryFrom<UpdateCollectionConfiguration> for InternalUpdateCollectionConfiguration {
    type Error = UpdateCollectionConfigurationToInternalUpdateConfigurationError;

    fn try_from(value: UpdateCollectionConfiguration) -> Result<Self, Self::Error> {
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(Self::Error::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => Ok(InternalUpdateCollectionConfiguration {
                vector_index: Some(UpdateVectorIndexConfiguration::Hnsw(Some(hnsw))),
                embedding_function: value.embedding_function,
            }),
            (None, Some(spann)) => Ok(InternalUpdateCollectionConfiguration {
                vector_index: Some(UpdateVectorIndexConfiguration::Spann(Some(spann))),
                embedding_function: value.embedding_function,
            }),
            (None, None) => Ok(InternalUpdateCollectionConfiguration {
                vector_index: None,
                embedding_function: value.embedding_function,
            }),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::collection_schema::Schema;
    use crate::hnsw_configuration::HnswConfiguration;
    use crate::hnsw_configuration::Space;
    use crate::metadata::MetadataValue;
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
        };

        let overridden_config = config
            .get_hnsw_config_with_legacy_fallback(&segment)
            .unwrap()
            .unwrap();

        // Setting from metadata is ignored since the config is not default
        assert_eq!(overridden_config.ef_construction, 2);
    }

    #[test]
    fn metadata_populates_config_when_not_set() {
        let mut metadata = Metadata::new();
        metadata.insert("hnsw:sync_threshold".to_string(), MetadataValue::Int(10));
        metadata.insert("hnsw:batch_size".to_string(), MetadataValue::Int(7));

        let config = InternalCollectionConfiguration::try_from_config(
            CollectionConfiguration {
                hnsw: None,
                spann: None,
                embedding_function: None,
            },
            KnnIndex::Hnsw,
            Some(metadata),
        )
        .expect("config from metadata should succeed");

        match config.vector_index {
            VectorIndexConfiguration::Hnsw(hnsw) => {
                assert_eq!(hnsw.sync_threshold, 10);
                assert_eq!(hnsw.batch_size, 7);
            }
            _ => panic!("expected HNSW configuration"),
        }
    }

    #[test]
    fn schema_reconcile_preserves_metadata_overrides() {
        let mut metadata = Metadata::new();
        metadata.insert("hnsw:sync_threshold".to_string(), MetadataValue::Int(10));
        metadata.insert("hnsw:batch_size".to_string(), MetadataValue::Int(7));

        let config = InternalCollectionConfiguration::try_from_config(
            CollectionConfiguration {
                hnsw: None,
                spann: None,
                embedding_function: None,
            },
            KnnIndex::Hnsw,
            Some(metadata),
        )
        .expect("config from metadata should succeed");

        let schema = Schema::reconcile_schema_and_config(None, Some(&config), KnnIndex::Hnsw)
            .expect("schema reconcile should succeed");

        let hnsw_config = schema
            .get_internal_hnsw_config()
            .expect("schema should contain hnsw config");
        assert_eq!(hnsw_config.sync_threshold, 10);
        assert_eq!(hnsw_config.batch_size, 7);
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
            space: Some(Space::Cosine),
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
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
            space: Some(Space::Cosine),
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Spann,
            None,
        );

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        let expected_vector_index = VectorIndexConfiguration::Spann(InternalSpannConfiguration {
            space: hnsw_config.space.unwrap_or(Space::L2),
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
            space: Some(Space::Cosine),
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
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
            space: Some(Space::Cosine),
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
        };

        let internal_config_result = InternalCollectionConfiguration::try_from_config(
            collection_config,
            KnnIndex::Hnsw,
            None,
        );

        let expected_vector_index = VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
            space: spann_config.space.unwrap_or(Space::L2),
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
                space: Space::Cosine,
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
                space: Space::Cosine,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_update_collection_configuration_with_hnsw() {
        let mut config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Cosine,
                ..Default::default()
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test".to_string(),
                    config: serde_json::Value::Null,
                },
            )),
        };
        let update_config = UpdateCollectionConfiguration {
            hnsw: Some(UpdateHnswConfiguration {
                ef_search: Some(1),
                ..Default::default()
            }),
            spann: None,
            embedding_function: None,
        };
        config.update(&update_config.try_into().unwrap());
        assert_eq!(
            config.vector_index,
            VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                space: Space::Cosine,
                ef_search: 1,
                ..Default::default()
            })
        );

        assert_eq!(
            config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test".to_string(),
                    config: serde_json::Value::Null,
                },
            ))
        );
    }

    #[test]
    fn test_update_collection_configuration_with_spann() {
        let mut config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                space: Space::Cosine,
                ..Default::default()
            }),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test".to_string(),
                    config: serde_json::Value::Null,
                },
            )),
        };
        let update_config = UpdateCollectionConfiguration {
            hnsw: None,
            spann: Some(UpdateSpannConfiguration {
                ef_search: Some(1),
                ..Default::default()
            }),
            embedding_function: None,
        };
        config.update(&update_config.try_into().unwrap());
        assert_eq!(
            config.vector_index,
            VectorIndexConfiguration::Spann(InternalSpannConfiguration {
                space: Space::Cosine,
                ef_search: 1,
                ..Default::default()
            })
        );

        assert_eq!(
            config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test".to_string(),
                    config: serde_json::Value::Null,
                },
            ))
        );
    }

    #[test]
    fn test_update_collection_configuration_with_embedding_function() {
        let mut config = InternalCollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(InternalHnswConfiguration::default()),
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test".to_string(),
                    config: serde_json::Value::Null,
                },
            )),
        };
        let emb_fn_config = EmbeddingFunctionNewConfiguration {
            name: "test2".to_string(),
            config: serde_json::Value::Object(serde_json::Map::from_iter([(
                "test".to_string(),
                serde_json::Value::String("test".to_string()),
            )])),
        };
        let update_config = UpdateCollectionConfiguration {
            hnsw: None,
            spann: None,
            embedding_function: Some(EmbeddingFunctionConfiguration::Known(emb_fn_config)),
        };
        config.update(&update_config.try_into().unwrap());
        assert_eq!(
            config.embedding_function,
            Some(EmbeddingFunctionConfiguration::Known(
                EmbeddingFunctionNewConfiguration {
                    name: "test2".to_string(),
                    config: serde_json::Value::Object(serde_json::Map::from_iter([(
                        "test".to_string(),
                        serde_json::Value::String("test".to_string()),
                    )])),
                },
            ))
        );
    }
}

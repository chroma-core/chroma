use crate::{
    HnswConfiguration, HnswParametersFromSegmentError, InternalHnswConfiguration,
    InternalSpannConfiguration, Metadata, Segment, SpannConfiguration, UpdateHnswConfiguration,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

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

    pub fn update(&mut self, configuration: &UpdateCollectionConfiguration) {
        // Update vector_index if it exists in the update configuration

        if let Some(hnsw_config) = &configuration.hnsw {
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
        if let Some(spann_config) = &configuration.spann {
            if let VectorIndexConfiguration::Spann(current_config) = &mut self.vector_index {
                if let Some(search_nprobe) = spann_config.search_nprobe {
                    current_config.search_nprobe = search_nprobe;
                }
                if let Some(write_nprobe) = spann_config.write_nprobe {
                    current_config.write_nprobe = write_nprobe;
                }
                let space = spann_config.space.clone();
                current_config.space = space;
                if let Some(construction_ef) = spann_config.ef_construction {
                    current_config.ef_construction = construction_ef;
                }
                if let Some(search_ef) = spann_config.ef_search {
                    current_config.ef_search = search_ef;
                }
                if let Some(m) = spann_config.max_neighbors {
                    current_config.max_neighbors = m;
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
    ) -> Result<Self, CollectionConfigurationToInternalConfigurationError> {
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(CollectionConfigurationToInternalConfigurationError::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => {
                match default_knn_index {
                    // Create a spann index. Only inherit the space if it exists in the hnsw config.
                    // This is for backwards compatibility so that users who migrate to distributed
                    // from local don't break their code.
                    KnnIndex::Spann => {
                        let internal_config = InternalSpannConfiguration {
                            space: hnsw.space,
                            ..Default::default()
                        };
                        Ok(InternalCollectionConfiguration {
                            vector_index: VectorIndexConfiguration::Spann(internal_config),
                            embedding_function: value.embedding_function,
                        })
                    },
                    KnnIndex::Hnsw => {
                        let hnsw: InternalHnswConfiguration = hnsw.into();
                        Ok(InternalCollectionConfiguration {
                            vector_index: hnsw.into(),
                            embedding_function: value.embedding_function,
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
                        let internal_config = InternalHnswConfiguration {
                            space: spann.space,
                            ..Default::default()
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

#[derive(Debug, Error)]
pub enum CollectionConfigurationToInternalConfigurationError {
    #[error("Multiple vector index configurations provided")]
    MultipleVectorIndexConfigurations,
}

impl ChromaError for CollectionConfigurationToInternalConfigurationError {
    fn code(&self) -> ErrorCodes {
        match self {
            Self::MultipleVectorIndexConfigurations => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Default, Deserialize, Serialize, ToSchema, Debug, Clone)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateVectorIndexConfiguration {
    Hnsw(Option<UpdateHnswConfiguration>),
    Spann(Option<InternalSpannConfiguration>),
}

impl From<UpdateHnswConfiguration> for UpdateVectorIndexConfiguration {
    fn from(config: UpdateHnswConfiguration) -> Self {
        UpdateVectorIndexConfiguration::Hnsw(Some(config))
    }
}

impl From<InternalSpannConfiguration> for UpdateVectorIndexConfiguration {
    fn from(config: InternalSpannConfiguration) -> Self {
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

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct UpdateCollectionConfiguration {
    pub hnsw: Option<UpdateHnswConfiguration>,
    pub spann: Option<SpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
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
            space: HnswSpace::Cosine,
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
        };

        let internal_config_result =
            InternalCollectionConfiguration::try_from_config(collection_config, KnnIndex::Hnsw);

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
            space: HnswSpace::Cosine,
        };

        let collection_config = CollectionConfiguration {
            hnsw: Some(hnsw_config.clone()),
            spann: None,
            embedding_function: None,
        };

        let internal_config_result =
            InternalCollectionConfiguration::try_from_config(collection_config, KnnIndex::Spann);

        assert!(internal_config_result.is_ok());
        let internal_config = internal_config_result.unwrap();

        let expected_vector_index = VectorIndexConfiguration::Spann(InternalSpannConfiguration {
            space: hnsw_config.space,
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
            space: HnswSpace::Cosine,
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
        };

        let internal_config_result =
            InternalCollectionConfiguration::try_from_config(collection_config, KnnIndex::Spann);

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
            space: HnswSpace::Cosine,
            reassign_neighbor_count: Some(64),
            split_threshold: Some(200),
            merge_threshold: Some(100),
        };

        let collection_config = CollectionConfiguration {
            hnsw: None,
            spann: Some(spann_config.clone()),
            embedding_function: None,
        };

        let internal_config_result =
            InternalCollectionConfiguration::try_from_config(collection_config, KnnIndex::Hnsw);

        let expected_vector_index = VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
            space: spann_config.space,
            ..Default::default()
        });
        assert_eq!(
            internal_config_result.unwrap().vector_index,
            expected_vector_index
        );
    }
}

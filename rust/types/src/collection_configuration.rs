use crate::{
    HnswConfiguration, HnswParametersFromSegmentError, InternalSpannConfiguration, Metadata,
    Segment, SpannConfiguration, UpdateHnswConfiguration,
};
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorIndexConfiguration {
    Hnsw(HnswConfiguration),
    Spann(InternalSpannConfiguration),
}

impl From<HnswConfiguration> for VectorIndexConfiguration {
    fn from(config: HnswConfiguration) -> Self {
        VectorIndexConfiguration::Hnsw(config)
    }
}

impl From<InternalSpannConfiguration> for VectorIndexConfiguration {
    fn from(config: InternalSpannConfiguration) -> Self {
        VectorIndexConfiguration::Spann(config)
    }
}

fn default_vector_index_config() -> VectorIndexConfiguration {
    VectorIndexConfiguration::Hnsw(HnswConfiguration::default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InternalCollectionConfiguration {
    #[serde(default = "default_vector_index_config")]
    pub vector_index: VectorIndexConfiguration,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

impl InternalCollectionConfiguration {
    pub fn from_legacy_metadata(
        metadata: Metadata,
    ) -> Result<Self, HnswParametersFromSegmentError> {
        let hnsw = HnswConfiguration::from_legacy_segment_metadata(&Some(metadata))?;
        Ok(Self {
            vector_index: VectorIndexConfiguration::Hnsw(hnsw),
            embedding_function: None,
        })
    }

    pub fn default_hnsw() -> Self {
        Self {
            vector_index: VectorIndexConfiguration::Hnsw(HnswConfiguration::default()),
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
    ) -> Result<Option<HnswConfiguration>, HnswParametersFromSegmentError> {
        if let Some(config) = self.get_hnsw_config() {
            let config_from_metadata =
                HnswConfiguration::from_legacy_segment_metadata(&segment.metadata)?;

            if config == HnswConfiguration::default() && config != config_from_metadata {
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

    fn get_hnsw_config(&self) -> Option<HnswConfiguration> {
        match &self.vector_index {
            VectorIndexConfiguration::Hnsw(config) => Some(config.clone()),
            _ => None,
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

#[derive(Deserialize, Serialize, ToSchema, Debug, Clone)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct CollectionConfiguration {
    pub hnsw: Option<HnswConfiguration>,
    pub spann: Option<SpannConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

impl TryFrom<CollectionConfiguration> for InternalCollectionConfiguration {
    type Error = CollectionConfigurationToInternalConfigurationError;

    fn try_from(value: CollectionConfiguration) -> Result<Self, Self::Error> {
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(Self::Error::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => Ok(InternalCollectionConfiguration {
                vector_index: hnsw.into(),
                embedding_function: value.embedding_function,
            }),
            (None, Some(spann)) => {
                let spann: InternalSpannConfiguration = spann.into();
                Ok(InternalCollectionConfiguration {
                    vector_index: spann.into(),
                    embedding_function: value.embedding_function,
                })
            }
            (None, None) => Ok(InternalCollectionConfiguration {
                vector_index: HnswConfiguration::default().into(),
                embedding_function: value.embedding_function,
            }),
        }
    }
}

impl From<InternalCollectionConfiguration> for CollectionConfiguration {
    fn from(value: InternalCollectionConfiguration) -> Self {
        Self {
            hnsw: match value.vector_index.clone() {
                VectorIndexConfiguration::Hnsw(config) => Some(config),
                _ => None,
            },
            spann: match value.vector_index.clone() {
                VectorIndexConfiguration::Spann(config) => Some(config.into()),
                _ => None,
            },
            embedding_function: value.embedding_function,
        }
    }
}

#[cfg(test)]
mod tests {
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
            vector_index: VectorIndexConfiguration::Hnsw(HnswConfiguration {
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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InternalUpdateCollectionConfiguration {
    pub vector_index: Option<UpdateVectorIndexConfiguration>,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

impl InternalUpdateCollectionConfiguration {
    pub fn get_spann_config(&self) -> Option<InternalSpannConfiguration> {
        match &self.vector_index {
            Some(UpdateVectorIndexConfiguration::Spann(Some(config))) => Some(config.clone()),
            _ => None,
        }
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

impl TryFrom<UpdateCollectionConfiguration> for InternalUpdateCollectionConfiguration {
    type Error = UpdateCollectionConfigurationToInternalConfigurationError;

    fn try_from(value: UpdateCollectionConfiguration) -> Result<Self, Self::Error> {
        match (value.hnsw, value.spann) {
            (Some(_), Some(_)) => Err(Self::Error::MultipleVectorIndexConfigurations),
            (Some(hnsw), None) => Ok(InternalUpdateCollectionConfiguration {
                vector_index: Some(hnsw.into()),
                embedding_function: value.embedding_function,
            }),
            (None, Some(spann)) => {
                let spann: InternalSpannConfiguration = spann.into();
                Ok(InternalUpdateCollectionConfiguration {
                    vector_index: Some(spann.into()),
                    embedding_function: value.embedding_function,
                })
            }
            (None, None) => Ok(InternalUpdateCollectionConfiguration {
                vector_index: None,
                embedding_function: value.embedding_function,
            }),
        }
    }
}

impl From<InternalUpdateCollectionConfiguration> for UpdateCollectionConfiguration {
    fn from(value: InternalUpdateCollectionConfiguration) -> Self {
        Self {
            hnsw: match value.vector_index.clone() {
                Some(UpdateVectorIndexConfiguration::Hnsw(Some(config))) => Some(config),
                _ => None,
            },
            spann: match value.vector_index.clone() {
                Some(UpdateVectorIndexConfiguration::Spann(Some(config))) => Some(config.into()),
                _ => None,
            },
            embedding_function: value.embedding_function,
        }
    }
}

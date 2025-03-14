use crate::{
    HnswConfiguration, HnswParametersFromSegmentError, Metadata, Segment, SpannConfiguration,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EmbeddingFunctionConfiguration {
    pub name: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VectorIndexConfiguration {
    Hnsw(HnswConfiguration),
    Spann(SpannConfiguration),
}

impl From<HnswConfiguration> for VectorIndexConfiguration {
    fn from(config: HnswConfiguration) -> Self {
        VectorIndexConfiguration::Hnsw(config)
    }
}

impl From<SpannConfiguration> for VectorIndexConfiguration {
    fn from(config: SpannConfiguration) -> Self {
        VectorIndexConfiguration::Spann(config)
    }
}

fn default_vector_index_config() -> VectorIndexConfiguration {
    VectorIndexConfiguration::Hnsw(HnswConfiguration::default())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CollectionConfiguration {
    #[serde(default = "default_vector_index_config")]
    pub vector_index: VectorIndexConfiguration,
    pub embedding_function: Option<EmbeddingFunctionConfiguration>,
}

impl CollectionConfiguration {
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

    pub fn get_spann_config(&self) -> Option<SpannConfiguration> {
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

        let config = CollectionConfiguration::default_hnsw();
        let overridden_config = config
            .get_hnsw_config_with_legacy_fallback(&segment)
            .unwrap()
            .unwrap();

        assert_eq!(overridden_config.construction_ef, 1);
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

        let config = CollectionConfiguration {
            vector_index: VectorIndexConfiguration::Hnsw(HnswConfiguration {
                construction_ef: 2,
                ..Default::default()
            }),
            embedding_function: None,
        };

        let overridden_config = config
            .get_hnsw_config_with_legacy_fallback(&segment)
            .unwrap()
            .unwrap();

        // Setting from metadata is ignored since the config is not default
        assert_eq!(overridden_config.construction_ef, 2);
    }
}

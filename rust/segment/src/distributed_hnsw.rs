use crate::types::ChromaSegmentFlusher;

use super::blockfile_record::{ApplyMaterializedLogError, RecordSegmentReader};
use super::types::MaterializeLogsResult;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::{
    HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderForkError,
    HnswIndexProviderOpenError, HnswIndexRef,
};
use chroma_index::{Index, IndexUuid};
use chroma_types::{Collection, HnswParametersFromSegmentError, Schema, SchemaError, SegmentUuid};
use chroma_types::{MaterializedLogOperation, Segment};
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;

const HNSW_INDEX: &str = "hnsw_index";

pub struct HnswIndexParamsFromSegment {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

#[derive(Clone)]
pub struct DistributedHNSWSegmentWriter {
    index: HnswIndexRef,
    hnsw_index_provider: HnswIndexProvider,
    pub id: SegmentUuid,
}

impl Debug for DistributedHNSWSegmentWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedHNSWSegmentWriter")
            .field("id", &self.id)
            .finish()
    }
}

#[derive(Error, Debug)]
pub enum DistributedHNSWSegmentFromSegmentError {
    #[error("No HNSW file found for segment")]
    NoHnswFileFound,
    #[error("HNSW file id not a valid uuid")]
    InvalidUUID,
    #[error("HNSW segment uninitialized")]
    Uninitialized,
    #[error("HNSW index provider open error")]
    HnswIndexProviderOpenError(#[from] HnswIndexProviderOpenError),
    #[error("HNSW index provider fork error")]
    HnswIndexProviderForkError(#[from] HnswIndexProviderForkError),
    #[error("HNSW index provider create error")]
    HnswIndexProviderCreateError(#[from] HnswIndexProviderCreateError),
    #[error("Collection is missing HNSW configuration")]
    MissingHnswConfiguration,
    #[error("Could not parse HNSW configuration: {0}")]
    InvalidHnswConfiguration(#[from] HnswParametersFromSegmentError),
    #[error("Invalid schema: {0}")]
    InvalidSchema(#[source] SchemaError),
}

impl ChromaError for DistributedHNSWSegmentFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound => ErrorCodes::NotFound,
            DistributedHNSWSegmentFromSegmentError::InvalidUUID => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::Uninitialized => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderForkError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderCreateError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::MissingHnswConfiguration => {
                ErrorCodes::Internal
            }
            DistributedHNSWSegmentFromSegmentError::InvalidHnswConfiguration(_) => {
                ErrorCodes::Internal
            }
            DistributedHNSWSegmentFromSegmentError::InvalidSchema(e) => e.code(),
        }
    }
}

impl DistributedHNSWSegmentWriter {
    pub(crate) fn new(
        index: HnswIndexRef,
        hnsw_index_provider: HnswIndexProvider,
        id: SegmentUuid,
    ) -> Self {
        DistributedHNSWSegmentWriter {
            index,
            hnsw_index_provider,
            id,
        }
    }

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentWriter>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        let schema = if let Some(schema) = &collection.schema {
            schema
        } else {
            &Schema::try_from(&collection.config)
                .map_err(DistributedHNSWSegmentFromSegmentError::InvalidSchema)?
        };
        let hnsw_configuration = schema
            .get_internal_hnsw_config_with_legacy_fallback(segment)
            .map_err(DistributedHNSWSegmentFromSegmentError::InvalidHnswConfiguration)?
            .ok_or(DistributedHNSWSegmentFromSegmentError::MissingHnswConfiguration)?;

        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if !segment.file_path.is_empty() {
            // Check if its in the providers cache, if not load the index from the files
            let index_path = match &segment.file_path.get(HNSW_INDEX) {
                None => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                    ));
                }
                Some(files) => {
                    if files.is_empty() {
                        return Err(Box::new(
                            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                        ));
                    } else {
                        &files[0]
                    }
                }
            };

            let (prefix_path, index_uuid) = Segment::extract_prefix_and_id(index_path)
                .map_err(|_| Box::new(DistributedHNSWSegmentFromSegmentError::InvalidUUID))?;
            let index_uuid = IndexUuid(index_uuid);

            let index = match hnsw_index_provider
                .fork(
                    &index_uuid,
                    &segment.collection,
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                    hnsw_configuration.ef_search,
                    prefix_path,
                )
                .await
            {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderForkError(*e),
                    ))
                }
            };

            Ok(Box::new(DistributedHNSWSegmentWriter::new(
                index,
                hnsw_index_provider,
                segment.id,
            )))
        } else {
            let prefix_path =
                segment.construct_prefix_path(&collection.tenant, &collection.database_id);
            let index = match hnsw_index_provider
                .create(
                    &segment.collection,
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                    &prefix_path,
                )
                .await
            {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderCreateError(*e),
                    ))
                }
            };
            Ok(Box::new(DistributedHNSWSegmentWriter::new(
                index,
                hnsw_index_provider,
                segment.id,
            )))
        }
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &MaterializeLogsResult,
    ) -> Result<(), ApplyMaterializedLogError> {
        for record in materialized {
            match record.get_operation() {
                // If embedding is not found in case of adds it means that user
                // did not supply them and thus we should return an error as
                // opposed to panic.
                MaterializedLogOperation::AddNew
                | MaterializedLogOperation::UpdateExisting
                | MaterializedLogOperation::OverwriteExisting => {
                    let record = record
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(ApplyMaterializedLogError::Materialization)?;
                    let embedding = record.merged_embeddings_ref();

                    let mut index = self.index.inner.upgradable_read();
                    let index_len = index.hnsw_index.len_with_deleted();
                    let index_capacity = index.hnsw_index.capacity();
                    if index_len + 1 > index_capacity {
                        index.with_upgraded(|index| {
                            // Bump allocation by 2x
                            index
                                .hnsw_index
                                .resize(index_capacity * 2)
                                .map(|_| ApplyMaterializedLogError::Allocation)
                        })?;
                    }

                    match index
                        .hnsw_index
                        .add(record.get_offset_id() as usize, embedding)
                    {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(ApplyMaterializedLogError::HnswIndex(e));
                        }
                    }
                }
                MaterializedLogOperation::DeleteExisting => {
                    // HNSW segment does not perform validation of any sort. So,
                    // the assumption here is that the materialized log records
                    // contain the correct offset ids pertaining to records that
                    // are actually meant to be deleted.
                    match self
                        .index
                        .inner
                        .read()
                        .hnsw_index
                        .delete(record.get_offset_id() as usize)
                    {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(ApplyMaterializedLogError::HnswIndex(e));
                        }
                    }
                }
                MaterializedLogOperation::Initial => panic!(
                    "Invariant violation. Mat records should not contain logs in initial state"
                ),
            }
        }
        Ok(())
    }

    pub async fn commit(self) -> Result<DistributedHNSWSegmentWriter, Box<dyn ChromaError>> {
        let res = self.hnsw_index_provider.commit(self.index.clone());
        match res {
            Ok(_) => Ok(self),
            Err(e) => Err(e),
        }
    }

    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let (hnsw_index_id, prefix_path) = {
            let read_guard = self.index.inner.read();
            (read_guard.hnsw_index.id, read_guard.prefix_path.clone())
        };
        match self
            .hnsw_index_provider
            .flush(&prefix_path, &hnsw_index_id, &self.index)
            .await
        {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        let mut flushed_files = HashMap::new();
        flushed_files.insert(
            HNSW_INDEX.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &hnsw_index_id.0,
            )],
        );
        Ok(flushed_files)
    }

    pub fn index_uuid(&self) -> IndexUuid {
        self.index.inner.read().hnsw_index.id
    }
}

#[derive(Clone)]
pub struct DistributedHNSWSegmentReader {
    index: HnswIndexRef,
    pub id: SegmentUuid,
}

impl Debug for DistributedHNSWSegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedHNSWSegmentReader")
            .field("id", &self.id)
            .finish()
    }
}

impl DistributedHNSWSegmentReader {
    fn new(index: HnswIndexRef, id: SegmentUuid) -> Self {
        DistributedHNSWSegmentReader { index, id }
    }

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentReader>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        let schema = collection.schema.as_ref().ok_or_else(|| {
            DistributedHNSWSegmentFromSegmentError::InvalidSchema(SchemaError::InvalidSchema {
                reason: "Schema is None".to_string(),
            })
        })?;
        let hnsw_configuration = schema
            .get_internal_hnsw_config_with_legacy_fallback(segment)
            .map_err(DistributedHNSWSegmentFromSegmentError::InvalidHnswConfiguration)?
            .ok_or(DistributedHNSWSegmentFromSegmentError::MissingHnswConfiguration)?;

        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if !segment.file_path.is_empty() {
            // Check if its in the providers cache, if not load the index from the files
            let index_path = match &segment.file_path.get(HNSW_INDEX) {
                None => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                    ));
                }
                Some(files) => {
                    if files.is_empty() {
                        return Err(Box::new(
                            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound,
                        ));
                    } else {
                        &files[0]
                    }
                }
            };

            let (prefix_path, index_uuid) = Segment::extract_prefix_and_id(index_path)
                .map_err(|_| Box::new(DistributedHNSWSegmentFromSegmentError::InvalidUUID))?;
            let index_uuid = IndexUuid(index_uuid);
            let index = match hnsw_index_provider
                .open(
                    &index_uuid,
                    &segment.collection,
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                    hnsw_configuration.ef_search,
                    prefix_path,
                )
                .await
            {
                Ok(index) => index,
                Err(e) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(*e),
                    ))
                }
            };

            Ok(Box::new(DistributedHNSWSegmentReader::new(
                index, segment.id,
            )))
        } else {
            Err(Box::new(
                DistributedHNSWSegmentFromSegmentError::Uninitialized,
            ))
        }
    }

    pub fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowd_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        let index = self.index.inner.read();
        index
            .hnsw_index
            .query(vector, k, allowed_ids, disallowd_ids)
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;

    use chroma_index::{HnswIndexConfig, DEFAULT_MAX_ELEMENTS};
    use chroma_types::{
        Collection, CollectionUuid, InternalCollectionConfiguration, InternalHnswConfiguration,
        Schema, Segment, SegmentUuid,
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn parameter_defaults() {
        let persist_path = tempdir().unwrap().path().to_owned();

        let hnsw_configuration = InternalHnswConfiguration::default();
        let config = HnswIndexConfig::new_persistent(
            hnsw_configuration.max_neighbors,
            hnsw_configuration.ef_construction,
            hnsw_configuration.ef_search,
            &persist_path,
        )
        .expect("Error creating hnsw index config");

        let default_hnsw_params = InternalHnswConfiguration::default();

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, default_hnsw_params.max_neighbors);
        assert_eq!(config.ef_construction, default_hnsw_params.ef_construction);
        assert_eq!(config.ef_search, default_hnsw_params.ef_search);
        assert_eq!(config.random_seed, 0);
        assert_eq!(
            config.persist_path,
            Some(persist_path.to_str().unwrap().to_string())
        );
        let config = InternalCollectionConfiguration {
            vector_index: chroma_types::VectorIndexConfiguration::Hnsw(InternalHnswConfiguration {
                max_neighbors: 10,
                ..Default::default()
            }),
            embedding_function: None,
        };

        // Try partial override
        let collection = Collection {
            config: config.clone(),
            schema: Some(Schema::try_from(&config).unwrap()),
            ..Default::default()
        };

        let segment = Segment {
            id: SegmentUuid(Uuid::new_v4()),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: None,
            collection: CollectionUuid(Uuid::new_v4()),
            file_path: HashMap::new(),
        };

        let hnsw_params = collection
            .schema
            .as_ref()
            .map(|schema| schema.get_internal_hnsw_config_with_legacy_fallback(&segment))
            .transpose()
            .unwrap()
            .flatten()
            .unwrap();
        let config = HnswIndexConfig::new_persistent(
            hnsw_params.max_neighbors,
            hnsw_params.ef_construction,
            hnsw_params.ef_search,
            &persist_path,
        )
        .expect("Error creating hnsw index config");

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, 10);
        assert_eq!(config.ef_construction, default_hnsw_params.ef_construction);
        assert_eq!(config.ef_search, default_hnsw_params.ef_search);
        assert_eq!(config.random_seed, 0);
        assert_eq!(
            config.persist_path,
            Some(persist_path.to_str().unwrap().to_string())
        );
    }
}

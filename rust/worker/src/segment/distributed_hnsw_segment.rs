use super::record_segment::ApplyMaterializedLogError;
use super::{SegmentFlusher, SegmentWriter};
use async_trait::async_trait;
use chroma_distance::{DistanceFunction, DistanceFunctionError};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::{
    HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderForkError,
    HnswIndexProviderOpenError, HnswIndexRef,
};
use chroma_index::{Index, IndexUuid};
use chroma_index::{DEFAULT_HNSW_EF_CONSTRUCTION, DEFAULT_HNSW_EF_SEARCH, DEFAULT_HNSW_M};
use chroma_types::SegmentUuid;
use chroma_types::{get_metadata_value_as, MaterializedLogOperation, MetadataValue, Segment};
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;
use uuid::Uuid;

const HNSW_INDEX: &str = "hnsw_index";

pub struct HnswIndexParamsFromSegment {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentWriter {
    index: HnswIndexRef,
    hnsw_index_provider: HnswIndexProvider,
    pub(crate) id: SegmentUuid,
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
    #[error("Error extracting distance function")]
    DistanceFunctionError(#[from] DistanceFunctionError),
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
            DistributedHNSWSegmentFromSegmentError::DistanceFunctionError(e) => e.code(),
        }
    }
}

fn hnsw_params_from_segment(segment: &Segment) -> HnswIndexParamsFromSegment {
    let metadata = match &segment.metadata {
        Some(metadata) => metadata,
        None => {
            return HnswIndexParamsFromSegment {
                m: DEFAULT_HNSW_M,
                ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
                ef_search: DEFAULT_HNSW_EF_SEARCH,
            };
        }
    };

    let m = match get_metadata_value_as::<i64>(metadata, "hnsw:M") {
        Ok(m) => m as usize,
        Err(_) => DEFAULT_HNSW_M,
    };
    let ef_construction = match get_metadata_value_as::<i64>(metadata, "hnsw:construction_ef") {
        Ok(ef_construction) => ef_construction as usize,
        Err(_) => DEFAULT_HNSW_EF_CONSTRUCTION,
    };
    let ef_search = match get_metadata_value_as::<i64>(metadata, "hnsw:search_ef") {
        Ok(ef_search) => ef_search as usize,
        Err(_) => DEFAULT_HNSW_EF_SEARCH,
    };

    HnswIndexParamsFromSegment {
        m,
        ef_construction,
        ef_search,
    }
}

pub fn distance_function_from_segment(
    segment: &Segment,
) -> Result<DistanceFunction, Box<DistributedHNSWSegmentFromSegmentError>> {
    let space = match segment.metadata {
        Some(ref metadata) => match metadata.get("hnsw:space") {
            Some(MetadataValue::Str(space)) => space,
            _ => "l2",
        },
        None => "l2",
    };
    match DistanceFunction::try_from(space) {
        Ok(distance_function) => Ok(distance_function),
        Err(e) => Err(Box::new(
            DistributedHNSWSegmentFromSegmentError::DistanceFunctionError(e),
        )),
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

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentWriter>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if !segment.file_path.is_empty() {
            println!("Loading HNSW index from files");
            // Check if its in the providers cache, if not load the index from the files
            let index_id = match &segment.file_path.get(HNSW_INDEX) {
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

            let index_uuid = match Uuid::parse_str(index_id.as_str()) {
                Ok(uuid) => uuid,
                Err(_) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::InvalidUUID,
                    ))
                }
            };
            let index_uuid = IndexUuid(index_uuid);

            let distance_function = match distance_function_from_segment(segment) {
                Ok(distance_function) => distance_function,
                Err(e) => {
                    return Err(e);
                }
            };

            let index = match hnsw_index_provider
                .fork(
                    &index_uuid,
                    &segment.collection,
                    dimensionality as i32,
                    distance_function,
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
            let hnsw_params = hnsw_params_from_segment(segment);

            let distance_function = match distance_function_from_segment(segment) {
                Ok(distance_function) => distance_function,
                Err(e) => {
                    return Err(e);
                }
            };
            let index = match hnsw_index_provider
                .create(
                    &segment.collection,
                    hnsw_params.m,
                    hnsw_params.ef_construction,
                    hnsw_params.ef_search,
                    dimensionality as i32,
                    distance_function,
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
}

impl SegmentWriter for DistributedHNSWSegmentWriter {
    type Flusher = DistributedHNSWSegmentWriter;

    fn get_id(&self) -> SegmentUuid {
        self.id
    }

    fn get_name(&self) -> &'static str {
        "DistributedHNSWSegmentWriter"
    }

    async fn apply_materialized_log_chunk(
        &self,
        records: chroma_types::Chunk<super::MaterializedLogRecord<'_>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        for (record, _) in records.iter() {
            match record.final_operation {
                // If embedding is not found in case of adds it means that user
                // did not supply them and thus we should return an error as
                // opposed to panic.
                MaterializedLogOperation::AddNew
                | MaterializedLogOperation::UpdateExisting
                | MaterializedLogOperation::OverwriteExisting => {
                    let embedding = record.merged_embeddings();

                    let mut index = self.index.inner.upgradable_read();
                    let index_len = index.len();
                    let index_capacity = index.capacity();
                    if index_len + 1 > index_capacity {
                        index.with_upgraded(|index| {
                            // Bump allocation by 2x
                            index
                                .resize(index_capacity * 2)
                                .map(|_| ApplyMaterializedLogError::Allocation)
                        })?;
                    }

                    match index.add(record.offset_id as usize, embedding) {
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
                    match self.index.inner.read().delete(record.offset_id as usize) {
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

    async fn commit(self) -> Result<Self::Flusher, Box<dyn ChromaError>> {
        let res = self.hnsw_index_provider.commit(self.index.clone());
        match res {
            Ok(_) => Ok(self),
            Err(e) => Err(e),
        }
    }
}

impl SegmentWriter for Box<DistributedHNSWSegmentWriter> {
    type Flusher = Box<DistributedHNSWSegmentWriter>;

    fn get_id(&self) -> SegmentUuid {
        self.id
    }

    fn get_name(&self) -> &'static str {
        "DistributedHNSWSegmentWriter"
    }

    async fn apply_materialized_log_chunk(
        &self,
        records: chroma_types::Chunk<super::MaterializedLogRecord<'_>>,
    ) -> Result<(), ApplyMaterializedLogError> {
        self.as_ref().apply_materialized_log_chunk(records).await
    }

    async fn commit(self) -> Result<Self::Flusher, Box<dyn ChromaError>> {
        DistributedHNSWSegmentWriter::commit(*self)
            .await
            .map(Box::new)
    }
}

#[async_trait]
impl SegmentFlusher for DistributedHNSWSegmentWriter {
    fn get_id(&self) -> SegmentUuid {
        self.id
    }

    fn get_name(&self) -> &'static str {
        "DistributedHNSWSegmentWriter"
    }

    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let hnsw_index_id = self.index.inner.read().id;
        match self.hnsw_index_provider.flush(&hnsw_index_id).await {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        let mut flushed_files = HashMap::new();
        flushed_files.insert(HNSW_INDEX.to_string(), vec![hnsw_index_id.to_string()]);
        Ok(flushed_files)
    }
}

#[async_trait]
impl SegmentFlusher for Box<DistributedHNSWSegmentWriter> {
    fn get_id(&self) -> SegmentUuid {
        self.id
    }

    fn get_name(&self) -> &'static str {
        "DistributedHNSWSegmentWriter"
    }

    async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        DistributedHNSWSegmentWriter::flush(*self).await
    }
}

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentReader {
    index: HnswIndexRef,
    pub(crate) id: SegmentUuid,
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

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentReader>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        // TODO: this is hacky, we use the presence of files to determine if we need to load or create the index
        // ideally, an explicit state would be better. When we implement distributed HNSW segments,
        // we can introduce a state in the segment metadata for this
        if !segment.file_path.is_empty() {
            println!("Loading HNSW index from files");
            // Check if its in the providers cache, if not load the index from the files
            let index_id = match &segment.file_path.get(HNSW_INDEX) {
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

            let index_uuid = match Uuid::parse_str(index_id.as_str()) {
                Ok(uuid) => uuid,
                Err(_) => {
                    return Err(Box::new(
                        DistributedHNSWSegmentFromSegmentError::InvalidUUID,
                    ))
                }
            };
            let index_uuid = IndexUuid(index_uuid);

            let index =
                match hnsw_index_provider
                    .get(&index_uuid, &segment.collection)
                    .await
                {
                    Some(index) => index,
                    None => {
                        let distance_function = match distance_function_from_segment(segment) {
                            Ok(distance_function) => distance_function,
                            Err(e) => {
                                return Err(e);
                            }
                        };
                        match hnsw_index_provider
                            .open(
                                &index_uuid,
                                &segment.collection,
                                dimensionality as i32,
                                distance_function,
                            )
                            .await
                        {
                            Ok(index) => index,
                            Err(e) => return Err(Box::new(
                                DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(
                                    *e,
                                ),
                            )),
                        }
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

    pub(crate) fn query(
        &self,
        vector: &[f32],
        k: usize,
        allowed_ids: &[usize],
        disallowd_ids: &[usize],
    ) -> Result<(Vec<usize>, Vec<f32>), Box<dyn ChromaError>> {
        let index = self.index.inner.read();
        index.query(vector, k, allowed_ids, disallowd_ids)
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;

    use chroma_index::{
        HnswIndexConfig, DEFAULT_HNSW_EF_CONSTRUCTION, DEFAULT_HNSW_EF_SEARCH, DEFAULT_HNSW_M,
        DEFAULT_MAX_ELEMENTS,
    };
    use chroma_types::{CollectionUuid, MetadataValue, Segment, SegmentUuid};
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::segment::distributed_hnsw_segment::hnsw_params_from_segment;

    #[test]
    fn parameter_defaults() {
        let persist_path = tempdir().unwrap().path().to_owned();

        let segment = Segment {
            id: SegmentUuid(Uuid::new_v4()),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: Some(HashMap::new()),
            collection: CollectionUuid(Uuid::new_v4()),
            file_path: HashMap::new(),
        };

        let hnsw_params = hnsw_params_from_segment(&segment);
        let config = HnswIndexConfig::new(
            hnsw_params.m,
            hnsw_params.ef_construction,
            hnsw_params.ef_search,
            &persist_path,
        )
        .expect("Error creating hnsw index config");

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, DEFAULT_HNSW_M);
        assert_eq!(config.ef_construction, DEFAULT_HNSW_EF_CONSTRUCTION);
        assert_eq!(config.ef_search, DEFAULT_HNSW_EF_SEARCH);
        assert_eq!(config.random_seed, 0);
        assert_eq!(config.persist_path, persist_path.to_str().unwrap());

        // Try partial metadata
        let mut metadata = HashMap::new();
        metadata.insert("hnsw:M".to_string(), MetadataValue::Int(10_i64));

        let segment = Segment {
            id: SegmentUuid(Uuid::new_v4()),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            metadata: Some(metadata),
            collection: CollectionUuid(Uuid::new_v4()),
            file_path: HashMap::new(),
        };

        let hnsw_params = hnsw_params_from_segment(&segment);
        let config = HnswIndexConfig::new(
            hnsw_params.m,
            hnsw_params.ef_construction,
            hnsw_params.ef_search,
            &persist_path,
        )
        .expect("Error creating hnsw index config");

        assert_eq!(config.max_elements, DEFAULT_MAX_ELEMENTS);
        assert_eq!(config.m, 10);
        assert_eq!(config.ef_construction, DEFAULT_HNSW_EF_CONSTRUCTION);
        assert_eq!(config.ef_search, DEFAULT_HNSW_EF_SEARCH);
        assert_eq!(config.random_seed, 0);
        assert_eq!(config.persist_path, persist_path.to_str().unwrap());
    }
}

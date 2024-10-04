use super::record_segment::ApplyMaterializedLogError;
use super::{SegmentFlusher, SegmentWriter};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::{
    HnswIndexProvider, HnswIndexProviderCreateError, HnswIndexProviderForkError,
    HnswIndexProviderOpenError, HnswIndexRef,
};
use chroma_index::{
    HnswIndexConfig, HnswIndexFromSegmentError, Index, IndexConfig, IndexConfigFromSegmentError,
};
use chroma_types::{MaterializedLogOperation, Segment};
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;
use uuid::Uuid;

const HNSW_INDEX: &str = "hnsw_index";

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentWriter {
    index: HnswIndexRef,
    hnsw_index_provider: HnswIndexProvider,
    pub(crate) id: Uuid,
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
    #[error("Index configuration error")]
    IndexConfigError(#[from] IndexConfigFromSegmentError),
    #[error("HNSW index configuration error")]
    HnswIndexConfigError(#[from] HnswIndexFromSegmentError),
    #[error("HNSW index provider open error")]
    HnswIndexProviderOpenError(#[from] HnswIndexProviderOpenError),
    #[error("HNSW index provider fork error")]
    HnswIndexProviderForkError(#[from] HnswIndexProviderForkError),
    #[error("HNSW index provider create error")]
    HnswIndexProviderCreateError(#[from] HnswIndexProviderCreateError),
}

impl ChromaError for DistributedHNSWSegmentFromSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            DistributedHNSWSegmentFromSegmentError::NoHnswFileFound => ErrorCodes::NotFound,
            DistributedHNSWSegmentFromSegmentError::InvalidUUID => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::Uninitialized => ErrorCodes::InvalidArgument,
            DistributedHNSWSegmentFromSegmentError::IndexConfigError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexConfigError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderOpenError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderForkError(e) => e.code(),
            DistributedHNSWSegmentFromSegmentError::HnswIndexProviderCreateError(e) => e.code(),
        }
    }
}

impl DistributedHNSWSegmentWriter {
    pub(crate) fn new(
        index: HnswIndexRef,
        hnsw_index_provider: HnswIndexProvider,
        id: Uuid,
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
        let _index_config = match IndexConfig::from_segment(segment, dimensionality as i32) {
            Ok(ic) => ic,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::IndexConfigError(*e),
                ));
            }
        };
        let persist_path = &hnsw_index_provider.temporary_storage_path;

        let _hnsw_config = match HnswIndexConfig::from_segment(segment, persist_path) {
            Ok(hc) => hc,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::HnswIndexConfigError(*e),
                ));
            }
        };

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

            let index = match hnsw_index_provider
                .fork(&index_uuid, segment, dimensionality as i32)
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
            let index = match hnsw_index_provider
                .create(segment, dimensionality as i32)
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

impl<'a> SegmentWriter<'a> for DistributedHNSWSegmentWriter {
    async fn apply_materialized_log_chunk(
        &self,
        records: chroma_types::Chunk<super::MaterializedLogRecord<'a>>,
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

    fn commit(self) -> Result<impl SegmentFlusher, Box<dyn ChromaError>> {
        let res = self.hnsw_index_provider.commit(self.index.clone());
        match res {
            Ok(_) => Ok(self),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl SegmentFlusher for DistributedHNSWSegmentWriter {
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

#[derive(Clone)]
pub(crate) struct DistributedHNSWSegmentReader {
    index: HnswIndexRef,
    pub(crate) id: Uuid,
}

impl Debug for DistributedHNSWSegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedHNSWSegmentReader")
            .field("id", &self.id)
            .finish()
    }
}

impl DistributedHNSWSegmentReader {
    fn new(index: HnswIndexRef, id: Uuid) -> Self {
        DistributedHNSWSegmentReader { index, id }
    }

    pub(crate) async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        hnsw_index_provider: HnswIndexProvider,
    ) -> Result<Box<DistributedHNSWSegmentReader>, Box<DistributedHNSWSegmentFromSegmentError>>
    {
        let index_config = IndexConfig::from_segment(segment, dimensionality as i32);
        let _index_config = match index_config {
            Ok(ic) => ic,
            Err(e) => {
                return Err(Box::new(
                    DistributedHNSWSegmentFromSegmentError::IndexConfigError(*e),
                ));
            }
        };
        let _persist_path = &hnsw_index_provider.temporary_storage_path;

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

            let index =
                match hnsw_index_provider
                    .get(&index_uuid, &segment.collection)
                    .await
                {
                    Some(index) => index,
                    None => {
                        match hnsw_index_provider
                            .open(&index_uuid, segment, dimensionality as i32)
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

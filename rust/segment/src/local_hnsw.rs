use std::{collections::HashMap, path::Path, sync::Arc};

use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{HnswIndex, HnswIndexConfig, Index, IndexConfig, PersistentIndex};
use chroma_types::{Chunk, LogRecord, Operation, Segment};
use serde::{Deserialize, Serialize};
use serde_pickle::{DeOptions, SerOptions};
use thiserror::Error;

use crate::utils::{distance_function_from_segment, hnsw_params_from_segment};

#[allow(dead_code)]
const METADATA_FILE: &str = "index_metadata.pickle";

#[allow(dead_code)]
pub struct LocalHnswSegmentReader {
    index: Arc<tokio::sync::RwLock<LocalHnswIndex>>,
    index_inited: bool,
    allow_reset: bool,
}

#[derive(Error, Debug)]
pub enum LocalHnswSegmentReaderError {
    #[error("Error opening pickle file")]
    PickleFileOpenError(#[from] std::io::Error),
    #[error("Error deserializing pickle file")]
    PickleFileDeserializeError(#[from] serde_pickle::Error),
    #[error("Error loading hnsw index")]
    HnswIndexLoadError,
    #[error("Nothing found on disk")]
    UninitializedSegment,
    #[error("Cannot obtain hnsw distance function from segment")]
    DistanceFunctionError(#[from] Box<chroma_distance::DistanceFunctionError>),
}

impl ChromaError for LocalHnswSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentReaderError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::DistanceFunctionError(e) => e.code(),
        }
    }
}

impl LocalHnswSegmentReader {
    #[allow(dead_code)]
    async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        persist_path: &Path,
    ) -> Result<Self, LocalHnswSegmentReaderError> {
        let index_folder = persist_path.join(segment.id.to_string());
        if !index_folder.exists() {
            // Return uninitialized reader.
            return Err(LocalHnswSegmentReaderError::UninitializedSegment);
        }
        let pickle_file_path = persist_path
            .join(segment.id.to_string())
            .join(METADATA_FILE);
        if pickle_file_path.exists() {
            let file = tokio::fs::File::open(pickle_file_path)
                .await?
                .into_std()
                .await;
            let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
            if !id_map.id_to_label.is_empty() {
                // Load hnsw index.
                let distance_function = distance_function_from_segment(segment)?;
                let index_config = IndexConfig::new(dimensionality as i32, distance_function);
                let index = HnswIndex::load(
                    index_folder.to_str().unwrap(),
                    &index_config,
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                // TODO(Sanket): Set allow reset appropriately.
                return Ok(Self {
                    index: Arc::new(tokio::sync::RwLock::new(LocalHnswIndex { index, id_map })),
                    index_inited: true,
                    allow_reset: false,
                });
            } else {
                // An empty reader.
                return Err(LocalHnswSegmentReaderError::UninitializedSegment);
            }
        }
        // Return uninitialized reader.
        Err(LocalHnswSegmentReaderError::UninitializedSegment)
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct IdMap {
    dimensionality: Option<usize>,
    total_elements_added: u32,
    max_seq_id: u32,
    id_to_label: HashMap<String, u32>,
    label_to_id: HashMap<u32, String>,
    id_to_seq_id: HashMap<String, u32>,
}

#[allow(dead_code)]
struct LocalHnswIndex {
    index: HnswIndex,
    // Loaded from pickle file.
    id_map: IdMap,
}

#[allow(dead_code)]
pub struct LocalHnswSegmentWriter {
    index: Arc<tokio::sync::RwLock<LocalHnswIndex>>,
    persist_path: String,
    index_inited: bool,
    allow_reset: bool,
}

#[derive(Error, Debug)]
pub enum LocalHnswSegmentWriterError {
    #[error("Error creating hnsw config object")]
    HnswConfigError(#[from] Box<chroma_index::HnswIndexConfigError>),
    #[error("Error opening pickle file")]
    PickleFileOpenError(#[from] std::io::Error),
    #[error("Error deserializing pickle file")]
    PickleFileDeserializeError(#[from] serde_pickle::Error),
    #[error("Error loading hnsw index")]
    HnswIndexLoadError,
    #[error("Nothing found on disk")]
    UninitializedSegment,
    #[error("Cannot obtain hnsw distance function from segment")]
    DistanceFunctionError(#[from] Box<chroma_distance::DistanceFunctionError>),
    #[error("Error creating hnsw index")]
    HnswIndexInitError,
    #[error("Error persisting hnsw index")]
    HnswIndexPersistError,
    #[error("Error applying log chunk")]
    EmbeddingNotFound,
    #[error("Error applying log chunk")]
    HnwsIndexAddError,
    #[error("Error applying log chunk")]
    HnswIndexResizeError,
    #[error("Error applying log chunk")]
    HnswIndexDeleteError,
}

impl ChromaError for LocalHnswSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentWriterError::HnswConfigError(e) => e.code(),
            LocalHnswSegmentWriterError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::DistanceFunctionError(e) => e.code(),
            LocalHnswSegmentWriterError::HnswIndexInitError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexPersistError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::EmbeddingNotFound => ErrorCodes::InvalidArgument,
            LocalHnswSegmentWriterError::HnwsIndexAddError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexResizeError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexDeleteError => ErrorCodes::Internal,
        }
    }
}

impl LocalHnswSegmentWriter {
    #[allow(dead_code)]
    async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        persist_path: &Path,
    ) -> Result<Self, LocalHnswSegmentWriterError> {
        let index_folder = persist_path.join(segment.id.to_string());
        if !index_folder.exists() {
            tokio::fs::create_dir_all(&index_folder).await?;
        }
        let pickle_file_path = persist_path
            .join(segment.id.to_string())
            .join(METADATA_FILE);
        if pickle_file_path.exists() {
            let file = tokio::fs::File::open(pickle_file_path)
                .await?
                .into_std()
                .await;
            let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
            if !id_map.id_to_label.is_empty() {
                // Load hnsw index.
                let distance_function = distance_function_from_segment(segment)?;
                let index_config = IndexConfig::new(dimensionality as i32, distance_function);
                let index = HnswIndex::load(
                    index_folder.to_str().unwrap(),
                    &index_config,
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                // TODO(Sanket): Set allow reset appropriately.
                return Ok(Self {
                    index: Arc::new(tokio::sync::RwLock::new(LocalHnswIndex { index, id_map })),
                    persist_path: persist_path.to_str().unwrap().to_string(),
                    index_inited: true,
                    allow_reset: false,
                });
            }
        }
        // Initialize index.
        let distance_function = distance_function_from_segment(segment)?;
        let hnsw_params = hnsw_params_from_segment(segment);
        let index_config = IndexConfig::new(dimensionality as i32, distance_function);
        let hnsw_config = HnswIndexConfig::new(
            hnsw_params.m,
            hnsw_params.ef_construction,
            hnsw_params.ef_search,
            &index_folder,
        )?;

        // HnswIndex init is not thread safe. We should not call it from multiple threads
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            chroma_index::IndexUuid(segment.id.0),
        )
        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexInitError)?;
        // Return uninitialized reader.
        Ok(Self {
            index: Arc::new(tokio::sync::RwLock::new(LocalHnswIndex {
                index,
                id_map: IdMap::default(),
            })),
            persist_path: index_folder.to_str().unwrap().to_string(),
            index_inited: true,
            allow_reset: false,
        })
    }

    #[allow(dead_code)]
    async fn persist(&mut self) -> Result<(), LocalHnswSegmentWriterError> {
        let guard = self.index.write().await;
        // Persist hnsw index.
        guard
            .index
            .save()
            .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
        // Persist id map.
        let metadata_file_path = Path::new(&self.persist_path).join(METADATA_FILE);
        let mut file = tokio::fs::File::create(metadata_file_path)
            .await?
            .into_std()
            .await;
        serde_pickle::to_writer(&mut file, &guard.id_map, SerOptions::new())?;
        Ok(())
    }

    // Returns the updated log seq id.
    #[allow(dead_code)]
    async fn apply_log_chunk(
        &mut self,
        log_chunk: Chunk<LogRecord>,
    ) -> Result<u32, LocalHnswSegmentWriterError> {
        let mut guard = self.index.write().await;
        let mut next_label = guard.id_map.total_elements_added + 1;
        for (log, _) in log_chunk.iter() {
            match log.record.operation {
                Operation::Add => {
                    // only update if the id is not already present
                    if !guard.id_map.id_to_label.contains_key(&log.record.id) {
                        match &log.record.embedding {
                            Some(embedding) => {
                                guard
                                    .id_map
                                    .id_to_label
                                    .insert(log.record.id.clone(), next_label);
                                guard
                                    .id_map
                                    .label_to_id
                                    .insert(next_label, log.record.id.clone());
                                let index_len = guard.index.len_with_deleted();
                                let index_capacity = guard.index.capacity();
                                if index_len + 1 > index_capacity {
                                    guard.index.resize(index_capacity * 2).map_err(|_| {
                                        LocalHnswSegmentWriterError::HnswIndexResizeError
                                    })?;
                                }
                                guard
                                    .index
                                    .add(next_label as usize, embedding.as_slice())
                                    .map_err(|_| LocalHnswSegmentWriterError::HnwsIndexAddError)?;
                                next_label += 1;
                            }
                            None => {
                                return Err(LocalHnswSegmentWriterError::EmbeddingNotFound);
                            }
                        }
                    }
                }
                Operation::Update => {
                    if let Some(label) = guard.id_map.id_to_label.get(&log.record.id).cloned() {
                        if let Some(embedding) = &log.record.embedding {
                            let index_len = guard.index.len_with_deleted();
                            let index_capacity = guard.index.capacity();
                            if index_len + 1 > index_capacity {
                                guard.index.resize(index_capacity * 2).map_err(|_| {
                                    LocalHnswSegmentWriterError::HnswIndexResizeError
                                })?;
                            }
                            guard
                                .index
                                .add(label as usize, embedding.as_slice())
                                .map_err(|_| LocalHnswSegmentWriterError::HnwsIndexAddError)?;
                        }
                    }
                }
                Operation::Delete => {
                    if let Some(label) = guard.id_map.id_to_label.get(&log.record.id).cloned() {
                        guard.id_map.id_to_label.remove(&log.record.id);
                        guard.id_map.label_to_id.remove(&label);
                        guard
                            .index
                            .delete(label as usize)
                            .map_err(|_| LocalHnswSegmentWriterError::HnswIndexDeleteError)?;
                    }
                }
                Operation::Upsert => {
                    let mut update_label = false;
                    let label = match guard.id_map.id_to_label.get(&log.record.id) {
                        Some(label) => *label,
                        None => {
                            update_label = true;
                            next_label
                        }
                    };
                    match &log.record.embedding {
                        Some(embedding) => {
                            guard
                                .id_map
                                .id_to_label
                                .insert(log.record.id.clone(), label);
                            guard
                                .id_map
                                .label_to_id
                                .insert(label, log.record.id.clone());
                            let index_len = guard.index.len_with_deleted();
                            let index_capacity = guard.index.capacity();
                            if index_len + 1 > index_capacity {
                                guard.index.resize(index_capacity * 2).map_err(|_| {
                                    LocalHnswSegmentWriterError::HnswIndexResizeError
                                })?;
                            }
                            guard
                                .index
                                .add(label as usize, embedding.as_slice())
                                .map_err(|_| LocalHnswSegmentWriterError::HnwsIndexAddError)?;
                            if update_label {
                                next_label += 1;
                            }
                        }
                        None => {
                            return Err(LocalHnswSegmentWriterError::EmbeddingNotFound);
                        }
                    }
                }
            }
        }

        Ok(next_label)
    }
}

use std::{collections::HashMap, path::Path, sync::Arc};

use chroma_cache::Weighted;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{HnswIndex, HnswIndexConfig, Index, IndexConfig, PersistentIndex};
use chroma_sqlite::{db::SqliteDb, table::MaxSeqId};
use chroma_types::{
    operator::RecordDistance, Chunk, HnswParametersFromSegmentError, LogRecord, Operation, Segment,
    SegmentUuid, SingleNodeHnswParameters,
};
use sea_query::{Expr, Query, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_pickle::{DeOptions, SerOptions};
use sqlx::Row;
use thiserror::Error;

#[allow(dead_code)]
const METADATA_FILE: &str = "index_metadata.pickle";

#[allow(dead_code)]
#[derive(Clone)]
pub struct LocalHnswSegmentReader {
    pub index: LocalHnswIndex,
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
    #[error("Could not parse HNSW configuration: {0}")]
    InvalidHnswConfiguration(#[from] HnswParametersFromSegmentError),
    #[error("Error serializing path to string")]
    PersistPathError,
    #[error("Error finding id")]
    IdNotFound,
    #[error("Error getting embedding")]
    GetEmbeddingError,
    #[error("Error querying knn")]
    QueryError,
    #[error("Error reading from sqlite")]
    SqliteError(#[from] sqlx::error::Error),
}

impl ChromaError for LocalHnswSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentReaderError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::InvalidHnswConfiguration(err) => err.code(),
            LocalHnswSegmentReaderError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::IdNotFound => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::GetEmbeddingError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::QueryError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::SqliteError(_) => ErrorCodes::Internal,
        }
    }
}

impl LocalHnswSegmentReader {
    pub fn from_index(hnsw_index: LocalHnswIndex) -> Self {
        Self { index: hnsw_index }
    }

    pub async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        persist_path: &Path,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentReaderError> {
        let index_folder = persist_path.join(segment.id.to_string());
        if !index_folder.exists() {
            // Return uninitialized reader.
            return Err(LocalHnswSegmentReaderError::UninitializedSegment);
        }
        let index_folder_str = match index_folder.to_str() {
            Some(path) => path,
            None => return Err(LocalHnswSegmentReaderError::PersistPathError),
        };
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
                let hnsw_configuration = SingleNodeHnswParameters::try_from(segment)?;
                let index_config =
                    IndexConfig::new(dimensionality as i32, hnsw_configuration.space.into());
                let index = HnswIndex::load(
                    index_folder_str,
                    &index_config,
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                // TODO(Sanket): Set allow reset appropriately.
                return Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(tokio::sync::RwLock::new(Inner {
                            index,
                            id_map,
                            index_init: true,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: index_folder_str.to_string(),
                            sqlite: sql_db,
                        })),
                    },
                });
            } else {
                // An empty reader.
                return Err(LocalHnswSegmentReaderError::UninitializedSegment);
            }
        }
        // Return uninitialized reader.
        Err(LocalHnswSegmentReaderError::UninitializedSegment)
    }

    pub async fn get_embedding_by_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<Vec<f32>, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        guard
            .index
            .get(offset_id as usize)
            .map_err(|_| LocalHnswSegmentReaderError::GetEmbeddingError)?
            .ok_or(LocalHnswSegmentReaderError::GetEmbeddingError)
    }

    pub async fn current_max_seq_id(
        &self,
        segment_id: &SegmentUuid,
    ) -> Result<u64, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        let (sql, values) = Query::select()
            .column(MaxSeqId::SeqId)
            .from(MaxSeqId::Table)
            .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment_id.to_string()))
            .build_sqlx(SqliteQueryBuilder);
        let row_opt = sqlx::query_with(&sql, values)
            .fetch_optional(guard.sqlite.get_conn())
            .await?;
        Ok(row_opt
            .map(|row| row.try_get::<u64, _>(0))
            .transpose()?
            .unwrap_or_default())
    }

    pub async fn get_embedding_by_user_id(
        &self,
        user_id: &String,
    ) -> Result<Vec<f32>, LocalHnswSegmentReaderError> {
        let offset_id = self.get_offset_id_by_user_id(user_id).await?;
        self.get_embedding_by_offset_id(offset_id).await
    }

    pub async fn get_offset_id_by_user_id(
        &self,
        user_id: &String,
    ) -> Result<u32, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        guard
            .id_map
            .id_to_label
            .get(user_id)
            .cloned()
            .ok_or(LocalHnswSegmentReaderError::IdNotFound)
    }

    pub async fn get_user_id_by_offset_id(
        &self,
        offset_id: u32,
    ) -> Result<String, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        guard
            .id_map
            .label_to_id
            .get(&offset_id)
            .cloned()
            .ok_or(LocalHnswSegmentReaderError::IdNotFound)
    }

    pub async fn query_embedding(
        &self,
        allowed_offset_ids: &[u32],
        embedding: Vec<f32>,
        k: u32,
    ) -> Result<Vec<RecordDistance>, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        let allowed_ids = allowed_offset_ids
            .iter()
            .map(|oid| *oid as usize)
            .collect::<Vec<_>>();
        let (offset_ids, distances) = guard
            .index
            .query(&embedding, k as usize, allowed_ids.as_slice(), &[])
            .map_err(|_| LocalHnswSegmentReaderError::QueryError)?;
        Ok(offset_ids
            .into_iter()
            .zip(distances)
            .map(|(offset_id, measure)| RecordDistance {
                offset_id: offset_id as u32,
                measure,
            })
            .collect())
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct IdMap {
    dimensionality: Option<usize>,
    total_elements_added: u32,
    max_seq_id: u64,
    id_to_label: HashMap<String, u32>,
    label_to_id: HashMap<u32, String>,
    id_to_seq_id: HashMap<String, u32>,
}

#[allow(dead_code)]
pub struct Inner {
    index: HnswIndex,
    // Loaded from pickle file.
    id_map: IdMap,
    index_init: bool,
    allow_reset: bool,
    num_elements_since_last_persist: u64,
    sync_threshold: usize,
    persist_path: String,
    sqlite: SqliteDb,
}

#[derive(Clone)]
pub struct LocalHnswIndex {
    inner: Arc<tokio::sync::RwLock<Inner>>,
}

impl LocalHnswIndex {
    pub async fn close(&self) {
        self.inner.write().await.index.close_fd();
    }
    pub async fn start(&self) {
        self.inner.write().await.index.open_fd();
    }
}

impl Weighted for LocalHnswIndex {
    fn weight(&self) -> usize {
        1
    }
}

#[allow(dead_code)]
pub struct LocalHnswSegmentWriter {
    pub index: LocalHnswIndex,
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
    #[error("Could not parse HNSW configuration: {0}")]
    InvalidHnswConfiguration(#[from] HnswParametersFromSegmentError),
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
    #[error("Error converting persistant path to string")]
    PersistPathError,
    #[error("Error updating max sequence id")]
    QueryBuilderError(#[from] sea_query::error::Error),
    #[error("Error updating max sequence id")]
    MaxSeqIdUpdateError(#[from] sqlx::error::Error),
}

impl ChromaError for LocalHnswSegmentWriterError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentWriterError::HnswConfigError(e) => e.code(),
            LocalHnswSegmentWriterError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::InvalidHnswConfiguration(err) => err.code(),
            LocalHnswSegmentWriterError::HnswIndexInitError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexPersistError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::EmbeddingNotFound => ErrorCodes::InvalidArgument,
            LocalHnswSegmentWriterError::HnwsIndexAddError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexResizeError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexDeleteError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::QueryBuilderError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::MaxSeqIdUpdateError(_) => ErrorCodes::Internal,
        }
    }
}

impl LocalHnswSegmentWriter {
    pub fn from_index(hnsw_index: LocalHnswIndex) -> Result<Self, LocalHnswSegmentWriterError> {
        Ok(Self { index: hnsw_index })
    }

    #[allow(dead_code)]
    pub async fn from_segment(
        segment: &Segment,
        dimensionality: usize,
        persist_path: &Path,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentWriterError> {
        let index_folder = persist_path.join(segment.id.to_string());
        if !index_folder.exists() {
            tokio::fs::create_dir_all(&index_folder).await?;
        }
        let index_folder_str = match index_folder.to_str() {
            Some(path) => path,
            None => return Err(LocalHnswSegmentWriterError::PersistPathError),
        };
        let hnsw_configuration = SingleNodeHnswParameters::try_from(segment)?;
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
                let index_config =
                    IndexConfig::new(dimensionality as i32, hnsw_configuration.space.into());
                let index = HnswIndex::load(
                    index_folder_str,
                    &index_config,
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                // TODO(Sanket): Set allow reset appropriately.
                return Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(tokio::sync::RwLock::new(Inner {
                            index,
                            id_map,
                            index_init: true,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: index_folder_str.to_string(),
                            sqlite: sql_db,
                        })),
                    },
                });
            }
        }
        // Initialize index.
        let index_config = IndexConfig::new(dimensionality as i32, hnsw_configuration.space.into());
        let hnsw_config = HnswIndexConfig::new(
            hnsw_configuration.m,
            hnsw_configuration.construction_ef,
            hnsw_configuration.search_ef,
            &index_folder,
        )?;

        // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            chroma_index::IndexUuid(segment.id.0),
        )
        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexInitError)?;
        // Return uninitialized reader.
        Ok(Self {
            index: LocalHnswIndex {
                inner: Arc::new(tokio::sync::RwLock::new(Inner {
                    index,
                    id_map: IdMap::default(),
                    index_init: true,
                    allow_reset: false,
                    num_elements_since_last_persist: 0,
                    sync_threshold: hnsw_configuration.sync_threshold,
                    persist_path: index_folder_str.to_string(),
                    sqlite: sql_db,
                })),
            },
        })
    }

    // Returns the updated log seq id.
    #[allow(dead_code)]
    pub async fn apply_log_chunk(
        &mut self,
        log_chunk: Chunk<LogRecord>,
    ) -> Result<u32, LocalHnswSegmentWriterError> {
        let mut guard = self.index.inner.write().await;
        let mut next_label = guard.id_map.total_elements_added + 1;
        for (log, _) in log_chunk.iter() {
            guard.num_elements_since_last_persist += 1;
            guard.id_map.max_seq_id = std::cmp::max(guard.id_map.max_seq_id, log.log_offset as u64);
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
        guard.id_map.total_elements_added = next_label - 1;
        if guard.num_elements_since_last_persist >= guard.sync_threshold as u64 {
            guard = persist(guard).await?;
            let id = guard.index.id.to_string().into();
            let max_id = guard.id_map.max_seq_id.into();
            // Persist max_seq_id to sqlite.
            let (query, values) = Query::insert()
                .into_table(MaxSeqId::Table)
                .replace()
                .columns([MaxSeqId::SegmentId, MaxSeqId::SeqId])
                .values([id, max_id])?
                .build_sqlx(SqliteQueryBuilder);
            let _ = sqlx::query_with(&query, values)
                .execute(guard.sqlite.get_conn())
                .await?;
            guard.num_elements_since_last_persist = 0;
        }

        Ok(next_label)
    }
}

async fn persist(
    guard: tokio::sync::RwLockWriteGuard<'_, Inner>,
) -> Result<tokio::sync::RwLockWriteGuard<'_, Inner>, LocalHnswSegmentWriterError> {
    // Persist hnsw index.
    guard
        .index
        .save()
        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
    // Persist id map.
    let metadata_file_path = Path::new(&guard.persist_path).join(METADATA_FILE);
    let mut file = tokio::fs::File::create(metadata_file_path)
        .await?
        .into_std()
        .await;
    serde_pickle::to_writer(&mut file, &guard.id_map, SerOptions::new())?;
    Ok(guard)
}

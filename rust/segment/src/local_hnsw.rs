use std::{
    collections::{BinaryHeap, HashMap},
    io::Write,
    path::Path,
    sync::Arc,
};

use chroma_cache::Weighted;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{HnswIndex, HnswIndexConfig, Index, IndexConfig, PersistentIndex};
use chroma_sqlite::{db::SqliteDb, table::MaxSeqId};
use chroma_types::{
    operator::RecordMeasure, Chunk, Collection, HnswParametersFromSegmentError, LogRecord,
    Operation, OperationRecord, Segment, SegmentUuid,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_query::{Expr, OnConflict, Query, SqliteQueryBuilder};
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
    #[error("Error opening pickle file: {0}")]
    PickleFileOpenError(#[from] std::io::Error),
    #[error("Error deserializing pickle file: {0}")]
    PickleFileDeserializeError(#[from] serde_pickle::Error),
    #[error("Error loading hnsw index")]
    HnswIndexLoadError,
    #[error("Nothing found on disk")]
    UninitializedSegment,
    #[error("Collection is missing HNSW configuration")]
    MissingHnswConfiguration,
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
    #[error("Error reading from sqlite: {0}")]
    SqliteError(#[from] sqlx::error::Error),
}

impl ChromaError for LocalHnswSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentReaderError::PickleFileOpenError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::PickleFileDeserializeError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::HnswIndexLoadError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::UninitializedSegment => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::MissingHnswConfiguration => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::InvalidHnswConfiguration(err) => err.code(),
            LocalHnswSegmentReaderError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::IdNotFound => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::GetEmbeddingError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::QueryError => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::SqliteError(_) => ErrorCodes::Internal,
        }
    }
}

async fn get_current_seq_id(
    segment: &Segment,
    sql_db: &SqliteDb,
) -> Result<u64, sqlx::error::Error> {
    let (query, values) = Query::select()
        .column(MaxSeqId::SeqId)
        .from(MaxSeqId::Table)
        .and_where(Expr::col(MaxSeqId::SegmentId).eq(segment.id.to_string()))
        .build_sqlx(SqliteQueryBuilder);
    let row = sqlx::query_with(&query, values)
        .fetch_optional(sql_db.get_conn())
        .await?;
    let seq_id = row
        .map(|row| row.try_get::<u64, _>(0))
        .transpose()?
        .unwrap_or_default();
    Ok(seq_id)
}

impl LocalHnswSegmentReader {
    pub fn from_index(hnsw_index: LocalHnswIndex) -> Self {
        Self { index: hnsw_index }
    }

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        persist_root: Option<String>,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentReaderError> {
        let hnsw_configuration = collection
            .schema
            .as_ref()
            .map(|schema| schema.get_internal_hnsw_config_with_legacy_fallback(segment))
            .transpose()?
            .flatten()
            .ok_or(LocalHnswSegmentReaderError::MissingHnswConfiguration)?;

        match persist_root {
            Some(path_str) => {
                let path = Path::new(&path_str);
                let index_folder = path.join(segment.id.to_string());
                if !index_folder.exists() {
                    // Return uninitialized reader.
                    return Err(LocalHnswSegmentReaderError::UninitializedSegment);
                }
                let index_folder_str = match index_folder.to_str() {
                    Some(path) => path,
                    None => return Err(LocalHnswSegmentReaderError::PersistPathError),
                };
                let pickle_file_path = path.join(segment.id.to_string()).join(METADATA_FILE);
                if pickle_file_path.exists() {
                    let file = tokio::fs::File::open(pickle_file_path)
                        .await?
                        .into_std()
                        .await;
                    let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if !id_map.id_to_label.is_empty() {
                        // Load hnsw index.
                        let index_config = IndexConfig::new(
                            dimensionality as i32,
                            hnsw_configuration.space.clone().into(),
                        );
                        let index = HnswIndex::load(
                            index_folder_str,
                            &index_config,
                            hnsw_configuration.ef_search,
                            chroma_index::IndexUuid(segment.id.0),
                        )
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;

                        let current_seq_id = get_current_seq_id(segment, &sql_db).await?;

                        // TODO(Sanket): Set allow reset appropriately.
                        return Ok(Self {
                            index: LocalHnswIndex {
                                inner: Arc::new(tokio::sync::RwLock::new(Inner {
                                    index,
                                    id_map,
                                    index_init: true,
                                    allow_reset: false,
                                    num_elements_since_last_persist: 0,
                                    last_seen_seq_id: current_seq_id,
                                    sync_threshold: hnsw_configuration.sync_threshold,
                                    persist_path: Some(index_folder_str.to_string()),
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
            None => {
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_ephemeral(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                );

                // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
                let index = HnswIndex::init(
                    &index_config,
                    Some(&hnsw_config),
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;

                Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(tokio::sync::RwLock::new(Inner {
                            index,
                            id_map: Default::default(),
                            index_init: true,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: None,
                            sqlite: sql_db,
                        })),
                    },
                })
            }
        }
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
    ) -> Result<Vec<RecordMeasure>, LocalHnswSegmentReaderError> {
        let guard = self.index.inner.read().await;
        let len_with_deleted = guard.index.len_with_deleted();
        let actual_len = guard.index.len();

        // Bail if the index is empty
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        let delete_percentage = (len_with_deleted - actual_len) as f32 / len_with_deleted as f32;

        // If the index is small and the delete percentage is high, its quite likely that the index is
        // degraded, so we brute force the search
        // Otherwise search the index normally
        if delete_percentage > 0.2 && actual_len < 100 {
            match guard.index.get_all_ids() {
                Ok((valid_ids, _deleted_ids)) => {
                    let mut max_heap = BinaryHeap::new();
                    let allowed_ids_as_set = allowed_offset_ids
                        .iter()
                        .collect::<std::collections::HashSet<_>>();
                    for curr_id in valid_ids.iter() {
                        if !allowed_ids_as_set.is_empty()
                            && !allowed_ids_as_set.contains(&(*curr_id as u32))
                        {
                            continue;
                        }
                        let curr_embedding = guard.index.get(*curr_id);
                        match curr_embedding {
                            Ok(Some(curr_embedding)) => {
                                let curr_embedding = match guard.index.distance_function {
                                    chroma_distance::DistanceFunction::Cosine => {
                                        chroma_distance::normalize(&curr_embedding)
                                    }
                                    _ => curr_embedding,
                                };
                                let curr_distance = guard
                                    .index
                                    .distance_function
                                    .distance(curr_embedding.as_slice(), embedding.as_slice());
                                if max_heap.len() < k as usize {
                                    max_heap.push(RecordMeasure {
                                        offset_id: *curr_id as u32,
                                        measure: curr_distance,
                                    });
                                } else {
                                    // SAFETY(hammadb): We are sure that the heap has at least one element
                                    // because we insert until we have k elements.
                                    let top = max_heap.peek().unwrap();
                                    if top.measure > curr_distance {
                                        max_heap.pop();
                                        max_heap.push(RecordMeasure {
                                            offset_id: *curr_id as u32,
                                            measure: curr_distance,
                                        });
                                    }
                                }
                            }
                            _ => {
                                return Err(LocalHnswSegmentReaderError::QueryError);
                            }
                        }
                    }
                    Ok(max_heap.into_sorted_vec())
                }
                Err(_) => Err(LocalHnswSegmentReaderError::QueryError),
            }
        } else {
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
                .map(|(offset_id, measure)| RecordMeasure {
                    offset_id: offset_id as u32,
                    measure,
                })
                .collect())
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
struct IdMap {
    dimensionality: Option<usize>,
    total_elements_added: u32,
    /// The max_seq_id field is deprecated in favor of the sqlite table
    #[serde(default)]
    max_seq_id: Option<u64>,
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
    last_seen_seq_id: u64,
    sync_threshold: usize,
    persist_path: Option<String>,
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
    #[error("Collection is missing HNSW configuration")]
    MissingHnswConfiguration,
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
            LocalHnswSegmentWriterError::MissingHnswConfiguration => ErrorCodes::Internal,
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

    pub async fn from_segment(
        collection: &Collection,
        segment: &Segment,
        dimensionality: usize,
        persist_root: Option<String>,
        sql_db: SqliteDb,
    ) -> Result<Self, LocalHnswSegmentWriterError> {
        let hnsw_configuration = collection
            .schema
            .as_ref()
            .map(|schema| schema.get_internal_hnsw_config_with_legacy_fallback(segment))
            .transpose()?
            .flatten()
            .ok_or(LocalHnswSegmentWriterError::MissingHnswConfiguration)?;

        match persist_root {
            Some(path_str) => {
                let path = Path::new(&path_str);
                let index_folder = path.join(segment.id.to_string());
                if !index_folder.exists() {
                    tokio::fs::create_dir_all(&index_folder).await?;
                }
                let index_folder_str = match index_folder.to_str() {
                    Some(path) => path,
                    None => return Err(LocalHnswSegmentWriterError::PersistPathError),
                };
                let pickle_file_path = path.join(segment.id.to_string()).join(METADATA_FILE);
                if pickle_file_path.exists() {
                    let file = tokio::fs::File::open(pickle_file_path)
                        .await?
                        .into_std()
                        .await;
                    let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if !id_map.id_to_label.is_empty() {
                        // Migrate legacy max_seq_id if present
                        if let Some(max_seq_id) = id_map.max_seq_id {
                            let id = segment.id.to_string().into();
                            let max_id = max_seq_id.into();
                            let (query, values) = Query::insert()
                                .into_table(MaxSeqId::Table)
                                .columns([MaxSeqId::SegmentId, MaxSeqId::SeqId])
                                .values([id, max_id])?
                                .on_conflict(
                                    OnConflict::column(MaxSeqId::SegmentId)
                                        .do_nothing()
                                        .to_owned(),
                                )
                                .build_sqlx(SqliteQueryBuilder);
                            let _ = sqlx::query_with(&query, values)
                                .execute(sql_db.get_conn())
                                .await?;
                        }
                        // Load hnsw index.
                        let index_config = IndexConfig::new(
                            dimensionality as i32,
                            hnsw_configuration.space.clone().into(),
                        );
                        let index = HnswIndex::load(
                            index_folder_str,
                            &index_config,
                            hnsw_configuration.ef_search,
                            chroma_index::IndexUuid(segment.id.0),
                        )
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;

                        let current_seq_id = get_current_seq_id(segment, &sql_db).await?;

                        // TODO(Sanket): Set allow reset appropriately.
                        return Ok(Self {
                            index: LocalHnswIndex {
                                inner: Arc::new(tokio::sync::RwLock::new(Inner {
                                    index,
                                    id_map,
                                    index_init: true,
                                    allow_reset: false,
                                    num_elements_since_last_persist: 0,
                                    last_seen_seq_id: current_seq_id,
                                    sync_threshold: hnsw_configuration.sync_threshold,
                                    persist_path: Some(index_folder_str.to_string()),
                                    sqlite: sql_db,
                                })),
                            },
                        });
                    }
                }
                // Initialize index.
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_persistent(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
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
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: Some(index_folder_str.to_string()),
                            sqlite: sql_db,
                        })),
                    },
                })
            }
            None => {
                let index_config = IndexConfig::new(
                    dimensionality as i32,
                    hnsw_configuration.space.clone().into(),
                );
                let hnsw_config = HnswIndexConfig::new_ephemeral(
                    hnsw_configuration.max_neighbors,
                    hnsw_configuration.ef_construction,
                    hnsw_configuration.ef_search,
                );

                // TODO(Sanket): HnswIndex init is not thread safe. We should not call it from multiple threads
                let index = HnswIndex::init(
                    &index_config,
                    Some(&hnsw_config),
                    chroma_index::IndexUuid(segment.id.0),
                )
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexInitError)?;
                Ok(Self {
                    index: LocalHnswIndex {
                        inner: Arc::new(tokio::sync::RwLock::new(Inner {
                            index,
                            id_map: Default::default(),
                            index_init: true,
                            allow_reset: false,
                            num_elements_since_last_persist: 0,
                            last_seen_seq_id: 0,
                            sync_threshold: hnsw_configuration.sync_threshold,
                            persist_path: None,
                            sqlite: sql_db,
                        })),
                    },
                })
            }
        }
    }

    // Returns the updated log seq id.
    #[allow(dead_code)]
    pub async fn apply_log_chunk(
        &mut self,
        log_chunk: Chunk<LogRecord>,
    ) -> Result<u32, LocalHnswSegmentWriterError> {
        let mut guard = self.index.inner.write().await;
        let mut next_label = guard.id_map.total_elements_added + 1;
        if log_chunk.is_empty() {
            return Ok(next_label);
        }
        let mut max_seq_id = u64::MIN;
        // In order to insert into hnsw index in parallel, we need to collect all the embeddings
        let mut hnsw_batch: HashMap<u32, Vec<(u32, &OperationRecord)>> =
            HashMap::with_capacity(log_chunk.len());
        for (log, _) in log_chunk.iter() {
            if log.log_offset <= guard.last_seen_seq_id as i64 {
                continue;
            }

            guard.num_elements_since_last_persist += 1;
            max_seq_id = max_seq_id.max(log.log_offset as u64);
            match log.record.operation {
                Operation::Add => {
                    // only update if the id is not already present
                    if !guard.id_map.id_to_label.contains_key(&log.record.id) {
                        match &log.record.embedding {
                            Some(_embedding) => {
                                guard
                                    .id_map
                                    .id_to_label
                                    .insert(log.record.id.clone(), next_label);
                                guard
                                    .id_map
                                    .label_to_id
                                    .insert(next_label, log.record.id.clone());
                                let records_for_label = match hnsw_batch.get_mut(&next_label) {
                                    Some(records) => records,
                                    None => {
                                        hnsw_batch.insert(next_label, Vec::new());
                                        // SAFETY: We just inserted the key. We have exclusive access to the map.
                                        hnsw_batch.get_mut(&next_label).unwrap()
                                    }
                                };
                                records_for_label.push((next_label, &log.record));
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
                        if let Some(_embedding) = &log.record.embedding {
                            let records_for_label = match hnsw_batch.get_mut(&label) {
                                Some(records) => records,
                                None => {
                                    hnsw_batch.insert(label, Vec::new());
                                    // SAFETY: We just inserted the key. We have exclusive access to the map.
                                    hnsw_batch.get_mut(&label).unwrap()
                                }
                            };
                            records_for_label.push((label, &log.record));
                        }
                    }
                }
                Operation::Delete => {
                    if let Some(label) = guard.id_map.id_to_label.get(&log.record.id).cloned() {
                        guard.id_map.id_to_label.remove(&log.record.id);
                        guard.id_map.label_to_id.remove(&label);
                        let records_for_label = match hnsw_batch.get_mut(&label) {
                            Some(records) => records,
                            None => {
                                hnsw_batch.insert(label, Vec::new());
                                // SAFETY: We just inserted the key. We have exclusive access to the map.
                                hnsw_batch.get_mut(&label).unwrap()
                            }
                        };
                        records_for_label.push((label, &log.record));
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
                        Some(_embedding) => {
                            guard
                                .id_map
                                .id_to_label
                                .insert(log.record.id.clone(), label);
                            guard
                                .id_map
                                .label_to_id
                                .insert(label, log.record.id.clone());
                            let records_for_label = match hnsw_batch.get_mut(&label) {
                                Some(records) => records,
                                None => {
                                    hnsw_batch.insert(label, Vec::new());
                                    // SAFETY: We just inserted the key. We have exclusive access to the map.
                                    hnsw_batch.get_mut(&label).unwrap()
                                }
                            };
                            records_for_label.push((label, &log.record));
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

        // Add to hnsw index in parallel using rayon.
        // Resize the index if needed
        let index_len = guard.index.len_with_deleted();
        let index_capacity = guard.index.capacity();
        if index_len + hnsw_batch.len() >= index_capacity {
            let needed_capacity = (index_len + hnsw_batch.len()).next_power_of_two();
            guard
                .index
                .resize(needed_capacity)
                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexResizeError)?;
        }
        let index_for_pool = &guard.index;

        hnsw_batch
            .into_par_iter()
            .map(|(_, records)| {
                for (label, log_record) in records {
                    match log_record.operation {
                        Operation::Add | Operation::Upsert | Operation::Update => {
                            let embedding = log_record.embedding.as_ref().expect(
                                "Add, update or upsert should have an embedding at this point",
                            );
                            match index_for_pool.add(label as usize, embedding) {
                                Ok(_) => {}
                                Err(_e) => {
                                    return Err(LocalHnswSegmentWriterError::HnwsIndexAddError);
                                }
                            }
                        }
                        Operation::Delete => match index_for_pool.delete(label as usize) {
                            Ok(_) => {}
                            Err(_e) => {
                                return Err(LocalHnswSegmentWriterError::HnswIndexDeleteError);
                            }
                        },
                    }
                }
                Ok(())
            })
            .find_any(|result| result.is_err())
            .unwrap_or(Ok(()))?;

        guard.id_map.total_elements_added = next_label - 1;
        if guard.num_elements_since_last_persist >= guard.sync_threshold as u64 {
            guard = persist(guard).await?;
            let id = guard.index.id.to_string().into();
            let max_id = max_seq_id.into();
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

        guard.last_seen_seq_id = max_seq_id;

        Ok(next_label)
    }
}

async fn persist(
    guard: tokio::sync::RwLockWriteGuard<'_, Inner>,
) -> Result<tokio::sync::RwLockWriteGuard<'_, Inner>, LocalHnswSegmentWriterError> {
    if let Some(path) = guard.persist_path.as_ref() {
        // Persist hnsw index.
        guard
            .index
            .save()
            .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
        // Persist id map.
        let metadata_file_path = Path::new(path).join(METADATA_FILE);

        let mut file = std::fs::File::create(metadata_file_path)?;
        // Using serde_pickle results in lots of small writes
        let mut buffered_file = std::io::BufWriter::new(&mut file);
        serde_pickle::to_writer(&mut buffered_file, &guard.id_map, SerOptions::new())?;
        buffered_file.flush()?;
    }
    Ok(guard)
}

mod persistence;
#[cfg(test)]
mod regression;
pub use persistence::{inspect_persisted_hnsw_index, PersistedHnswIndex};

use std::{
    collections::{BinaryHeap, HashMap},
    io::Write,
    mem::size_of,
    path::Path,
    sync::Arc,
};

use chroma_cache::Weighted;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{HnswIndex, HnswIndexConfig, IndexConfig};
use chroma_sqlite::{db::SqliteDb, table::MaxSeqId};
use chroma_types::{
    operator::RecordMeasure, Chunk, Collection, HnswParametersFromSegmentError, LogRecord,
    Operation, Segment, SegmentUuid,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sea_query::{Expr, OnConflict, Query, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_pickle::{DeOptions, SerOptions};
use sqlx::Row;
use thiserror::Error;

pub const METADATA_FILE: &str = "index_metadata.pickle";
pub const HNSW_HEADER_FILE: &str = "header.bin";
pub const HNSW_INDEX_FILES: [&str; 4] = chroma_index::hnsw_provider::FILES;
const HNSW_PERSISTENCE_VERSION: i32 = 1;

#[allow(dead_code)]
#[derive(Clone)]
pub struct LocalHnswSegmentReader {
    pub index: LocalHnswIndex,
}

#[derive(Error, Debug)]
pub enum LocalHnswSegmentReaderError {
    #[error("Segment has been deleted")]
    Deleted,
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
    #[error("Persisted HNSW dimensionality {actual} does not match collection dimensionality {expected}")]
    DimensionalityMismatch { expected: usize, actual: usize },
    #[error("Error reading from sqlite: {0}")]
    SqliteError(#[from] sqlx::error::Error),
    #[error("Error building max sequence id migration query")]
    QueryBuilderError(#[from] sea_query::error::Error),
}

impl ChromaError for LocalHnswSegmentReaderError {
    fn code(&self) -> ErrorCodes {
        match self {
            LocalHnswSegmentReaderError::Deleted => ErrorCodes::NotFound,
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
            LocalHnswSegmentReaderError::DimensionalityMismatch { .. } => ErrorCodes::DataLoss,
            LocalHnswSegmentReaderError::SqliteError(_) => ErrorCodes::Internal,
            LocalHnswSegmentReaderError::QueryBuilderError(_) => ErrorCodes::Internal,
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

pub use chroma_index::parse_persisted_hnsw_dim;

async fn persisted_hnsw_dim(index_folder: &Path) -> Result<usize, std::io::Error> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(index_folder.join(HNSW_HEADER_FILE)).await?;
    let mut header = [0; size_of::<i32>() + 6 * size_of::<usize>()];
    file.read_exact(&mut header).await?;
    parse_persisted_hnsw_dim(&header).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid persisted HNSW header",
        )
    })
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
                if !index_folder.join(METADATA_FILE).is_file()
                    && get_current_seq_id(segment, &sql_db).await? > 0
                {
                    return Err(LocalHnswSegmentReaderError::HnswIndexLoadError);
                }
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
                    let mut id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if let Some(actual) = id_map.dimensionality {
                        if actual != dimensionality {
                            return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                                expected: dimensionality,
                                actual,
                            });
                        }
                    }
                    let actual = persisted_hnsw_dim(&index_folder)
                        .await
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
                    if actual != dimensionality {
                        return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                            expected: dimensionality,
                            actual,
                        });
                    }
                    id_map.dimensionality = Some(dimensionality);
                    persistence::validate_files(&index_folder, Some(&id_map))
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
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

                    // Migrate legacy max_seq_id if present.
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

                    let current_seq_id = get_current_seq_id(segment, &sql_db).await?;

                    // TODO(Sanket): Set allow reset appropriately.
                    return Ok(Self {
                        index: LocalHnswIndex {
                            inner: Arc::new(tokio::sync::RwLock::new(Inner {
                                index,
                                id_map,
                                index_init: true,
                                deleted: false,
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
                if HNSW_INDEX_FILES
                    .iter()
                    .any(|name| index_folder.join(name).exists())
                {
                    persistence::validate_files(&index_folder, None)
                        .map_err(|_| LocalHnswSegmentReaderError::HnswIndexLoadError)?;
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
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
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
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        if let Some(actual) = guard.id_map.dimensionality {
            let expected = guard.index.dimensionality() as usize;
            if actual != expected {
                return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                    expected,
                    actual,
                });
            }
        }
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
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
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
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
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
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
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
        if guard.deleted {
            return Err(LocalHnswSegmentReaderError::Deleted);
        }
        if let Some(actual) = guard.id_map.dimensionality {
            let expected = guard.index.dimensionality() as usize;
            if actual != expected {
                return Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                    expected,
                    actual,
                });
            }
        }
        if embedding.len() != guard.index.dimensionality() as usize {
            return Err(LocalHnswSegmentReaderError::QueryError);
        }
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

impl IdMap {
    fn new(dimensionality: usize) -> Self {
        Self {
            dimensionality: Some(dimensionality),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedHnswLabelMismatch {
    LabelMapsToDifferentId {
        id: String,
        label: u32,
        reverse_id: String,
    },
    MissingReverseLabel {
        id: String,
        label: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedHnswMetadata {
    pub dimensionality: Option<usize>,
    pub total_elements_added: u32,
    pub legacy_max_seq_id: Option<u64>,
    pub id_to_label_count: usize,
    pub label_to_id_count: usize,
    pub first_label_mismatch: Option<PersistedHnswLabelMismatch>,
}

impl From<IdMap> for PersistedHnswMetadata {
    fn from(id_map: IdMap) -> Self {
        let first_label_mismatch =
            id_map
                .id_to_label
                .iter()
                .find_map(|(id, label)| match id_map.label_to_id.get(label) {
                    Some(reverse_id) if reverse_id == id => None,
                    Some(reverse_id) => Some(PersistedHnswLabelMismatch::LabelMapsToDifferentId {
                        id: id.clone(),
                        label: *label,
                        reverse_id: reverse_id.clone(),
                    }),
                    None => Some(PersistedHnswLabelMismatch::MissingReverseLabel {
                        id: id.clone(),
                        label: *label,
                    }),
                });

        Self {
            dimensionality: id_map.dimensionality,
            total_elements_added: id_map.total_elements_added,
            legacy_max_seq_id: id_map.max_seq_id,
            id_to_label_count: id_map.id_to_label.len(),
            label_to_id_count: id_map.label_to_id.len(),
            first_label_mismatch,
        }
    }
}

#[derive(Error, Debug)]
pub enum PersistedHnswMetadataError {
    #[error("Error opening persisted HNSW metadata: {0}")]
    Open(#[from] std::io::Error),
    #[error("Error deserializing persisted HNSW metadata: {0}")]
    Deserialize(#[from] serde_pickle::Error),
}

pub fn inspect_persisted_hnsw_metadata(
    metadata_path: &Path,
) -> Result<PersistedHnswMetadata, PersistedHnswMetadataError> {
    let file = std::fs::File::open(metadata_path)?;
    let id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
    Ok(id_map.into())
}

#[allow(dead_code)]
pub struct Inner {
    index: HnswIndex,
    // Loaded from pickle file.
    id_map: IdMap,
    index_init: bool,
    deleted: bool,
    allow_reset: bool,
    num_elements_since_last_persist: u64,
    last_seen_seq_id: u64,
    sync_threshold: usize,
    persist_path: Option<String>,
    sqlite: SqliteDb,
}

#[derive(Clone)]
pub struct LocalHnswIndex {
    pub(crate) inner: Arc<tokio::sync::RwLock<Inner>>,
}

impl LocalHnswIndex {
    pub async fn close(&self) {
        self.inner.write().await.index.close_fd();
    }
    pub async fn start(&self) {
        let guard = self.inner.write().await;
        if !guard.deleted {
            guard.index.open_fd();
        }
    }

    pub(crate) async fn mark_deleted(&self) {
        let mut guard = self.inner.write().await;
        guard.deleted = true;
        guard.index.close_fd();
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
    #[error("Segment has been deleted")]
    Deleted,
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
    #[error(
        "Embedding dimensionality {actual} does not match collection dimensionality {expected}"
    )]
    DimensionalityMismatch { expected: usize, actual: usize },
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
            LocalHnswSegmentWriterError::Deleted => ErrorCodes::NotFound,
            LocalHnswSegmentWriterError::HnswIndexInitError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexPersistError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::EmbeddingNotFound => ErrorCodes::InvalidArgument,
            LocalHnswSegmentWriterError::DimensionalityMismatch { .. } => {
                ErrorCodes::InvalidArgument
            }
            LocalHnswSegmentWriterError::HnwsIndexAddError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexResizeError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::HnswIndexDeleteError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::PersistPathError => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::QueryBuilderError(_) => ErrorCodes::Internal,
            LocalHnswSegmentWriterError::MaxSeqIdUpdateError(_) => ErrorCodes::Internal,
        }
    }
}

fn validate_embedding_dim(
    embedding: &[f32],
    expected: usize,
) -> Result<(), LocalHnswSegmentWriterError> {
    if embedding.len() != expected {
        return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
            expected,
            actual: embedding.len(),
        });
    }
    Ok(())
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
                if !index_folder.join(METADATA_FILE).is_file()
                    && get_current_seq_id(segment, &sql_db).await? > 0
                {
                    return Err(LocalHnswSegmentWriterError::HnswIndexLoadError);
                }
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
                    let mut id_map: IdMap = serde_pickle::from_reader(file, DeOptions::new())?;
                    if let Some(actual) = id_map.dimensionality {
                        if actual != dimensionality {
                            return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                                expected: dimensionality,
                                actual,
                            });
                        }
                    }
                    let actual = persisted_hnsw_dim(&index_folder)
                        .await
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
                    if actual != dimensionality {
                        return Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                            expected: dimensionality,
                            actual,
                        });
                    }
                    id_map.dimensionality = Some(dimensionality);
                    persistence::validate_files(&index_folder, Some(&id_map))
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
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
                    let current_seq_id = get_current_seq_id(segment, &sql_db).await?;

                    // TODO(Sanket): Set allow reset appropriately.
                    return Ok(Self {
                        index: LocalHnswIndex {
                            inner: Arc::new(tokio::sync::RwLock::new(Inner {
                                index,
                                id_map,
                                index_init: true,
                                deleted: false,
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
                // Files without a pickle are only a new, unpersisted index if
                // the native header has no elements. Never overwrite partial saves.
                if HNSW_INDEX_FILES
                    .iter()
                    .any(|name| index_folder.join(name).exists())
                {
                    persistence::validate_files(&index_folder, None)
                        .map_err(|_| LocalHnswSegmentWriterError::HnswIndexLoadError)?;
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
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
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
                            id_map: IdMap::new(dimensionality),
                            index_init: true,
                            deleted: false,
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
        if guard.deleted {
            return Err(LocalHnswSegmentWriterError::Deleted);
        }
        let mut next_label = guard.id_map.total_elements_added + 1;
        if log_chunk.is_empty() {
            return Ok(next_label);
        }
        // Validate the entire batch before changing either the ID map or HNSW.
        // Track only IDs touched by this batch, including add/delete transitions.
        let expected_dim = guard.index.dimensionality() as usize;
        let mut present = HashMap::new();
        for (log, _) in log_chunk.iter() {
            if log.log_offset <= guard.last_seen_seq_id as i64 {
                continue;
            }
            let exists = present
                .entry(log.record.id.as_str())
                .or_insert_with(|| guard.id_map.id_to_label.contains_key(&log.record.id));
            match log.record.operation {
                Operation::Add if !*exists => {
                    let embedding = log
                        .record
                        .embedding
                        .as_ref()
                        .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?;
                    validate_embedding_dim(embedding, expected_dim)?;
                    *exists = true;
                }
                Operation::Upsert => {
                    let embedding = log
                        .record
                        .embedding
                        .as_ref()
                        .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?;
                    validate_embedding_dim(embedding, expected_dim)?;
                    *exists = true;
                }
                Operation::Update if *exists => {
                    if let Some(embedding) = &log.record.embedding {
                        validate_embedding_dim(embedding, expected_dim)?;
                    }
                }
                Operation::Delete => *exists = false,
                _ => {}
            }
        }
        let mut max_seq_id = guard.last_seen_seq_id;
        // In order to insert into hnsw index in parallel, we need to collect all the embeddings
        enum Mutation<'a> {
            Set(&'a [f32]),
            Delete,
        }
        let mut hnsw_batch: HashMap<u32, Vec<Mutation<'_>>> =
            HashMap::with_capacity(log_chunk.len());
        for (log, _) in log_chunk.iter() {
            if log.log_offset <= guard.last_seen_seq_id as i64 {
                continue;
            }
            guard.num_elements_since_last_persist += 1;
            max_seq_id = max_seq_id.max(log.log_offset as u64);
            let id = &log.record.id;
            let exists = guard.id_map.id_to_label.contains_key(id);
            let set_vector = match log.record.operation {
                Operation::Add => !exists,
                Operation::Upsert => true,
                Operation::Update => exists && log.record.embedding.is_some(),
                Operation::Delete | Operation::BackfillFn => false,
            };
            if set_vector || matches!(log.record.operation, Operation::Delete) {
                if let Some(old_label) = guard.id_map.id_to_label.remove(id) {
                    guard.id_map.label_to_id.remove(&old_label);
                    hnsw_batch
                        .entry(old_label)
                        .or_default()
                        .push(Mutation::Delete);
                }
            }
            if set_vector {
                let embedding = log
                    .record
                    .embedding
                    .as_ref()
                    .ok_or(LocalHnswSegmentWriterError::EmbeddingNotFound)?;
                // Native isolated-point updates are not marked dirty. Allocate a
                // fresh label so persistence includes every replacement vector.
                // Keep the old label tombstoned to preserve graph connectivity.
                let label = next_label;
                next_label += 1;
                guard.id_map.id_to_label.insert(id.clone(), label);
                guard.id_map.label_to_id.insert(label, id.clone());
                hnsw_batch
                    .entry(label)
                    .or_default()
                    .push(Mutation::Set(embedding));
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
            .map(
                |(label, mutations)| -> Result<(), LocalHnswSegmentWriterError> {
                    for mutation in mutations {
                        match mutation {
                            Mutation::Set(embedding) => index_for_pool
                                .add(label as usize, embedding)
                                .map_err(|_| LocalHnswSegmentWriterError::HnwsIndexAddError)?,
                            Mutation::Delete => index_for_pool
                                .delete(label as usize)
                                .map_err(|_| LocalHnswSegmentWriterError::HnswIndexDeleteError)?,
                        }
                    }
                    Ok(())
                },
            )
            .find_any(|result| result.is_err())
            .unwrap_or(Ok(()))?;

        guard.id_map.total_elements_added = next_label - 1;
        if guard.persist_path.is_some()
            && guard.num_elements_since_last_persist >= guard.sync_threshold as u64
        {
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
        // Eviction may close the files while callers retain this index. The
        // write guard serializes reopening and saving with eviction/deletion.
        guard.index.open_fd();
        // Persist hnsw index.
        guard
            .index
            .save()
            .map_err(|_| LocalHnswSegmentWriterError::HnswIndexPersistError)?;
        // Persist id map.
        let metadata_file_path = Path::new(path).join(METADATA_FILE);

        // Sync native files before publishing the matching ID map and SQLite
        // watermark. Replacing the pickle avoids truncating the last good copy.
        for filename in HNSW_INDEX_FILES {
            std::fs::OpenOptions::new()
                .write(true)
                .open(Path::new(path).join(filename))?
                .sync_all()?;
        }
        let mut file = tempfile::NamedTempFile::new_in(path)?;
        {
            let mut buffered_file = std::io::BufWriter::new(file.as_file_mut());
            serde_pickle::to_writer(&mut buffered_file, &guard.id_map, SerOptions::new())?;
            buffered_file.flush()?;
        }
        file.as_file().sync_all()?;
        file.persist(metadata_file_path).map_err(|err| err.error)?;
        std::fs::File::open(path)?.sync_all()?;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use chroma_sqlite::db::test_utils::get_new_sqlite_db;
    use chroma_types::{
        Chunk, Collection, KnnIndex, LogRecord, Operation, OperationRecord, Schema, Segment,
        SegmentScope, SegmentType, SegmentUuid,
    };
    use serde_pickle::DeOptions;

    #[tokio::test]
    async fn reader_migrates_legacy_max_seq_id() {
        let sqlite = get_new_sqlite_db().await;
        let persist_dir = tempfile::tempdir().expect("persist dir");
        let persist_path = persist_dir.path().to_str().expect("utf-8 path").to_string();

        let mut collection = Collection::test_collection(3);
        collection.schema = Some(Schema::new_default(KnnIndex::Hnsw));

        let vector_segment = Segment {
            id: SegmentUuid::new(),
            r#type: SegmentType::HnswLocalPersisted,
            scope: SegmentScope::VECTOR,
            collection: collection.collection_id,
            metadata: None,
            file_path: Default::default(),
        };

        let mut writer = LocalHnswSegmentWriter::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path.clone()),
            sqlite.clone(),
        )
        .await
        .expect("writer");

        writer
            .apply_log_chunk(Chunk::new(
                vec![LogRecord {
                    log_offset: 42,
                    record: OperationRecord {
                        id: "id-1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                }]
                .into(),
            ))
            .await
            .expect("apply log");

        {
            let mut guard = writer.index.inner.write().await;
            guard.id_map.max_seq_id = Some(42);
            drop(persist(guard).await.expect("persist"));
        }
        writer.index.close().await;
        drop(writer);

        let metadata_path = persist_dir
            .path()
            .join(vector_segment.id.to_string())
            .join(METADATA_FILE);
        let file = tokio::fs::File::open(metadata_path)
            .await
            .expect("metadata file")
            .into_std()
            .await;
        let id_map: IdMap =
            serde_pickle::from_reader(file, DeOptions::new()).expect("legacy id map");
        assert_eq!(id_map.max_seq_id, Some(42));
        assert_eq!(
            get_current_seq_id(&vector_segment, &sqlite).await.unwrap(),
            0
        );

        // Legacy pickles may omit dimensionality; the binary header must still
        // prevent loading vectors into a differently sized native index.
        let mut legacy_map = id_map;
        legacy_map.dimensionality = None;
        let metadata_path = persist_dir
            .path()
            .join(vector_segment.id.to_string())
            .join(METADATA_FILE);
        let mut file = std::fs::File::create(metadata_path).unwrap();
        serde_pickle::to_writer(&mut file, &legacy_map, SerOptions::new()).unwrap();
        drop(file);
        assert!(matches!(
            LocalHnswSegmentReader::from_segment(
                &collection,
                &vector_segment,
                2,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
            .await,
            Err(LocalHnswSegmentReaderError::DimensionalityMismatch {
                expected: 2,
                actual: 3
            })
        ));
        assert!(matches!(
            LocalHnswSegmentWriter::from_segment(
                &collection,
                &vector_segment,
                4,
                Some(persist_path.clone()),
                sqlite.clone(),
            )
            .await,
            Err(LocalHnswSegmentWriterError::DimensionalityMismatch {
                expected: 4,
                actual: 3
            })
        ));

        let reader = LocalHnswSegmentReader::from_segment(
            &collection,
            &vector_segment,
            3,
            Some(persist_path),
            sqlite,
        )
        .await
        .expect("reader");

        assert_eq!(
            reader.current_max_seq_id(&vector_segment.id).await.unwrap(),
            42
        );
        assert_eq!(reader.index.inner.read().await.last_seen_seq_id, 42);
        assert!(reader
            .query_embedding(&[], vec![1.0, 2.0], 1)
            .await
            .is_err());
        assert!(reader.query_embedding(&[], vec![1.0; 4], 1).await.is_err());
    }
    use super::*;
    use chroma_config::{registry::Registry, Configurable};
    use chroma_distance::DistanceFunction;
    use chroma_index::IndexUuid;
    use chroma_sqlite::config::SqliteDBConfig;

    fn add_record(id: &str, embedding: Vec<f32>) -> OperationRecord {
        OperationRecord {
            id: id.to_string(),
            embedding: Some(embedding),
            encoding: None,
            metadata: None,
            document: None,
            operation: Operation::Add,
        }
    }

    fn push_i32(buf: &mut Vec<u8>, value: i32) {
        buf.extend_from_slice(&value.to_ne_bytes());
    }

    fn push_usize(buf: &mut Vec<u8>, value: usize) {
        buf.extend_from_slice(&value.to_ne_bytes());
    }

    fn header_for_dim(dim: usize) -> Vec<u8> {
        let offset_data = 68;
        let label_offset = offset_data + dim * size_of::<f32>();
        let size_data_per_element = label_offset + size_of::<usize>();
        let mut header = Vec::new();
        push_i32(&mut header, HNSW_PERSISTENCE_VERSION);
        push_usize(&mut header, 0);
        push_usize(&mut header, 100);
        push_usize(&mut header, 1);
        push_usize(&mut header, size_data_per_element);
        push_usize(&mut header, label_offset);
        push_usize(&mut header, offset_data);
        header
    }

    #[test]
    fn persisted_hnsw_header_reports_dimensionality() {
        assert_eq!(parse_persisted_hnsw_dim(&header_for_dim(8)), Some(8));
        assert_eq!(parse_persisted_hnsw_dim(&header_for_dim(768)), Some(768));
    }

    #[test]
    fn persisted_hnsw_header_matches_hnswlib_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let index_config = IndexConfig::new(8, DistanceFunction::Euclidean);
        let hnsw_config =
            HnswIndexConfig::new_persistent(16, 100, 100, dir.path()).expect("hnsw config");
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .expect("hnsw init");
        index.add(0, &[0.0; 8]).expect("hnsw add");
        index.save().expect("hnsw save");
        index.close_fd();

        let header = std::fs::read(dir.path().join(HNSW_HEADER_FILE)).expect("header");
        assert_eq!(parse_persisted_hnsw_dim(&header), Some(8));
    }

    #[test]
    fn persisted_hnsw_header_rejects_invalid_layout() {
        let mut header = header_for_dim(8);
        header[0..size_of::<i32>()].copy_from_slice(&2i32.to_ne_bytes());
        assert_eq!(parse_persisted_hnsw_dim(&header), None);

        let mut header = header_for_dim(8);
        let label_offset_offset = size_of::<i32>() + 4 * size_of::<usize>();
        header[label_offset_offset..label_offset_offset + size_of::<usize>()]
            .copy_from_slice(&69usize.to_ne_bytes());
        assert_eq!(parse_persisted_hnsw_dim(&header), None);
    }

    #[test]
    fn persisted_hnsw_metadata_summarizes_legacy_watermark_and_mismatches() {
        let mut id_map = IdMap::new(3);
        id_map.max_seq_id = Some(42);
        id_map.total_elements_added = 1;
        id_map.id_to_label.insert("a".to_string(), 7);
        id_map.label_to_id.insert(7, "b".to_string());

        let metadata = PersistedHnswMetadata::from(id_map);

        assert_eq!(metadata.dimensionality, Some(3));
        assert_eq!(metadata.legacy_max_seq_id, Some(42));
        assert_eq!(metadata.id_to_label_count, 1);
        assert_eq!(metadata.label_to_id_count, 1);
        assert_eq!(
            metadata.first_label_mismatch,
            Some(PersistedHnswLabelMismatch::LabelMapsToDifferentId {
                id: "a".to_string(),
                label: 7,
                reverse_id: "b".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn apply_log_chunk_rejects_bad_dim_without_id_map_side_effects() {
        let sqlite = SqliteDb::try_from_config(&SqliteDBConfig::default(), &Registry::new())
            .await
            .expect("sqlite");
        let index_config = IndexConfig::new(2, DistanceFunction::Euclidean);
        let hnsw_config = HnswIndexConfig::new_ephemeral(16, 100, 100);
        let index = HnswIndex::init(
            &index_config,
            Some(&hnsw_config),
            IndexUuid(uuid::Uuid::new_v4()),
        )
        .expect("hnsw init");
        let mut writer = LocalHnswSegmentWriter {
            index: LocalHnswIndex {
                inner: Arc::new(tokio::sync::RwLock::new(Inner {
                    index,
                    id_map: IdMap::new(2),
                    index_init: true,
                    deleted: false,
                    allow_reset: false,
                    num_elements_since_last_persist: 0,
                    last_seen_seq_id: 0,
                    sync_threshold: 1000,
                    persist_path: None,
                    sqlite,
                })),
            },
        };
        let chunk = Chunk::new(
            vec![
                LogRecord {
                    log_offset: 1,
                    record: add_record("valid", vec![1.0, 2.0]),
                },
                LogRecord {
                    log_offset: 2,
                    record: add_record("invalid", vec![1.0, 2.0, 3.0]),
                },
            ]
            .into(),
        );

        let err = writer
            .apply_log_chunk(chunk)
            .await
            .expect_err("bad dimension should fail");
        assert!(matches!(
            err,
            LocalHnswSegmentWriterError::DimensionalityMismatch {
                expected: 2,
                actual: 3,
            }
        ));
        let guard = writer.index.inner.read().await;
        assert!(guard.id_map.id_to_label.is_empty());
        assert!(guard.id_map.label_to_id.is_empty());
        assert_eq!(guard.id_map.total_elements_added, 0);
        assert_eq!(guard.num_elements_since_last_persist, 0);
        assert_eq!(guard.last_seen_seq_id, 0);
    }
}

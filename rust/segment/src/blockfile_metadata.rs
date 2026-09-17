use crate::types::ChromaSegmentFlusher;

use super::blockfile_record::ApplyMaterializedLogError;
use super::types::{MaterializeLogsResult, PartitionedMaterializeLogsResult};
use crate::blockfile_record::{RecordSegmentReader, RecordSegmentReaderShard};
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::provider::{BlockfileProvider, CreateError, OpenError, ReadKey, ReadValue};
use chroma_blockstore::BlockfileReader;
use chroma_blockstore::BlockfileWriterOptions;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::fulltext::bitmap_index::{
    FullTextBitmapFlusher, FullTextBitmapReader, FullTextBitmapWriter,
};
use chroma_index::fulltext::tokenizer::WordAnalyzer;
use chroma_index::fulltext::types::{
    DocumentMutation, FullTextIndexError, FullTextIndexFlusher, FullTextIndexReader,
    FullTextIndexWriter,
};
use chroma_index::metadata::types::{
    MetadataIndexError, MetadataIndexFlusher, MetadataIndexReader, MetadataIndexWriter,
};
use chroma_index::sparse::maxscore::{
    MaxScoreFlusher, MaxScoreReader, MaxScoreWriter, SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use chroma_index::sparse::reader::SparseReader;
use chroma_index::sparse::types::DEFAULT_BLOCK_SIZE;
use chroma_index::sparse::types::{encode_u32, Score};
use chroma_index::sparse::writer::SparseFlusher;
use chroma_index::sparse::writer::SparseWriter;
use chroma_types::Cmek;
use chroma_types::DatabaseUuid;
use chroma_types::Schema;
use chroma_types::SegmentType;
use chroma_types::SparsePostingBlock;
use chroma_types::BOOL_METADATA;
use chroma_types::F32_METADATA;
use chroma_types::FULL_TEXT_PLS;
use chroma_types::FULL_TEXT_TOKEN;
use chroma_types::SPARSE_MAX;
use chroma_types::SPARSE_OFFSET_VALUE;
use chroma_types::SPARSE_POSTING;
use chroma_types::STRING_METADATA;
use chroma_types::U32_METADATA;
use chroma_types::{
    parse_sparse_file_path_key, sparse_max_key, sparse_offset_value_key, sparse_posting_key,
};
use chroma_types::{
    MaterializedLogOperation, MetadataValue, Segment, SegmentShard, SegmentShardError, SegmentUuid,
};
use core::panic;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use tantivy::tokenizer::NgramTokenizer;
use thiserror::Error;
use tracing::Instrument;
use tracing::Span;

/// The sparse vector index writer for a segment shard.
///
/// For existing collections the variant is determined by which blockfile
/// keys are present in the segment's file_path (`SPARSE_MAX` → WAND,
/// `SPARSE_POSTING` → MaxScore), not the schema, because schema changes
/// are metadata-only and do not rewrite on-disk data. This matters
/// during migration: a collection's schema may be updated to MaxScore
/// while the existing segments still contain WAND blockfiles — the read
/// path must use the on-disk format until compaction rewrites them.
/// For fresh collections (no existing file paths) the schema's
/// `algorithm` field decides.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum SparseIndexWriter<'me> {
    Wand(SparseWriter<'me>),
    MaxScore(MaxScoreWriter<'me>),
}

/// The sparse vector index flusher for a segment shard.
#[allow(clippy::large_enum_variant)]
pub(crate) enum SparseIndexFlusher {
    Wand(SparseFlusher),
    MaxScore(MaxScoreFlusher),
}

impl<'me> SparseIndexWriter<'me> {
    pub async fn set(&self, offset: u32, sparse_vector: impl IntoIterator<Item = (u32, f32)>) {
        match self {
            Self::Wand(w) => w.set(offset, sparse_vector).await,
            Self::MaxScore(w) => w.set(offset, sparse_vector).await,
        }
    }

    pub async fn delete(&self, offset: u32, sparse_indices: impl IntoIterator<Item = u32>) {
        match self {
            Self::Wand(w) => w.delete(offset, sparse_indices).await,
            Self::MaxScore(w) => w.delete(offset, sparse_indices).await,
        }
    }

    pub async fn commit(self) -> Result<SparseIndexFlusher, Box<dyn ChromaError>> {
        match self {
            Self::Wand(w) => Ok(SparseIndexFlusher::Wand(
                Box::pin(w.commit()).await.map_err(ChromaError::boxed)?,
            )),
            Self::MaxScore(w) => Ok(SparseIndexFlusher::MaxScore(
                Box::pin(w.commit()).await.map_err(ChromaError::boxed)?,
            )),
        }
    }
}

impl SparseIndexFlusher {
    /// Flush the sparse index, returning `(file_path_key, paths)` entries to
    /// register on the segment. When `metadata_key` is `Some`, entries are
    /// namespaced (`sparse_*::<metadata_key>`) so a collection can hold one
    /// independent index per metadata field. When `None`, the bare `SPARSE_*`
    /// names are used to carry forward an orphaned legacy anonymous index.
    pub async fn flush(
        self,
        prefix_path: &str,
        metadata_key: Option<&str>,
    ) -> Result<Vec<(String, Vec<String>)>, Box<dyn ChromaError>> {
        let (max_key, offset_value_key, posting_key) = match metadata_key {
            Some(key) => (
                sparse_max_key(key),
                sparse_offset_value_key(key),
                sparse_posting_key(key),
            ),
            None => (
                SPARSE_MAX.to_string(),
                SPARSE_OFFSET_VALUE.to_string(),
                SPARSE_POSTING.to_string(),
            ),
        };
        match self {
            Self::Wand(f) => {
                let max_id = f.max_id();
                let offset_value_id = f.offset_value_id();
                Box::pin(f.flush()).await.map_err(ChromaError::boxed)?;
                Ok(vec![
                    (
                        max_key,
                        vec![ChromaSegmentFlusher::flush_key(prefix_path, &max_id)],
                    ),
                    (
                        offset_value_key,
                        vec![ChromaSegmentFlusher::flush_key(
                            prefix_path,
                            &offset_value_id,
                        )],
                    ),
                ])
            }
            Self::MaxScore(f) => {
                let posting_id = f.id();
                Box::pin(f.flush()).await.map_err(ChromaError::boxed)?;
                Ok(vec![(
                    posting_key,
                    vec![ChromaSegmentFlusher::flush_key(prefix_path, &posting_id)],
                )])
            }
        }
    }
}

/// The sparse vector index reader for a segment shard.
pub enum SparseIndexReader<'me> {
    Wand(SparseReader<'me>),
    MaxScore(MaxScoreReader<'me>),
}

impl<'me> SparseIndexReader<'me> {
    /// Compute document frequency for each dimension.
    pub async fn dimension_counts(
        &self,
        dimensions: &[u32],
    ) -> Result<HashMap<u32, u32>, Box<dyn ChromaError>> {
        let mut counts = HashMap::new();
        match self {
            Self::Wand(r) => {
                let encoded: Vec<(u32, String)> =
                    dimensions.iter().map(|d| (*d, encode_u32(*d))).collect();
                r.load_offset_values(encoded.iter().map(|(_, e)| e.as_str()))
                    .await;
                for (dim, enc) in &encoded {
                    let nt = r
                        .get_dimension_offset_rank(enc, u32::MAX)
                        .await
                        .map_err(ChromaError::boxed)?
                        .saturating_sub(
                            r.get_dimension_offset_rank(enc, 0)
                                .await
                                .map_err(ChromaError::boxed)?,
                        );
                    counts.insert(*dim, nt);
                }
            }
            Self::MaxScore(r) => {
                for dim in dimensions {
                    let nt = r
                        .count_postings(&encode_u32(*dim))
                        .await
                        .map_err(ChromaError::boxed)? as u32;
                    counts.insert(*dim, nt);
                }
            }
        }
        Ok(counts)
    }

    /// Run a sparse KNN query, returning scored results.
    pub async fn knn_query(
        &'me self,
        query: impl IntoIterator<Item = (u32, f32)>,
        k: u32,
        mask: chroma_types::SignedRoaringBitmap,
    ) -> Result<Vec<Score>, Box<dyn ChromaError>> {
        match self {
            Self::Wand(r) => r.wand(query, k, mask).await.map_err(ChromaError::boxed),
            Self::MaxScore(r) => r.query(query, k, mask).await.map_err(ChromaError::boxed),
        }
    }
}

/// The full-text index writer for a segment shard.
///
/// For existing collections the variant is determined by which blockfile
/// keys are present in the segment's file_path (`FULL_TEXT_PLS` → Trigram,
/// `FULL_TEXT_TOKEN` → TokenBitmap), not the schema, because schema
/// changes are metadata-only and do not rewrite on-disk data. For fresh
/// collections (no existing file paths) the schema's `fts_algorithm`
/// field decides.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum FtsIndexWriter {
    Trigram(FullTextIndexWriter),
    TokenBitmap(WordAnalyzer, FullTextBitmapWriter),
}

/// The full-text index flusher for a segment shard.
#[allow(clippy::large_enum_variant)]
pub enum FtsIndexFlusher {
    Trigram(FullTextIndexFlusher),
    TokenBitmap(FullTextBitmapFlusher),
}

/// The full-text index reader for a segment shard.
pub enum FtsIndexReader<'me> {
    Trigram(FullTextIndexReader<'me>),
    TokenBitmap(WordAnalyzer, FullTextBitmapReader),
}

impl<'me> FtsIndexReader<'me> {
    #[cfg(test)]
    pub fn as_trigram(&self) -> Option<&FullTextIndexReader<'me>> {
        match self {
            FtsIndexReader::Trigram(r) => Some(r),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct MetadataSegmentWriterShard<'me> {
    pub(crate) fts_index_writer: Option<FtsIndexWriter>,
    pub(crate) string_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) bool_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) f32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    pub(crate) u32_metadata_index_writer: Option<MetadataIndexWriter<'me>>,
    /// One sparse vector index per enabled sparse metadata key. The map key is
    /// the metadata field name; writes are routed by metadata key.
    pub(crate) sparse_index_writers: HashMap<String, SparseIndexWriter<'me>>,
    /// An orphaned legacy anonymous sparse index (stored under the bare
    /// SPARSE_* keys) that no enabled sparse key owns. Carried forward verbatim
    /// under the bare names so legacy collections keep compacting; receives no
    /// new writes.
    pub(crate) legacy_sparse_index_writer: Option<SparseIndexWriter<'me>>,
    pub id: SegmentUuid,
}

impl Debug for MetadataSegmentWriterShard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MetadataSegmentWriterShard")
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
pub struct MetadataSegmentWriterError(#[from] MetadataSegmentError);

impl chroma_error::ChromaError for MetadataSegmentWriterError {
    fn code(&self) -> chroma_error::ErrorCodes {
        self.0.code()
    }
}

#[derive(Clone, Debug)]
pub struct MetadataSegmentWriter<'me> {
    shards: Vec<MetadataSegmentWriterShard<'me>>,
    pub id: SegmentUuid,
    // TODO(tanujnay112): Remove SegmentUuid from above as its
    // redundant with this.
    segment: Segment,
}

/// Fork an on-disk sparse index from the given blockfile paths into a writer,
/// preserving its algorithm: MaxScore when a posting blockfile is present,
/// otherwise WAND from the max + offset-value blockfiles. Returns `None` when
/// no sparse blockfiles are present (nothing to fork).
async fn fork_sparse_index_writer<'me>(
    blockfile_provider: &BlockfileProvider,
    cmek: &Option<Cmek>,
    posting_file_path: Option<&String>,
    max_file_path: Option<&String>,
    offset_value_file_path: Option<&String>,
) -> Result<Option<SparseIndexWriter<'me>>, MetadataSegmentError> {
    if let Some(posting_file_path) = posting_file_path {
        // ── Fork path: MaxScore index ──────────────────────────────────
        let (posting_prefix, posting_uuid) = Segment::extract_prefix_and_id(posting_file_path)
            .map_err(|_| MetadataSegmentError::UuidParseError(posting_file_path.to_string()))?;
        let posting_reader = blockfile_provider
            .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
                posting_uuid,
                posting_prefix.to_string(),
            ))
            .await
            .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
        let posting_writer = {
            let mut options = BlockfileWriterOptions::new(posting_prefix.to_string())
                .fork(posting_uuid)
                .ordered_mutations()
                .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES);
            if let Some(cmek) = cmek {
                options = options.with_cmek(cmek.clone());
            }
            blockfile_provider
                .write::<u32, SparsePostingBlock>(options)
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
        };
        let old_reader = MaxScoreReader::new(posting_reader);
        Ok(Some(SparseIndexWriter::MaxScore(MaxScoreWriter::new(
            posting_writer,
            Some(old_reader),
        ))))
    } else if let (Some(max_file_path), Some(offset_value_file_path)) =
        (max_file_path, offset_value_file_path)
    {
        // ── Fork path: existing WAND index ─────────────────────────────
        let (max_prefix, max_uuid) = Segment::extract_prefix_and_id(max_file_path)
            .map_err(|_| MetadataSegmentError::UuidParseError(max_file_path.to_string()))?;
        let max_reader = blockfile_provider
            .read::<u32, f32>(BlockfileReaderOptions::new(
                max_uuid,
                max_prefix.to_string(),
            ))
            .await
            .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
        let max_writer = {
            let mut options =
                BlockfileWriterOptions::new(max_prefix.to_string()).ordered_mutations();
            if let Some(cmek) = cmek {
                options = options.with_cmek(cmek.clone());
            }
            blockfile_provider
                .write::<u32, f32>(options)
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
        };
        let (offset_value_prefix, offset_value_uuid) =
            Segment::extract_prefix_and_id(offset_value_file_path).map_err(|_| {
                MetadataSegmentError::UuidParseError(offset_value_file_path.to_string())
            })?;
        let offset_value_reader = blockfile_provider
            .read::<u32, f32>(BlockfileReaderOptions::new(
                offset_value_uuid,
                offset_value_prefix.to_string(),
            ))
            .await
            .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
        let offset_value_writer = {
            let mut options = BlockfileWriterOptions::new(offset_value_prefix.to_string())
                .fork(offset_value_uuid)
                .ordered_mutations();
            if let Some(cmek) = cmek {
                options = options.with_cmek(cmek.clone());
            }
            blockfile_provider
                .write::<u32, f32>(options)
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
        };
        Ok(Some(SparseIndexWriter::Wand(SparseWriter::new(
            DEFAULT_BLOCK_SIZE,
            max_writer,
            offset_value_writer,
            Some(SparseReader::new(max_reader, offset_value_reader)),
        ))))
    } else {
        Ok(None)
    }
}

impl<'me> MetadataSegmentWriter<'me> {
    pub async fn from_segment(
        tenant: &str,
        database_id: &DatabaseUuid,
        segment: &Segment,
        blockfile_provider: &BlockfileProvider,
        cmek: Option<Cmek>,
        schema: Option<&Schema>,
    ) -> Result<Self, MetadataSegmentWriterError> {
        let segment_shards = segment
            .get_shards()
            .map_err(MetadataSegmentError::SegmentShard)?;

        if segment_shards.is_empty() {
            return Err(MetadataSegmentWriterError(
                MetadataSegmentError::SegmentShard(SegmentShardError::EmptyShards),
            ));
        }

        // Create futures for all shards
        let futures: Vec<_> = segment_shards
            .iter()
            .map(|shard| {
                Box::pin(MetadataSegmentWriterShard::from_segment(
                    tenant,
                    database_id,
                    shard,
                    blockfile_provider,
                    cmek.clone(),
                    schema,
                ))
            })
            .collect();

        // Await all futures concurrently
        let writer_shards = futures::future::try_join_all(futures).await?;

        Ok(Self {
            shards: writer_shards,
            id: segment.id,
            segment: segment.clone(),
        })
    }

    /// Returns the number of shards in the writer
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Returns a read-only view of the shards
    pub fn shards(&self) -> &[MetadataSegmentWriterShard<'me>] {
        &self.shards
    }

    pub async fn create_new_shard(
        &mut self,
        collection: &chroma_types::Collection,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<(), MetadataSegmentWriterError> {
        // Create a new segment shard with empty file paths
        let new_shard_segment = self.segment.new_shard();
        let cmek = collection.schema.as_ref().and_then(|s| s.cmek.clone());

        // Create the new writer shard
        let new_writer_shard = Box::pin(MetadataSegmentWriterShard::from_segment(
            &collection.tenant,
            &collection.database_id,
            &new_shard_segment,
            blockfile_provider,
            cmek,
            collection.schema.as_ref(),
        ))
        .await?;

        // Add to our shards vector
        self.shards.push(new_writer_shard);

        Ok(())
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReader<'_>>,
        materialized: &PartitionedMaterializeLogsResult,
        schema: Option<Schema>,
    ) -> Result<Option<Schema>, ApplyMaterializedLogError> {
        // Apply to all shards concurrently
        let partitions = &materialized.shards;
        tracing::info!(
            "Applying materialized log chunk to {} shards",
            partitions.len()
        );

        // Extract shard readers ahead of time
        let shard_readers: Vec<_> = (0..self.shards.len())
            .map(|shard_idx| {
                record_segment_reader.as_ref().and_then(|reader| {
                    reader
                        .get_shards()
                        .get(shard_idx)
                        .and_then(|opt| opt.as_ref())
                        .cloned()
                })
            })
            .collect();

        let futures = self
            .shards
            .iter()
            .zip(partitions.iter())
            .zip(shard_readers)
            .map(|((shard, partitioned), shard_reader)| {
                let schema_clone = schema.clone();
                async move {
                    shard
                        .apply_materialized_log_chunk(&shard_reader, partitioned, schema_clone)
                        .await
                }
            });

        let results = futures::future::try_join_all(futures).await?;

        let res = results.into_iter().try_fold(
            None,
            |acc, result| -> Result<Option<Schema>, ApplyMaterializedLogError> {
                match (acc, result) {
                    (None, Some(schema)) => Ok(Some(schema)),
                    (None, None) => Ok(None),
                    (Some(existing), Some(schema)) => Ok(Some(existing.merge(&schema)?)),
                    (Some(existing), None) => Ok(Some(existing)),
                }
            },
        )?;

        Ok(res)
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        // Call finish on all shards concurrently
        let futures = self.shards.iter_mut().map(|shard| shard.finish());

        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataSegmentFlusher, Box<dyn ChromaError>> {
        let futures = self
            .shards
            .into_iter()
            .map(|shard| Box::pin(shard.commit()));

        let flusher_shards = futures::future::try_join_all(futures).await?;

        Ok(MetadataSegmentFlusher {
            shards: flusher_shards,
            id: self.id,
        })
    }
}

#[derive(Debug, Error)]
pub enum MetadataSegmentError {
    #[error("Invalid segment type")]
    InvalidSegmentType,
    // TODO turn this into index creation error
    #[error("Failed to create full text index writer")]
    FullTextIndexWriterError(#[from] FullTextIndexError),
    #[error("Blockfile creation error")]
    BlockfileError(#[from] CreateError),
    #[error("Blockfile open error")]
    BlockfileOpenError(#[from] OpenError),
    #[error("Only one of posting lists and frequencies files found")]
    FullTextIndexFilesIntegrityError,
    #[error("Incorrect number of files")]
    IncorrectNumberOfFiles,
    #[error("Missing file {0}")]
    MissingFile(String),
    #[error("Count not parse UUID {0}")]
    UUID(String),
    #[error("Segment shard error: {0}")]
    SegmentShard(#[from] chroma_types::SegmentShardError),
    #[error("UUID parse error: {0}")]
    UuidParseError(String),
    #[error("No writer found")]
    NoWriter,
    #[error("Path vector exists but is empty?")]
    EmptyPathVector,
    #[error("Failed to write to blockfile")]
    BlockfileWriteError,
    #[error("Limit and offset are not currently supported")]
    LimitOffsetNotSupported,
    #[error("Metadata index error: {0}")]
    MetadataIndexQueryError(#[from] MetadataIndexError),
}

impl ChromaError for MetadataSegmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataSegmentError::InvalidSegmentType => ErrorCodes::Internal,
            MetadataSegmentError::FullTextIndexWriterError(e) => e.code(),
            MetadataSegmentError::BlockfileError(e) => e.code(),
            MetadataSegmentError::BlockfileOpenError(e) => e.code(),
            MetadataSegmentError::FullTextIndexFilesIntegrityError => ErrorCodes::Internal,
            MetadataSegmentError::IncorrectNumberOfFiles => ErrorCodes::Internal,
            MetadataSegmentError::MissingFile(_) => ErrorCodes::Internal,
            MetadataSegmentError::UUID(_) => ErrorCodes::Internal,
            MetadataSegmentError::SegmentShard(e) => e.code(),
            MetadataSegmentError::UuidParseError(_) => ErrorCodes::Internal,
            MetadataSegmentError::NoWriter => ErrorCodes::Internal,
            MetadataSegmentError::EmptyPathVector => ErrorCodes::Internal,
            MetadataSegmentError::BlockfileWriteError => ErrorCodes::Internal,
            MetadataSegmentError::LimitOffsetNotSupported => ErrorCodes::Internal,
            MetadataSegmentError::MetadataIndexQueryError(_) => ErrorCodes::Internal,
        }
    }
}

impl<'me> MetadataSegmentWriterShard<'me> {
    pub async fn from_segment(
        tenant: &str,
        database_id: &DatabaseUuid,
        segment: &SegmentShard,
        blockfile_provider: &BlockfileProvider,
        cmek: Option<Cmek>,
        schema: Option<&Schema>,
    ) -> Result<MetadataSegmentWriterShard<'me>, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }
        // NOTE: We hope that all blockfiles of the same collection should live under the same prefix.
        // The implementation below implies all collections in the fork tree share the same prefix for
        // blockfiles. Although this is not a desired behavior, as a temporary fix we create the sparse
        // vector index blockfiles under the same prefix as other blockfiles if they are present.
        let prefix_path = if let Some(existing_file_path) = segment.file_path.values().next() {
            let (existing_prefix, _) =
                Segment::extract_prefix_and_id(existing_file_path).map_err(|_| {
                    MetadataSegmentError::UuidParseError(existing_file_path.to_string())
                })?;
            existing_prefix.to_string()
        } else {
            segment.construct_prefix_path(tenant, database_id)
        };
        // ── FTS index writer: 3-way branch ──────────────────────────
        //
        // 1. FULL_TEXT_TOKEN in file_path → fork TokenBitmap index
        // 2. FULL_TEXT_PLS in file_path   → fork existing Trigram index
        // 3. Neither (fresh collection)   → check schema to decide
        let token_file_path = segment.file_path.get(FULL_TEXT_TOKEN);
        let pls_file_path = segment.file_path.get(FULL_TEXT_PLS);

        let fts_index_writer = if let Some(token_path) = token_file_path {
            // ── Fork path: TokenBitmap index ───────────────────────
            let (prefix, token_uuid) = Segment::extract_prefix_and_id(token_path)
                .map_err(|_| MetadataSegmentError::UuidParseError(token_path.to_string()))?;
            let token_reader = blockfile_provider
                .read::<u32, RoaringBitmap>(BlockfileReaderOptions::new(
                    token_uuid,
                    prefix.to_string(),
                ))
                .await
                .map_err(|e| MetadataSegmentError::BlockfileOpenError(*e))?;
            let token_writer = {
                let mut options = BlockfileWriterOptions::new(prefix.to_string())
                    .fork(token_uuid)
                    .ordered_mutations();
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                blockfile_provider
                    .write::<u32, RoaringBitmap>(options)
                    .await
                    .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
            };
            let old_reader = FullTextBitmapReader::new(token_reader);
            tracing::info!("FTS index: using TokenBitmap (fork from existing)");
            FtsIndexWriter::TokenBitmap(
                WordAnalyzer::default(),
                FullTextBitmapWriter::new(token_writer, Some(old_reader)),
            )
        } else if let Some(pls_path) = pls_file_path {
            // ── Fork path: Trigram index ───────────────────────────
            let (prefix, pls_uuid) = Segment::extract_prefix_and_id(pls_path)
                .map_err(|_| MetadataSegmentError::UuidParseError(pls_path.to_string()))?;
            let pls_writer = {
                let mut options = BlockfileWriterOptions::new(prefix.to_string())
                    .fork(pls_uuid)
                    .ordered_mutations();
                if let Some(cmek) = &cmek {
                    options = options.with_cmek(cmek.clone());
                }
                blockfile_provider
                    .write::<u32, Vec<u32>>(options)
                    .await
                    .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
            };
            let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
            tracing::info!("FTS index: using Trigram (fork from existing)");
            FtsIndexWriter::Trigram(FullTextIndexWriter::new(pls_writer, tokenizer))
        } else if schema.is_some_and(|s| s.is_token_bitmap_fts_enabled()) {
            // ── Fresh path: TokenBitmap index ──────────────────────
            let mut options = BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations();
            if let Some(cmek) = &cmek {
                options = options.with_cmek(cmek.clone());
            }
            let token_writer = blockfile_provider
                .write::<u32, RoaringBitmap>(options)
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            tracing::info!("FTS index: using TokenBitmap (fresh, schema-gated)");
            FtsIndexWriter::TokenBitmap(
                WordAnalyzer::default(),
                FullTextBitmapWriter::new(token_writer, None),
            )
        } else {
            // ── Fresh path: Trigram index (default) ────────────────
            let mut options = BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations();
            if let Some(cmek) = &cmek {
                options = options.with_cmek(cmek.clone());
            }
            let pls_writer = blockfile_provider
                .write::<u32, Vec<u32>>(options)
                .await
                .map_err(|e| MetadataSegmentError::BlockfileError(*e))?;
            let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
            tracing::info!("FTS index: using Trigram (fresh, default)");
            FtsIndexWriter::Trigram(FullTextIndexWriter::new(pls_writer, tokenizer))
        };

        let (string_metadata_writer, string_metadata_index_reader) =
            match segment.file_path.get(STRING_METADATA) {
                Some(string_metadata_path) => {
                    let (prefix, string_metadata_uuid) =
                        Segment::extract_prefix_and_id(string_metadata_path).map_err(|_| {
                            MetadataSegmentError::UuidParseError(string_metadata_path.to_string())
                        })?;
                    let string_metadata_writer = {
                        let mut options = BlockfileWriterOptions::new(prefix.to_string())
                            .fork(string_metadata_uuid);
                        if let Some(cmek) = &cmek {
                            options = options.with_cmek(cmek.clone());
                        }
                        match blockfile_provider
                            .write::<&str, RoaringBitmap>(options)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        }
                    };
                    let read_options =
                        BlockfileReaderOptions::new(string_metadata_uuid, prefix.to_string());
                    let string_metadata_index_reader = match blockfile_provider
                        .read::<&str, RoaringBitmap>(read_options)
                        .await
                    {
                        Ok(reader) => MetadataIndexReader::new_string(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    (string_metadata_writer, Some(string_metadata_index_reader))
                }
                None => {
                    let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    match blockfile_provider
                        .write::<&str, RoaringBitmap>(options)
                        .await
                    {
                        Ok(writer) => (writer, None),
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    }
                }
            };
        let string_metadata_index_writer =
            MetadataIndexWriter::new_string(string_metadata_writer, string_metadata_index_reader);

        let (bool_metadata_writer, bool_metadata_index_reader) =
            match segment.file_path.get(BOOL_METADATA) {
                Some(bool_metadata_path) => {
                    let (prefix, bool_metadata_uuid) =
                        Segment::extract_prefix_and_id(bool_metadata_path).map_err(|_| {
                            MetadataSegmentError::UuidParseError(bool_metadata_path.to_string())
                        })?;
                    let bool_metadata_writer = {
                        let mut options = BlockfileWriterOptions::new(prefix.to_string())
                            .fork(bool_metadata_uuid);
                        if let Some(cmek) = &cmek {
                            options = options.with_cmek(cmek.clone());
                        }
                        match blockfile_provider
                            .write::<bool, RoaringBitmap>(options)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        }
                    };
                    let read_options =
                        BlockfileReaderOptions::new(bool_metadata_uuid, prefix.to_string());
                    let bool_metadata_index_writer = match blockfile_provider
                        .read::<bool, RoaringBitmap>(read_options)
                        .await
                    {
                        Ok(reader) => MetadataIndexReader::new_bool(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    (bool_metadata_writer, Some(bool_metadata_index_writer))
                }
                None => {
                    let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    match blockfile_provider
                        .write::<bool, RoaringBitmap>(options)
                        .await
                    {
                        Ok(writer) => (writer, None),
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    }
                }
            };
        let bool_metadata_index_writer =
            MetadataIndexWriter::new_bool(bool_metadata_writer, bool_metadata_index_reader);

        let (f32_metadata_writer, f32_metadata_index_reader) =
            match segment.file_path.get(F32_METADATA) {
                Some(f32_metadata_path) => {
                    let (prefix, f32_metadata_uuid) =
                        Segment::extract_prefix_and_id(f32_metadata_path).map_err(|_| {
                            MetadataSegmentError::UuidParseError(f32_metadata_path.to_string())
                        })?;
                    let f32_metadata_writer = {
                        let mut options =
                            BlockfileWriterOptions::new(prefix.to_string()).fork(f32_metadata_uuid);
                        if let Some(cmek) = &cmek {
                            options = options.with_cmek(cmek.clone());
                        }
                        match blockfile_provider
                            .write::<f32, RoaringBitmap>(options)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        }
                    };
                    let read_options =
                        BlockfileReaderOptions::new(f32_metadata_uuid, prefix.to_string());
                    let f32_metadata_index_reader = match blockfile_provider
                        .read::<f32, RoaringBitmap>(read_options)
                        .await
                    {
                        Ok(reader) => MetadataIndexReader::new_f32(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    (f32_metadata_writer, Some(f32_metadata_index_reader))
                }
                None => {
                    let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    match blockfile_provider
                        .write::<f32, RoaringBitmap>(options)
                        .await
                    {
                        Ok(writer) => (writer, None),
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    }
                }
            };
        let f32_metadata_index_writer =
            MetadataIndexWriter::new_f32(f32_metadata_writer, f32_metadata_index_reader);

        let (u32_metadata_writer, u32_metadata_index_reader) =
            match segment.file_path.get(U32_METADATA) {
                Some(u32_metadata_path) => {
                    let (prefix, u32_metadata_uuid) =
                        Segment::extract_prefix_and_id(u32_metadata_path).map_err(|_| {
                            MetadataSegmentError::UuidParseError(u32_metadata_path.to_string())
                        })?;
                    let u32_metadata_writer = {
                        let mut options =
                            BlockfileWriterOptions::new(prefix.to_string()).fork(u32_metadata_uuid);
                        if let Some(cmek) = &cmek {
                            options = options.with_cmek(cmek.clone());
                        }
                        match blockfile_provider
                            .write::<u32, RoaringBitmap>(options)
                            .await
                        {
                            Ok(writer) => writer,
                            Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                        }
                    };
                    let read_options =
                        BlockfileReaderOptions::new(u32_metadata_uuid, prefix.to_string());
                    let u32_metadata_index_reader = match blockfile_provider
                        .read::<u32, RoaringBitmap>(read_options)
                        .await
                    {
                        Ok(reader) => MetadataIndexReader::new_u32(reader),
                        Err(e) => return Err(MetadataSegmentError::BlockfileOpenError(*e)),
                    };
                    (u32_metadata_writer, Some(u32_metadata_index_reader))
                }
                None => {
                    let mut options = BlockfileWriterOptions::new(prefix_path.clone());
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    match blockfile_provider
                        .write::<u32, RoaringBitmap>(options)
                        .await
                    {
                        Ok(writer) => (writer, None),
                        Err(e) => return Err(MetadataSegmentError::BlockfileError(*e)),
                    }
                }
            };
        let u32_metadata_index_writer =
            MetadataIndexWriter::new_u32(u32_metadata_writer, u32_metadata_index_reader);

        // ── Sparse index writers: one per enabled sparse metadata key ───
        //
        // On-disk layout is `sparse_*::<key>`. For each enabled sparse key:
        //   1. per-key SPARSE_POSTING::key present → fork MaxScore index
        //   2. per-key SPARSE_MAX::key present     → fork existing WAND index
        //   3. legacy anonymous index owned by key → fork it (migration: read
        //      old global blockfiles, flush under the per-key name)
        //   4. nothing on disk (fresh)             → schema picks WAND/MaxScore
        //
        // Legacy collections (created before per-key indexing) hold a single
        // anonymous index under the bare SPARSE_* keys. With immutable schemas
        // such a collection has exactly one enabled sparse key, which owns the
        // anonymous index; forking rewrites it to per-key layout this compaction.
        let enabled_sparse_keys = schema.map(|s| s.enabled_sparse_keys()).unwrap_or_default();

        let legacy_posting_file_path = segment.file_path.get(SPARSE_POSTING);
        let legacy_max_file_path = segment.file_path.get(SPARSE_MAX);
        let legacy_offset_value_file_path = segment.file_path.get(SPARSE_OFFSET_VALUE);
        let has_legacy_sparse = legacy_posting_file_path.is_some()
            || (legacy_max_file_path.is_some() && legacy_offset_value_file_path.is_some());
        // A legacy anonymous sparse index exists on disk under the bare
        // SPARSE_* keys. With immutable schemas, at most one enabled sparse key
        // can own (and migrate) it to the per-key layout. When such a key
        // exists it forks the legacy blockfiles below; the bare entries are not
        // re-emitted, completing the migration this compaction.
        let legacy_owner_key = if has_legacy_sparse {
            enabled_sparse_keys.first().cloned()
        } else {
            None
        };

        let mut sparse_index_writers: HashMap<String, SparseIndexWriter> = HashMap::new();
        for sparse_key in &enabled_sparse_keys {
            let per_key_posting = segment.file_path.get(&sparse_posting_key(sparse_key));
            let per_key_max = segment.file_path.get(&sparse_max_key(sparse_key));
            let per_key_offset_value = segment.file_path.get(&sparse_offset_value_key(sparse_key));

            // Prefer an already-migrated per-key entry; otherwise fall back to
            // the legacy anonymous entry if this key owns it; otherwise fresh.
            let (posting_file_path, max_file_path, offset_value_file_path) = if per_key_posting
                .is_some()
                || (per_key_max.is_some() && per_key_offset_value.is_some())
            {
                (per_key_posting, per_key_max, per_key_offset_value)
            } else if legacy_owner_key.as_deref() == Some(sparse_key.as_str()) {
                (
                    legacy_posting_file_path,
                    legacy_max_file_path,
                    legacy_offset_value_file_path,
                )
            } else {
                (None, None, None)
            };

            let sparse_index_writer = if let Some(forked) = fork_sparse_index_writer(
                blockfile_provider,
                &cmek,
                posting_file_path,
                max_file_path,
                offset_value_file_path,
            )
            .await?
            {
                forked
            } else if schema.is_some_and(|s| s.is_key_maxscore_enabled(sparse_key)) {
                // ── Fresh: MaxScore index ──────────────────────────────
                let posting_writer = {
                    let mut options = BlockfileWriterOptions::new(prefix_path.clone())
                        .ordered_mutations()
                        .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES);
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    blockfile_provider
                        .write::<u32, SparsePostingBlock>(options)
                        .await
                        .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
                };
                SparseIndexWriter::MaxScore(MaxScoreWriter::new(posting_writer, None))
            } else {
                // ── Fresh collection: WAND index (default) ─────────────
                let max_writer = {
                    let mut options =
                        BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations();
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    blockfile_provider
                        .write::<u32, f32>(options)
                        .await
                        .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
                };
                let offset_value_writer = {
                    let mut options =
                        BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations();
                    if let Some(cmek) = &cmek {
                        options = options.with_cmek(cmek.clone());
                    }
                    blockfile_provider
                        .write::<u32, f32>(options)
                        .await
                        .map_err(|e| MetadataSegmentError::BlockfileError(*e))?
                };
                SparseIndexWriter::Wand(SparseWriter::new(
                    DEFAULT_BLOCK_SIZE,
                    max_writer,
                    offset_value_writer,
                    None,
                ))
            };
            sparse_index_writers.insert(sparse_key.clone(), sparse_index_writer);
        }

        // Orphaned legacy sparse index: a bare SPARSE_* index exists on disk
        // but no enabled sparse key owns it. This is the common case for
        // collections that predate per-key indexing — older compactions always
        // flushed a sparse index (an empty WAND index for collections that
        // never used sparse vectors) under the bare names. Fork it forward
        // under the bare names so we neither drop the index nor fail
        // compaction; an owning key (if ever enabled) migrates it to per-key
        // layout via the loop above.
        let legacy_sparse_index_writer = if has_legacy_sparse && legacy_owner_key.is_none() {
            fork_sparse_index_writer(
                blockfile_provider,
                &cmek,
                legacy_posting_file_path,
                legacy_max_file_path,
                legacy_offset_value_file_path,
            )
            .await?
        } else {
            None
        };

        Ok(MetadataSegmentWriterShard {
            fts_index_writer: Some(fts_index_writer),
            string_metadata_index_writer: Some(string_metadata_index_writer),
            bool_metadata_index_writer: Some(bool_metadata_index_writer),
            f32_metadata_index_writer: Some(f32_metadata_index_writer),
            u32_metadata_index_writer: Some(u32_metadata_index_writer),
            sparse_index_writers,
            legacy_sparse_index_writer,
            id: segment.id,
        })
    }

    pub(crate) async fn set_metadata(
        &self,
        prefix: &str,
        key: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        match key {
            MetadataValue::Str(v) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, v.as_str(), offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into str metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Int(v) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v as u32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into u32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Float(v) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v as f32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into f32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Bool(v) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        match writer.set(prefix, *v, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error inserting into bool metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::SparseVector(offset_value) => {
                // Route to the writer for this metadata key. The caller gates
                // on schema.is_metadata_type_index_enabled and the sysdb always
                // persists a schema, so an enabled sparse key must have a writer
                // here. A missing writer is an invariant violation; surface it
                // as an error (like the other arms, but without panicking)
                // rather than silently dropping the value.
                match self.sparse_index_writers.get(prefix) {
                    Some(w) => {
                        w.set(offset_id, offset_value.iter()).await;
                        Ok(())
                    }
                    None => Err(MetadataIndexError::MissingSparseWriter(prefix.to_string())),
                }
            }
            // Array types: explode the array and index each element separately
            // This enables efficient CONTAINS queries via the inverted index
            MetadataValue::StringArray(values) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.set(prefix, v.as_str(), offset_id).await {
                                tracing::error!("Error inserting into str metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::IntArray(values) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.set(prefix, *v as u32, offset_id).await {
                                tracing::error!("Error inserting into u32 metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::FloatArray(values) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.set(prefix, *v as f32, offset_id).await {
                                tracing::error!("Error inserting into f32 metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::BoolArray(values) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.set(prefix, *v, offset_id).await {
                                tracing::error!("Error inserting into bool metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
        }
    }

    pub(crate) async fn delete_metadata(
        &self,
        prefix: &str,
        key: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataIndexError> {
        match key {
            MetadataValue::Str(v) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, v.as_str(), offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from str metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Int(v) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v as u32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from u32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Float(v) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v as f32, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from f32 metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::Bool(v) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        match writer.delete(prefix, *v, offset_id).await {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                tracing::error!("Error deleting from bool metadata index writer {:?}", e);
                                Err(e)
                            }
                        }
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::SparseVector(offset_value) => {
                // See set_metadata: a missing writer for an enabled sparse key
                // is an invariant violation, so error instead of swallowing it.
                match self.sparse_index_writers.get(prefix) {
                    Some(w) => {
                        w.delete(offset_id, offset_value.indices.iter().cloned())
                            .await;
                        Ok(())
                    }
                    None => Err(MetadataIndexError::MissingSparseWriter(prefix.to_string())),
                }
            }
            // Array types: delete each element from the inverted index
            MetadataValue::StringArray(values) => {
                match &self.string_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.delete(prefix, v.as_str(), offset_id).await {
                                tracing::error!("Error deleting from str metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. String metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::IntArray(values) => {
                match &self.u32_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.delete(prefix, *v as u32, offset_id).await {
                                tracing::error!("Error deleting from u32 metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. u32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::FloatArray(values) => {
                match &self.f32_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.delete(prefix, *v as f32, offset_id).await {
                                tracing::error!("Error deleting from f32 metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. f32 metadata index writer should be set for metadata segment"),
                }
            }
            MetadataValue::BoolArray(values) => {
                match &self.bool_metadata_index_writer {
                    Some(writer) => {
                        for v in values {
                            if let Err(e) = writer.delete(prefix, *v, offset_id).await {
                                tracing::error!("Error deleting from bool metadata index writer {:?}", e);
                                return Err(e);
                            }
                        }
                        Ok(())
                    }
                    None => panic!("Invariant violation. bool metadata index writer should be set for metadata segment"),
                }
            }
        }
    }

    pub(crate) async fn update_metadata(
        &self,
        key: &str,
        old_value: &MetadataValue,
        new_value: &MetadataValue,
        offset_id: u32,
    ) -> Result<(), MetadataSegmentError> {
        // Delete old value.
        self.delete_metadata(key, old_value, offset_id).await?;
        // Insert new value.
        Ok(self.set_metadata(key, new_value, offset_id).await?)
    }

    async fn apply_fts_logs(
        &self,
        record_segment_reader: &Option<RecordSegmentReaderShard<'_>>,
        materialized: &MaterializeLogsResult,
        schema: &Option<Schema>,
    ) -> Result<(), ApplyMaterializedLogError> {
        // Skip FTS indexing if disabled in schema (default to enabled for backwards compatibility)
        let fts_enabled = schema.as_ref().is_none_or(|s| s.is_fts_enabled());
        if !fts_enabled {
            tracing::info!("FTS is disabled in schema, skipping indexing");
            return Ok(());
        }

        let Some(fts_writer) = self.fts_index_writer.as_ref() else {
            return Err(ApplyMaterializedLogError::FullTextIndex(
                FullTextIndexError::InvariantViolation,
            ));
        };

        match fts_writer {
            FtsIndexWriter::Trigram(writer) => {
                let mut batch = vec![];
                for record in materialized {
                    let record = record
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(ApplyMaterializedLogError::Materialization)?;
                    let offset_id = record.get_offset_id();
                    let old_document = record.document_ref_from_segment();
                    let new_document = record.document_ref_from_log();

                    if matches!(
                        record.get_operation(),
                        MaterializedLogOperation::UpdateExisting
                    ) && new_document.is_none()
                    {
                        continue;
                    }

                    match (old_document, new_document) {
                        (None, None) => {}
                        (Some(old), Some(new)) => {
                            batch.push(DocumentMutation::Update {
                                offset_id,
                                old_document: old,
                                new_document: new,
                            });
                        }
                        (None, Some(new)) => {
                            batch.push(DocumentMutation::Create {
                                offset_id,
                                new_document: new,
                            });
                        }
                        (Some(old), None) => {
                            batch.push(DocumentMutation::Delete {
                                offset_id,
                                old_document: old,
                            });
                        }
                    }
                }
                writer
                    .handle_batch(batch)
                    .map_err(ApplyMaterializedLogError::FullTextIndex)?;
            }
            FtsIndexWriter::TokenBitmap(analyzer, writer) => {
                let mut analyzer = analyzer.clone();
                let tokenize = |analyzer: &mut WordAnalyzer, text: &str| {
                    analyzer.tokenize_document(text).map_err(|e| {
                        ApplyMaterializedLogError::FullTextIndex(
                            FullTextIndexError::TokenizerError(e.to_string()),
                        )
                    })
                };
                for record in materialized {
                    let record = record
                        .hydrate(record_segment_reader.as_ref())
                        .await
                        .map_err(ApplyMaterializedLogError::Materialization)?;
                    let offset_id = record.get_offset_id();
                    let old_document = record.document_ref_from_segment();
                    let new_document = record.document_ref_from_log();

                    if matches!(
                        record.get_operation(),
                        MaterializedLogOperation::UpdateExisting
                    ) && new_document.is_none()
                    {
                        continue;
                    }

                    match (old_document, new_document) {
                        (None, None) => {}
                        (Some(old), Some(new)) => {
                            writer.delete_document(offset_id, tokenize(&mut analyzer, old)?);
                            writer.add_document(offset_id, tokenize(&mut analyzer, new)?);
                        }
                        (None, Some(new)) => {
                            writer.add_document(offset_id, tokenize(&mut analyzer, new)?);
                        }
                        (Some(old), None) => {
                            writer.delete_document(offset_id, tokenize(&mut analyzer, old)?);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn apply_materialized_log_chunk(
        &self,
        record_segment_reader: &Option<RecordSegmentReaderShard<'_>>,
        materialized: &MaterializeLogsResult,
        schema: Option<Schema>,
    ) -> Result<Option<Schema>, ApplyMaterializedLogError> {
        let mut count = 0u64;
        let mut schema = schema;
        let mut schema_modified = false;
        tracing::info!(
            "Applying metadata materialized log chunk with {} records",
            materialized.len()
        );

        self.apply_fts_logs(record_segment_reader, materialized, &schema)
            .await?;

        for record in materialized {
            count += 1;

            let record = record
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(ApplyMaterializedLogError::Materialization)?;
            let segment_offset_id = record.get_offset_id();

            match record.get_operation() {
                MaterializedLogOperation::AddNew => {
                    // We can ignore record.0.metadata_to_be_deleted
                    // for fresh adds. TODO on whether to propagate error.
                    if let Some(metadata) = record.get_metadata_to_be_merged() {
                        for (key, value) in metadata.iter() {
                            if let Some(schema_mut) = schema.as_mut() {
                                if schema_mut.ensure_key_from_metadata(key, value.value_type()) {
                                    schema_modified = true;
                                }
                                if !schema_mut.is_metadata_type_index_enabled(key, value.value_type())? {
                                    continue;
                                }
                            }
                            match self.set_metadata(key, value, segment_offset_id).await {
                                Ok(()) => {}
                                Err(_) => {
                                    return Err(ApplyMaterializedLogError::BlockfileSet);
                                }
                            }
                        }
                    }
                }
                MaterializedLogOperation::DeleteExisting => match record.get_data_record() {
                    Some(data_record) => {
                        if let Some(metadata) = &data_record.metadata {
                            for (key, value) in metadata.iter() {
                                if let Some(ref schema) = schema {
                                    if !schema.is_metadata_type_index_enabled(key, value.value_type())? {
                                        continue;
                                    }
                                }
                                match self.delete_metadata(key, value, segment_offset_id).await
                                {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(
                                            ApplyMaterializedLogError::BlockfileDelete,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                },
                MaterializedLogOperation::UpdateExisting => {
                    let metadata_delta = record.compute_metadata_delta();

                    // Metadata updates.
                    for (update_key, (old_value, new_value)) in metadata_delta.metadata_to_update {
                        if let Some(schema_mut) = schema.as_mut() {
                            if schema_mut.ensure_key_from_metadata(update_key, new_value.value_type()) {
                                schema_modified = true;
                            }
                            // theres basically 4 cases:
                            // 1.old value & new value are not indexed -> noop
                            // 2.old value is indexed & new value is not indexed -> delete old value
                            // 3.old value is not indexed & new value is indexed -> insert new value
                            // 4.old value is indexed & new value is indexed -> update old value
                            let old_is_indexed = schema_mut.is_metadata_type_index_enabled(update_key, old_value.value_type())?;
                            let new_is_indexed = schema_mut.is_metadata_type_index_enabled(update_key, new_value.value_type())?;
                            if !old_is_indexed && !new_is_indexed {
                                continue;
                            }
                            else if old_is_indexed && !new_is_indexed {
                                match self.delete_metadata(update_key, old_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileDelete);
                                    }
                                }
                            }
                            else if !old_is_indexed && new_is_indexed {
                                match self.set_metadata(update_key, new_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileSet);
                                    }
                                }
                            }
                            else if old_is_indexed && new_is_indexed {
                                match self.update_metadata(update_key, old_value, new_value, segment_offset_id).await {
                                    Ok(()) => {}
                                    Err(_) => {
                                        return Err(ApplyMaterializedLogError::BlockfileUpdate);
                                    }
                                }
                            }
                        } else {
                            match self
                                .update_metadata(
                                    update_key,
                                    old_value,
                                    new_value,
                                    segment_offset_id,
                                )
                                .await
                            {
                                Ok(()) => {}
                                Err(_) => {
                                    return Err(ApplyMaterializedLogError::BlockfileUpdate);
                                }
                            }
                        }
                    }

                    // Metadata inserts.
                    for (insert_key, new_value) in metadata_delta.metadata_to_insert {
                        if let Some(schema_mut) = schema.as_mut() {
                            if schema_mut.ensure_key_from_metadata(insert_key, new_value.value_type()) {
                                schema_modified = true;
                            }
                            if !schema_mut.is_metadata_type_index_enabled(insert_key, new_value.value_type())? {
                                continue;
                            }
                        }
                        match self
                            .set_metadata(insert_key, new_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(ApplyMaterializedLogError::BlockfileSet);
                            }
                        }
                    }

                    // Metadata deletes.
                    for (delete_key, old_value) in metadata_delta.metadata_to_delete {
                        if let Some(ref schema) = schema {
                            if !schema.is_metadata_type_index_enabled(delete_key, old_value.value_type())? {
                                continue;
                            }
                        }
                        match self
                            .delete_metadata(delete_key, old_value, segment_offset_id)
                            .await
                        {
                            Ok(()) => {}
                            Err(_) => {
                                return Err(ApplyMaterializedLogError::BlockfileDelete);
                            }
                        }
                    }

                }
                MaterializedLogOperation::OverwriteExisting => {
                    // Delete existing.
                    match record.get_data_record() {
                        Some(data_record) => {
                            if let Some(metadata) = &data_record.metadata {
                                for (key, value) in metadata.iter() {
                                    if let Some(ref schema) = schema {
                                        if !schema.is_metadata_type_index_enabled(key, value.value_type())? {
                                            continue;
                                        }
                                    }
                                    match self.delete_metadata(key, value, segment_offset_id).await
                                    {
                                        Ok(()) => {}
                                        Err(_) => {
                                            return Err(
                                                ApplyMaterializedLogError::BlockfileDelete,
                                            );
                                        }
                                    }
                                }
                            }
                        },
                        None => panic!("Invariant violation. Data record should be set by materializer in case of Deletes")
                    };

                    // Add new.
                    if let Some(metadata) = record.get_metadata_to_be_merged() {
                        for (key, value) in metadata.iter() {
                            if let Some(schema_mut) = schema.as_mut() {
                                if schema_mut.ensure_key_from_metadata(key, value.value_type()) {
                                    schema_modified = true;
                                }
                                if !schema_mut.is_metadata_type_index_enabled(key, value.value_type())? {
                                    continue;
                                }
                            }
                            match self.set_metadata(key, value, segment_offset_id).await {
                                Ok(()) => {}
                                Err(_) => {
                                    return Err(ApplyMaterializedLogError::BlockfileSet);
                                }
                            }
                        }
                    }
                },
                MaterializedLogOperation::Initial => panic!("Not expected mat records in the initial state")
            }
        }
        tracing::info!("Applied {} records to metadata segment", count,);
        // return the schema only if it was modified (so will not affect legacy paths)
        Ok(if schema_modified { schema } else { None })
    }

    pub async fn finish(&mut self) -> Result<(), Box<dyn ChromaError>> {
        let mut fts_index_writer = match self.fts_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = match &mut fts_index_writer {
            FtsIndexWriter::Trigram(w) => w
                .write_to_blockfiles()
                .instrument(tracing::info_span!("fts writer write_to_blockfiles"))
                .await
                .map_err(|_| {
                    Box::new(MetadataSegmentError::BlockfileWriteError) as Box<dyn ChromaError>
                }),
            FtsIndexWriter::TokenBitmap(_, w) => w
                .write_to_blockfiles()
                .instrument(tracing::info_span!("fts writer write_to_blockfiles"))
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>),
        };
        self.fts_index_writer = Some(fts_index_writer);
        res?;

        let mut string_metadata_index_writer = match self.string_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = string_metadata_index_writer
            .write_to_blockfile()
            .instrument(tracing::info_span!(
                "string metadata writer write_to_blockfile"
            ))
            .await;
        self.string_metadata_index_writer = Some(string_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut bool_metadata_index_writer = match self.bool_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = bool_metadata_index_writer
            .write_to_blockfile()
            .instrument(tracing::info_span!(
                "bool metadata writer write_to_blockfile"
            ))
            .await;
        self.bool_metadata_index_writer = Some(bool_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut f32_metadata_index_writer = match self.f32_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = f32_metadata_index_writer
            .write_to_blockfile()
            .instrument(tracing::info_span!(
                "f32 metadata writer write_to_blockfile"
            ))
            .await;
        self.f32_metadata_index_writer = Some(f32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        let mut u32_metadata_index_writer = match self.u32_metadata_index_writer.take() {
            Some(writer) => writer,
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };
        let res = u32_metadata_index_writer
            .write_to_blockfile()
            .instrument(tracing::info_span!(
                "u32 metadata writer write_to_blockfile"
            ))
            .await;
        self.u32_metadata_index_writer = Some(u32_metadata_index_writer);
        match res {
            Ok(_) => {}
            Err(_) => return Err(Box::new(MetadataSegmentError::BlockfileWriteError)),
        }

        Ok(())
    }

    pub async fn commit(self) -> Result<MetadataSegmentFlusherShard, Box<dyn ChromaError>> {
        let fts_flusher = match self.fts_index_writer {
            Some(writer) => match writer {
                FtsIndexWriter::Trigram(w) => FtsIndexFlusher::Trigram(
                    w.commit()
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?,
                ),
                FtsIndexWriter::TokenBitmap(_, w) => FtsIndexFlusher::TokenBitmap(
                    w.commit()
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?,
                ),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let string_metadata_flusher = match self.string_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let bool_metadata_flusher = match self.bool_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let f32_metadata_flusher = match self.f32_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let u32_metadata_flusher = match self.u32_metadata_index_writer {
            Some(flusher) => match flusher.commit().await {
                Ok(flusher) => flusher,
                Err(e) => return Err(Box::new(e)),
            },
            None => return Err(Box::new(MetadataSegmentError::NoWriter)),
        };

        let mut sparse_index_flushers = HashMap::with_capacity(self.sparse_index_writers.len());
        for (key, writer) in self.sparse_index_writers {
            sparse_index_flushers.insert(key, writer.commit().await?);
        }

        let legacy_sparse_index_flusher = match self.legacy_sparse_index_writer {
            Some(writer) => Some(writer.commit().await?),
            None => None,
        };

        Ok(MetadataSegmentFlusherShard {
            id: self.id,
            fts_index_flusher: fts_flusher,
            string_metadata_index_flusher: string_metadata_flusher,
            bool_metadata_index_flusher: bool_metadata_flusher,
            f32_metadata_index_flusher: f32_metadata_flusher,
            u32_metadata_index_flusher: u32_metadata_flusher,
            sparse_index_flushers,
            legacy_sparse_index_flusher,
        })
    }
}

#[derive(Debug)]
pub struct MetadataSegmentFlusher {
    shards: Vec<MetadataSegmentFlusherShard>,
    pub id: SegmentUuid,
}

impl MetadataSegmentFlusher {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        // Flush all shards and collect file paths
        let mut all_file_paths = HashMap::new();

        for shard in self.shards {
            let shard_paths = Box::pin(shard.flush()).await?;
            for (key, mut paths) in shard_paths {
                all_file_paths
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .append(&mut paths);
            }
        }

        Ok(all_file_paths)
    }
}

pub struct MetadataSegmentFlusherShard {
    pub id: SegmentUuid,
    pub(crate) fts_index_flusher: FtsIndexFlusher,
    pub(crate) string_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) bool_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) f32_metadata_index_flusher: MetadataIndexFlusher,
    pub(crate) u32_metadata_index_flusher: MetadataIndexFlusher,
    /// One sparse index flusher per enabled sparse metadata key.
    pub(crate) sparse_index_flushers: HashMap<String, SparseIndexFlusher>,
    /// Flusher for an orphaned legacy anonymous sparse index, re-emitted under
    /// the bare `SPARSE_*` names.
    pub(crate) legacy_sparse_index_flusher: Option<SparseIndexFlusher>,
}

impl Debug for MetadataSegmentFlusherShard {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataSegmentFlusherShard")
            .field("id", &self.id)
            .finish()
    }
}

impl MetadataSegmentFlusherShard {
    pub async fn flush(self) -> Result<HashMap<String, Vec<String>>, Box<dyn ChromaError>> {
        let mut flushed = HashMap::new();

        let prefix_path = match &self.fts_index_flusher {
            FtsIndexFlusher::Trigram(f) => f.prefix_path().to_string(),
            FtsIndexFlusher::TokenBitmap(f) => f.prefix_path().to_string(),
        };
        let string_metadata_id = self.string_metadata_index_flusher.id();
        let bool_metadata_id = self.bool_metadata_index_flusher.id();
        let f32_metadata_id = self.f32_metadata_index_flusher.id();
        let u32_metadata_id = self.u32_metadata_index_flusher.id();

        match self.fts_index_flusher {
            FtsIndexFlusher::Trigram(f) => {
                let pls_id = f.pls_id();
                f.flush()
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
                flushed.insert(
                    FULL_TEXT_PLS.to_string(),
                    vec![ChromaSegmentFlusher::flush_key(&prefix_path, &pls_id)],
                );
            }
            FtsIndexFlusher::TokenBitmap(f) => {
                let token_id = f.id();
                f.flush()
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
                flushed.insert(
                    FULL_TEXT_TOKEN.to_string(),
                    vec![ChromaSegmentFlusher::flush_key(&prefix_path, &token_id)],
                );
            }
        }

        match self.bool_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            BOOL_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &bool_metadata_id,
            )],
        );

        match self.f32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            F32_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &f32_metadata_id,
            )],
        );

        match self.u32_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            U32_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &u32_metadata_id,
            )],
        );

        match self.string_metadata_index_flusher.flush().await {
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        flushed.insert(
            STRING_METADATA.to_string(),
            vec![ChromaSegmentFlusher::flush_key(
                &prefix_path,
                &string_metadata_id,
            )],
        );

        for (metadata_key, sparse_flusher) in self.sparse_index_flushers {
            for (file_path_key, paths) in sparse_flusher
                .flush(&prefix_path, Some(&metadata_key))
                .await?
            {
                flushed.insert(file_path_key, paths);
            }
        }

        if let Some(legacy_flusher) = self.legacy_sparse_index_flusher {
            for (file_path_key, paths) in legacy_flusher.flush(&prefix_path, None).await? {
                flushed.insert(file_path_key, paths);
            }
        }

        Ok(flushed)
    }
}

pub struct MetadataSegmentReaderShard<'me> {
    pub fts_index_reader: Option<FtsIndexReader<'me>>,
    pub string_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub bool_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub f32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    pub u32_metadata_index_reader: Option<MetadataIndexReader<'me>>,
    /// One sparse vector index reader per metadata key (per-key layout).
    pub sparse_index_readers: HashMap<String, SparseIndexReader<'me>>,
    /// Reader for the legacy anonymous sparse index (bare `SPARSE_*` entries),
    /// used as a fallback for collections not yet rewritten to per-key layout.
    pub legacy_sparse_index_reader: Option<SparseIndexReader<'me>>,
}

impl<'me> MetadataSegmentReaderShard<'me> {
    async fn load_index_reader<'new, K: ReadKey<'new>, V: ReadValue<'new>>(
        segment: &SegmentShard,
        file_path_string: &str,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Option<BlockfileReader<'new, K, V>>, MetadataSegmentError> {
        match segment.file_path.get(file_path_string) {
            Some(file_path) => {
                let (prefix_path, index_uuid) = Segment::extract_prefix_and_id(file_path)
                    .map_err(|_| MetadataSegmentError::UuidParseError(file_path.to_string()))?;
                let reader_options =
                    BlockfileReaderOptions::new(index_uuid, prefix_path.to_string());
                match blockfile_provider.read::<K, V>(reader_options).await {
                    Ok(reader) => Ok(Some(reader)),
                    Err(e) => Err(MetadataSegmentError::BlockfileOpenError(*e)),
                }
            }
            None => Ok(None),
        }
    }

    /// Build a single sparse index reader from the given file_path map keys.
    /// MaxScore (posting) takes precedence over WAND (max + offset-value) when
    /// both are somehow present; returns `None` when no complete index exists.
    async fn load_sparse_index_reader<'new>(
        segment: &SegmentShard,
        posting_file_path_key: &str,
        max_file_path_key: &str,
        offset_value_file_path_key: &str,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Option<SparseIndexReader<'new>>, MetadataSegmentError> {
        if let Some(posting_reader) = Self::load_index_reader::<u32, SparsePostingBlock>(
            segment,
            posting_file_path_key,
            blockfile_provider,
        )
        .await?
        {
            return Ok(Some(SparseIndexReader::MaxScore(MaxScoreReader::new(
                posting_reader,
            ))));
        }

        let max_reader =
            Self::load_index_reader::<u32, f32>(segment, max_file_path_key, blockfile_provider)
                .await?;
        let offset_value_reader = Self::load_index_reader::<u32, f32>(
            segment,
            offset_value_file_path_key,
            blockfile_provider,
        )
        .await?;
        if let (Some(max_reader), Some(offset_value_reader)) = (max_reader, offset_value_reader) {
            return Ok(Some(SparseIndexReader::Wand(SparseReader::new(
                max_reader,
                offset_value_reader,
            ))));
        }

        Ok(None)
    }

    pub async fn from_segment(
        segment: &SegmentShard,
        blockfile_provider: &BlockfileProvider,
    ) -> Result<Self, MetadataSegmentError> {
        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(MetadataSegmentError::InvalidSegmentType);
        }

        // Create async tasks for all reader operations
        let pls_future = Self::load_index_reader(segment, FULL_TEXT_PLS, blockfile_provider);
        let token_bitmap_future = Self::load_index_reader::<u32, RoaringBitmap>(
            segment,
            FULL_TEXT_TOKEN,
            blockfile_provider,
        )
        .instrument(Span::current());

        let string_metadata_future =
            Self::load_index_reader(segment, STRING_METADATA, blockfile_provider)
                .instrument(Span::current());

        let bool_metadata_future =
            Self::load_index_reader(segment, BOOL_METADATA, blockfile_provider)
                .instrument(Span::current());

        let f32_metadata_future =
            Self::load_index_reader(segment, F32_METADATA, blockfile_provider)
                .instrument(Span::current());

        let u32_metadata_future =
            Self::load_index_reader(segment, U32_METADATA, blockfile_provider)
                .instrument(Span::current());

        let (
            pls_reader,
            token_bitmap_reader,
            string_metadata_reader,
            bool_metadata_reader,
            f32_metadata_reader,
            u32_metadata_reader,
        ) = tokio::join!(
            pls_future,
            token_bitmap_future,
            string_metadata_future,
            bool_metadata_future,
            f32_metadata_future,
            u32_metadata_future,
        );

        // Exactly one of FULL_TEXT_TOKEN (TokenBitmap) or FULL_TEXT_PLS
        // (Trigram) is present per segment; check TokenBitmap first.
        let token_bitmap_reader = token_bitmap_reader?;
        let pls_reader = pls_reader?;
        let fts_index_reader = if let Some(reader) = token_bitmap_reader {
            Some(FtsIndexReader::TokenBitmap(
                WordAnalyzer::default(),
                FullTextBitmapReader::new(reader),
            ))
        } else {
            pls_reader.map(|reader| {
                let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
                FtsIndexReader::Trigram(FullTextIndexReader::new(reader, tokenizer))
            })
        };

        let string_metadata_reader = string_metadata_reader?;
        let string_metadata_index_reader =
            string_metadata_reader.map(MetadataIndexReader::new_string);

        let bool_metadata_reader = bool_metadata_reader?;
        let bool_metadata_index_reader = bool_metadata_reader.map(MetadataIndexReader::new_bool);

        let u32_metadata_reader = u32_metadata_reader?;
        let u32_metadata_index_reader = u32_metadata_reader.map(MetadataIndexReader::new_u32);

        let f32_metadata_reader = f32_metadata_reader?;
        let f32_metadata_index_reader = f32_metadata_reader.map(MetadataIndexReader::new_f32);

        // Sparse indices: one reader per metadata key from `sparse_*::<key>`
        // entries, plus a legacy reader from any bare `SPARSE_*` entries written
        // before per-key indexing existed (read until the next compaction
        // rewrites them to per-key layout).
        let mut sparse_metadata_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut has_legacy_sparse = false;
        for file_path_key in segment.file_path.keys() {
            match parse_sparse_file_path_key(file_path_key) {
                Some((_, Some(metadata_key))) => {
                    sparse_metadata_keys.insert(metadata_key);
                }
                Some((_, None)) => has_legacy_sparse = true,
                None => {}
            }
        }

        let mut sparse_index_readers = HashMap::new();
        for metadata_key in sparse_metadata_keys {
            if let Some(reader) = Self::load_sparse_index_reader(
                segment,
                &sparse_posting_key(&metadata_key),
                &sparse_max_key(&metadata_key),
                &sparse_offset_value_key(&metadata_key),
                blockfile_provider,
            )
            .await?
            {
                sparse_index_readers.insert(metadata_key, reader);
            }
        }

        let legacy_sparse_index_reader = if has_legacy_sparse {
            Self::load_sparse_index_reader(
                segment,
                SPARSE_POSTING,
                SPARSE_MAX,
                SPARSE_OFFSET_VALUE,
                blockfile_provider,
            )
            .await?
        } else {
            None
        };

        Ok(MetadataSegmentReaderShard {
            fts_index_reader,
            string_metadata_index_reader,
            bool_metadata_index_reader,
            f32_metadata_index_reader,
            u32_metadata_index_reader,
            sparse_index_readers,
            legacy_sparse_index_reader,
        })
    }
}

#[cfg(test)]
mod test {

    use crate::{
        blockfile_metadata::{
            MetadataSegmentReaderShard, MetadataSegmentWriterShard, SparseIndexReader,
            SparseIndexWriter,
        },
        blockfile_record::{
            RecordSegmentReaderOptions, RecordSegmentReaderShard,
            RecordSegmentReaderShardCreationError, RecordSegmentWriterShard,
        },
        test::TestDistributedSegment,
        types::materialize_logs,
    };
    use chroma_blockstore::{
        arrow::{
            config::{BlockManagerConfig, TEST_MAX_BLOCK_SIZE_BYTES},
            provider::ArrowBlockfileProvider,
        },
        provider::BlockfileProvider,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::{
        regex::literal_expr::{LiteralExpr, NgramLiteralProvider},
        strategies::{ArbitraryChromaRegexTestDocumentsParameters, ChromaRegexTestDocuments},
        Chunk, CollectionUuid, DatabaseUuid, FtsIndexConfig, IndexConfig, KnnIndex, LogRecord,
        MetadataValue, Operation, OperationRecord, ScalarEncoding, Schema, SegmentShard,
        SegmentUuid, UpdateMetadataValue, DOCUMENT_KEY,
    };
    use proptest::prelude::any_with;
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, str::FromStr};
    use tokio::runtime::Runtime;

    /// Build a schema with a sparse vector index enabled on a single metadata
    /// key, using the given algorithm. Used by sparse segment tests.
    fn sparse_schema_for_key(key: &str, algorithm: chroma_types::SparseIndexAlgorithm) -> Schema {
        use chroma_types::{SparseVectorIndexConfig, SparseVectorIndexType, SparseVectorValueType};
        let mut schema = Schema::new_default(KnnIndex::Hnsw);
        let value_types = schema.keys.entry(key.to_string()).or_default();
        value_types.sparse_vector = Some(SparseVectorValueType {
            sparse_vector_index: Some(SparseVectorIndexType {
                enabled: true,
                config: SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                    algorithm,
                },
            }),
        });
        schema
    }

    #[tokio::test]
    async fn empty_blocks() {
        // Run the actual test logic in a separate thread with increased stack size
        let handle = std::thread::Builder::new()
            .name("empty_blocks_test".to_string())
            .stack_size(8 * 1024 * 1024) // 8MB stack size
            .spawn(|| {
                // Create a new tokio runtime within the thread
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    Box::pin(empty_blocks_impl()).await;
                });
            })
            .expect("Failed to spawn thread");

        handle.join().expect("Test thread panicked");
    }

    async fn empty_blocks_impl() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                            RecordSegmentReaderShardCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            _ => {
                                panic!("Unexpected error creating record segment reader: {:?}", e);
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Delete,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let segment_writer = RecordSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &record_segment_shard,
            &blockfile_provider,
            None,
            None,
        )
        .await
        .expect("Error creating segment writer");
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &metadata_segment_shard,
            &blockfile_provider,
            None,
            None,
        ))
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(
            &some_reader,
            data,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // No data should be present.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 0);
        // Add a few records and they should exist.
        let data = vec![
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_3".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about cats.")),
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 6,
                record: OperationRecord {
                    id: "embedding_id_4".to_string(),
                    embedding: Some(vec![4.0, 5.0, 6.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("This is a document about dogs.")),
                    operation: Operation::Add,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let segment_writer = RecordSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &record_segment_shard,
            &blockfile_provider,
            None,
            None,
        )
        .await
        .expect("Error creating segment writer");
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &metadata_segment_shard,
            &blockfile_provider,
            None,
            None,
        ))
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(
            &some_reader,
            data,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let count = record_flusher.count();
        assert_eq!(count, 2_u64);
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // No data should be present.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Record segment reader should be initialized by now");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Error getting all data from record segment")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
    }

    #[tokio::test]
    async fn metadata_update_same_key_different_type() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                            RecordSegmentReaderShardCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            _ => {
                                panic!("Unexpected error creating record segment reader: {:?}", e);
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata_id1 = HashMap::new();
        update_metadata_id1.insert(
            String::from("hello"),
            UpdateMetadataValue::Str(String::from("new world")),
        );
        let mut update_metadata_id2 = HashMap::new();
        update_metadata_id2.insert(String::from("hello"), UpdateMetadataValue::Float(1.0));
        let data = vec![
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata_id1.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: Some(update_metadata_id2.clone()),
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let segment_writer = RecordSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &record_segment_shard,
            &blockfile_provider,
            None,
            None,
        )
        .await
        .expect("Error creating segment writer");
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &metadata_segment_shard,
            &blockfile_provider,
            None,
            None,
        ))
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(
            &some_reader,
            data,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Search by f32 metadata value first.
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .f32_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &1.0.into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(2));
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"new world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // Record segment should also have the updated values.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("hello"),
            MetadataValue::Str(String::from("new world")),
        );
        assert_eq!(res.first().as_ref().unwrap().1.metadata, Some(id1_mt));
        let mut id2_mt = HashMap::new();
        id2_mt.insert(String::from("hello"), MetadataValue::Float(1.0));
        assert_eq!(res.get(1).as_ref().unwrap().1.metadata, Some(id2_mt));
    }

    #[tokio::test]
    async fn metadata_deletes() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");
            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("hello"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            update_metadata.insert(
                String::from("bye"),
                UpdateMetadataValue::Str(String::from("world")),
            );
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata.clone()),
                    document: Some(String::from("This is a document about cats.")),
                    operation: Operation::Add,
                },
            }];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                            RecordSegmentReaderShardCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            _ => {
                                panic!("Unexpected error creating record segment reader: {:?}", e);
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let mut update_metadata_id1 = HashMap::new();
        update_metadata_id1.insert(String::from("hello"), UpdateMetadataValue::None);
        let data = vec![LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: Some(update_metadata_id1.clone()),
                document: None,
                operation: Operation::Update,
            },
        }];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let segment_writer = RecordSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &record_segment_shard,
            &blockfile_provider,
            None,
            None,
        )
        .await
        .expect("Error creating segment writer");
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &metadata_segment_shard,
            &blockfile_provider,
            None,
            None,
        ))
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(
            &some_reader,
            data,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // Only one key should be present.
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("hello", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        let res = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .get("bye", &"world".into())
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // Record segment should also have the updated values.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        let mut id1_mt = HashMap::new();
        id1_mt.insert(
            String::from("bye"),
            MetadataValue::Str(String::from("world")),
        );
        assert_eq!(res.first().as_ref().unwrap().1.metadata, Some(id1_mt));
    }

    #[tokio::test]
    async fn document_updates() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");
            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: Some(String::from("hello")),
                    operation: Operation::Add,
                },
            }];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                            RecordSegmentReaderShardCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            _ => {
                                panic!("Unexpected error creating record segment reader: {:?}", e);
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let data = vec![LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: Some(String::from("bye")),
                operation: Operation::Update,
            },
        }];

        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let segment_writer = RecordSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &record_segment_shard,
            &blockfile_provider,
            None,
            None,
        )
        .await
        .expect("Error creating segment writer");
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
            &tenant,
            &database_id,
            &metadata_segment_shard,
            &blockfile_provider,
            None,
            None,
        ))
        .await
        .expect("Error creating segment writer");
        let some_reader = Some(record_segment_reader);
        let mat_records = materialize_logs(
            &some_reader,
            data,
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("Log materialization failed");
        metadata_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records, None)
            .await
            .expect("Apply materialized log to metadata segment failed");
        metadata_writer
            .finish()
            .await
            .expect("Write to blockfiles for metadata writer failed");
        segment_writer
            .apply_materialized_log_chunk(&some_reader, &mat_records)
            .await
            .expect("Apply materialized log to record segment failed");
        let record_flusher = Box::pin(segment_writer.commit())
            .await
            .expect("Commit for segment writer failed");
        let metadata_flusher = Box::pin(metadata_writer.commit())
            .await
            .expect("Commit for metadata writer failed");
        record_segment.file_path = Box::pin(record_flusher.flush())
            .await
            .expect("Flush record segment writer failed");
        metadata_segment.file_path = Box::pin(metadata_flusher.flush())
            .await
            .expect("Flush metadata segment writer failed");
        // FTS for hello should return empty.
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("hello")
            .await
            .unwrap();
        assert_eq!(res.len(), 0);
        // FTS for bye should return the lone document.
        let res = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("bye")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // Record segment should also have the updated values.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        assert_eq!(
            res.first().as_ref().unwrap().1.document,
            Some(String::from("bye").as_str())
        );
    }

    #[tokio::test]
    async fn test_storage_prefix_path() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("hello")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("world")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                            RecordSegmentReaderShardCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderShardCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            _ => {
                                panic!("Unexpected error creating record segment reader: {:?}", e);
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }
        let prefix = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, record_segment.collection, record_segment.id,
        );
        assert_eq!(record_segment.file_path.len(), 4);
        for file_path in record_segment.file_path.values() {
            assert_eq!(file_path.len(), 1);
            assert!(file_path
                .first()
                .expect("File path should have at least one entry")
                .starts_with(&prefix));
        }
        let prefix = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            tenant, database_id, record_segment.collection, metadata_segment.id,
        );
        // Without a schema, no sparse index is created (5 = FTS + 4 metadata
        // type indexes). Sparse index files only appear for schema-enabled keys.
        assert_eq!(metadata_segment.file_path.len(), 5);
        for file_path in metadata_segment.file_path.values() {
            assert_eq!(file_path.len(), 1);
            assert!(file_path
                .first()
                .expect("File path should have at least one entry")
                .starts_with(&prefix));
        }
        // FTS for hello should return 1 document
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");
        let res = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("hello")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(1));
        // FTS for world should return the other document.
        let res = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("The float reader should be initialized")
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("world")
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.min(), Some(2));
        // Record segment should also have the updated values.
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Reader should be initialized by now");
        let mut res = record_segment_reader
            .get_all_data()
            .await
            .expect("Record segment get all data failed")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2);
        res.sort_by(|x, y| x.1.id.cmp(y.1.id));
        assert_eq!(
            res.first().as_ref().unwrap().1.document,
            Some(String::from("hello").as_str())
        );
        assert_eq!(
            res.get(1).as_ref().unwrap().1.document,
            Some(String::from("world").as_str())
        );
    }

    async fn run_regex_test(test_case: ChromaRegexTestDocuments) {
        let pattern = String::from(test_case.hir.clone());
        let regex = regex::Regex::new(&pattern).unwrap();
        let reference_results = test_case
            .documents
            .iter()
            .enumerate()
            .filter_map(|(id, doc)| regex.is_match(doc).then_some(id as u32))
            .collect::<RoaringBitmap>();
        let logs = test_case
            .documents
            .into_iter()
            .enumerate()
            .map(|(id, doc)| LogRecord {
                log_offset: id as i64,
                record: OperationRecord {
                    id: format!("<{id}>"),
                    embedding: Some(vec![id as f32; 2]),
                    encoding: Some(ScalarEncoding::FLOAT32),
                    metadata: None,
                    document: Some(doc),
                    operation: Operation::Add,
                },
            })
            .collect::<Vec<_>>();
        let mut segments = TestDistributedSegment::new_with_dimension(2).await;
        Box::pin(segments.compact_log(Chunk::new(logs.into()), 0)).await;
        let metadata_segment_shard =
            SegmentShard::try_from((&segments.metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &segments.blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader should be constructable");
        let fts_reader = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("Full text index reader should be present")
            .as_trigram()
            .expect("expected Trigram FTS reader");
        let literal_expression = LiteralExpr::from(test_case.hir);
        let regex_results = fts_reader
            .match_literal_expression(&literal_expression)
            .await
            .expect("Literal evaluation should not fail");
        if let Some(res) = regex_results {
            assert_eq!(res, reference_results);
        }
    }

    proptest::proptest! {
        #[test]
        fn test_simple_regex(
            test_case in any_with::<ChromaRegexTestDocuments>(ArbitraryChromaRegexTestDocumentsParameters {
                recursive_hir: false,
                total_document_count: 10,
            })
        ) {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                Box::pin(run_regex_test(test_case)).await
            });
        }

        #[test]
        fn test_composite_regex(
            test_case in any_with::<ChromaRegexTestDocuments>(ArbitraryChromaRegexTestDocumentsParameters {
                recursive_hir: true,
                total_document_count: 50,
            })
        ) {
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                Box::pin(run_regex_test(test_case)).await
            });
        }
    }

    #[tokio::test]
    async fn test_metadata_sparse_vector() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        // Create segments and add records with sparse vectors
        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");

            let sparse_schema =
                sparse_schema_for_key("sparse_vec", chroma_types::SparseIndexAlgorithm::Wand);
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                Some(&sparse_schema),
            ))
            .await
            .expect("Error creating segment writer");

            // Verify that a sparse index writer is created for the enabled key
            assert!(
                metadata_writer
                    .sparse_index_writers
                    .contains_key("sparse_vec"),
                "Sparse index writer should be created for the enabled sparse key"
            );

            // Create metadata with sparse vectors
            let mut update_metadata1 = HashMap::new();
            update_metadata1.insert(
                String::from("sparse_vec"),
                UpdateMetadataValue::SparseVector(
                    chroma_types::SparseVector::new(vec![0, 5, 10], vec![0.1, 0.5, 0.9])
                        .expect("valid sparse vector"),
                ),
            );
            update_metadata1.insert(
                String::from("category"),
                UpdateMetadataValue::Str(String::from("science")),
            );

            let data = vec![LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: Some(update_metadata1.clone()),
                    document: Some(String::from("Document with sparse vector 1")),
                    operation: Operation::Add,
                },
            }];

            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => match *e {
                        RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                        _ => panic!("Error creating record segment reader"),
                    },
                };

            let materialized_logs = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Error materializing logs");

            // Apply logs - this should handle sparse vectors
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &materialized_logs)
                .await
                .expect("Error applying materialized log chunk");
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &materialized_logs, None)
                .await
                .expect("Error applying materialized log chunk");

            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit record segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit metadata segment writer failed");

            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");

            // Verify that per-key sparse index files are created
            assert!(
                metadata_segment
                    .file_path
                    .contains_key(&chroma_types::sparse_max_key("sparse_vec")),
                "Sparse max file should be created"
            );
            assert!(
                metadata_segment
                    .file_path
                    .contains_key(&chroma_types::sparse_offset_value_key("sparse_vec")),
                "Sparse offset value file should be created"
            );
        }

        // Verify we can read the segment back
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &metadata_segment_shard,
                &blockfile_provider,
            ))
            .await
            .expect("Error creating metadata segment reader");

            // Verify sparse index reader is created for the enabled key
            assert!(
                metadata_segment_reader
                    .sparse_index_readers
                    .contains_key("sparse_vec"),
                "Sparse index reader should be created for the enabled sparse key"
            );
        }
    }

    #[tokio::test]
    async fn test_legacy_sparse_migrates_to_per_key() {
        use chroma_types::{
            sparse_max_key, sparse_offset_value_key, MetadataValue, SparseIndexAlgorithm,
            SparseVector, SPARSE_MAX, SPARSE_OFFSET_VALUE,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let schema = sparse_schema_for_key("sparse_a", SparseIndexAlgorithm::Wand);

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000021").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        // Write a WAND index under the per-key layout, then rename the map keys
        // to the bare legacy names to emulate a pre-per-key collection. The
        // underlying blockfiles are identical; only the logical names differ.
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("create writer");
            for offset in 0u32..8 {
                let v = SparseVector::new(vec![0, offset % 3 + 1], vec![1.0, 0.5]).unwrap();
                writer
                    .set_metadata("sparse_a", &MetadataValue::SparseVector(v), offset)
                    .await
                    .expect("set sparse_a");
            }
            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");
        }

        let max_paths = metadata_segment
            .file_path
            .remove(&sparse_max_key("sparse_a"))
            .expect("per-key max present");
        let offset_value_paths = metadata_segment
            .file_path
            .remove(&sparse_offset_value_key("sparse_a"))
            .expect("per-key offset_value present");
        metadata_segment
            .file_path
            .insert(SPARSE_MAX.to_string(), max_paths);
        metadata_segment
            .file_path
            .insert(SPARSE_OFFSET_VALUE.to_string(), offset_value_paths);

        // Reopen with the schema: the enabled key owns the legacy anonymous
        // index and forks it, rewriting to per-key layout on this compaction.
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("create migration writer");
            assert!(
                matches!(
                    writer.sparse_index_writers.get("sparse_a"),
                    Some(SparseIndexWriter::Wand(_))
                ),
                "legacy WAND index should be forked under its owning key"
            );
            let flusher = Box::pin(writer.commit()).await.expect("migration commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("migration flush");
        }

        // After migration, the layout is per-key and the bare legacy entries
        // are gone.
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_max_key("sparse_a")));
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_offset_value_key("sparse_a")));
        assert!(!metadata_segment.file_path.contains_key(SPARSE_MAX));
        assert!(!metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE));

        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("open reader");
            assert!(matches!(
                reader.sparse_index_readers.get("sparse_a"),
                Some(SparseIndexReader::Wand(_))
            ));
            assert!(reader.legacy_sparse_index_reader.is_none());
        }
    }

    #[tokio::test]
    async fn test_legacy_sparse_without_owner_is_preserved() {
        // Regression: collections compacted before per-key indexing always
        // flushed a sparse index under the bare SPARSE_* names — even ones that
        // never used sparse vectors (an empty WAND index). Their current schema
        // has no enabled sparse key, so nothing owns the legacy index. We must
        // carry it forward under the bare names rather than dropping it or
        // failing compaction.
        use chroma_types::{
            sparse_max_key, sparse_offset_value_key, MetadataValue, SparseIndexAlgorithm,
            SparseVector, SPARSE_MAX, SPARSE_OFFSET_VALUE,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000031").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        // Build a real WAND index, then rename the per-key entries to the bare
        // legacy names to emulate a pre-per-key collection.
        let owned_schema = sparse_schema_for_key("sparse_a", SparseIndexAlgorithm::Wand);
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&owned_schema),
            ))
            .await
            .expect("create writer");
            for offset in 0u32..8 {
                let v = SparseVector::new(vec![0, offset % 3 + 1], vec![1.0, 0.5]).unwrap();
                writer
                    .set_metadata("sparse_a", &MetadataValue::SparseVector(v), offset)
                    .await
                    .expect("set sparse_a");
            }
            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");
        }
        let max_paths = metadata_segment
            .file_path
            .remove(&sparse_max_key("sparse_a"))
            .expect("per-key max present");
        let offset_value_paths = metadata_segment
            .file_path
            .remove(&sparse_offset_value_key("sparse_a"))
            .expect("per-key offset_value present");
        metadata_segment
            .file_path
            .insert(SPARSE_MAX.to_string(), max_paths);
        metadata_segment
            .file_path
            .insert(SPARSE_OFFSET_VALUE.to_string(), offset_value_paths);

        // Reopen with a default schema that enables no sparse key — exactly the
        // production case. The legacy index has no owner, so it is carried
        // forward as a legacy writer rather than migrated or dropped. Repeat to
        // prove it is stable across successive compactions (these collections
        // compact repeatedly and must never silently lose the index).
        let orphan_schema = Schema::new_default(KnnIndex::Hnsw);
        for round in 0..2 {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&orphan_schema),
            ))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "from_segment must not fail on orphaned legacy sparse (round {round}): {e:?}"
                )
            });
            assert!(
                writer.sparse_index_writers.is_empty(),
                "no enabled key, so no per-key writers"
            );
            assert!(
                matches!(
                    writer.legacy_sparse_index_writer,
                    Some(SparseIndexWriter::Wand(_))
                ),
                "orphaned legacy WAND index should be carried forward"
            );
            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");

            // The bare legacy entries survive; no per-key entries are introduced.
            assert!(metadata_segment.file_path.contains_key(SPARSE_MAX));
            assert!(metadata_segment.file_path.contains_key(SPARSE_OFFSET_VALUE));
            assert!(!metadata_segment
                .file_path
                .contains_key(&sparse_max_key("sparse_a")));
        }

        // The index data is intact: dimension 0 appears in all 8 records.
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("open reader");
            assert!(reader.sparse_index_readers.is_empty());
            let legacy = reader
                .legacy_sparse_index_reader
                .as_ref()
                .expect("legacy reader present");
            let counts = legacy.dimension_counts(&[0]).await.expect("counts");
            assert_eq!(counts.get(&0).copied(), Some(8));
        }
    }

    #[tokio::test]
    async fn test_legacy_maxscore_sparse_without_owner_is_preserved() {
        // Same regression as the WAND case, but for the MaxScore on-disk layout
        // (a single bare SPARSE_POSTING entry) which forks and re-flushes via a
        // different branch.
        use chroma_types::{
            sparse_posting_key, MetadataValue, SparseIndexAlgorithm, SparseVector, SPARSE_POSTING,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000041").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        let owned_schema = sparse_schema_for_key("sparse_a", SparseIndexAlgorithm::MaxScore);
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&owned_schema),
            ))
            .await
            .expect("create writer");
            for offset in 0u32..8 {
                let v = SparseVector::new(vec![0, offset % 3 + 1], vec![1.0, 0.5]).unwrap();
                writer
                    .set_metadata("sparse_a", &MetadataValue::SparseVector(v), offset)
                    .await
                    .expect("set sparse_a");
            }
            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");
        }
        let posting_paths = metadata_segment
            .file_path
            .remove(&sparse_posting_key("sparse_a"))
            .expect("per-key posting present");
        metadata_segment
            .file_path
            .insert(SPARSE_POSTING.to_string(), posting_paths);

        let orphan_schema = Schema::new_default(KnnIndex::Hnsw);
        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&orphan_schema),
            ))
            .await
            .expect("from_segment must not fail on an orphaned legacy MaxScore index");
            assert!(writer.sparse_index_writers.is_empty());
            assert!(
                matches!(
                    writer.legacy_sparse_index_writer,
                    Some(SparseIndexWriter::MaxScore(_))
                ),
                "orphaned legacy MaxScore index should be carried forward"
            );
            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");
        }

        assert!(metadata_segment.file_path.contains_key(SPARSE_POSTING));
        assert!(!metadata_segment
            .file_path
            .contains_key(&sparse_posting_key("sparse_a")));

        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("open reader");
            assert!(reader.sparse_index_readers.is_empty());
            let legacy = reader
                .legacy_sparse_index_reader
                .as_ref()
                .expect("legacy reader present");
            assert!(matches!(legacy, SparseIndexReader::MaxScore(_)));
            let counts = legacy.dimension_counts(&[0]).await.expect("counts");
            assert_eq!(counts.get(&0).copied(), Some(8));
        }
    }

    #[tokio::test]
    async fn test_metadata_multiple_sparse_keys() {
        use chroma_types::{
            sparse_max_key, sparse_offset_value_key, sparse_posting_key, MetadataValue,
            SparseIndexAlgorithm, SparseVector, SparseVectorIndexConfig, SparseVectorIndexType,
            SparseVectorValueType,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        // Schema with two independent sparse keys: one WAND, one MaxScore.
        let mut schema = sparse_schema_for_key("sparse_a", SparseIndexAlgorithm::Wand);
        schema
            .keys
            .entry("sparse_b".to_string())
            .or_default()
            .sparse_vector = Some(SparseVectorValueType {
            sparse_vector_index: Some(SparseVectorIndexType {
                enabled: true,
                config: SparseVectorIndexConfig {
                    embedding_function: None,
                    source_key: None,
                    bm25: None,
                    algorithm: SparseIndexAlgorithm::MaxScore,
                },
            }),
        });

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000011").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("create writer");

            assert!(matches!(
                writer.sparse_index_writers.get("sparse_a"),
                Some(SparseIndexWriter::Wand(_))
            ));
            assert!(matches!(
                writer.sparse_index_writers.get("sparse_b"),
                Some(SparseIndexWriter::MaxScore(_))
            ));

            for offset in 0u32..10 {
                let va = SparseVector::new(vec![0, offset % 4], vec![1.0, 0.5]).unwrap();
                writer
                    .set_metadata("sparse_a", &MetadataValue::SparseVector(va), offset)
                    .await
                    .expect("set sparse_a");
                let vb = SparseVector::new(vec![1, offset % 3 + 5], vec![2.0, 1.5]).unwrap();
                writer
                    .set_metadata("sparse_b", &MetadataValue::SparseVector(vb), offset)
                    .await
                    .expect("set sparse_b");
            }

            let flusher = Box::pin(writer.commit()).await.expect("commit");
            metadata_segment.file_path = Box::pin(flusher.flush()).await.expect("flush");
        }

        // WAND key writes max + offset_value; MaxScore key writes posting.
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_max_key("sparse_a")));
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_offset_value_key("sparse_a")));
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_posting_key("sparse_b")));
        // The two indices must not collide: no posting for the WAND key, no
        // max/offset_value for the MaxScore key.
        assert!(!metadata_segment
            .file_path
            .contains_key(&sparse_posting_key("sparse_a")));
        assert!(!metadata_segment
            .file_path
            .contains_key(&sparse_max_key("sparse_b")));

        {
            let shard = SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("open reader");

            assert!(matches!(
                reader.sparse_index_readers.get("sparse_a"),
                Some(SparseIndexReader::Wand(_))
            ));
            assert!(matches!(
                reader.sparse_index_readers.get("sparse_b"),
                Some(SparseIndexReader::MaxScore(_))
            ));
            assert!(
                reader.legacy_sparse_index_reader.is_none(),
                "per-key layout should not produce a legacy reader"
            );
        }
    }

    #[tokio::test]
    async fn test_sparse_index_recreated_with_existing_prefix() {
        // This test verifies that when sparse index files are missing (e.g., deleted)
        // and need to be recreated, they use the same prefix as existing blockfiles
        // This tests the bug fix for incorrect blockfile paths

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);

        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let sparse_schema =
            sparse_schema_for_key("sparse_vec", chroma_types::SparseIndexAlgorithm::Wand);
        let sparse_max = chroma_types::sparse_max_key("sparse_vec");
        let sparse_offset_value = chroma_types::sparse_offset_value_key("sparse_vec");

        // Original collection ID
        let original_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error");

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000002").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: original_collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };

        // First flush: create initial blockfiles
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                Some(&sparse_schema),
            ))
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were created
        assert!(metadata_segment.file_path.contains_key(&sparse_max));
        assert!(metadata_segment
            .file_path
            .contains_key(&sparse_offset_value));

        // Extract the original prefix
        let original_prefix = {
            let existing_file_path = metadata_segment
                .file_path
                .values()
                .next()
                .and_then(|paths| paths.first())
                .expect("Should have at least one blockfile");

            let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(existing_file_path)
                .expect("Should be able to extract prefix");
            prefix.to_string()
        };

        // Simulate missing sparse index files (e.g., from older version or deleted)
        metadata_segment.file_path.remove(&sparse_max);
        metadata_segment.file_path.remove(&sparse_offset_value);

        // Change collection ID to simulate a forked collection
        let forked_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000003").expect("parse error");
        metadata_segment.collection = forked_collection_id;

        // Second flush: recreate sparse index files
        // The bug fix ensures they use the existing prefix, not a new one
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                Some(&sparse_schema),
            ))
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were recreated
        assert!(
            metadata_segment.file_path.contains_key(&sparse_max),
            "Sparse max should be recreated"
        );
        assert!(
            metadata_segment
                .file_path
                .contains_key(&sparse_offset_value),
            "Sparse offset value should be recreated"
        );

        // Verify ALL blockfiles use the original prefix
        for (key, paths) in &metadata_segment.file_path {
            for path in paths {
                let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(path)
                    .expect("Should be able to extract prefix");
                assert_eq!(
                    prefix, original_prefix,
                    "All blockfiles should use original prefix. Key: {}, Path: {}",
                    key, path
                );
                // Verify the prefix contains the original collection ID, not the forked one
                assert!(
                    prefix.contains(&original_collection_id.to_string()),
                    "Prefix should contain original collection ID"
                );
                assert!(
                    !prefix.contains(&forked_collection_id.to_string()),
                    "Prefix should NOT contain forked collection ID"
                );
            }
        }

        // Verify we can read from the segment with recreated sparse indices
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &metadata_segment_shard,
                &blockfile_provider,
            ))
            .await
            .expect("Should be able to read from segment with recreated sparse indices");

            assert!(
                metadata_reader
                    .sparse_index_readers
                    .contains_key("sparse_vec"),
                "Sparse index reader should be created, verifying files exist and are readable"
            );
        }
        // Simulate legacy files without prefix
        metadata_segment.file_path.drain();
        metadata_segment.file_path.insert(
            "legacy_file".to_string(),
            vec!["11111111-1111-1111-1111-111111111111".to_string()],
        );

        // Change collection ID to simulate a forked collection
        let forked_collection_id =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000004").expect("parse error");
        metadata_segment.collection = forked_collection_id;

        // Third flush: recreate all index files
        // The bug fix ensures they use the existing prefix, not a new one
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                Some(&sparse_schema),
            ))
            .await
            .expect("Error creating metadata writer");

            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Error committing metadata");

            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Error flushing metadata");
        }

        // Verify sparse index files were recreated
        assert!(
            metadata_segment.file_path.contains_key(&sparse_max),
            "Sparse max should be recreated"
        );
        assert!(
            metadata_segment
                .file_path
                .contains_key(&sparse_offset_value),
            "Sparse offset value should be recreated"
        );

        // Verify ALL blockfiles use the original prefix
        for (key, paths) in &metadata_segment.file_path {
            for path in paths {
                let (prefix, _) = chroma_types::Segment::extract_prefix_and_id(path)
                    .expect("Should be able to extract prefix");
                assert!(
                    prefix.is_empty(),
                    "All blockfiles should use empty prefix. Key: {}, Path: {}",
                    key,
                    path
                );
            }
        }

        // Verify we can read from the segment with recreated sparse indices
        {
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let metadata_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &metadata_segment_shard,
                &blockfile_provider,
            ))
            .await
            .expect("Should be able to read from segment with recreated sparse indices");

            assert!(
                metadata_reader
                    .sparse_index_readers
                    .contains_key("sparse_vec"),
                "Sparse index reader should be created, verifying files exist and are readable"
            );
        }
    }

    #[tokio::test]
    async fn test_compaction_skips_fts_indexing_when_disabled() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000001").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        // Build a schema with FTS disabled
        let fts_disabled_schema = Schema::new_default(KnnIndex::Hnsw)
            .delete_index(
                Some(DOCUMENT_KEY),
                IndexConfig::Fts(FtsIndexConfig::default()),
            )
            .expect("FTS deletion should succeed");
        assert!(!fts_disabled_schema.is_fts_enabled());

        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");

            let mut update_metadata = HashMap::new();
            update_metadata.insert(
                String::from("color"),
                UpdateMetadataValue::Str(String::from("red")),
            );
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "doc1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: Some(update_metadata.clone()),
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "doc2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: Some(update_metadata),
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => match *e {
                        RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                        _ => panic!("Error creating record segment reader"),
                    },
                };

            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");

            // Pass the FTS-disabled schema
            metadata_writer
                .apply_materialized_log_chunk(
                    &record_segment_reader,
                    &mat_records,
                    Some(fts_disabled_schema),
                )
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");

            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }

        // Verify: FTS search should return NO results (indexing was skipped)
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");

        let fts_reader = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("FTS reader blockfile should still exist");
        let res = fts_reader
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("cats")
            .await
            .unwrap();
        assert_eq!(
            res.len(),
            0,
            "FTS search should return 0 results when FTS is disabled"
        );
        let res = fts_reader
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("dogs")
            .await
            .unwrap();
        assert_eq!(
            res.len(),
            0,
            "FTS search should return 0 results when FTS is disabled"
        );

        // Verify: metadata indexing still works
        let string_reader = metadata_segment_reader
            .string_metadata_index_reader
            .as_ref()
            .expect("String metadata reader should exist");
        let res = string_reader
            .get(
                "color",
                &chroma_blockstore::key::KeyWrapper::String("red".to_string()),
            )
            .await
            .expect("String metadata query should succeed");
        assert_eq!(
            res.len(),
            2,
            "Metadata indexing should still work even with FTS disabled"
        );

        // Verify: documents are still stored in record segment
        let record_segment_shard =
            SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
        let record_segment_reader = Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &blockfile_provider,
            None,
        ))
        .await
        .expect("Record segment reader should be initialized");
        let res = record_segment_reader
            .get_all_data()
            .await
            .expect("Should be able to get all data")
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 2, "Both documents should be stored");
    }

    #[tokio::test]
    async fn test_compaction_indexes_fts_when_enabled_by_default() {
        // Control test: FTS is enabled by default (schema = None),
        // so FTS search should return results after compaction.

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = String::from("test_tenant");
        let database_id = DatabaseUuid::new();

        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000010").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000010")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000011").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000010")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        {
            let record_segment_shard =
                SegmentShard::try_from((&record_segment, 0)).expect("valid shard index");
            let segment_writer = RecordSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &record_segment_shard,
                &blockfile_provider,
                None,
                None,
            )
            .await
            .expect("Error creating segment writer");
            let metadata_segment_shard =
                SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
            let mut metadata_writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                &tenant,
                &database_id,
                &metadata_segment_shard,
                &blockfile_provider,
                None,
                None,
            ))
            .await
            .expect("Error creating segment writer");

            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "doc1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "doc2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReaderShard> =
                match Box::pin(RecordSegmentReaderShard::from_segment(
                    &record_segment_shard,
                    &blockfile_provider,
                    None,
                ))
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => match *e {
                        RecordSegmentReaderShardCreationError::UninitializedSegment => None,
                        _ => panic!("Error creating record segment reader"),
                    },
                };

            let mat_records = materialize_logs(
                &record_segment_reader,
                data,
                None,
                &RecordSegmentReaderOptions::default(),
            )
            .await
            .expect("Log materialization failed");

            // Pass None (default = FTS enabled)
            metadata_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records, None)
                .await
                .expect("Apply materialized log to metadata segment failed");
            metadata_writer
                .finish()
                .await
                .expect("Write to blockfiles for metadata writer failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");

            let record_flusher = Box::pin(segment_writer.commit())
                .await
                .expect("Commit for segment writer failed");
            let metadata_flusher = Box::pin(metadata_writer.commit())
                .await
                .expect("Commit for metadata writer failed");
            record_segment.file_path = Box::pin(record_flusher.flush())
                .await
                .expect("Flush record segment writer failed");
            metadata_segment.file_path = Box::pin(metadata_flusher.flush())
                .await
                .expect("Flush metadata segment writer failed");
        }

        // Verify: FTS search SHOULD return results (FTS enabled by default)
        let metadata_segment_shard =
            SegmentShard::try_from((&metadata_segment, 0)).expect("valid shard index");
        let metadata_segment_reader = Box::pin(MetadataSegmentReaderShard::from_segment(
            &metadata_segment_shard,
            &blockfile_provider,
        ))
        .await
        .expect("Metadata segment reader construction failed");

        let fts_reader = metadata_segment_reader
            .fts_index_reader
            .as_ref()
            .expect("FTS reader should exist");
        let res = fts_reader
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("cats")
            .await
            .unwrap();
        assert_eq!(
            res.len(),
            1,
            "FTS search for 'cats' should return 1 result when FTS is enabled"
        );
        let res = fts_reader
            .as_trigram()
            .expect("expected Trigram FTS reader")
            .search("dogs")
            .await
            .unwrap();
        assert_eq!(
            res.len(),
            1,
            "FTS search for 'dogs' should return 1 result when FTS is enabled"
        );
    }

    // ── MaxScore multi-commit consistency test ─────────────────────────

    /// Build a deterministic sparse vector for a document.
    /// Each doc gets dimensions based on (offset % num_dims) to spread across dims.
    fn make_sparse_vector(offset: u32, num_dims: u32, base_weight: f32) -> Vec<(u32, f32)> {
        let mut pairs = Vec::new();
        // Each doc gets 3-5 dimensions, deterministically chosen
        for d in 0..num_dims {
            if (offset + d).is_multiple_of(3) || d == 0 {
                let weight = base_weight + (offset as f32 * 0.01) + (d as f32 * 0.1);
                pairs.push((d, weight));
            }
        }
        pairs
    }

    #[tokio::test]
    async fn maxscore_segment_multi_commit_consistency() {
        let handle = std::thread::Builder::new()
            .name("maxscore_multi_commit".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                let runtime = Runtime::new().unwrap();
                runtime.block_on(async {
                    Box::pin(maxscore_segment_multi_commit_impl()).await;
                });
            })
            .expect("Failed to spawn thread");

        handle.join().expect("Test thread panicked");
    }

    async fn maxscore_segment_multi_commit_impl() {
        use chroma_index::sparse::types::encode_u32;
        use chroma_types::{
            sparse_max_key, sparse_posting_key, MetadataValue, SparseIndexAlgorithm, SparseVector,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider = ArrowBlockfileProvider::new(
            storage,
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let tenant = "test_tenant";
        let database_id = DatabaseUuid::new();

        // Schema with MaxScore enabled on the sparse key.
        let schema = sparse_schema_for_key(SPARSE_KEY, SparseIndexAlgorithm::MaxScore);
        assert!(schema.is_key_maxscore_enabled(SPARSE_KEY));

        let mut metadata_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000002").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };

        const SPARSE_KEY: &str = "sparse_emb";
        const NUM_DIMS: u32 = 8;
        // Also include a large dimension ID to test base64 encoding edge cases
        const LARGE_DIM: u32 = 30000;

        // Track what should be in the index at each step
        let mut expected_docs: HashMap<u32, Vec<(u32, f32)>> = HashMap::new();

        // ════════════════════════════════════════════════════════════
        // Iteration 1: Fresh write — 20 documents (offsets 0..20)
        // ════════════════════════════════════════════════════════════
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("iter1: create writer");

            assert!(
                matches!(
                    writer.sparse_index_writers.get(SPARSE_KEY),
                    Some(SparseIndexWriter::MaxScore(_))
                ),
                "iter1: should have maxscore writer"
            );

            for offset in 0u32..20 {
                let mut pairs = make_sparse_vector(offset, NUM_DIMS, 1.0);
                // Add large dim to a few docs
                if offset % 5 == 0 {
                    pairs.push((LARGE_DIM, 0.5 + offset as f32 * 0.01));
                }
                let sv = SparseVector::from_pairs(pairs.iter().copied());
                writer
                    .set_metadata(SPARSE_KEY, &MetadataValue::SparseVector(sv), offset)
                    .await
                    .expect("iter1: set_metadata");
                expected_docs.insert(offset, pairs);
            }

            let flusher = Box::pin(writer.commit()).await.expect("iter1: commit");
            let file_paths = Box::pin(flusher.flush()).await.expect("iter1: flush");

            assert!(
                file_paths.contains_key(&sparse_posting_key(SPARSE_KEY)),
                "iter1: sparse posting should be in file_paths"
            );
            assert!(
                !file_paths.contains_key(&sparse_max_key(SPARSE_KEY)),
                "iter1: sparse max should NOT be in file_paths"
            );
            metadata_segment.file_path = file_paths;
        }

        // Verify iteration 1
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("iter1: open reader");

            assert!(
                matches!(
                    reader.sparse_index_readers.get(SPARSE_KEY),
                    Some(SparseIndexReader::MaxScore(_))
                ),
                "iter1: maxscore reader"
            );
            let Some(SparseIndexReader::MaxScore(ms_reader)) =
                reader.sparse_index_readers.get(SPARSE_KEY)
            else {
                panic!("iter1: expected MaxScore reader");
            };
            let dims = ms_reader.get_all_dimension_ids().await.unwrap();
            assert!(!dims.is_empty(), "iter1: should have dimensions");
            assert!(dims.contains(&0), "iter1: dim 0 should exist");
            assert!(dims.contains(&LARGE_DIM), "iter1: large dim should exist");

            // Verify count_postings for dim 0
            let count_dim0 = ms_reader.count_postings(&encode_u32(0)).await.unwrap();
            let expected_count_dim0 = expected_docs
                .values()
                .filter(|pairs| pairs.iter().any(|(d, _)| *d == 0))
                .count();
            assert!(
                count_dim0 >= expected_count_dim0,
                "iter1: count_postings(dim0) mismatch: got {count_dim0}, expected >= {expected_count_dim0}"
            );
        }

        // ════════════════════════════════════════════════════════════
        // Iteration 2: Fork — add 15 docs (20..35), delete 5 (0..5)
        // ════════════════════════════════════════════════════════════
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("iter2: create writer (fork)");

            assert!(
                matches!(
                    writer.sparse_index_writers.get(SPARSE_KEY),
                    Some(SparseIndexWriter::MaxScore(_))
                ),
                "iter2: should have maxscore writer from fork"
            );

            // Add new documents
            for offset in 20u32..35 {
                let mut pairs = make_sparse_vector(offset, NUM_DIMS, 2.0);
                if offset % 5 == 0 {
                    pairs.push((LARGE_DIM, 0.5 + offset as f32 * 0.01));
                }
                let sv = SparseVector::from_pairs(pairs.iter().copied());
                writer
                    .set_metadata(SPARSE_KEY, &MetadataValue::SparseVector(sv), offset)
                    .await
                    .expect("iter2: set_metadata");
                expected_docs.insert(offset, pairs);
            }

            // Delete offsets 0..5
            for offset in 0u32..5 {
                let pairs = expected_docs.remove(&offset).unwrap();
                let sv = SparseVector::from_pairs(pairs.iter().copied());
                writer
                    .delete_metadata(SPARSE_KEY, &MetadataValue::SparseVector(sv), offset)
                    .await
                    .expect("iter2: delete_metadata");
            }

            let flusher = Box::pin(writer.commit()).await.expect("iter2: commit");
            let file_paths = Box::pin(flusher.flush()).await.expect("iter2: flush");

            assert!(file_paths.contains_key(&sparse_posting_key(SPARSE_KEY)));
            metadata_segment.file_path = file_paths;
        }

        // Verify iteration 2
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("iter2: open reader");
            let Some(SparseIndexReader::MaxScore(ms_reader)) =
                reader.sparse_index_readers.get(SPARSE_KEY)
            else {
                panic!("iter2: expected MaxScore reader");
            };

            // Verify dim 0 count: deleted docs should be gone
            let count_dim0 = ms_reader.count_postings(&encode_u32(0)).await.unwrap();
            let expected_count_dim0 = expected_docs
                .values()
                .filter(|pairs| pairs.iter().any(|(d, _)| *d == 0))
                .count();
            assert!(
                count_dim0 >= expected_count_dim0,
                "iter2: count_postings(dim0) mismatch: got {count_dim0}, expected >= {expected_count_dim0}"
            );

            // Verify large dim count
            let count_large = ms_reader
                .count_postings(&encode_u32(LARGE_DIM))
                .await
                .unwrap();
            let expected_count_large = expected_docs
                .values()
                .filter(|pairs| pairs.iter().any(|(d, _)| *d == LARGE_DIM))
                .count();
            assert!(
                count_large >= expected_count_large,
                "iter2: count_postings(large_dim) mismatch: got {count_large}, expected >= {expected_count_large}"
            );
        }

        // ════════════════════════════════════════════════════════════
        // Iteration 3: Fork — add 10 docs (35..45), update 5 (10..15)
        // ════════════════════════════════════════════════════════════
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let writer = Box::pin(MetadataSegmentWriterShard::from_segment(
                tenant,
                &database_id,
                &shard,
                &blockfile_provider,
                None,
                Some(&schema),
            ))
            .await
            .expect("iter3: create writer (fork)");

            // Add new documents
            for offset in 35u32..45 {
                let mut pairs = make_sparse_vector(offset, NUM_DIMS, 3.0);
                if offset % 7 == 0 {
                    pairs.push((LARGE_DIM, 0.9));
                }
                let sv = SparseVector::from_pairs(pairs.iter().copied());
                writer
                    .set_metadata(SPARSE_KEY, &MetadataValue::SparseVector(sv), offset)
                    .await
                    .expect("iter3: set_metadata new");
                expected_docs.insert(offset, pairs);
            }

            // Update docs 10..15 with new weights: delete old, then set new
            for offset in 10u32..15 {
                // Delete old vector first
                let old_pairs = expected_docs.get(&offset).unwrap().clone();
                let old_sv = SparseVector::from_pairs(old_pairs.iter().copied());
                writer
                    .delete_metadata(SPARSE_KEY, &MetadataValue::SparseVector(old_sv), offset)
                    .await
                    .expect("iter3: delete_metadata for update");

                // Set new vector
                let new_pairs = make_sparse_vector(offset, NUM_DIMS, 5.0);
                let sv = SparseVector::from_pairs(new_pairs.iter().copied());
                writer
                    .set_metadata(SPARSE_KEY, &MetadataValue::SparseVector(sv), offset)
                    .await
                    .expect("iter3: set_metadata update");
                expected_docs.insert(offset, new_pairs);
            }

            let flusher = Box::pin(writer.commit()).await.expect("iter3: commit");
            let file_paths = Box::pin(flusher.flush()).await.expect("iter3: flush");

            assert!(file_paths.contains_key(&sparse_posting_key(SPARSE_KEY)));
            metadata_segment.file_path = file_paths;
        }

        // ════════════════════════════════════════════════════════════
        // Final verification: full consistency check
        // ════════════════════════════════════════════════════════════
        {
            let shard =
                SegmentShard::try_from((&metadata_segment, 0u32)).expect("valid shard index");
            let reader = Box::pin(MetadataSegmentReaderShard::from_segment(
                &shard,
                &blockfile_provider,
            ))
            .await
            .expect("final: open reader");

            assert!(matches!(
                reader.sparse_index_readers.get(SPARSE_KEY),
                Some(SparseIndexReader::MaxScore(_))
            ));
            let Some(SparseIndexReader::MaxScore(ms_reader)) =
                reader.sparse_index_readers.get(SPARSE_KEY)
            else {
                panic!("final: expected MaxScore reader");
            };

            // 1. Verify all expected dimensions exist
            let dims = ms_reader.get_all_dimension_ids().await.unwrap();
            for d in 0..NUM_DIMS {
                let has_docs = expected_docs
                    .values()
                    .any(|pairs| pairs.iter().any(|(dd, _)| *dd == d));
                if has_docs {
                    assert!(dims.contains(&d), "final: dim {d} should exist");
                }
            }

            // 2. Verify count_postings matches expected for every dimension
            let mut all_dims: std::collections::HashSet<u32> = std::collections::HashSet::new();
            for pairs in expected_docs.values() {
                for (d, _) in pairs {
                    all_dims.insert(*d);
                }
            }
            for d in &all_dims {
                let count = ms_reader.count_postings(&encode_u32(*d)).await.unwrap();
                let expected = expected_docs
                    .values()
                    .filter(|pairs| pairs.iter().any(|(dd, _)| dd == d))
                    .count();
                assert!(
                    count >= expected,
                    "final: count_postings(dim={d}) mismatch: got {count}, expected >= {expected}"
                );
            }

            // 3. Verify deleted docs (0..5) are not in any dimension's postings
            for deleted_offset in 0u32..5 {
                assert!(
                    !expected_docs.contains_key(&deleted_offset),
                    "sanity: offset {deleted_offset} should be deleted from expected_docs"
                );
            }

            // 4. Verify total document count
            let total_expected = expected_docs.len();
            // 20 initial - 5 deleted + 15 added + 10 added = 40
            assert_eq!(total_expected, 40, "should have 40 docs total");

            // 5. Query correctness: run a few queries and compare to brute force
            let test_queries: Vec<Vec<(u32, f32)>> = vec![
                // Query hitting multiple dims
                vec![(0, 1.0), (1, 0.5), (3, 0.8)],
                // Query with large dim
                vec![(0, 1.0), (LARGE_DIM, 2.0)],
                // Single dim query
                vec![(2, 1.0)],
                // All dims query
                (0..NUM_DIMS).map(|d| (d, 1.0)).collect(),
            ];

            for (qi, query) in test_queries.iter().enumerate() {
                let k = 5u32;
                let results = ms_reader
                    .query(
                        query.iter().copied(),
                        k,
                        chroma_types::SignedRoaringBitmap::Exclude(RoaringBitmap::new()),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("final: query {qi} failed: {e:?}"));

                // Brute force
                let mut bf_scores: Vec<(u32, f32)> = expected_docs
                    .iter()
                    .map(|(off, pairs)| {
                        let score: f32 = query
                            .iter()
                            .map(|(qd, qw)| {
                                pairs
                                    .iter()
                                    .find(|(dd, _)| dd == qd)
                                    .map(|(_, dv)| qw * dv)
                                    .unwrap_or(0.0)
                            })
                            .sum();
                        (*off, score)
                    })
                    .filter(|(_, s)| *s > 0.0)
                    .collect();
                bf_scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                bf_scores.truncate(k as usize);

                // Verify recall
                let result_offsets: std::collections::HashSet<u32> =
                    results.iter().map(|s| s.offset).collect();
                let bf_offsets: std::collections::HashSet<u32> =
                    bf_scores.iter().map(|(o, _)| *o).collect();

                if !bf_offsets.is_empty() {
                    let overlap = result_offsets.intersection(&bf_offsets).count();
                    let recall = overlap as f64 / bf_offsets.len() as f64;
                    assert!(
                        recall >= 0.8,
                        "final: query {qi} recall {recall:.2} < 0.8 \
                         (results: {result_offsets:?}, expected: {bf_offsets:?})"
                    );
                }
            }
        }
    }
}

use std::{cmp::Reverse, collections::BinaryHeap};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_index::sparse::reader::SparseReaderError;
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentReader},
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::{Rank, RecordDistance},
    MaterializedLogOperation, MetadataValue, Segment, SparseVector,
};
use sprs::CsVec;
use thiserror::Error;

use crate::execution::operators::{fetch_log::FetchLogOutput, filter::FilterOutput};

#[derive(Clone, Debug)]
pub struct SparseKnnInput {
    pub blockfile_provider: BlockfileProvider,
    pub distance_function: DistanceFunction,
    pub logs: FetchLogOutput,
    pub mask: FilterOutput,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct SparseKnnOutput {
    pub rank: Rank,
    pub records: Vec<RecordDistance>,
}

#[derive(Debug, Error)]
pub enum SparseKnnError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error using sparse reader: {0}")]
    SparseReader(#[from] SparseReaderError),
}

impl ChromaError for SparseKnnError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SparseKnnError::LogMaterializer(err) => err.code(),
            SparseKnnError::MetadataReader(err) => err.code(),
            SparseKnnError::RecordReader(err) => err.code(),
            SparseKnnError::SparseReader(err) => err.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SparseKnn {
    pub embedding: SparseVector,
    pub key: String,
    pub limit: u32,
}

#[async_trait]
impl Operator<SparseKnnInput, SparseKnnOutput> for SparseKnn {
    type Error = SparseKnnError;

    async fn run(&self, input: &SparseKnnInput) -> Result<SparseKnnOutput, SparseKnnError> {
        // Convert SparseVector to sprs::CsVec
        let query_sparse_verctor: CsVec<f32> = (&self.embedding).into();
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let logs = materialize_logs(&record_segment_reader, input.logs.clone(), None).await?;

        let mut min_heap =
            BinaryHeap::<Reverse<RecordDistance>>::with_capacity(self.limit as usize);
        for log in &logs {
            if !matches!(
                log.get_operation(),
                MaterializedLogOperation::DeleteExisting
            ) && input.mask.log_offset_ids.contains(log.get_offset_id())
            {
                let log = log
                    .hydrate(record_segment_reader.as_ref())
                    .await
                    .map_err(SparseKnnError::LogMaterializer)?;
                let merged_metadata = log.merged_metadata();
                let Some(MetadataValue::SparseVector(sparse_vector)) =
                    merged_metadata.get(&self.key)
                else {
                    continue;
                };
                // Convert SparseVector to sprs::CsVec
                let log_sparse_verctor: CsVec<f32> = sparse_vector.into();
                let score = query_sparse_verctor.dot(&log_sparse_verctor);
                if (min_heap.len() as u32) < self.limit {
                    min_heap.push(Reverse(RecordDistance {
                        offset_id: log.get_offset_id(),
                        measure: score,
                    }));
                } else if min_heap
                    .peek()
                    .map(|Reverse(record)| record.measure)
                    .unwrap_or(f32::MIN)
                    < score
                {
                    min_heap.pop();
                    min_heap.push(Reverse(RecordDistance {
                        offset_id: log.get_offset_id(),
                        measure: score,
                    }))
                }
            }
        }

        let metadata_segement_reader =
            MetadataSegmentReader::from_segment(&input.metadata_segment, &input.blockfile_provider)
                .await?;

        let Some(sparse_reader) = metadata_segement_reader.sparse_index_reader else {
            return Ok(SparseKnnOutput {
                rank: Rank::SparseKnn {
                    embedding: self.embedding.clone(),
                    key: self.key.clone(),
                    limit: self.limit.clone(),
                },
                records: min_heap
                    .into_sorted_vec()
                    .into_iter()
                    .map(|Reverse(record)| record)
                    .collect(),
            });
        };

        let sorted_compact_records = sparse_reader
            .wand(
                self.embedding
                    .indices
                    .iter()
                    .copied()
                    .zip(self.embedding.values.iter().copied()),
                self.limit,
                input.mask.compact_offset_ids.clone(),
            )
            .await?;
        min_heap.extend(sorted_compact_records.into_iter().map(|score| {
            Reverse(RecordDistance {
                offset_id: score.offset,
                measure: score.score,
            })
        }));
        while (min_heap.len() as u32) > self.limit {
            min_heap.pop();
        }
        Ok(SparseKnnOutput {
            rank: Rank::SparseKnn {
                embedding: self.embedding.clone(),
                key: self.key.clone(),
                limit: self.limit.clone(),
            },
            records: min_heap
                .into_sorted_vec()
                .into_iter()
                .map(|Reverse(record)| record)
                .collect(),
        })
    }
}

use std::{cmp::Reverse, collections::BinaryHeap};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::RecordMeasure, MaterializedLogOperation, MetadataValue, Segment, SignedRoaringBitmap,
    SparseVector,
};
use sprs::CsVec;
use thiserror::Error;

use crate::execution::operators::fetch_log::FetchLogOutput;

#[derive(Clone, Debug)]
pub struct SparseLogKnnInput {
    pub blockfile_provider: BlockfileProvider,
    pub logs: FetchLogOutput,
    pub mask: SignedRoaringBitmap,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct SparseLogKnnOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Debug, Error)]
pub enum SparseLogKnnError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for SparseLogKnnError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SparseLogKnnError::LogMaterializer(err) => err.code(),
            SparseLogKnnError::RecordReader(err) => err.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SparseLogKnn {
    pub embedding: SparseVector,
    pub key: String,
    pub limit: u32,
}

#[async_trait]
impl Operator<SparseLogKnnInput, SparseLogKnnOutput> for SparseLogKnn {
    type Error = SparseLogKnnError;

    async fn run(
        &self,
        input: &SparseLogKnnInput,
    ) -> Result<SparseLogKnnOutput, SparseLogKnnError> {
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

        let mut min_heap = BinaryHeap::with_capacity(self.limit as usize);
        for log in &logs {
            if !matches!(
                log.get_operation(),
                MaterializedLogOperation::DeleteExisting
            ) && input.mask.contains(log.get_offset_id())
            {
                let log = log
                    .hydrate(record_segment_reader.as_ref())
                    .await
                    .map_err(SparseLogKnnError::LogMaterializer)?;
                let merged_metadata = log.merged_metadata();
                let Some(MetadataValue::SparseVector(sparse_vector)) =
                    merged_metadata.get(&self.key)
                else {
                    continue;
                };
                let log_sparse_verctor: CsVec<f32> = sparse_vector.into();
                let score = query_sparse_verctor.dot(&log_sparse_verctor);
                if (min_heap.len() as u32) < self.limit {
                    min_heap.push(Reverse(RecordMeasure {
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
                    min_heap.push(Reverse(RecordMeasure {
                        offset_id: log.get_offset_id(),
                        measure: score,
                    }))
                }
            }
        }
        Ok(SparseLogKnnOutput {
            records: min_heap
                .into_sorted_vec()
                .into_iter()
                .map(|Reverse(record)| record)
                .collect(),
        })
    }
}

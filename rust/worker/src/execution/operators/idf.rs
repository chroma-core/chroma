use std::collections::HashMap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_index::sparse::{reader::SparseReaderError, types::encode_u32};
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentReader},
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    MaterializedLogOperation, MetadataValue, Segment, SignedRoaringBitmap, SparseVector,
};
use thiserror::Error;

use crate::execution::operators::fetch_log::FetchLogOutput;

/// Calculates the inverse document frequency (idf) for the dimensions present in the embedding
/// and scales the embedding correspondingly. The formula is:
///     idf(t) = ln((n - n_t + 0.5) / (n_t + 0.5) + 1)
/// where
///     n: total number of documents in the collection
///     n_t: number of documents with term t

#[derive(Debug)]
pub struct Idf {
    pub embedding: SparseVector,
    pub key: String,
}

#[derive(Debug)]
pub struct IdfInput {
    pub blockfile_provider: BlockfileProvider,
    pub logs: FetchLogOutput,
    pub mask: SignedRoaringBitmap,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct IdfOutput {
    pub scaled_embedding: SparseVector,
}

#[derive(Debug, Error)]
pub enum IdfError {
    #[error(transparent)]
    Chroma(#[from] Box<dyn ChromaError>),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error using sparse reader: {0}")]
    SparseReader(#[from] SparseReaderError),
}

impl ChromaError for IdfError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            IdfError::Chroma(err) => err.code(),
            IdfError::LogMaterializer(err) => err.code(),
            IdfError::MetadataReader(err) => err.code(),
            IdfError::RecordReader(err) => err.code(),
            IdfError::SparseReader(err) => err.code(),
        }
    }
}

#[async_trait]
impl Operator<IdfInput, IdfOutput> for Idf {
    type Error = IdfError;

    async fn run(&self, input: &IdfInput) -> Result<IdfOutput, IdfError> {
        let mut n = 0;
        let mut nts = HashMap::new();
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => {
                n += reader.count().await?;
                Ok(Some(reader))
            }
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let logs = materialize_logs(&record_segment_reader, input.logs.clone(), None).await?;

        let metadata_segement_reader =
            MetadataSegmentReader::from_segment(&input.metadata_segment, &input.blockfile_provider)
                .await?;

        if let Some(sparse_index_reader) = metadata_segement_reader.sparse_index_reader.as_ref() {
            for &dimension_id in &self.embedding.indices {
                let encoded_dimension_id = encode_u32(dimension_id);
                let nt = sparse_index_reader
                    .get_dimension_offset_rank(&encoded_dimension_id, u32::MAX)
                    .await?
                    .saturating_sub(
                        sparse_index_reader
                            .get_dimension_offset_rank(&encoded_dimension_id, 0)
                            .await?,
                    );
                nts.insert(dimension_id, nt);
            }
        }

        for log in &logs {
            let log = log
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(IdfError::LogMaterializer)?;

            if match log.get_operation() {
                MaterializedLogOperation::Initial | MaterializedLogOperation::AddNew => false,
                MaterializedLogOperation::OverwriteExisting
                | MaterializedLogOperation::DeleteExisting => true,
                MaterializedLogOperation::UpdateExisting => log
                    .get_metadata_to_be_merged()
                    .map(|meta| matches!(meta.get(&self.key), Some(MetadataValue::SparseVector(_))))
                    .unwrap_or_default(),
            } {
                if let Some(MetadataValue::SparseVector(existing_embedding)) = log
                    .get_data_record()
                    .and_then(|record| record.metadata.as_ref())
                    .and_then(|meta| meta.get(&self.key))
                {
                    for index in &existing_embedding.indices {
                        if let Some(nt) = nts.get_mut(index) {
                            *nt = nt.saturating_sub(1);
                        }
                    }
                }
            }

            if let Some(MetadataValue::SparseVector(new_embedding)) = log
                .get_metadata_to_be_merged()
                .and_then(|meta| meta.get(&self.key))
            {
                for index in &new_embedding.indices {
                    if let Some(nt) = nts.get_mut(index) {
                        *nt = nt.saturating_add(1);
                    }
                }
            }

            n = match log.get_operation() {
                MaterializedLogOperation::Initial
                | MaterializedLogOperation::OverwriteExisting
                | MaterializedLogOperation::UpdateExisting => n,
                MaterializedLogOperation::AddNew => n.saturating_add(1),
                MaterializedLogOperation::DeleteExisting => n.saturating_sub(1),
            };
        }

        let scaled_embedding =
            SparseVector::from_pairs(self.embedding.iter().map(|(index, value)| {
                let nt = nts.get(&index).cloned().unwrap_or_default() as f32;
                let scale = ((n as f32 - nt + 0.5) / (nt + 0.5)).ln_1p();
                (index, scale * value)
            }));

        Ok(IdfOutput { scaled_embedding })
    }
}

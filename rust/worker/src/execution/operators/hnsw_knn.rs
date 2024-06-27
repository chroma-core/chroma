use crate::execution::data::data_chunk::Chunk;
use crate::segment::record_segment::RecordSegmentReaderCreationError;
use crate::segment::{LogMaterializer, LogMaterializerError, MaterializedLogRecord};
use crate::types::{LogRecord, Operation};
use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegmentReader, record_segment::RecordSegmentReader,
    },
    types::Segment,
};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug)]
pub struct HnswKnnOperator {}

#[derive(Debug)]
pub struct HnswKnnOperatorInput {
    pub segment: Box<DistributedHNSWSegmentReader>,
    pub query: Vec<f32>,
    pub k: usize,
    pub record_segment: Segment,
    pub blockfile_provider: BlockfileProvider,
    pub allowed_ids: Arc<[String]>,
    pub logs: Chunk<LogRecord>,
}

#[derive(Debug)]
pub struct HnswKnnOperatorOutput {
    pub offset_ids: Vec<usize>,
    pub distances: Vec<f32>,
}

#[derive(Error, Debug)]
pub enum HnswKnnOperatorError {
    #[error("Error creating Record Segment")]
    RecordSegmentError,
    #[error("Error reading Record Segment")]
    RecordSegmentReadError,
    #[error("Invalid allowed and disallowed ids")]
    InvalidAllowedAndDisallowedIds,
    #[error("Error materializing logs {0}")]
    LogMaterializationError(#[from] LogMaterializerError),
}

impl ChromaError for HnswKnnOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswKnnOperatorError::RecordSegmentError => ErrorCodes::Internal,
            HnswKnnOperatorError::RecordSegmentReadError => ErrorCodes::Internal,
            HnswKnnOperatorError::InvalidAllowedAndDisallowedIds => ErrorCodes::InvalidArgument,
            HnswKnnOperatorError::LogMaterializationError(e) => e.code(),
        }
    }
}

impl HnswKnnOperator {
    async fn get_disallowed_ids<'referred_data>(
        &self,
        logs: Chunk<MaterializedLogRecord<'_>>,
        record_segment_reader: &RecordSegmentReader<'_>,
    ) -> Result<Vec<u32>, Box<dyn ChromaError>> {
        let mut disallowed_ids = Vec::new();
        for item in logs.iter() {
            let log = item.0;
            if log.final_operation == Operation::Delete || log.final_operation == Operation::Update
            {
                let offset_id = record_segment_reader
                    .get_offset_id_for_user_id(log.merged_user_id_ref())
                    .await;
                match offset_id {
                    Ok(offset_id) => disallowed_ids.push(offset_id),
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }
        Ok(disallowed_ids)
    }

    // Validate that the allowed ids are not in the disallowed ids
    fn validate_allowed_and_disallowed_ids(
        &self,
        allowed_ids: &[u32],
        disallowed_ids: &[u32],
    ) -> Result<(), Box<dyn ChromaError>> {
        for allowed_id in allowed_ids {
            if disallowed_ids.contains(allowed_id) {
                return Err(Box::new(
                    HnswKnnOperatorError::InvalidAllowedAndDisallowedIds,
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Operator<HnswKnnOperatorInput, HnswKnnOperatorOutput> for HnswKnnOperator {
    type Error = Box<dyn ChromaError>;

    async fn run(
        &self,
        input: &HnswKnnOperatorInput,
    ) -> Result<HnswKnnOperatorOutput, Self::Error> {
        let record_segment_reader = match RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => match *e {
                RecordSegmentReaderCreationError::UninitializedSegment => {
                    tracing::error!(
                        "[HnswKnnOperation]: Error creating record segment reader {:?}",
                        *e
                    );
                    return Ok(HnswKnnOperatorOutput {
                        offset_ids: vec![],
                        distances: vec![],
                    });
                }
                _ => {
                    tracing::error!("[HnswKnnOperation]: Error creating record segment {:?}", e);
                    return Err(Box::new(HnswKnnOperatorError::RecordSegmentError));
                }
            },
        };
        let log_materializer = LogMaterializer::new(
            Some(record_segment_reader.clone()),
            input.logs.clone(),
            None,
        );
        let logs = match log_materializer.materialize().await {
            Ok(logs) => logs,
            Err(e) => {
                tracing::error!("[HnswKnnOperation]: Error materializing logs {:?}", e);
                return Err(Box::new(HnswKnnOperatorError::LogMaterializationError(e)));
            }
        };
        let mut remaining_allowed_ids: HashSet<&String> =
            HashSet::from_iter(input.allowed_ids.iter());
        for (log, _) in logs.iter() {
            remaining_allowed_ids.remove(&log.merged_user_id_ref().to_string());
        }
        // If a filter list is supplied but it does not have anything for the segment, as it implies the data is all in the log
        // then return an empty response.
        if !input.allowed_ids.is_empty() && remaining_allowed_ids.is_empty() {
            return Ok(HnswKnnOperatorOutput {
                offset_ids: vec![],
                distances: vec![],
            });
        }
        let mut allowed_offset_ids = Vec::new();
        for user_id in remaining_allowed_ids {
            let offset_id = record_segment_reader
                .get_offset_id_for_user_id(user_id)
                .await;
            match offset_id {
                Ok(offset_id) => allowed_offset_ids.push(offset_id),
                Err(e) => {
                    tracing::error!(
                        "[HnswKnnOperation]: Record segment read error for allowed ids {:?}",
                        e
                    );
                    return Err(Box::new(HnswKnnOperatorError::RecordSegmentReadError));
                }
            }
        }
        tracing::info!(
            "[HnswKnnOperation]: Allowed {} offset ids",
            allowed_offset_ids.len()
        );
        let disallowed_offset_ids =
            match self.get_disallowed_ids(logs, &record_segment_reader).await {
                Ok(disallowed_offset_ids) => disallowed_offset_ids,
                Err(e) => {
                    tracing::error!("[HnswKnnOperation]: Error fetching disallowed ids {:?}", e);
                    return Err(Box::new(HnswKnnOperatorError::RecordSegmentReadError));
                }
            };
        tracing::info!(
            "[HnswKnnOperation]: Disallowed {} offset ids",
            disallowed_offset_ids.len()
        );

        match self.validate_allowed_and_disallowed_ids(&allowed_offset_ids, &disallowed_offset_ids)
        {
            Ok(_) => {}
            Err(e) => {
                tracing::error!(
                    "[HnswKnnOperation]: Error validating allowed and disallowed ids {:?}",
                    e
                );
                return Err(e);
            }
        };

        // Convert to usize
        let allowed_offset_ids: Vec<usize> =
            allowed_offset_ids.iter().map(|&x| x as usize).collect();
        let disallowed_offset_ids: Vec<usize> =
            disallowed_offset_ids.iter().map(|&x| x as usize).collect();

        let (offset_ids, distances) = input.segment.query(
            &input.query,
            input.k,
            &allowed_offset_ids,
            &disallowed_offset_ids,
        );
        Ok(HnswKnnOperatorOutput {
            offset_ids,
            distances,
        })
    }
}

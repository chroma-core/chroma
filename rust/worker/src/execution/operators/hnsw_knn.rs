use crate::execution::data::data_chunk::Chunk;
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
}

impl ChromaError for HnswKnnOperatorError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswKnnOperatorError::RecordSegmentError => ErrorCodes::Internal,
            HnswKnnOperatorError::RecordSegmentReadError => ErrorCodes::Internal,
            HnswKnnOperatorError::InvalidAllowedAndDisallowedIds => ErrorCodes::InvalidArgument,
        }
    }
}

impl HnswKnnOperator {
    async fn get_disallowed_ids(
        &self,
        logs: Chunk<LogRecord>,
        record_segment_reader: &RecordSegmentReader<'_>,
    ) -> Result<Vec<u32>, Box<dyn ChromaError>> {
        let mut disallowed_ids = Vec::new();
        for item in logs.iter() {
            let log = item.0;
            let operation_record = &log.record;
            if operation_record.operation == Operation::Delete
                || operation_record.operation == Operation::Update
            {
                let offset_id = record_segment_reader
                    .get_offset_id_for_user_id(&operation_record.id)
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
            Err(e) => {
                return Err(Box::new(HnswKnnOperatorError::RecordSegmentError));
            }
        };
        let mut allowed_offset_ids = Vec::new();
        for user_id in input.allowed_ids.iter() {
            let offset_id = record_segment_reader
                .get_offset_id_for_user_id(user_id)
                .await;
            match offset_id {
                Ok(offset_id) => allowed_offset_ids.push(offset_id),
                Err(e) => {
                    return Err(Box::new(HnswKnnOperatorError::RecordSegmentReadError));
                }
            }
        }
        let disallowed_offset_ids = match self
            .get_disallowed_ids(input.logs.clone(), &record_segment_reader)
            .await
        {
            Ok(disallowed_offset_ids) => disallowed_offset_ids,
            Err(e) => {
                return Err(Box::new(HnswKnnOperatorError::RecordSegmentReadError));
            }
        };

        match self.validate_allowed_and_disallowed_ids(&allowed_offset_ids, &disallowed_offset_ids)
        {
            Ok(_) => {}
            Err(e) => {
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

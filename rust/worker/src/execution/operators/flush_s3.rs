use std::sync::Arc;

use crate::errors::ChromaError;
use crate::segment::SegmentFlusher;
use crate::types::SegmentFlushInfo;
use crate::{
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegment, record_segment::RecordSegmentWriter,
        SegmentWriter,
    },
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct FlushS3Operator {}

impl FlushS3Operator {
    pub fn new() -> Box<Self> {
        Box::new(FlushS3Operator {})
    }
}

#[derive(Debug)]
pub struct FlushS3Input {
    record_segment_writer: RecordSegmentWriter,
    hnsw_segment_writer: Box<DistributedHNSWSegment>,
}

impl FlushS3Input {
    pub fn new(
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegment>,
    ) -> Self {
        Self {
            record_segment_writer,
            hnsw_segment_writer,
        }
    }
}

#[derive(Debug)]
pub struct FlushS3Output {
    pub(crate) segment_flush_info: Arc<[SegmentFlushInfo]>,
}

pub type FlushS3Result = Result<FlushS3Output, Box<dyn ChromaError>>;

#[async_trait]
impl Operator<FlushS3Input, FlushS3Output> for FlushS3Operator {
    type Error = Box<dyn ChromaError>;

    async fn run(&self, input: &FlushS3Input) -> FlushS3Result {
        let record_segment_flusher = input.record_segment_writer.clone().commit();
        let record_segment_flush_info = match record_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.record_segment_writer.id;
                let res = flusher.flush().await;
                match res {
                    Ok(res) => {
                        println!("Record Segment Flushed");
                        SegmentFlushInfo {
                            segment_id,
                            file_paths: res,
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        };

        let hnsw_segment_flusher = input.hnsw_segment_writer.clone().commit();
        let hnsw_segment_flush_info = match hnsw_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.hnsw_segment_writer.id;
                let res = flusher.flush().await;
                match res {
                    Ok(res) => {
                        println!("HNSW Segment Flushed");
                        SegmentFlushInfo {
                            segment_id,
                            file_paths: res,
                        }
                    }
                    Err(e) => {
                        // TODO: use logging
                        println!("Error Flushing HNSW Segment: {:?}", e);
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                // TODO: use logging;
                println!("Error Commiting HNSW Segment: {:?}", e);
                return Err(e);
            }
        };

        // TODO: use logging
        println!("Flush to S3 complete");
        Ok(FlushS3Output {
            segment_flush_info: Arc::new([record_segment_flush_info, hnsw_segment_flush_info]),
        })
    }
}

use std::sync::Arc;

use crate::errors::ChromaError;
use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::SegmentFlusher;
use crate::types::SegmentFlushInfo;
use crate::{
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegmentWriter,
        record_segment::RecordSegmentWriter, SegmentWriter,
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
    hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
    metadata_segment_writer: MetadataSegmentWriter<'static>,
}

impl FlushS3Input {
    pub fn new(
        record_segment_writer: RecordSegmentWriter,
        hnsw_segment_writer: Box<DistributedHNSWSegmentWriter>,
        metadata_segment_writer: MetadataSegmentWriter<'static>,
    ) -> Self {
        Self {
            record_segment_writer,
            hnsw_segment_writer,
            metadata_segment_writer,
        }
    }
}

#[derive(Debug)]
pub struct FlushS3Output {
    pub(crate) segment_flush_info: Arc<[SegmentFlushInfo]>,
}

#[async_trait]
impl Operator<FlushS3Input, FlushS3Output> for FlushS3Operator {
    type Error = Box<dyn ChromaError>;

    async fn run(&self, input: &FlushS3Input) -> Result<FlushS3Output, Self::Error> {
        let record_segment_flusher = input.record_segment_writer.clone().commit();
        let record_segment_flush_info = match record_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.record_segment_writer.id;
                let res = flusher.flush().await;
                match res {
                    Ok(res) => {
                        tracing::info!("Record Segment Flushed. File paths {:?}", res);
                        SegmentFlushInfo {
                            segment_id,
                            file_paths: res,
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error flushing metadata Segment: {:?}", e);
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error Commiting record Segment: {:?}", e);
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
                        tracing::info!("HNSW Segment Flushed. File paths {:?}", res);
                        SegmentFlushInfo {
                            segment_id,
                            file_paths: res,
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error Flushing HNSW Segment: {:?}", e);
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error Commiting HNSW Segment: {:?}", e);
                return Err(e);
            }
        };

        let metadata_segment_flusher = input.metadata_segment_writer.clone().commit();
        let metadata_segment_flush_info = match metadata_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.metadata_segment_writer.id;
                let res = flusher.flush().await;
                match res {
                    Ok(res) => {
                        tracing::info!("Metadata Segment Flushed. File paths {:?}", res);
                        SegmentFlushInfo {
                            segment_id,
                            file_paths: res,
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error Flushing metadata Segment: {:?}", e);
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error Commiting metadata Segment: {:?}", e);
                return Err(e);
            }
        };

        tracing::info!("Flush to S3 complete");
        Ok(FlushS3Output {
            segment_flush_info: Arc::new([
                record_segment_flush_info,
                hnsw_segment_flush_info,
                metadata_segment_flush_info,
            ]),
        })
    }
}

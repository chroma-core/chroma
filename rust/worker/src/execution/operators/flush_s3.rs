use crate::segment::metadata_segment::MetadataSegmentWriter;
use crate::segment::SegmentFlusher;
use crate::{
    execution::operator::Operator,
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegmentWriter,
        record_segment::RecordSegmentWriter, SegmentWriter,
    },
};
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_types::SegmentFlushInfo;
use std::sync::Arc;
use tracing::Instrument;

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

    fn get_name(&self) -> &'static str {
        "FlushS3Operator"
    }

    async fn run(&self, input: &FlushS3Input) -> Result<FlushS3Output, Self::Error> {
        // TODO: Ideally we shouldn't even have to make an explicit call to
        // write_to_blockfiles since it is not the workflow for other segments
        // and is exclusive to metadata segment. We should figure out a way
        // to make this call a part of commit itself. It's not obvious directly
        // how to do that since commit is per partition but write_to_blockfiles
        // only need to be called once across all partitions combined.
        // Eventually, we want the blockfile itself to support read then write semantics
        // so we will get rid of this write_to_blockfile() extravaganza.
        let mut metadata_segment_writer = input.metadata_segment_writer.clone();
        match metadata_segment_writer
            .write_to_blockfiles()
            .instrument(tracing::info_span!("Writing to blockfiles"))
            .await
        {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("Error writing metadata segment out to blockfiles: {:?}", e);
                return Err(Box::new(e));
            }
        }
        let record_segment_flusher = input.record_segment_writer.clone().commit().await;
        let record_segment_flush_info = match record_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.record_segment_writer.id;
                let res = flusher
                    .flush()
                    .instrument(tracing::info_span!("Flush record segment"))
                    .await;
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

        let hnsw_segment_flusher = input.hnsw_segment_writer.clone().commit().await;
        let hnsw_segment_flush_info = match hnsw_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.hnsw_segment_writer.id;
                let res = flusher
                    .flush()
                    .instrument(tracing::info_span!("Flush HNSW segment"))
                    .await;
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

        let metadata_segment_flusher = metadata_segment_writer.commit().await;
        let metadata_segment_flush_info = match metadata_segment_flusher {
            Ok(flusher) => {
                let segment_id = input.metadata_segment_writer.id;
                let res = flusher
                    .flush()
                    .instrument(tracing::info_span!("Flush metadata segment"))
                    .await;
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

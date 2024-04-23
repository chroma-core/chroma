use crate::segment::LogMaterializer;
use crate::segment::SegmentWriter;
use crate::{
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::{
        distributed_hnsw_segment::DistributedHNSWSegment, record_segment::RecordSegmentWriter,
    },
    types::LogRecord,
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct WriteSegmentsOperator {}

impl WriteSegmentsOperator {
    pub fn new() -> Box<Self> {
        Box::new(WriteSegmentsOperator {})
    }
}

#[derive(Debug)]
pub struct WriteSegmentsInput<'me> {
    record_segment_writer: RecordSegmentWriter<'me>,
    hnsw_segment_writer: Box<DistributedHNSWSegment>,
    chunk: Chunk<LogRecord>,
}

impl<'me> WriteSegmentsInput<'me> {
    pub fn new(
        record_segment_writer: RecordSegmentWriter<'me>,
        hnsw_segment_writer: Box<DistributedHNSWSegment>,
        chunk: Chunk<LogRecord>,
    ) -> Self {
        WriteSegmentsInput {
            record_segment_writer,
            hnsw_segment_writer,
            chunk,
        }
    }
}

#[derive(Debug)]
pub struct WriteSegmentsOutput {}

pub type WriteSegmentsResult = Result<WriteSegmentsOutput, ()>;

#[async_trait]
impl<'input> Operator<WriteSegmentsInput<'input>, WriteSegmentsOutput> for WriteSegmentsOperator {
    type Error = ();

    async fn run(&self, input: &WriteSegmentsInput<'input>) -> WriteSegmentsResult {
        println!("Materializing N Records: {:?}", input.chunk.len());
        let res = input.record_segment_writer.materialize(&input.chunk).await;
        println!("Materialized Records: {:?}", res);
        input.hnsw_segment_writer.apply_materialized_log_chunk(res);
        println!("Applied Materialized Records to HNSW Segment");
        Ok(WriteSegmentsOutput {})
    }
}

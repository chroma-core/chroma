use crate::compactor::executor::DedicatedExecutor;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::types::EmbeddingRecord;
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct SegmentWriter {
    executor: DedicatedExecutor,
}

impl SegmentWriter {
    pub(crate) fn new(executor: DedicatedExecutor) -> Self {
        SegmentWriter { executor }
    }

    pub(crate) async fn write_records(
        &self,
        records: &Vec<Box<EmbeddingRecord>>,
        segment: Arc<Box<DistributedHNSWSegment>>,
        deadline: Instant,
    ) {
        let num_batches = 10;
        let batch_size = records.len() / num_batches;
        let mut batches: Vec<Vec<Box<EmbeddingRecord>>> = Vec::new();
        for i in 0..records.len() {
            if i % batch_size == 0 {
                batches.push(Vec::new());
            }
            batches.last_mut().unwrap().push(records[i].clone());
        }
        let mut handles = Vec::new();
        let start = Instant::now();
        let duration = deadline - start;
        for batch in batches {
            let segment = segment.clone();
            let handle = self.executor.spawn(async move {
                let result = tokio::time::timeout(duration, async move {
                    segment.write_records(&batch);
                    batch
                })
                .await;
                match result {
                    Ok(records) => Ok(()),
                    Err(_) => Err(()),
                }
            });
            handles.push(handle);
        }
        let mut num_success = 0;
        for handle in handles {
            let result = handle.await;
            match result {
                Ok(Ok(())) => {
                    num_success += 1;
                }
                Ok(Err(())) => {}
                Err(_) => {}
            }
        }
        if num_success < num_batches {
            panic!("Failed to write records to segment");
        }
    }
}

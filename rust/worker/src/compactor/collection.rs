use crate::sysdb::sysdb::SysDb;
use crate::types::EmbeddingRecord;
use crate::types::SegmentScope;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::compactor::segment_writer::SegmentWriter;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegment;

pub(crate) struct Collection {
    collection_id: String,
    sysdb: Box<dyn SysDb>,
    segment_writer: Arc<SegmentWriter>,
}

impl Collection {
    pub(crate) fn new(
        collection_id: String,
        sysdb: Box<dyn SysDb>,
        segment_writer: Arc<SegmentWriter>,
    ) -> Self {
        Collection {
            collection_id,
            sysdb,
            segment_writer,
        }
    }

    async fn get_segments(&mut self, collection_id: &Uuid) -> Vec<Box<DistributedHNSWSegment>> {
        let segments = self
            .sysdb
            .get_segments(
                None,
                None,
                Some(SegmentScope::VECTOR),
                None,
                Some(collection_id.clone()),
            )
            .await;
        let mut result = Vec::new();
        result
    }

    pub(crate) async fn compact(
        &mut self,
        collection_id: String,
        records: Vec<Box<EmbeddingRecord>>,
        deadline: Instant,
    ) {
        let segments = self
            .get_segments(&Uuid::parse_str(&collection_id).unwrap())
            .await;

        for segment in segments {
            self.segment_writer
                .write_records(&records, Arc::new(segment), deadline)
                .await;
        }
        self.commit().await;
        self.flush().await;
    }

    async fn commit(&mut self) {
        // for segment in segments {
        //     segment.commit();
        // }
    }

    async fn flush(&mut self) {
        // for segment in segments {
        //     segment.flush();
        // }
    }
}

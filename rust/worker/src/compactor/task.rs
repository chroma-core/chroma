use crate::compactor::executor::DedicatedExecutor;
use crate::compactor::log::Log;
use crate::segment::distributed_hnsw_segment::DistributedHNSWSegment;
use crate::types::EmbeddingRecord;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Task {
    pub(crate) collection_id: String,
    pub(crate) tenant_id: String,
    pub(crate) cursor: i64,
}

pub(crate) trait Operator {
    fn execute(&self) -> Vec<Box<EmbeddingRecord>>;
}

pub(crate) struct LogScanOperator {
    collection_id: String,
    log: Arc<RwLock<dyn Log + Send + Sync>>,
    index: usize,
    batch_size: usize,
}

impl LogScanOperator {
    pub(crate) fn new(
        collection_id: String,
        log: Arc<RwLock<dyn Log + Send + Sync>>,
        index: usize,
        batch_size: usize,
    ) -> Self {
        LogScanOperator {
            collection_id,
            log,
            index,
            batch_size,
        }
    }
}

impl Operator for LogScanOperator {
    fn execute(&self) -> Vec<Box<EmbeddingRecord>> {
        let log = self.log.read().unwrap();
        let records = log.read(self.collection_id.clone(), self.index, self.batch_size);
        let boxed_records: Vec<Box<EmbeddingRecord>> = records
            .iter()
            .flat_map(|record| record.clone())
            .map(|record| Box::new(record))
            .collect();
        boxed_records
    }
}

pub(crate) struct DedupOperator {
    records: Vec<Box<EmbeddingRecord>>,
}

impl DedupOperator {
    pub(crate) fn new(records: Vec<Box<EmbeddingRecord>>) -> Self {
        DedupOperator { records }
    }
}

impl Operator for DedupOperator {
    fn execute(&self) -> Vec<Box<EmbeddingRecord>> {
        let mut seen = std::collections::HashSet::new();
        self.records
            .iter()
            .filter(|record| seen.insert(record.id.clone()))
            .cloned()
            .collect()
    }
}

pub(crate) struct WriteOperator {
    records: Vec<Box<EmbeddingRecord>>,
    segment: Arc<Box<DistributedHNSWSegment>>,
}

impl WriteOperator {
    pub(crate) fn new(
        records: Vec<Box<EmbeddingRecord>>,
        segment: Arc<Box<DistributedHNSWSegment>>,
    ) -> Self {
        WriteOperator { records, segment }
    }
}

impl Operator for WriteOperator {
    fn execute(&self) -> Vec<Box<EmbeddingRecord>> {
        self.segment.write_records(&self.records);
        self.records.clone()
    }
}

pub(crate) struct CompactionTask {
    parents: Vec<CompactionTask>,
    dependencies: Vec<CompactionTask>,
    operator: Box<dyn Operator + Send + Sync>,
    execturor: Arc<DedicatedExecutor>,
    finished_dependencies: usize,
    total_dependencies: usize,
}

impl CompactionTask {
    pub(crate) fn new(
        parents: Vec<CompactionTask>,
        dependencies: Vec<CompactionTask>,
        operator: Box<dyn Operator + Send + Sync>,
        execturor: Arc<DedicatedExecutor>,
    ) -> Self {
        CompactionTask {
            parents,
            dependencies,
            operator,
            execturor,
            finished_dependencies: 0,
            total_dependencies: 0,
        }
    }

    pub(crate) async fn execute(&self) {
        let operator = self.operator.clone();
        let handle = self.execturor.spawn(async move {
            let records = operator.execute();
            records
        });
    }

    pub fn finish(&self) {
        for parent in self.parents.iter() {
            parent.complete_dependency();
        }
    }

    fn complete_dependency(&self) {
        self.finished_dependencies += 1;
        if self.finished_dependencies == self.total_dependencies {
            self.execute();
        }
    }
}

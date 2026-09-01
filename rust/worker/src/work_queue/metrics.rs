use opentelemetry::metrics::{Counter, Gauge};

#[derive(Debug, Clone)]
pub(crate) struct WorkQueueMetrics {
    depth: Gauge<u64>,
    items_with_failures: Gauge<u64>,
    failure_count: Gauge<u64>,
    snapshot_size_bytes: Gauge<u64>,
    push_count: Counter<u64>,
    finish_count: Counter<u64>,
    persist_failure_count: Counter<u64>,
}

impl Default for WorkQueueMetrics {
    fn default() -> Self {
        let meter = opentelemetry::global::meter("chroma.work_queue");

        Self {
            depth: meter
                .u64_gauge("work_queue_depth")
                .with_description("Number of entries currently in the work queue")
                .build(),
            items_with_failures: meter
                .u64_gauge("work_queue_items_with_failures")
                .with_description("Number of queued entries with at least one failure")
                .build(),
            failure_count: meter
                .u64_gauge("work_queue_failure_count")
                .with_description("Sum of failure counts across queued entries")
                .build(),
            snapshot_size_bytes: meter
                .u64_gauge("work_queue_snapshot_size_bytes")
                .with_description("Size of the latest durable work queue snapshot")
                .build(),
            push_count: meter
                .u64_counter("work_queue_push_count")
                .with_description("Number of PushWork requests handled")
                .build(),
            finish_count: meter
                .u64_counter("work_queue_finish_count")
                .with_description("Number of FinishWork requests completed successfully")
                .build(),
            persist_failure_count: meter
                .u64_counter("work_queue_persist_failure_count")
                .with_description("Number of work queue snapshot persistence failures")
                .build(),
        }
    }
}

impl WorkQueueMetrics {
    pub(crate) fn record_state(
        &self,
        depth: u64,
        items_with_failures: u64,
        failure_count: u64,
        snapshot_size_bytes: u64,
    ) {
        self.depth.record(depth, &[]);
        self.items_with_failures.record(items_with_failures, &[]);
        self.failure_count.record(failure_count, &[]);
        self.snapshot_size_bytes.record(snapshot_size_bytes, &[]);
    }

    pub(crate) fn record_push(&self) {
        self.push_count.add(1, &[]);
    }

    pub(crate) fn record_finish(&self) {
        self.finish_count.add(1, &[]);
    }

    pub(crate) fn record_persist_failure(&self) {
        self.persist_failure_count.add(1, &[]);
    }
}

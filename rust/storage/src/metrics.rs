//! Metrics for storage operations.

use std::time::{Duration, Instant};

use opentelemetry::{
    metrics::{Counter, Histogram},
    KeyValue,
};

pub(crate) enum StopWatchUnit {
    Micros,
    Millis,
    Seconds,
}

pub(crate) struct Stopwatch<'a> {
    histogram: &'a Histogram<u64>,
    attributes: &'a [KeyValue],
    start: Instant,
    unit: StopWatchUnit,
    finished: bool,
}

impl<'a> Stopwatch<'a> {
    pub(crate) fn new(
        histogram: &'a Histogram<u64>,
        attributes: &'a [KeyValue],
        unit: StopWatchUnit,
    ) -> Self {
        Self {
            histogram,
            attributes,
            start: Instant::now(),
            unit,
            finished: false,
        }
    }

    pub(crate) fn finish(mut self) -> Duration {
        let duration = self.start.elapsed();
        self.record(duration);
        self.finished = true;
        duration
    }

    fn record(&self, duration: Duration) {
        let elapsed = match self.unit {
            StopWatchUnit::Micros => duration.as_micros() as u64,
            StopWatchUnit::Millis => duration.as_millis() as u64,
            StopWatchUnit::Seconds => duration.as_secs(),
        };
        self.histogram.record(elapsed, self.attributes);
    }
}

impl Drop for Stopwatch<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.record(self.start.elapsed());
        }
    }
}

#[cfg(test)]
mod stopwatch_tests {
    use std::sync::{Arc, Weak};

    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::{
        metrics::{
            data::{Histogram, ResourceMetrics},
            reader::MetricReader,
            InstrumentKind, ManualReader, MetricResult, Pipeline, SdkMeterProvider, Temporality,
        },
        Resource,
    };

    use super::{StopWatchUnit, Stopwatch};

    #[derive(Clone, Debug)]
    struct SharedReader(Arc<dyn MetricReader>);

    impl MetricReader for SharedReader {
        fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
            self.0.register_pipeline(pipeline);
        }

        fn collect(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
            self.0.collect(metrics)
        }

        fn force_flush(&self) -> MetricResult<()> {
            self.0.force_flush()
        }

        fn shutdown(&self) -> MetricResult<()> {
            self.0.shutdown()
        }

        fn temporality(&self, kind: InstrumentKind) -> Temporality {
            self.0.temporality(kind)
        }
    }

    fn recorded_count(record: impl FnOnce(&opentelemetry::metrics::Histogram<u64>)) -> u64 {
        let reader = SharedReader(Arc::new(ManualReader::default()));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        let histogram = provider
            .meter("stopwatch-test")
            .u64_histogram("operation-duration")
            .build();

        record(&histogram);
        let mut exported = ResourceMetrics {
            resource: Resource::empty(),
            scope_metrics: Vec::new(),
        };
        reader
            .collect(&mut exported)
            .expect("metrics should collect");

        let histogram = exported.scope_metrics[0].metrics[0]
            .data
            .as_any()
            .downcast_ref::<Histogram<u64>>()
            .expect("metric should be a u64 histogram");
        histogram.data_points[0].count
    }

    #[test]
    fn finish_records_the_duration_once() {
        let count = recorded_count(|histogram| {
            Stopwatch::new(histogram, &[], StopWatchUnit::Millis).finish();
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn drop_records_the_duration_once() {
        let count = recorded_count(|histogram| {
            let _stopwatch = Stopwatch::new(histogram, &[], StopWatchUnit::Millis);
        });
        assert_eq!(count, 1);
    }
}

/// Metrics for tracking S3 and object storage operations.
///
/// All metrics are registered under the `chroma.storage` meter.
#[derive(Clone)]
pub(crate) struct StorageMetrics {
    /// Number of S3 get operations.
    pub(crate) s3_get_count: Counter<u64>,
    /// Number of S3 put operations.
    pub(crate) s3_put_count: Counter<u64>,
    /// Number of S3 delete operations.
    pub(crate) s3_delete_count: Counter<u64>,
    /// Number of keys deleted via batch delete operations.
    pub(crate) s3_delete_many_count: Counter<u64>,
    /// Latency of S3 get operations in milliseconds.
    pub(crate) s3_get_latency_ms: Histogram<u64>,
    /// Latency of S3 put operations in milliseconds.
    pub(crate) s3_put_latency_ms: Histogram<u64>,
    /// Bytes written per S3 put operation.
    pub(crate) s3_put_bytes: Histogram<u64>,
    /// Bytes written per S3 put operation that took more than 1 second.
    pub(crate) s3_put_bytes_slow: Histogram<u64>,
    /// Number of parts in multipart uploads.
    pub(crate) s3_multipart_upload_parts: Histogram<u64>,
    /// Bytes per upload part in multipart uploads.
    pub(crate) s3_upload_part_bytes: Histogram<u64>,
    /// Number of failed S3 put operations.
    pub(crate) s3_put_error_count: Counter<u64>,
    /// Number of S3 copy operations.
    pub(crate) s3_copy_count: Counter<u64>,
    /// Latency of S3 copy operations in milliseconds.
    pub(crate) s3_copy_latency_ms: Histogram<u64>,
    /// Number of S3 rename operations.
    pub(crate) s3_rename_count: Counter<u64>,
    /// Latency of S3 rename operations in milliseconds.
    pub(crate) s3_rename_latency_ms: Histogram<u64>,
    /// Number of S3 list operations.
    pub(crate) s3_list_count: Counter<u64>,
    /// Latency of S3 list operations in milliseconds.
    pub(crate) s3_list_latency_ms: Histogram<u64>,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            s3_get_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_get_count")
                .with_description("Number of S3 get operations")
                .build(),
            s3_put_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_put_count")
                .with_description("Number of S3 put operations")
                .build(),
            s3_delete_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_delete_count")
                .with_description("Number of S3 delete operations")
                .build(),
            s3_delete_many_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_delete_many_count")
                .with_description("Number of S3 delete many operations")
                .build(),
            s3_get_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_get_latency_ms")
                .with_description("Latency of S3 get operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_put_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_latency_ms")
                .with_description("Latency of S3 put operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_put_bytes: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_bytes")
                .with_description("Bytes written per S3 put operation")
                .with_unit("bytes")
                .build(),
            s3_put_bytes_slow: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_bytes_slow")
                .with_description("Bytes written per S3 put operation that took more than 1 second")
                .with_unit("bytes")
                .build(),
            s3_multipart_upload_parts: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_multipart_upload_parts")
                .with_description("Number of parts in multipart uploads")
                .build(),
            s3_upload_part_bytes: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_upload_part_bytes")
                .with_description("Bytes per upload part in multipart uploads")
                .with_unit("bytes")
                .build(),
            s3_put_error_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_put_error_count")
                .with_description("Number of failed S3 put operations")
                .build(),
            s3_copy_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_copy_count")
                .with_description("Number of S3 copy operations")
                .build(),
            s3_copy_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_copy_latency_ms")
                .with_description("Latency of S3 copy operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_rename_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_rename_count")
                .with_description("Number of S3 rename operations")
                .build(),
            s3_rename_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_rename_latency_ms")
                .with_description("Latency of S3 rename operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_list_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_list_count")
                .with_description("Number of S3 list operations")
                .build(),
            s3_list_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_list_latency_ms")
                .with_description("Latency of S3 list operations in milliseconds")
                .with_unit("ms")
                .build(),
        }
    }
}

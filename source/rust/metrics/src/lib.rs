//! Lightweight helpers for recording OpenTelemetry metrics.

use std::time::{Duration, Instant};

use opentelemetry::{metrics::Histogram, KeyValue};

/// Unit used when recording elapsed time in a histogram.
pub enum StopWatchUnit {
    Micros,
    Millis,
    Seconds,
}

/// Records elapsed time once, on explicit completion or drop.
pub struct Stopwatch<'a> {
    histogram: &'a Histogram<u64>,
    attributes: &'a [KeyValue],
    start: Instant,
    unit: StopWatchUnit,
    finished: bool,
}

impl<'a> Stopwatch<'a> {
    /// Start timing an operation with the supplied histogram and attributes.
    pub fn new(
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

    /// Return elapsed microseconds without recording a histogram sample.
    pub fn elapsed_micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    /// Record one sample and return the elapsed duration.
    pub fn finish(mut self) -> Duration {
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

    #[test]
    fn reading_elapsed_time_does_not_record_an_extra_sample() {
        let count = recorded_count(|histogram| {
            let mut stopwatch = Stopwatch::new(histogram, &[], StopWatchUnit::Micros);
            stopwatch.start = std::time::Instant::now() - std::time::Duration::from_secs(2);
            assert!(stopwatch.elapsed_micros() >= 2_000_000);
            stopwatch.finish();
        });
        assert_eq!(count, 1);
    }
}

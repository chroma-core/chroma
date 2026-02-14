// NOTE:  This is file is copied to files of the same name in the
// load/src/opentelemetry_config.rs file
// Keep them in-sync manually.

use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, InstrumentationScope, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use tokio::runtime::Handle;
use tracing_subscriber::fmt;
use tracing_subscriber::Registry;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

use std::sync::OnceLock;

static TOKIO_METRICS_INSTRUMENTS: OnceLock<TokioMetricsInstruments> = OnceLock::new();

#[allow(dead_code)]
struct TokioMetricsInstruments {
    active_tasks_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    global_queue_depth_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    worker_park_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    worker_park_unpark_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    worker_busy_duration_gauge: opentelemetry::metrics::ObservableCounter<f64>,
    // Unstable metrics
    spawned_tasks_count_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    num_blocking_threads_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    num_idle_blocking_threads_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    blocking_queue_depth_gauge: opentelemetry::metrics::ObservableGauge<u64>,
    worker_local_queue_depth_gauge: opentelemetry::metrics::ObservableGauge<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtelFilterLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for OtelFilterLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OtelFilterLevel::Trace => f.write_str("trace"),
            OtelFilterLevel::Debug => f.write_str("debug"),
            OtelFilterLevel::Info => f.write_str("info"),
            OtelFilterLevel::Warn => f.write_str("warn"),
            OtelFilterLevel::Error => f.write_str("error"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelFilter {
    pub crate_name: String,
    pub filter_level: OtelFilterLevel,
}

pub fn init_global_filter_layer(
    custom_filters: &[OtelFilter],
) -> Box<dyn Layer<Registry> + Send + Sync> {
    // These need to have underscores because the Rust compiler automatically
    // converts all hyphens in crate names to underscores to make them valid
    // Rust identifiers
    let default_crate_names = vec![
        "chroma_blockstore",
        "chroma_config",
        "chroma_cache",
        "chroma_distance",
        "chroma_error",
        "chroma_frontend",
        "chroma_index",
        "chroma_log",
        "chroma_log_service",
        "chroma_memberlist",
        "chroma_metering",
        "chroma_metering_macros",
        "chroma_segment",
        "chroma_sqlite",
        "chroma_storage",
        "chroma_sysdb",
        "chroma_system",
        "chroma_test",
        "chroma_tracing",
        "chroma_types",
        "compaction_service",
        "distance_metrics",
        "full_text",
        "metadata_filtering",
        "query_service",
        "s3heap",
        "s3heap_service",
        "wal3",
        "worker",
    ];

    let global_filter = format!(
        "error,opentelemetry_sdk=info,chroma_storage=debug,{default_filters},{additional_custom_filters}",
        default_filters = default_crate_names
            .iter()
            .map(|s| format!("{s}=trace"))
            .collect::<Vec<_>>()
            .join(","),
        additional_custom_filters = custom_filters
            .iter()
            .map(|custom_filter| {
                format!("{}={}",
                    custom_filter.crate_name,
                    custom_filter.filter_level
                )
            })
            .collect::<Vec<String>>()
            .join(","),
    );

    EnvFilter::new(std::env::var("RUST_LOG").unwrap_or(global_filter)).boxed()
}

pub fn init_otel_layer(
    service_name: &String,
    otel_endpoint: &String,
) -> Box<dyn Layer<Registry> + Send + Sync> {
    tracing::info!(
        "Registering jaeger subscriber for {} at endpoint {}",
        service_name,
        otel_endpoint
    );
    let resource = opentelemetry_sdk::Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name.clone()),
        opentelemetry::KeyValue::new(
            "service.pod_name",
            std::env::var("HOSTNAME").unwrap_or("unknown".to_string()),
        ),
    ]);

    // Prepare tracer.
    let tracing_span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("could not build span exporter for tracing");
    let trace_config = opentelemetry_sdk::trace::Config::default().with_resource(resource.clone());
    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(tracing_span_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_config(trace_config)
        .build();
    let tracer = tracer_provider.tracer(service_name.clone());
    let fastrace_span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("could not build span exporter for fastrace");
    fastrace::set_reporter(
        fastrace_opentelemetry::OpenTelemetryReporter::new(
            fastrace_span_exporter,
            opentelemetry::trace::SpanKind::Server,
            Cow::Owned(resource.clone()),
            InstrumentationScope::builder("chroma").build(),
        ),
        fastrace::collector::Config::default(),
    );

    // Prepare meter.
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(
            std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").unwrap_or(otel_endpoint.clone()),
        )
        .build()
        .expect("could not build metric exporter");

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        metric_exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource.clone())
        .build();
    global::set_meter_provider(meter_provider);

    // Register runtime metrics callback
    register_runtime_metrics();

    tracing_opentelemetry::OpenTelemetryLayer::new(tracer).boxed()
}

fn register_runtime_metrics() {
    TOKIO_METRICS_INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("tokio_runtime");

        let active_tasks_gauge = meter
            .u64_observable_gauge("tokio_active_tasks")
            .with_description("Number of currently alive tasks in the Tokio runtime")
            .with_callback(|result| {
                if let Ok(handle) = Handle::try_current() {
                    let metrics = handle.metrics();
                    result.observe(metrics.num_alive_tasks() as u64, &[]);
                }
            })
            .build();

        let global_queue_depth_gauge = meter
            .u64_observable_gauge("tokio_global_queue_depth")
            .with_description("Size of the global queue in the Tokio runtime")
            .with_callback(|result| {
                if let Ok(handle) = Handle::try_current() {
                    let metrics = handle.metrics();
                    result.observe(metrics.global_queue_depth() as u64, &[]);
                }
            })
            .build();

        let worker_park_gauge = meter
            .u64_observable_gauge("tokio_worker_park_count")
            .with_description("Number of times a worker has parked")
            .with_callback(|result| {
                if let Ok(handle) = Handle::try_current() {
                    let metrics = handle.metrics();
                    for i in 0..metrics.num_workers() {
                        result.observe(
                            metrics.worker_park_count(i),
                            &[KeyValue::new("worker", i.to_string())],
                        );
                    }
                }
            })
            .build();

        let worker_park_unpark_gauge = meter
            .u64_observable_gauge("tokio_worker_park_unpark_count")
            .with_description("Number of times a worker has parked and unparked")
            .with_callback(|result| {
                if let Ok(handle) = Handle::try_current() {
                    let metrics = handle.metrics();
                    for i in 0..metrics.num_workers() {
                        result.observe(
                            metrics.worker_park_unpark_count(i),
                            &[KeyValue::new("worker", i.to_string())],
                        );
                    }
                }
            })
            .build();

        let worker_busy_duration_gauge = meter
            .f64_observable_counter("tokio_worker_busy_duration_seconds")
            .with_description("Total time worker has been busy in seconds")
            .with_callback(|result| {
                if let Ok(handle) = Handle::try_current() {
                    let metrics = handle.metrics();
                    for i in 0..metrics.num_workers() {
                        let duration = metrics.worker_total_busy_duration(i);
                        result.observe(
                            duration.as_secs_f64(),
                            &[KeyValue::new("worker", i.to_string())],
                        );
                    }
                }
            })
            .build();

        // Unstable metrics (requires tokio_unstable)
        let spawned_tasks_count_gauge = meter
            .u64_observable_gauge("tokio_spawned_tasks_count")
            .with_description("Total number of spawned tasks")
            .with_callback(|_result| {
                if let Ok(_handle) = Handle::try_current() {
                    let _metrics = _handle.metrics();
                    #[cfg(tokio_unstable)]
                    _result.observe(_metrics.spawned_tasks_count(), &[]);
                }
            })
            .build();

        let num_blocking_threads_gauge = meter
            .u64_observable_gauge("tokio_num_blocking_threads")
            .with_description("Number of blocking threads")
            .with_callback(|_result| {
                if let Ok(_handle) = Handle::try_current() {
                    let _metrics = _handle.metrics();
                    #[cfg(tokio_unstable)]
                    _result.observe(_metrics.num_blocking_threads() as u64, &[]);
                }
            })
            .build();

        let num_idle_blocking_threads_gauge = meter
            .u64_observable_gauge("tokio_num_idle_blocking_threads")
            .with_description("Number of idle blocking threads")
            .with_callback(|_result| {
                if let Ok(_handle) = Handle::try_current() {
                    let _metrics = _handle.metrics();
                    #[cfg(tokio_unstable)]
                    _result.observe(_metrics.num_idle_blocking_threads() as u64, &[]);
                }
            })
            .build();

        let blocking_queue_depth_gauge = meter
            .u64_observable_gauge("tokio_blocking_queue_depth")
            .with_description("Blocking queue depth")
            .with_callback(|_result| {
                if let Ok(_handle) = Handle::try_current() {
                    let _metrics = _handle.metrics();
                    #[cfg(tokio_unstable)]
                    _result.observe(_metrics.blocking_queue_depth() as u64, &[]);
                }
            })
            .build();

        let worker_local_queue_depth_gauge = meter
            .u64_observable_gauge("tokio_worker_local_queue_depth")
            .with_description("Worker local queue depth")
            .with_callback(|_result| {
                if let Ok(_handle) = Handle::try_current() {
                    let _metrics = _handle.metrics();
                    #[cfg(tokio_unstable)]
                    for i in 0.._metrics.num_workers() {
                        _result.observe(
                            _metrics.worker_local_queue_depth(i) as u64,
                            &[KeyValue::new("worker", i.to_string())],
                        );
                    }
                }
            })
            .build();

        TokioMetricsInstruments {
            active_tasks_gauge,
            global_queue_depth_gauge,
            worker_park_gauge,
            worker_park_unpark_gauge,
            worker_busy_duration_gauge,
            spawned_tasks_count_gauge,
            num_blocking_threads_gauge,
            num_idle_blocking_threads_gauge,
            blocking_queue_depth_gauge,
            worker_local_queue_depth_gauge,
        }
    });
}

pub fn init_stdout_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    fmt::layer().pretty().with_target(false).boxed()
}

pub fn init_tracing(layers: Vec<Box<dyn Layer<Registry> + Send + Sync>>) {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let layers = layers
        .into_iter()
        .reduce(|a, b| Box::new(a.and_then(b)))
        .expect("Should be able to create tracing layers");
    let subscriber = tracing_subscriber::registry().with(layers);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Should be able to set global tracing subscriber");
    tracing::info!("Global tracing subscriber set");
}

pub fn init_panic_tracing_hook() {
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = panic_info.payload();

        let payload = if let Some(s) = payload.downcast_ref::<&str>() {
            Some(&**s)
        } else {
            payload.downcast_ref::<String>().map(|s| s.as_str())
        };

        tracing::error!(
            panic.payload = payload,
            panic.location = panic_info.location().map(|l| l.to_string()),
            panic.backtrace = tracing::field::display(std::backtrace::Backtrace::capture()),
            "A panic occurred"
        );

        prev_hook(panic_info);
    }));
}

pub fn init_otel_tracing(
    service_name: &String,
    custom_filters: &[OtelFilter],
    otel_endpoint: &String,
) {
    let layers = vec![
        // The global filter applies to all subsequent layers
        init_global_filter_layer(custom_filters),
        init_otel_layer(service_name, otel_endpoint),
        init_stdout_layer(),
    ];
    init_tracing(layers);
    init_panic_tracing_hook();
}

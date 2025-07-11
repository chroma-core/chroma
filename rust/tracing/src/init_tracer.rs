// NOTE:  This is file is copied to files of the same name in the
// load/src/opentelemetry_config.rs file
// Keep them in-sync manually.

use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, InstrumentationScope};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use tracing_subscriber::fmt;
use tracing_subscriber::Registry;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

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
    let default_crate_names = vec![
        "chroma",
        "chroma-blockstore",
        "chroma-config",
        "chroma-cache",
        "chroma-distance",
        "chroma-error",
        "chroma-log",
        "chroma-index",
        "chroma-test",
        "chroma-types",
        "distance_metrics",
        "full_text",
        "metadata_filtering",
        "query_service",
        "wal3",
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
    let resource = opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        service_name.clone(),
    )]);

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
    tracing_opentelemetry::OpenTelemetryLayer::new(tracer).boxed()
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

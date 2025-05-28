// NOTE:  This is file is copied to files of the same name in the
// load/src/opentelemetry_config.rs file
// Keep them in-sync manually.

use std::borrow::Cow;

use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, InstrumentationScope};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_subscriber::fmt;
use tracing_subscriber::Registry;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

pub fn init_global_filter_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "error,opentelemetry_sdk=info,".to_string()
            + &vec![
                "chroma",
                "chroma-blockstore",
                "chroma-config",
                "chroma-cache",
                "chroma-distance",
                "chroma-error",
                "chroma-log",
                "chroma-log-service",
                "chroma-frontend",
                "chroma-index",
                "chroma-storage",
                "chroma-test",
                "chroma-types",
                "compaction_service",
                "distance_metrics",
                "full_text",
                "hosted-frontend",
                "metadata_filtering",
                "query_service",
                "wal3",
                "worker",
                "garbage_collector",
            ]
            .into_iter()
            .map(|s| s.to_string() + "=trace")
            .collect::<Vec<String>>()
            .join(",")
    }))
    .boxed()
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
    // Layer for adding our configured tracer.
    // Export everything at this layer. The backend i.e. honeycomb or jaeger will filter at its end.
    tracing_opentelemetry::OpenTelemetryLayer::new(tracer)
        .with_filter(tracing_subscriber::filter::LevelFilter::TRACE)
        .boxed()
}

pub fn init_stdout_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    fmt::layer()
        .pretty()
        .with_target(false)
        .with_filter(tracing_subscriber::filter::FilterFn::new(|metadata| {
            // NOTE(rescrv):  This is a hack, too.  Not an uppercase hack, just a hack.  This
            // one's localized to the cache module.  There's not much to do to unify it with
            // the otel filter because these are different output layers from the tracing.

            // This filter ensures that we don't cache calls for get/insert on stdout, but will
            // still see the clear call.
            !(metadata
                .module_path()
                .unwrap_or("")
                .starts_with("chroma_cache")
                && metadata.name() != "clear")
        }))
        .with_filter(tracing_subscriber::filter::FilterFn::new(|metadata| {
            metadata.module_path().unwrap_or("").starts_with("chroma")
                || metadata.module_path().unwrap_or("").starts_with("wal3")
                || metadata.module_path().unwrap_or("").starts_with("worker")
                || metadata
                    .module_path()
                    .unwrap_or("")
                    .starts_with("garbage_collector")
                || metadata
                    .module_path()
                    .unwrap_or("")
                    .starts_with("opentelemetry_sdk")
                || metadata
                    .module_path()
                    .unwrap_or("")
                    .starts_with("hosted-frontend")
        }))
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO)
        .boxed()
}

pub fn init_tracing(layers: Vec<Box<dyn Layer<Registry> + Send + Sync>>) {
    global::set_text_map_propagator(TraceContextPropagator::new());
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

pub fn init_otel_tracing(service_name: &String, otel_endpoint: &String) {
    let layers = vec![
        init_global_filter_layer(),
        init_otel_layer(service_name, otel_endpoint),
        init_stdout_layer(),
    ];
    init_tracing(layers);
    init_panic_tracing_hook();
}

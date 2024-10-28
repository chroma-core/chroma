use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

#[derive(Clone, Debug, Default)]
struct ChromaShouldSample;

const BUSY_NS: opentelemetry::Key = opentelemetry::Key::from_static_str("busy_ns");
const IDLE_NS: opentelemetry::Key = opentelemetry::Key::from_static_str("idle_ns");

fn is_slow(attributes: &[opentelemetry::KeyValue]) -> bool {
    let mut nanos = 0i64;
    for attr in attributes {
        if attr.key == BUSY_NS || attr.key == IDLE_NS {
            if let opentelemetry::Value::I64(ns) = attr.value {
                nanos += ns;
            }
        }
    }
    nanos > 1_000_000
}

impl opentelemetry_sdk::trace::ShouldSample for ChromaShouldSample {
    fn should_sample(
        &self,
        _: Option<&opentelemetry::Context>,
        _: opentelemetry::trace::TraceId,
        name: &str,
        _: &opentelemetry::trace::SpanKind,
        attributes: &[opentelemetry::KeyValue],
        _: &[opentelemetry::trace::Link],
    ) -> opentelemetry::trace::SamplingResult {
        if (name != "get" && name != "insert") || is_slow(attributes) {
            opentelemetry::trace::SamplingResult {
                decision: opentelemetry::trace::SamplingDecision::RecordAndSample,
                attributes: attributes.to_vec(),
                trace_state: opentelemetry::trace::TraceState::default(),
            }
        } else {
            opentelemetry::trace::SamplingResult {
                decision: opentelemetry::trace::SamplingDecision::Drop,
                attributes: vec![],
                trace_state: opentelemetry::trace::TraceState::default(),
            }
        }
    }
}

pub(crate) fn init_otel_tracing(service_name: &String, otel_endpoint: &String) {
    println!(
        "Registering jaeger subscriber for {} at endpoint {}",
        service_name, otel_endpoint
    );
    let resource = opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        service_name.clone(),
    )]);
    // Prepare trace config.
    let trace_config = opentelemetry_sdk::trace::Config::default()
        .with_sampler(ChromaShouldSample)
        .with_resource(resource);
    // Prepare exporter.
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otel_endpoint);
    let otlp_tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("could not build otlp trace provider")
        .tracer(service_name.clone());
    // Layer for adding our configured tracer.
    // Export everything at this layer. The backend i.e. honeycomb or jaeger will filter at its end.
    let exporter_layer = tracing_opentelemetry::OpenTelemetryLayer::new(otlp_tracer)
        .with_filter(tracing_subscriber::filter::LevelFilter::TRACE);
    // Layer for printing spans to stdout. Only print INFO logs by default.
    let stdout_layer =
        BunyanFormattingLayer::new(service_name.clone().to_string(), std::io::stdout)
            .with_filter(tracing_subscriber::filter::FilterFn::new(|metadata| {
                !(metadata
                    .module_path()
                    .unwrap_or("")
                    .starts_with("chroma_cache")
                    && metadata.name() != "clear")
            }))
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
    // global filter layer. Don't filter anything at above trace at the global layer for chroma.
    // And enable errors for every other library.
    let global_layer = EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "error,".to_string()
            + &vec![
                "chroma",
                "chroma-blockstore",
                "chroma-config",
                "chroma-cache",
                "chroma-distance",
                "chroma-error",
                "chroma-index",
                "chroma-storage",
                "chroma-test",
                "chroma-types",
                "compaction_service",
                "distance_metrics",
                "full_text",
                "metadata_filtering",
                "query_service",
                "worker",
            ]
            .into_iter()
            .map(|s| s.to_string() + "=trace")
            .collect::<Vec<String>>()
            .join(",")
    }));

    // Create subscriber.
    let subscriber = tracing_subscriber::registry()
        .with(global_layer)
        .with(stdout_layer)
        .with(exporter_layer);
    global::set_text_map_propagator(TraceContextPropagator::new());
    tracing::subscriber::set_global_default(subscriber)
        .expect("Set global default subscriber failed");
    println!("Set global subscriber for {}", service_name);

    // Add panics to tracing
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = panic_info.payload();

        #[allow(clippy::manual_map)]
        let payload = if let Some(s) = payload.downcast_ref::<&str>() {
            Some(&**s)
        } else if let Some(s) = payload.downcast_ref::<String>() {
            Some(s.as_str())
        } else {
            None
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

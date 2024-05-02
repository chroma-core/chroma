use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

pub(crate) fn init_otel_tracing(service_name: &String, otel_endpoint: &String) {
    let resource = opentelemetry::sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        service_name.clone(),
    )]);
    // Prepare trace config.
    let trace_config = trace::config()
        .with_sampler(opentelemetry::sdk::trace::Sampler::AlwaysOn)
        .with_resource(resource);
    // Prepare exporter. Jaeger only for now.
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otel_endpoint);
    let otlp_tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Error - Failed to create tracer.");
    // Layer for adding our configured tracer.
    let tracing_layer = tracing_opentelemetry::layer().with_tracer(otlp_tracer);
    // Level filter layer to filter traces based on level (trace, debug, info, warn, error).
    let level_filter_layer = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO"));
    let subscriber = Registry::default()
        .with(tracing_layer)
        .with(level_filter_layer);
    global::set_text_map_propagator(TraceContextPropagator::new());
    tracing::subscriber::set_global_default(subscriber)
        .expect("Set global default subscriber failed");
}

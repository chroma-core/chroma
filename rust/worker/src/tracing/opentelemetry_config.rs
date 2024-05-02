use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::trace;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

pub(crate) fn init_oltp_tracing() {
    let resource = opentelemetry::sdk::Resource::new(vec![opentelemetry::KeyValue::new(
        "service.name",
        "sanket-test",
    )]);
    // Prepare trace config.
    let trace_config = trace::config()
        .with_sampler(opentelemetry::sdk::trace::Sampler::AlwaysOn)
        .with_resource(resource);
    // Prepare exporter. Jaeger only for now.
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://jaeger:4317");
    let otlp_tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Error - Failed to create tracer.");
    // Layer for adding our configured tracer.
    let tracing_layer = tracing_opentelemetry::layer().with_tracer(otlp_tracer);
    global::set_text_map_propagator(TraceContextPropagator::new());

    Registry::default().with(tracing_layer).init();
}

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

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
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
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
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
    // global filter layer. Don't filter anything at above trace at the global layer for chroma.
    // And enable errors for every other library.
    let global_layer = EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(|_| {
        "error,".to_string()
            + &vec![
                "chroma-blockstore",
                "chroma-config",
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

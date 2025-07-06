// NOTE:  This is a copy of the file of the same name in the
// worker/src/tracing/opentelemetry_config.rs file.
//
// Keep them in-sync manually.

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

pub(crate) fn init_otel_tracing(service_name: &String, otel_endpoint: &String) {
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
    let exporter_layer = tracing_opentelemetry::OpenTelemetryLayer::new(tracer)
        .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
    // Layer for printing spans to stdout. Only print INFO logs by default.
    let stdout_layer =
        BunyanFormattingLayer::new(service_name.clone().to_string(), std::io::stdout)
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
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
    // global filter layer. Don't filter anything at above trace at the global layer for chroma.
    // And enable errors for every other library.
    let global_layer = EnvFilter::new(std::env::var("RUST_LOG").unwrap_or("error".to_string()));

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

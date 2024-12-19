// NOTE:  This is a copy of the file of the same name in the
// worker/src/tracing/opentelemetry_config.rs file.
//
// Keep them in-sync manually.

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
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
    nanos > 20_000_000
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
        // NOTE(rescrv):  THIS IS A HACK!  If you find yourself seriously extending it, it's time
        // to investigate honeycomb's sampling capabilities.

        // If the name is not get and not insert, or the request is slow, sample it.
        // Otherwise, drop.
        // This filters filters foyer calls in-process so they won't be overwhelming the tracing.
        if (name != "get" && name != "insert") || is_slow(attributes) {
            opentelemetry::trace::SamplingResult {
                decision: opentelemetry::trace::SamplingDecision::RecordAndSample,
                attributes: vec![],
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

    // Prepare tracer.
    let client = reqwest::Client::new();
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(client)
        .with_endpoint(otel_endpoint)
        .build()
        .expect("could not build span exporter");
    let trace_config = opentelemetry_sdk::trace::Config::default()
        .with_sampler(ChromaShouldSample)
        .with_resource(resource.clone());
    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_config(trace_config)
        .build();
    let tracer = tracer_provider.tracer(service_name.clone());
    // TODO(MrCroxx): Should we the tracer provider as global?
    // global::set_tracer_provider(tracer_provider);

    // Prepare meter.
    let client = reqwest::Client::new();
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_http_client(client)
        .with_endpoint(otel_endpoint)
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
        .with_filter(tracing_subscriber::filter::LevelFilter::ERROR);
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

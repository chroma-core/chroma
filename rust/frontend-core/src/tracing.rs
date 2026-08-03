use chroma_tracing::{
    init_global_filter_layer, init_otel_layer, init_panic_tracing_hook, init_stdout_layer,
    init_tracing,
};

use crate::config::OpenTelemetryConfig;

/// Initialize OTEL/stdout tracing for an HTTP server. If `otel_config` is
/// provided, both OTEL and stdout layers are installed; otherwise if
/// `stdout_tracing` is true, only stdout is installed; otherwise nothing.
pub fn init_server_otel_tracing(otel_config: Option<&OpenTelemetryConfig>, stdout_tracing: bool) {
    if let Some(otel_config) = otel_config {
        let tracing_layers = vec![
            init_global_filter_layer(&otel_config.filters),
            init_otel_layer(&otel_config.service_name, &otel_config.endpoint),
            init_stdout_layer(),
        ];
        init_tracing(tracing_layers);
        init_panic_tracing_hook();
    } else if stdout_tracing {
        let tracing_layers = vec![init_global_filter_layer(&[]), init_stdout_layer()];
        init_tracing(tracing_layers);
    } else {
        eprintln!("No telemetry is configured.");
    }
}

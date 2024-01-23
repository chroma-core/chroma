from typing import List, Optional

import psutil
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider


def instrument_fastapi(
    app: FastAPI,
    *,
    trace_provider: TracerProvider = None,
    meter_provider: MeterProvider = None,
    excluded_urls: Optional[List[str]] = None,
) -> None:
    """Instrument FastAPI to emit OpenTelemetry spans."""
    if not isinstance(app, FastAPI):
        raise TypeError("app must be a FastAPI instance")
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls=",".join(excluded_urls) if excluded_urls else None,
        tracer_provider=trace_provider,
        meter_provider=meter_provider,
    )


# We need to differ type resolution here
def register_baseline_metrics(
    telemetry: "chromadb.telemetry.opentelemetry.OpenTelemetryClient",  # type: ignore # noqa: F821
) -> None:
    telemetry.add_observable_gauge(
        name="system_cpu_usage",
        description="system cpu usage",
        unit="percent",
        callback=lambda: psutil.cpu_percent(),
    )
    telemetry.add_observable_gauge(
        name="system_memory_usage",
        description="system memory usage",
        unit="percent",
        callback=lambda: psutil.virtual_memory().percent,
    )
    telemetry.add_observable_gauge(
        name="system_disk_usage",
        description="system disk usage",
        unit="percent",
        callback=lambda: psutil.disk_usage("/").percent,
    )
    telemetry.add_observable_gauge(
        name="process_cpu_usage",
        description="process cpu usage",
        unit="percent",
        callback=lambda: psutil.Process().cpu_percent(),
    )
    telemetry.add_observable_gauge(
        name="process_memory_usage",
        description="process memory usage",
        unit="percent",
        callback=lambda: psutil.Process().memory_percent(),
    )
    telemetry.add_observable_counter(
        name="network_out",
        callback=lambda: psutil.net_io_counters().bytes_sent,
        unit="bytes",
        description="Bytes out",
    )

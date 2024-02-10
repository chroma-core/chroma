import logging
import threading
import time
from contextlib import contextmanager
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Union,
    List,
    Iterable,
    Generator,
)

from opentelemetry import metrics
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.metrics import Observation, CallbackOptions, Counter, Histogram
from opentelemetry.sdk._logs import LoggingHandler, LoggerProvider
from opentelemetry.sdk._logs._internal.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.trace import Span
from overrides import override

from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    OtelAttributes,
    telemetry_settings,
    OtelInstrumentation,
)


# tracer: Optional[trace.Tracer] = None
# granularity: OpenTelemetryGranularity = OpenTelemetryGranularity("none")
# meter_provider: Optional[MeterProvider] = None
# meter = metrics.get_meter(__name__)

# traces_enabled = False
# metrics_enabled = False
# logs_enabled = False


class OtelClient(OtelInstrumentation):
    """A client for OpenTelemetry."""

    def __init__(self) -> None:
        self.trace_provider = None
        self.meter_provider = None
        self.logger_provider = None
        self._counters: Dict[str, Counter] = {}
        self._histograms: Dict[str, Histogram] = {}
        self.meter = None
        self.tracer = None
        self.endpoint = telemetry_settings.chroma_otel_collection_endpoint
        self.resource = Resource(
            attributes={SERVICE_NAME: str(telemetry_settings.chroma_otel_service_name)}
        )
        self.headers = telemetry_settings.chroma_otel_collection_headers
        self._traces_enabled = telemetry_settings.chroma_otel_traces_enabled
        self._metrics_enabled = telemetry_settings.chroma_otel_metrics_enabled
        self._logs_enabled = telemetry_settings.chroma_otel_logs_enabled
        self.granularity = OpenTelemetryGranularity(
            telemetry_settings.chroma_otel_granularity
            if telemetry_settings.chroma_otel_granularity
            else "none"
        )

        if self.traces_enabled:
            self._trace_init()
        if self.metrics_enabled:
            self._metrics_init()
        if self.logs_enabled:
            self._logging_init()

    @property
    def traces_enabled(self) -> bool:
        return self._traces_enabled

    @property
    def metrics_enabled(self) -> bool:
        return self._metrics_enabled

    @property
    def logs_enabled(self) -> bool:
        return self._logs_enabled

    def _trace_init(self) -> None:
        self.trace_provider = TracerProvider(resource=self.resource)
        self.trace_provider.add_span_processor(  # type: ignore
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=self.endpoint,
                    headers=self.headers,
                )
            )
        )
        trace.set_tracer_provider(self.trace_provider)
        self.tracer = trace.get_tracer(__name__)

    def _metrics_init(self) -> None:
        exporter = OTLPMetricExporter(endpoint=self.endpoint, headers=self.headers)
        reader = PeriodicExportingMetricReader(
            exporter=exporter, export_interval_millis=5000
        )
        self.meter_provider = MeterProvider(
            resource=self.resource, metric_readers=[reader]
        )
        metrics.set_meter_provider(self.meter_provider)
        self.meter = metrics.get_meter(__name__)

    def _logging_init(self) -> None:
        self.logger_provider = LoggerProvider(resource=self.resource)
        otlp_exporter = OTLPLogExporter(endpoint=self.endpoint, headers=self.headers)
        self.logger_provider.add_log_record_processor(  # type: ignore
            BatchLogRecordProcessor(otlp_exporter)
        )
        handler = LoggingHandler(
            level=logging.INFO, logger_provider=self.logger_provider
        )
        logging.getLogger().addHandler(handler)
        uv_log = logging.getLogger("uvicorn")
        uv_log.addHandler(handler)

    @override
    def instrument_fastapi(
        self, app: Any, excluded_urls: Optional[List[str]] = None
    ) -> None:
        """Instrument FastAPI to emit OpenTelemetry spans."""
        if not any([self.trace_provider, self.meter_provider, self.logger_provider]):
            return
        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls=",".join(excluded_urls) if excluded_urls else None,
            tracer_provider=self.trace_provider,
            meter_provider=self.meter_provider,
        )

    @override
    def trace_method(
        self,
        trace_name: str,
        trace_granularity: OpenTelemetryGranularity,
        attributes: Optional[OtelAttributes] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that traces a method."""

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            if not self.traces_enabled or not self.tracer:
                return f

            @wraps(f)
            def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
                if trace_granularity < self.granularity:
                    return f(*args, **kwargs)
                if not self.tracer:
                    return f(*args, **kwargs)
                with self.tracer.start_as_current_span(
                    trace_name, attributes=attributes
                ):
                    return f(*args, **kwargs)

            return wrapper

        return decorator

    @override
    def histogram(
        self,
        name: str,
        unit: str,
        description: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that creates a histogram for a method execution."""

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            self.add_histogram(
                name=name,
                unit=unit,
                description=description,
            )

            @wraps(f)
            def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
                start_time = time.time()
                result = f(*args, **kwargs)
                end_time = time.time()
                self.record_histogram(
                    name=name,
                    value=end_time - start_time,
                    attributes={"thread": threading.get_ident()},
                )
                return result

            return wrapper

        return decorator

    @override
    def counter(
        self,
        name: str,
        unit: str,
        description: str,
        arg_counter_extractor: Optional[Callable[..., int]] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that creates a histogram for a method execution."""

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            self.add_counter(
                name=name,
                unit=unit,
                description=description,
            )

            @wraps(f)
            def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
                if arg_counter_extractor:
                    value = arg_counter_extractor(*args, **kwargs)
                    self.update_counter(name=name, value=value)
                return f(*args, **kwargs)

            return wrapper

        return decorator

    @override
    def add_attributes_to_current_span(self, attributes: OtelAttributes) -> None:
        """Add attributes to the current span."""
        if not self.traces_enabled:
            return
        if self.granularity == OpenTelemetryGranularity.NONE:
            return
        if not self.tracer:
            return
        span = trace.get_current_span()
        span.set_attributes(attributes)

    def get_tracer(self) -> Optional[trace.Tracer]:
        """Get the tracer."""
        return self.tracer

    @override
    def add_observable_gauge(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        if not self.metrics_enabled:
            return

        def cb(_: CallbackOptions) -> Iterable[Observation]:
            value = callback()
            if isinstance(value, (int, float)):
                return [Observation(value, {})]
            else:
                return [Observation(v, {}) for v in value]

        self.meter.create_observable_gauge(  # type: ignore
            name=name, callbacks=[cb], unit=unit, description=description
        )

    @override
    def add_observable_counter(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        if not self.metrics_enabled:
            return

        def cb(_: CallbackOptions) -> Iterable[Observation]:
            value = callback()
            if isinstance(value, (int, float)):
                return [Observation(value, {})]
            else:
                return [Observation(v, {}) for v in value]

        self.meter.create_observable_counter(  # type: ignore
            name=name, callbacks=[cb], unit=unit, description=description
        )

    @override
    def add_counter(self, name: str, unit: str, description: str) -> None:
        """Add a counter to the metrics."""
        if name not in self._counters.keys():
            self._counters[name] = self.meter.create_counter(  # type: ignore
                name=name, unit=unit, description=description
            )

    @override
    def update_counter(self, name: str, value: int) -> None:
        """Update a counter."""
        if name in self._counters.keys():
            self._counters[name].add(value)

    @override
    def add_histogram(
        self,
        name: str,
        unit: str,
        description: str,
    ) -> None:
        if not self.metrics_enabled:
            return
        if name not in self._histograms.keys():
            self._histograms[name] = self.meter.create_histogram(  # type: ignore
                name=name,
                unit=unit,
                description=description,
            )

    @override
    def record_histogram(
        self,
        name: str,
        value: Union[int, float],
        attributes: Optional[OtelAttributes] = None,
    ) -> None:
        if not self.metrics_enabled:
            return
        if name in self._histograms.keys():
            self._histograms[name].record(value, attributes=attributes)

    @override
    def get_current_span_id(self) -> Optional[str]:
        """Get the current span ID."""
        if not self.traces_enabled:
            return None
        if self.granularity == OpenTelemetryGranularity.NONE:
            return None
        if not self.tracer:
            return None
        ctx = trace.get_current_span().get_span_context()
        return "{trace:032x}".format(trace=ctx.trace_id)


otel_client = OtelClient()


@contextmanager
def span_proxy_context(span_name: str) -> Generator[Span, None, None]:
    global otel_client

    if not otel_client.traces_enabled:
        yield None
    else:
        _tracer = otel_client.get_tracer()
        if _tracer:
            with _tracer.start_as_current_span(span_name) as span:
                yield span
        else:
            yield None

import logging
from contextlib import contextmanager
from functools import wraps
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Sequence,
    Union,
    List,
    Iterable,
    Generator,
)

from opentelemetry import trace
from opentelemetry import metrics
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.metrics import Observation, CallbackOptions, Counter
from opentelemetry.sdk._logs import LoggingHandler, LoggerProvider
from opentelemetry.sdk._logs._internal.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Span

from chromadb.config import Component
from chromadb.config import System
from chromadb.telemetry.opentelemetry.fastapi import instrument_fastapi


class OpenTelemetryGranularity(Enum):
    """The granularity of the OpenTelemetry spans."""

    NONE = "none"
    """No spans are emitted."""

    OPERATION = "operation"
    """Spans are emitted for each operation."""

    OPERATION_AND_SEGMENT = "operation_and_segment"
    """Spans are emitted for each operation and segment."""

    ALL = "all"
    """Spans are emitted for almost every method call."""

    # Greater is more restrictive. So "all" < "operation" (and everything else),
    # "none" > everything.
    def __lt__(self, other: Any) -> bool:
        """Compare two granularities."""
        order = [
            OpenTelemetryGranularity.ALL,
            OpenTelemetryGranularity.OPERATION_AND_SEGMENT,
            OpenTelemetryGranularity.OPERATION,
            OpenTelemetryGranularity.NONE,
        ]
        return order.index(self) < order.index(other)


tracer: Optional[trace.Tracer] = None
granularity: OpenTelemetryGranularity = OpenTelemetryGranularity("none")
meter_provider: Optional[MeterProvider] = None
meter = metrics.get_meter(__name__)

traces_enabled = False
metrics_enabled = False
logs_enabled = False


class OpenTelemetryClient(Component):
    def __init__(self, system: System):
        super().__init__(system)
        global traces_enabled, metrics_enabled, logs_enabled, granularity
        self.resource = Resource(
            attributes={SERVICE_NAME: str(system.settings.chroma_otel_service_name)}
        )
        self.endpoint = system.settings.chroma_otel_collection_endpoint
        self.headers = system.settings.chroma_otel_collection_headers
        traces_enabled = system.settings.chroma_otel_traces_enabled
        metrics_enabled = system.settings.chroma_otel_metrics_enabled
        logs_enabled = system.settings.chroma_otel_logs_enabled
        self.granularity = OpenTelemetryGranularity(
            system.settings.chroma_otel_granularity
            if system.settings.chroma_otel_granularity
            else "none"
        )
        granularity = self.granularity
        self.trace_provider = None
        self.meter_provider = None
        self.logger_provider = None
        if traces_enabled:
            self._trace_init()
        if metrics_enabled:
            self._metrics_init()
        if logs_enabled:
            self._logging_init()
        self._counters: Dict[str, Counter] = {}

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
        global tracer, granularity
        tracer = trace.get_tracer(__name__)
        granularity = self.granularity

    def _metrics_init(self) -> None:
        global meter_provider
        exporter = OTLPMetricExporter(endpoint=self.endpoint, headers=self.headers)
        reader = PeriodicExportingMetricReader(
            exporter=exporter, export_interval_millis=1000
        )
        self.meter_provider = MeterProvider(
            resource=self.resource, metric_readers=[reader]
        )
        metrics.set_meter_provider(self.meter_provider)

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

    def instrument_fastapi(
        self, app: Any, excluded_urls: Optional[List[str]] = None
    ) -> None:
        """Instrument FastAPI to emit OpenTelemetry spans."""
        instrument_fastapi(
            app,
            meter_provider=self.meter_provider,
            trace_provider=self.trace_provider,
            excluded_urls=excluded_urls,
        )

    @staticmethod
    def trace_method(
        trace_name: str,
        trace_granularity: OpenTelemetryGranularity,
        attributes: Optional[
            Dict[
                str,
                Union[
                    str,
                    bool,
                    float,
                    int,
                    Sequence[str],
                    Sequence[bool],
                    Sequence[float],
                    Sequence[int],
                ],
            ]
        ] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that traces a method."""

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            global traces_enabled
            if not traces_enabled:
                return f

            @wraps(f)
            def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
                global tracer, granularity
                if trace_granularity < granularity:
                    return f(*args, **kwargs)
                if not tracer:
                    return f(*args, **kwargs)
                with tracer.start_as_current_span(trace_name, attributes=attributes):
                    return f(*args, **kwargs)

            return wrapper

        return decorator

    @staticmethod
    def add_attributes_to_current_span(
        attributes: Dict[
            str,
            Union[
                str,
                bool,
                float,
                int,
                Sequence[str],
                Sequence[bool],
                Sequence[float],
                Sequence[int],
            ],
        ]
    ) -> None:
        """Add attributes to the current span."""
        global tracer, granularity, traces_enabled
        if not traces_enabled:
            return
        if granularity == OpenTelemetryGranularity.NONE:
            return
        if not tracer:
            return
        span = trace.get_current_span()
        span.set_attributes(attributes)

    @staticmethod
    def get_tracer() -> Optional[trace.Tracer]:
        """Get the tracer."""
        global tracer
        return tracer

    def add_observable_gauge(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        """Add an observable gauge to the metrics.
        :param name: The name of the gauge.
        :param callback: A callback function that returns the value of the gauge.
        :param unit: The unit of the gauge.
        :param description: The description of the gauge.

        Example:
            >>> client.add_observable_gauge("my_gauge", lambda: return psutil.cpu_percent(), "percent", "CPU Usage")
        """

        def cb(_: CallbackOptions) -> Iterable[Observation]:
            value = callback()
            if isinstance(value, (int, float)):
                return [Observation(value, {})]
            else:
                return [Observation(v, {}) for v in value]

        meter.create_observable_gauge(
            name=name, callbacks=[cb], unit=unit, description=description
        )

    def add_observable_counter(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        """Add an observable counter to the metrics.
        :param name: The name of the counter.
        :param callback: A callback function that returns the value of the counter.
        :param unit: The unit of the counter.
        :param description: The description of the counter.

        Example:
            >>> client.add_observable_counter("my_counter", lambda: return psutil.net_io_counters().bytes_sent, "bytes", "Bytes out")
        """

        def cb(_: CallbackOptions) -> Iterable[Observation]:
            value = callback()
            if isinstance(value, (int, float)):
                return [Observation(value, {})]
            else:
                return [Observation(v, {}) for v in value]

        meter.create_observable_counter(
            name=name, callbacks=[cb], unit=unit, description=description
        )

    def add_counter(self, name: str, unit: str, description: str) -> Counter:
        """Add a counter to the metrics."""

        if name not in self._counters.keys():
            self._counters[name] = meter.create_counter(
                name=name, unit=unit, description=description
            )
        return self._counters[name]

    @staticmethod
    def get_current_span_id() -> Optional[str]:
        """Get the current span ID."""
        global tracer, granularity, traces_enabled
        if not traces_enabled:
            return None
        if granularity == OpenTelemetryGranularity.NONE:
            return None
        if not tracer:
            return None
        ctx = trace.get_current_span().get_span_context()
        return "{trace:032x}".format(trace=ctx.trace_id)


@contextmanager
def span_proxy_context(span_name: str) -> Generator[Span, None, None]:
    global traces_enabled
    if not traces_enabled:
        yield None
    else:
        _tracer = OpenTelemetryClient.get_tracer()
        if _tracer:
            with _tracer.start_as_current_span(span_name) as span:
                yield span
        else:
            yield None


def trace_method(
    trace_name: str,
    trace_granularity: OpenTelemetryGranularity,
    attributes: Optional[
        Dict[
            str,
            Union[
                str,
                bool,
                float,
                int,
                Sequence[str],
                Sequence[bool],
                Sequence[float],
                Sequence[int],
            ],
        ]
    ] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator that traces a method."""

    return OpenTelemetryClient.trace_method(trace_name, trace_granularity, attributes)


def add_attributes_to_current_span(
    attributes: Dict[
        str,
        Union[
            str,
            bool,
            float,
            int,
            Sequence[str],
            Sequence[bool],
            Sequence[float],
            Sequence[int],
        ],
    ]
) -> None:
    """Add attributes to the current span."""
    OpenTelemetryClient.add_attributes_to_current_span(attributes)

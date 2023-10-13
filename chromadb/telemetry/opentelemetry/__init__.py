from functools import wraps
from enum import Enum
from typing import Any, Callable, Dict, Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from chromadb.config import Component
from chromadb.config import System


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


class OpenTelemetryClient(Component):
    def __init__(self, system: System):
        super().__init__(system)
        otel_init(
            system.settings.chroma_otel_service_name,
            system.settings.chroma_otel_collection_endpoint,
            system.settings.chroma_otel_collection_headers,
            OpenTelemetryGranularity(system.settings.chroma_otel_granularity),
        )


tracer: Optional[trace.Tracer] = None
granularity: OpenTelemetryGranularity = OpenTelemetryGranularity("none")


def otel_init(
    otel_service_name: Optional[str],
    otel_collection_endpoint: Optional[str],
    otel_collection_headers: Optional[Dict[str, str]],
    otel_granularity: OpenTelemetryGranularity,
) -> None:
    """Initializes module-level state for OpenTelemetry."""
    if otel_granularity == OpenTelemetryGranularity.NONE:
        return
    resource = Resource(attributes={SERVICE_NAME: str(otel_service_name)})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(
            # TODO: we may eventually want to make this configurable.
            OTLPSpanExporter(
                endpoint=str(otel_collection_endpoint),
                headers=otel_collection_headers,
            )
        )
    )
    trace.set_tracer_provider(provider)

    global tracer, granularity
    tracer = trace.get_tracer(__name__)
    granularity = otel_granularity


def trace_method(
    trace_name: str,
    trace_granularity: OpenTelemetryGranularity,
    attributes: Dict[str, Any] = {},
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator that traces a method."""

    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Dict[Any, Any]) -> Any:
            global tracer, granularity, _transform_attributes
            if trace_granularity < granularity:
                return f(*args, **kwargs)
            if not tracer:
                return
            with tracer.start_as_current_span(
                trace_name, attributes=_transform_attributes(attributes)
            ):
                return f(*args, **kwargs)

        return wrapper

    return decorator


def add_attributes_to_current_span(attributes: Dict[str, Any]) -> None:
    """Add attributes to the current span."""
    global tracer, granularity, _transform_attributes
    if granularity == OpenTelemetryGranularity.NONE:
        return
    if not tracer:
        return
    span = trace.get_current_span()
    span.set_attributes(_transform_attributes(attributes))  # type: ignore


def _transform_attributes(attributes: Dict[str, Any]) -> Dict[str, str]:
    """Make an attributes dict suitable for passing to opentelemetry."""
    transformed = {}
    for k, v in attributes.items():
        if v is not None:
            # We may want to record values of 0
            transformed[k] = str(v)
    return transformed

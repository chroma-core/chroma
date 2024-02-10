import importlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    Callable,
    Optional,
    Sequence,
    Union,
    Iterable,
    List,
    Dict,
    cast,
)

from overrides import EnforceOverrides
from pydantic.v1 import BaseSettings

from chromadb.config import Component
from chromadb.config import System


# Settings are only read from the environment
class TelemetrySettings(BaseSettings):
    chroma_otel_collection_endpoint: Optional[str] = ""
    chroma_otel_service_name: Optional[str] = "chromadb"
    chroma_otel_collection_headers: Dict[str, str] = {}
    chroma_otel_granularity: Optional[str] = None
    chroma_otel_traces_enabled: bool = False
    chroma_otel_metrics_enabled: bool = False
    chroma_otel_logs_enabled: bool = False


telemetry_settings = TelemetrySettings()


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


traces_enabled = False
metrics_enabled = False
logs_enabled = False

OtelAttributes = Dict[
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


class OtelInstrumentation(ABC, EnforceOverrides):
    """
    An abstract class for OpenTelemetry instrumentation.
    """

    @abstractmethod
    def instrument_fastapi(
        self, app: Any, excluded_urls: Optional[List[str]] = None
    ) -> None:
        ...

    @abstractmethod
    def trace_method(
        self,
        trace_name: str,
        trace_granularity: OpenTelemetryGranularity,
        attributes: Optional[OtelAttributes] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that traces a method."""
        ...

    @abstractmethod
    def histogram(
        self,
        name: str,
        unit: str,
        description: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that creates a histogram for a method execution."""

    @abstractmethod
    def counter(
        self,
        name: str,
        unit: str,
        description: str,
        arg_counter_extractor: Optional[Callable[..., int]] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """A decorator that creates a counter for a method execution."""
        ...

    @abstractmethod
    def add_attributes_to_current_span(self, attributes: OtelAttributes) -> None:
        """Add attributes to the current span."""
        ...

    @abstractmethod
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
            >>> OtelInstrumentation.add_observable_gauge("my_gauge", lambda: return psutil.cpu_percent(), "percent", "CPU Usage")
        """
        ...

    @abstractmethod
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
            >>> OtelInstrumentation.add_observable_counter("my_counter", lambda: return psutil.net_io_counters().bytes_sent, "bytes", "Bytes out")
        """
        ...

    @abstractmethod
    def add_counter(self, name: str, unit: str, description: str) -> None:
        """Add a counter to the metrics."""
        ...

    @abstractmethod
    def update_counter(self, name: str, value: int) -> None:
        """Update a counter."""
        ...

    @abstractmethod
    def add_histogram(
        self,
        name: str,
        unit: str,
        description: str,
    ) -> None:
        """Add a histogram to the metrics."""
        ...

    @abstractmethod
    def record_histogram(
        self,
        name: str,
        value: Union[int, float],
        attributes: Optional[OtelAttributes] = None,
    ) -> None:
        """Record a histogram."""
        ...

    @abstractmethod
    def get_current_span_id(self) -> Optional[str]:
        """Get the current span ID."""
        ...


_otel_instance: Optional[OtelInstrumentation] = None


def get_otel_client() -> Optional[OtelInstrumentation]:
    global _otel_instance
    if (
        not telemetry_settings.chroma_otel_collection_endpoint
        or telemetry_settings.chroma_otel_collection_endpoint == ""
    ):
        return None

    if _otel_instance is None:
        _otel_package = importlib.import_module("chromadb.telemetry.opentelemetry.otel")
        _otel_instance = cast(OtelInstrumentation, _otel_package.otel_client)
    return _otel_instance


class OpenTelemetryClient(Component):
    def __init__(self, system: System):
        super().__init__(system)
        self._otel_client = get_otel_client()

    def add_observable_gauge(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        if self._otel_client is None:
            return
        self._otel_client.add_observable_gauge(
            name=name, callback=callback, unit=unit, description=description
        )

    def add_observable_counter(
        self,
        *,
        name: str,
        callback: Callable[[], Union[Union[int, float], Iterable[Union[int, float]]]],
        unit: str,
        description: str,
    ) -> None:
        if self._otel_client is None:
            return
        self._otel_client.add_observable_counter(
            name=name, callback=callback, unit=unit, description=description
        )


def get_current_span_id() -> Optional[str]:
    otel_instance = get_otel_client()
    if not otel_instance:
        return None
    return otel_instance.get_current_span_id()


def instrument_fastapi(app: Any, excluded_urls: Optional[List[str]] = None) -> None:
    """Instrument a FastAPI application."""
    otel_instance = get_otel_client()
    if not otel_instance:
        return
    otel_instance.instrument_fastapi(app, excluded_urls)


def trace_method(
    trace_name: str,
    trace_granularity: OpenTelemetryGranularity,
    attributes: Optional[OtelAttributes] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A decorator that traces a method."""
    otel_instance = get_otel_client()
    if not otel_instance:
        return lambda f: f
    return otel_instance.trace_method(trace_name, trace_granularity, attributes)


def histogram(
    name: str,
    unit: str,
    description: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator proxy for creating and updating histograms in OTEL."""
    otel_instance = get_otel_client()
    if not otel_instance:
        return lambda f: f
    return otel_instance.histogram(name, unit, description)


def counter(
    name: str,
    unit: str,
    description: str,
    arg_counter_extractor: Optional[Callable[..., int]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator proxy for creating and updating counters in OTEL."""
    otel_instance = get_otel_client()
    if not otel_instance:
        return lambda f: f
    return otel_instance.counter(name, unit, description, arg_counter_extractor)


def add_attributes_to_current_span(attributes: OtelAttributes) -> None:
    """Add attributes to the current span."""
    otel_instance = get_otel_client()
    if not otel_instance:
        return
    otel_instance.add_attributes_to_current_span(attributes)

from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Generator

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
        resource = Resource(
            attributes={SERVICE_NAME: str(system.settings.chroma_otel_service_name)}
        )
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=str(system.settings.chroma_otel_collection_endpoint),
                    headers=system.settings.chroma_otel_collection_headers,
                )
            )
        )
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)
        self.trace_granularity = OpenTelemetryGranularity(
            system.settings.chroma_otel_granularity or OpenTelemetryGranularity.NONE
        )

    @contextmanager
    def trace(
        self, name: str, granularity: OpenTelemetryGranularity, **kwargs: Dict[Any, Any]
    ) -> Generator[Any, Any, Any]:
        if self.trace_granularity > granularity:
            yield
            return
        if "attributes" not in kwargs:
            kwargs["attributes"] = {}
        if "granularity" not in kwargs["attributes"]:
            kwargs["attributes"]["granularity"] = granularity.value
        kwargs["attributes"] = self._transform_attributes(kwargs["attributes"])
        with self.tracer.start_as_current_span(name, **kwargs):  # type: ignore
            yield

    def _transform_attributes(self, attributes: Dict[str, Any]) -> Dict[str, str]:
        """Make an attributes dict suitable for passing to opentelemetry."""
        transformed = {}
        for k, v in attributes.items():
            if v is not None:
                # We may want to record values of 0
                transformed[k] = str(v)
        return transformed

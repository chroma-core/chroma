from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from chromadb.config import Component
from chromadb.config import System


# [] config for this object
# [] put spans ~everywhere
# [] refactor events to add to spans and emit posthog
# [] ensure trace IDs propagate on single node
# [] ensure trace IDs propagate in distributed mode
# [] look into using services for separate components (maybe only in distributed?)
# [] add a couple basic metrics
# [] figure out the log tail thing


class OpenTelemetryClient(Component):
    def __init__(self, system: System):
        super().__init__(system)
        resource = Resource(attributes={SERVICE_NAME: "chromadb"})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint="https://api.honeycomb.io"))
        )
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)

    @contextmanager
    def trace(self, name: str, **kwargs) -> None:
        with self.tracer.start_as_current_span(name, **kwargs):
            yield

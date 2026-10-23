from chromadb.telemetry.product import (
    ProductTelemetryClient,
    ProductTelemetryEvent,
)
from overrides import override


class Posthog(ProductTelemetryClient):
    @override
    def capture(self, event: ProductTelemetryEvent) -> None:
        pass

import posthog
import logging
import sys
from typing import Any, Optional, Set
from chromadb.config import System
from chromadb.telemetry import Telemetry, TelemetryEvent
from overrides import override

logger = logging.getLogger(__name__)


class Posthog(Telemetry):
    def __init__(self, system: System):
        if not system.settings.anonymized_telemetry or "pytest" in sys.modules:
            posthog.disabled = True
        else:
            logger.info(
                "Anonymized telemetry enabled. See https://docs.trychroma.com/telemetry for more information."
            )

        posthog.project_api_key = "phc_YeUxaojbKk5KPi8hNlx1bBKHzuZ4FDtl67kH1blv8Bh"
        posthog_logger = logging.getLogger("posthog")
        # Silence posthog's logging
        posthog_logger.disabled = True

        self.batched_event: Optional[TelemetryEvent] = None
        self.seen_event_types: Set[Any] = set()

        super().__init__(system)

    @override
    def capture(self, event: TelemetryEvent) -> None:
        if event.max_batch_size == 1 or event.batch_key not in self.seen_event_types:
            self.seen_event_types.add(event.batch_key)
            self._direct_capture(event)
            return
        if self.batched_event is None:
            self.batched_event = event
            return
        if not self.batched_event.batch_key == event.batch_key:
            self._direct_capture(self.batched_event)
            self.batched_event = event
            return
        self.batched_event = self.batched_event.batch(event)
        if self.batched_event.batch_size >= self.batched_event.max_batch_size:
            self._direct_capture(self.batched_event)
            self.batched_event = None

    def _direct_capture(self, event: TelemetryEvent) -> None:
        try:
            posthog.capture(
                self.user_id,
                event.name,
                {**(event.properties), "chroma_context": self.context},
            )
        except Exception as e:
            logger.error(f"Failed to send telemetry event {event.name}: {e}")

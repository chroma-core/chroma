import posthog
import logging
import sys
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
        super().__init__(system)

    @override
    def capture(self, event: TelemetryEvent) -> None:
        try:
            posthog.capture(
                self.user_id,
                event.name,
                {**(event.properties), "chroma_context": self.context},
            )
        except Exception as e:
            logger.error(f"Failed to send telemetry event {event.name}: {e}")

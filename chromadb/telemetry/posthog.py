import posthog
import logging
import sys
from chromadb.telemetry import Telemetry, TelemetryEvent

logger = logging.getLogger(__name__)


class Posthog(Telemetry):
    def __init__(self):
        from chromadb import get_settings

        if not get_settings().anonymized_telemetry or "pytest" in sys.modules:
            posthog.disabled = True
        else:
            logger.info("Anonymized telemetry enabled. See <XYZ> for more information.")

        posthog.project_api_key = "phc_YeUxaojbKk5KPi8hNlx1bBKHzuZ4FDtl67kH1blv8Bh"
        super().__init__()

    def capture(self, event: TelemetryEvent):
        try:
            posthog.capture(
                self.user_id, event.name, {**(event.properties), "chroma_context": self.context}
            )
        except Exception as e:
            logger.error(f"Failed to send telemetry event {event.name}: {e}")

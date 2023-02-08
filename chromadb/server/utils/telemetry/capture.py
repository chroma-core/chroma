import posthog
import uuid
import sys
import os
from chromadb.server.utils.telemetry import Telemetry
from chromadb import get_settings

class Capture(Telemetry):
    _conn = None
    _telemetry_anonymized_uuid = None

    def __init__(self):
        if get_settings().disable_anonymized_telemetry:
            posthog.disabled = True

        # disable telemetry if we're running tests
        if "pytest" in sys.modules:
            posthog.disabled = True

        posthog.project_api_key = 'phc_YeUxaojbKk5KPi8hNlx1bBKHzuZ4FDtl67kH1blv8Bh'
        posthog.host = 'https://app.posthog.com'
        self._conn = posthog

        if not get_settings().telemetry_anonymized_uuid:
            self._telemetry_anonymized_uuid = uuid.uuid4()

            with open(".env", "a") as f:
                f.write(f"\ntelemetry_anonymized_uuid={self._telemetry_anonymized_uuid}\n")

        else:
            self._telemetry_anonymized_uuid = get_settings().telemetry_anonymized_uuid

    def capture(self, event, properties=None):
        if properties is None:
            properties = {}

        properties['environment'] = os.getenv('environment', 'development')

        self._conn.capture(self._telemetry_anonymized_uuid, event, properties)

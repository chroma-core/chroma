from chromadb import get_settings

import sentry_sdk
from sentry_sdk.client import Client
from sentry_sdk import configure_scope
from posthog.sentry.posthog_integration import PostHogIntegration
PostHogIntegration.organization = "chroma"
sample_rate = 1.0
if get_settings().environment == "production":
    sample_rate = 0.1

def strip_sensitive_data(event, hint):
    if 'server_name' in event:
        del event['server_name']
        return event

def init_error_reporting():

    if get_settings().environment == "test":
        return

    sentry_sdk.init(
        dsn="https://ef5fae1e461f49b3a7a2adf3404378ab@o4504080408051712.ingest.sentry.io/4504080409296896",
        traces_sample_rate=sample_rate,
        integrations=[PostHogIntegration()],
        environment=get_settings().environment,
        before_send=strip_sensitive_data,
    )
    with configure_scope() as scope:
        scope.set_tag('posthog_distinct_id', get_settings().telemetry_anonymized_uuid)

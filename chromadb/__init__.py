import chromadb.config
import logging
from chromadb.telemetry.events import ClientStartEvent
from chromadb.telemetry.posthog import Posthog

logger = logging.getLogger(__name__)

__settings = chromadb.config.Settings()

__version__ = "0.3.20"


def configure(**kwargs):
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_db(settings=__settings):
    """Return a DB instance based on the provided or environmental settings,
    optionally overriding the DB instance."""
    return chromadb.config.get_component(settings, "chroma_db_impl")


def Client(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
    settings, optionally overriding the DB instance."""
    telemetry_client = chromadb.config.get_component(settings, "chroma_telemetry_impl")
    telemetry_client.capture(ClientStartEvent())
    return chromadb.config.get_component(settings, "chroma_api_impl")

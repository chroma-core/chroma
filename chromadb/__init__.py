import chromadb.config
import logging
from chromadb.telemetry.events import ClientStartEvent

logger = logging.getLogger(__name__)

__settings = chromadb.config.Settings()

__version__ = "0.3.21"


def configure(**kwargs):
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_settings():
    return __settings


def get_db(settings=__settings):
    """Return a chroma.DB instance based on the provided or environmental settings."""
    return settings.get_component("chroma_db_impl")


def Client(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
    settings, optionally overriding the DB instance."""

    telemetry_client = settings.get_component("chroma_telemetry_impl")

    # Submit event for client start
    telemetry_client.capture(ClientStartEvent())

    return settings.get_component("chroma_api_impl")
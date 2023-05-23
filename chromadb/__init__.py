import chromadb.config
import logging
from chromadb.telemetry.events import ClientStartEvent
from chromadb.config import Settings, System
from chromadb.api import API

logger = logging.getLogger(__name__)

__settings = Settings()

__version__ = "0.3.25"


def configure(**kwargs) -> None:  # type: ignore
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_settings() -> Settings:
    return __settings


def Client(settings: Settings = __settings) -> API:
    """Return a chroma.API instance based on the provided or environmental
    settings, optionally overriding the DB instance."""

    system = System(settings)

    telemetry_client = system.get_telemetry()

    # Submit event for client start
    telemetry_client.capture(ClientStartEvent())

    return system.get_api()

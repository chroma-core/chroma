import chromadb.config
import logging
from chromadb.telemetry.events import ClientStartEvent
from chromadb.telemetry import Telemetry
from chromadb.config import Settings, System
from chromadb.api import API

logger = logging.getLogger(__name__)

__settings = Settings()

__version__ = "0.3.26"


def configure(**kwargs) -> None:  # type: ignore
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_settings() -> Settings:
    return __settings


def Client(settings: Settings = __settings) -> API:
    """Return a running chroma.API instance"""

    system = System(settings)

    telemetry_client = system.instance(Telemetry)
    api = system.instance(API)

    system.start()

    # Submit event for client start
    telemetry_client.capture(ClientStartEvent())

    return api

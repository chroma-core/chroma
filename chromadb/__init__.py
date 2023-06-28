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


def EphemeralClient() -> API:
    """
    Creates an in-memory instance of Chroma. This is useful for testing and
    development, but not recommended for production use.
    """
    return Client()


def PersistentClient(path: str = "./chroma") -> API:
    """
    Creates a persistent instance of Chroma that saves to disk. This is useful for
    testing and development, but not recommended for production use.

    Args:
        path: The directory to save Chroma's data to. Defaults to "./chroma".
    """
    return Client(
        Settings(
            persist_directory=path,
            chroma_db_impl="chromadb.db.duckdb.PersistentDuckDB",
        )
    )


def HttpClient(host: str = "localhost", port: str = "8000", ssl: bool = False) -> API:
    """
    Creates a client that connects to a remote Chroma server. This supports
    many clients connecting to the same server, and is the recommended way to
    use Chroma in production.

    Args:
        host: The hostname of the Chroma server. Defaults to "localhost".
        port: The port of the Chroma server. Defaults to "8000".
        ssl: Whether to use SSL to connect to the Chroma server. Defaults to False.
    """
    return Client(
        Settings(
            chroma_api_impl="rest",
            chroma_server_host=host,
            chroma_server_http_port=port,
            chroma_server_ssl_enabled=ssl,
            # TODO: auth headers
        )
    )


def Client(settings: Settings = __settings) -> API:
    """Return a running chroma.API instance"""

    system = System(settings)

    telemetry_client = system.instance(Telemetry)
    api = system.instance(API)

    system.start()

    # Submit event for client start
    telemetry_client.capture(ClientStartEvent())

    return api

from typing import Dict
import logging
import chromadb.config
from chromadb.config import Settings, System
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    EmbeddingFunction,
    Embeddings,
    IDs,
    Include,
    Metadata,
    Where,
    QueryResult,
    GetResult,
    WhereDocument,
    UpdateCollectionMetadata,
)

# Re-export types from chromadb.types
__all__ = [
    "Collection",
    "Metadata",
    "Where",
    "WhereDocument",
    "Documents",
    "IDs",
    "Embeddings",
    "EmbeddingFunction",
    "Include",
    "CollectionMetadata",
    "UpdateCollectionMetadata",
    "QueryResult",
    "GetResult",
]
from chromadb.telemetry.events import ClientStartEvent
from chromadb.telemetry import Telemetry


logger = logging.getLogger(__name__)

__settings = Settings()

__version__ = "0.4.14"

# Workaround to deal with Colab's old sqlite3 version
try:
    import google.colab  # noqa: F401

    IN_COLAB = True
except ImportError:
    IN_COLAB = False

is_client = False
try:
    from chromadb.is_thin_client import is_thin_client  # type: ignore
    is_client = is_thin_client
except ImportError:
    is_client = False

if not is_client:
    import sqlite3
    if sqlite3.sqlite_version_info < (3, 35, 0):
        if IN_COLAB:
            # In Colab, hotswap to pysqlite-binary if it's too old
            import subprocess
            import sys

            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "pysqlite3-binary"]
            )
            __import__("pysqlite3")
            sys.modules["sqlite3"] = sys.modules.pop("pysqlite3")
        else:
            raise RuntimeError(
                "\033[91mYour system has an unsupported version of sqlite3. Chroma requires sqlite3 >= 3.35.0.\033[0m\n"
                "\033[94mPlease visit https://docs.trychroma.com/troubleshooting#sqlite to learn how to upgrade.\033[0m"
            )


def configure(**kwargs) -> None:  # type: ignore
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_settings() -> Settings:
    return __settings


def EphemeralClient(settings: Settings = Settings()) -> API:
    """
    Creates an in-memory instance of Chroma. This is useful for testing and
    development, but not recommended for production use.
    """
    settings.is_persistent = False

    return Client(settings)


def PersistentClient(path: str = "./chroma", settings: Settings = Settings()) -> API:
    """
    Creates a persistent instance of Chroma that saves to disk. This is useful for
    testing and development, but not recommended for production use.

    Args:
        path: The directory to save Chroma's data to. Defaults to "./chroma".
    """
    settings.persist_directory = path
    settings.is_persistent = True

    return Client(settings)


def HttpClient(
    host: str = "localhost",
    port: str = "8000",
    ssl: bool = False,
    headers: Dict[str, str] = {},
    settings: Settings = Settings(),
) -> API:
    """
    Creates a client that connects to a remote Chroma server. This supports
    many clients connecting to the same server, and is the recommended way to
    use Chroma in production.

    Args:
        host: The hostname of the Chroma server. Defaults to "localhost".
        port: The port of the Chroma server. Defaults to "8000".
        ssl: Whether to use SSL to connect to the Chroma server. Defaults to False.
        headers: A dictionary of headers to send to the Chroma server. Defaults to {}.
    """

    settings.chroma_api_impl = "chromadb.api.fastapi.FastAPI"
    settings.chroma_server_host = host
    settings.chroma_server_http_port = port
    settings.chroma_server_ssl_enabled = ssl
    settings.chroma_server_headers = headers

    return Client(settings)


def Client(settings: Settings = __settings) -> API:
    """Return a running chroma.API instance"""

    system = System(settings)

    telemetry_client = system.instance(Telemetry)
    api = system.instance(API)

    system.start()

    # Submit event for client start
    telemetry_client.capture(ClientStartEvent())

    return api

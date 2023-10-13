from typing import Dict
import logging
from chromadb.api.client import Client as ClientCreator
import chromadb.config
from chromadb.config import Settings
from chromadb.api import ClientAPI
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
    from chromadb.is_thin_client import is_thin_client

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


def EphemeralClient(settings: Settings = Settings()) -> ClientAPI:
    """
    Creates an in-memory instance of Chroma. This is useful for testing and
    development, but not recommended for production use.
    """
    settings.is_persistent = False

    return Client(settings)


def PersistentClient(
    path: str = "./chroma",
    tenant: str = "default",
    database: str = "default",
    settings: Settings = Settings(),
) -> ClientAPI:
    """
    Creates a persistent instance of Chroma that saves to disk. This is useful for
    testing and development, but not recommended for production use.

    Args:
        path: The directory to save Chroma's data to. Defaults to "./chroma".
    """
    settings.persist_directory = path
    settings.is_persistent = True

    return ClientCreator(tenant=tenant, database=database, settings=settings)


def HttpClient(
    host: str = "localhost",
    port: str = "8000",
    ssl: bool = False,
    headers: Dict[str, str] = {},
    tenant: str = "default",
    database: str = "default",
    settings: Settings = Settings(),
) -> ClientAPI:
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

    return ClientCreator(tenant=tenant, database=database, settings=settings)


# TODO: replace default tenant and database strings with constants
def Client(
    settings: Settings = __settings, tenant: str = "default", database: str = "default"
) -> ClientAPI:
    """Return a running chroma.API instance"""

    # Change this to actually check if an "API" instance already exists, wrap it in a
    # tenant/database aware "Client", and return it
    # this way we can support multiple clients in the same process but using the same
    # chroma instance

    # API is thread safe, so we can just return the same instance
    # This way a "Client" will just be a wrapper around an API instance that is
    # tenant/database aware

    # To do this we will
    # 1. Have a global dict of API instances, keyed by path
    # 2. When a client is requested, check if one exists in the dict, and if so check if its
    # settings match the requested settings
    # 3. If the settings match, construct a new Client that wraps the existing API instance with
    # the tenant/database
    # 4. If the settings don't match, error out because we don't support changing the settings
    # got a given database
    # 5. If no client exists in the dict, create a new API instance, wrap it in a Client, and
    # add it to the dict

    # The hierarchy then becomes
    # For local
    # Path -> Tenant -> Namespace -> API
    # For remote
    # Host -> Tenant -> Namespace -> API

    # A given API for a path is a singleton, and is shared between all tenants and namespaces
    # for that path

    # A DB exists at a path or host, and has tenants and namespaces

    # All our tests currently use system.instance(API) assuming thats the root object
    # This is likely fine,

    return ClientCreator(tenant=tenant, database=database, settings=settings)

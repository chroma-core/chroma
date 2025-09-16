from typing import Dict, Optional, Union
import logging
from chromadb.api.client import Client as ClientCreator
from chromadb.api.client import (
    AdminClient as AdminClientCreator,
)
from chromadb.api.async_client import AsyncClient as AsyncClientCreator
from chromadb.auth.token_authn import TokenTransportHeader
import chromadb.config
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings
from chromadb.api import AdminAPI, AsyncClientAPI, ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    CollectionMetadata,
    UpdateMetadata,
    Documents,
    EmbeddingFunction,
    Embeddings,
    URI,
    URIs,
    IDs,
    Include,
    Metadata,
    Metadatas,
    Where,
    QueryResult,
    GetResult,
    WhereDocument,
    UpdateCollectionMetadata,
)

# Import Search API components
from chromadb.execution.expression.plan import Search
from chromadb.execution.expression.operator import (
    # Key builder for where conditions and field selection
    Key,
    K,  # Alias for Key
    # KNN-based ranking for hybrid search
    Knn,
)
from pathlib import Path
import os

# Re-export types from chromadb.types
__all__ = [
    "Collection",
    "Metadata",
    "Metadatas",
    "Where",
    "WhereDocument",
    "Documents",
    "IDs",
    "URI",
    "URIs",
    "Embeddings",
    "EmbeddingFunction",
    "Include",
    "CollectionMetadata",
    "UpdateMetadata",
    "UpdateCollectionMetadata",
    "QueryResult",
    "GetResult",
    "TokenTransportHeader",
    # Search API components
    "Search",
    "Key",
    "K",
    "Knn",
]

from chromadb.types import CloudClientArg

logger = logging.getLogger(__name__)

__settings = Settings()

__version__ = "1.1.0"


# Workaround to deal with Colab's old sqlite3 version
def is_in_colab() -> bool:
    try:
        import google.colab  # noqa: F401

        return True
    except ImportError:
        return False


IN_COLAB = is_in_colab()

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
                "\033[91mYour system has an unsupported version of sqlite3. Chroma \
                    requires sqlite3 >= 3.35.0.\033[0m\n"
                "\033[94mPlease visit \
                    https://docs.trychroma.com/troubleshooting#sqlite to learn how \
                    to upgrade.\033[0m"
            )


def configure(**kwargs) -> None:  # type: ignore
    """Override Chroma's default settings, environment variables or .env files"""
    global __settings
    __settings = chromadb.config.Settings(**kwargs)


def get_settings() -> Settings:
    return __settings


def EphemeralClient(
    settings: Optional[Settings] = None,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> ClientAPI:
    """
    Creates an in-memory instance of Chroma. This is useful for testing and
    development, but not recommended for production use.

    Args:
        tenant: The tenant to use for this client. Defaults to the default tenant.
        database: The database to use for this client. Defaults to the default database.
    """
    if settings is None:
        settings = Settings()
    settings.is_persistent = False

    # Make sure paramaters are the correct types -- users can pass anything.
    tenant = str(tenant)
    database = str(database)

    return ClientCreator(settings=settings, tenant=tenant, database=database)


def PersistentClient(
    path: Union[str, Path] = "./chroma",
    settings: Optional[Settings] = None,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> ClientAPI:
    """
    Creates a persistent instance of Chroma that saves to disk. This is useful for
    testing and development, but not recommended for production use.

    Args:
        path: The directory to save Chroma's data to. Defaults to "./chroma".
        tenant: The tenant to use for this client. Defaults to the default tenant.
        database: The database to use for this client. Defaults to the default database.
    """
    if settings is None:
        settings = Settings()
    settings.persist_directory = str(path)
    settings.is_persistent = True

    # Make sure paramaters are the correct types -- users can pass anything.
    tenant = str(tenant)
    database = str(database)

    return ClientCreator(tenant=tenant, database=database, settings=settings)


def RustClient(
    path: Optional[str] = None,
    settings: Optional[Settings] = None,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> ClientAPI:
    """
    Creates an ephemeral or persistance instance of Chroma that saves to disk.
    This is useful for testing and development, but not recommended for production use.

    Args:
        path: An optional directory to save Chroma's data to. The client is ephemeral if a None value is provided. Defaults to None.
        tenant: The tenant to use for this client. Defaults to the default tenant.
        database: The database to use for this client. Defaults to the default database.
    """
    if settings is None:
        settings = Settings()

    settings.chroma_api_impl = "chromadb.api.rust.RustBindingsAPI"
    settings.is_persistent = path is not None
    settings.persist_directory = path or ""

    # Make sure paramaters are the correct types -- users can pass anything.
    tenant = str(tenant)
    database = str(database)

    return ClientCreator(tenant=tenant, database=database, settings=settings)


def HttpClient(
    host: str = "localhost",
    port: int = 8000,
    ssl: bool = False,
    headers: Optional[Dict[str, str]] = None,
    settings: Optional[Settings] = None,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> ClientAPI:
    """
    Creates a client that connects to a remote Chroma server. This supports
    many clients connecting to the same server, and is the recommended way to
    use Chroma in production.

    Args:
        host: The hostname of the Chroma server. Defaults to "localhost".
        port: The port of the Chroma server. Defaults to 8000.
        ssl: Whether to use SSL to connect to the Chroma server. Defaults to False.
        headers: A dictionary of headers to send to the Chroma server. Defaults to {}.
        settings: A dictionary of settings to communicate with the chroma server.
        tenant: The tenant to use for this client. Defaults to the default tenant.
        database: The database to use for this client. Defaults to the default database.
    """

    if settings is None:
        settings = Settings()

    # Make sure parameters are the correct types -- users can pass anything.
    host = str(host)
    port = int(port)
    ssl = bool(ssl)
    tenant = str(tenant)
    database = str(database)

    settings.chroma_api_impl = "chromadb.api.fastapi.FastAPI"
    if settings.chroma_server_host and settings.chroma_server_host != host:
        raise ValueError(
            f"Chroma server host provided in settings[{settings.chroma_server_host}] is different to the one provided in HttpClient: [{host}]"
        )
    settings.chroma_server_host = host
    if settings.chroma_server_http_port and settings.chroma_server_http_port != port:
        raise ValueError(
            f"Chroma server http port provided in settings[{settings.chroma_server_http_port}] is different to the one provided in HttpClient: [{port}]"
        )
    settings.chroma_server_http_port = port
    settings.chroma_server_ssl_enabled = ssl
    settings.chroma_server_headers = headers

    return ClientCreator(tenant=tenant, database=database, settings=settings)


async def AsyncHttpClient(
    host: str = "localhost",
    port: int = 8000,
    ssl: bool = False,
    headers: Optional[Dict[str, str]] = None,
    settings: Optional[Settings] = None,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> AsyncClientAPI:
    """
    Creates an async client that connects to a remote Chroma server. This supports
    many clients connecting to the same server, and is the recommended way to
    use Chroma in production.

    Args:
        host: The hostname of the Chroma server. Defaults to "localhost".
        port: The port of the Chroma server. Defaults to 8000.
        ssl: Whether to use SSL to connect to the Chroma server. Defaults to False.
        headers: A dictionary of headers to send to the Chroma server. Defaults to {}.
        settings: A dictionary of settings to communicate with the chroma server.
        tenant: The tenant to use for this client. Defaults to the default tenant.
        database: The database to use for this client. Defaults to the default database.
    """

    if settings is None:
        settings = Settings()

    # Make sure parameters are the correct types -- users can pass anything.
    host = str(host)
    port = int(port)
    ssl = bool(ssl)
    tenant = str(tenant)
    database = str(database)

    settings.chroma_api_impl = "chromadb.api.async_fastapi.AsyncFastAPI"
    if settings.chroma_server_host and settings.chroma_server_host != host:
        raise ValueError(
            f"Chroma server host provided in settings[{settings.chroma_server_host}] is different to the one provided in HttpClient: [{host}]"
        )
    settings.chroma_server_host = host
    if settings.chroma_server_http_port and settings.chroma_server_http_port != port:
        raise ValueError(
            f"Chroma server http port provided in settings[{settings.chroma_server_http_port}] is different to the one provided in HttpClient: [{port}]"
        )
    settings.chroma_server_http_port = port
    settings.chroma_server_ssl_enabled = ssl
    settings.chroma_server_headers = headers

    return await AsyncClientCreator.create(
        tenant=tenant, database=database, settings=settings
    )


def CloudClient(
    tenant: Optional[str] = None,
    database: Optional[str] = None,
    api_key: Optional[str] = None,
    settings: Optional[Settings] = None,
    *,  # Following arguments are keyword-only, intended for testing only.
    cloud_host: str = "api.trychroma.com",
    cloud_port: int = 443,
    enable_ssl: bool = True,
) -> ClientAPI:
    """
    Creates a client to connect to a tenant and database on Chroma cloud.

    Args:
        tenant: The tenant to use for this client. Optional. If not provided, it will be inferred from the API key if the key is scoped to a single tenant. If provided, it will be validated against the API key's scope.
        database: The database to use for this client. Optional. If not provided, it will be inferred from the API key if the key is scoped to a single database. If provided, it will be validated against the API key's scope.
        api_key: The api key to use for this client.
    """

    required_args = [
        CloudClientArg(name="api_key", env_var="CHROMA_API_KEY", value=api_key),
    ]

    # If api_key is not provided, try to load it from the environment variable
    if not all([arg.value for arg in required_args]):
        for arg in required_args:
            arg.value = arg.value or os.environ.get(arg.env_var)

    missing_args = [arg for arg in required_args if arg.value is None]
    if missing_args:
        raise ValueError(
            f"Missing required arguments: {', '.join([arg.name for arg in missing_args])}. "
            f"Please provide them or set the environment variables: {', '.join([arg.env_var for arg in missing_args])}"
        )

    if settings is None:
        settings = Settings()

    # Make sure paramaters are the correct types -- users can pass anything.
    tenant = tenant or os.environ.get("CHROMA_TENANT")
    if tenant is not None:
        tenant = str(tenant)
    database = database or os.environ.get("CHROMA_DATABASE")
    if database is not None:
        database = str(database)
    api_key = str(api_key)
    cloud_host = str(cloud_host)
    cloud_port = int(cloud_port)
    enable_ssl = bool(enable_ssl)

    settings.chroma_api_impl = "chromadb.api.fastapi.FastAPI"
    settings.chroma_server_host = cloud_host
    settings.chroma_server_http_port = cloud_port
    settings.chroma_server_ssl_enabled = enable_ssl

    settings.chroma_client_auth_provider = (
        "chromadb.auth.token_authn.TokenAuthClientProvider"
    )
    settings.chroma_client_auth_credentials = api_key
    settings.chroma_auth_token_transport_header = TokenTransportHeader.X_CHROMA_TOKEN
    settings.chroma_overwrite_singleton_tenant_database_access_from_auth = True

    return ClientCreator(tenant=tenant, database=database, settings=settings)


def Client(
    settings: Settings = __settings,
    tenant: str = DEFAULT_TENANT,
    database: str = DEFAULT_DATABASE,
) -> ClientAPI:
    """
    Return a running chroma.API instance

    tenant: The tenant to use for this client. Defaults to the default tenant.
    database: The database to use for this client. Defaults to the default database.
    """

    # Make sure paramaters are the correct types -- users can pass anything.
    tenant = str(tenant)
    database = str(database)

    return ClientCreator(tenant=tenant, database=database, settings=settings)


def AdminClient(settings: Settings = Settings()) -> AdminAPI:
    """

    Creates an admin client that can be used to create tenants and databases.

    """
    return AdminClientCreator(settings=settings)

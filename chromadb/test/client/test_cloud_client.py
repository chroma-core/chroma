import multiprocessing
from typing import Any, Dict, Generator, Optional, Tuple
import pytest
from chromadb import CloudClient
from chromadb.api import ServerAPI
from chromadb.auth.token import TokenTransportHeader
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.errors import AuthorizationError

from chromadb.test.conftest import _await_server, _run_server, find_free_port

TOKEN_TRANSPORT_HEADER = TokenTransportHeader.X_CHROMA_TOKEN.name
TEST_CLOUD_HOST = "localhost"


@pytest.fixture(scope="module")
def valid_token() -> str:
    return "valid_token"


@pytest.fixture(scope="module")
def mock_cloud_server(valid_token: str) -> Generator[System, None, None]:
    chroma_server_auth_provider: str = "chromadb.auth.token.TokenAuthServerProvider"
    chroma_server_auth_credentials_provider: str = (
        "chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
    )
    chroma_server_auth_credentials: str = valid_token
    chroma_server_auth_token_transport_header: str = TOKEN_TRANSPORT_HEADER

    port = find_free_port()

    args: Tuple[
        int,
        bool,
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[Dict[str, Any]],
    ] = (
        port,
        False,
        None,
        chroma_server_auth_provider,
        chroma_server_auth_credentials_provider,
        None,
        chroma_server_auth_credentials,
        chroma_server_auth_token_transport_header,
        None,
        None,
        None,
    )
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_run_server, args=args, daemon=True)
    proc.start()

    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host=TEST_CLOUD_HOST,
        chroma_server_http_port=str(port),
        chroma_client_auth_provider="chromadb.auth.token.TokenAuthClientProvider",
        chroma_client_auth_credentials=valid_token,
        chroma_client_auth_token_transport_header=TOKEN_TRANSPORT_HEADER,
    )

    system = System(settings)
    api = system.instance(ServerAPI)
    system.start()
    _await_server(api)
    yield system
    system.stop()
    proc.kill()


def test_valid_key(mock_cloud_server: System, valid_token: str) -> None:
    valid_client = CloudClient(
        tenant=DEFAULT_TENANT,
        database=DEFAULT_DATABASE,
        api_key=valid_token,
        cloud_host=TEST_CLOUD_HOST,
        cloud_port=mock_cloud_server.settings.chroma_server_http_port,  # type: ignore
        enable_ssl=False,
    )

    assert valid_client.heartbeat()


def test_invalid_key(mock_cloud_server: System, valid_token: str) -> None:
    # Try to connect to the default tenant and database with an invalid token
    invalid_token = valid_token + "_invalid"
    with pytest.raises(AuthorizationError):
        client = CloudClient(
            tenant=DEFAULT_TENANT,
            database=DEFAULT_DATABASE,
            api_key=invalid_token,
            cloud_host=TEST_CLOUD_HOST,
            cloud_port=mock_cloud_server.settings.chroma_server_http_port,  # type: ignore
            enable_ssl=False,
        )
        client.heartbeat()

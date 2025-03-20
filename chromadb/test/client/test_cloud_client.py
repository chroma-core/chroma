import multiprocessing
from typing import Generator
import pytest
import httpx
from fastapi import FastAPI, Request, Response
from chromadb import CloudClient
from chromadb.api import ServerAPI
from chromadb.auth.token_authn import TokenTransportHeader
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.errors import ChromaAuthError
import uvicorn
import json

from chromadb.test.conftest import _await_server, find_free_port

TOKEN_TRANSPORT_HEADER = TokenTransportHeader.X_CHROMA_TOKEN
TEST_VALID_TOKEN = "valid_token"


def create_passthrough_proxy(target_url: str, port: int) -> None:
    """
    Creates a server that listens on 'port' and forwards requests to 'target_url'.
    It checks the 'x-chroma-token' of incoming requests against the expected token.
    """
    app = FastAPI()

    # Catch-all route that forwards GET/POST/PUT/DELETE/PATCH/OPTIONS/HEAD
    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
    )
    async def proxy(request: Request, path: str) -> Response:
        # Check for the x-chroma-token header
        token = request.headers.get(TOKEN_TRANSPORT_HEADER)
        if token != TEST_VALID_TOKEN:
            return Response(
                content=json.dumps(
                    {"error": "AuthError", "message": "Invalid or missing token"}
                ),
                status_code=403,
                media_type="application/json",
            )

        # Forward the request to the target server using an async HTTP client
        async with httpx.AsyncClient() as client:
            # Copy all headers except 'host'
            forward_headers = {
                k: v for k, v in request.headers.items() if k.lower() != "host"
            }
            # Read request body (if any)
            body = await request.body()

            # Make the proxied request
            proxied_response = await client.request(
                method=request.method,
                url=f"{target_url}/{path}",
                params=dict(request.query_params),  # preserve ?query=params
                headers=forward_headers,
                content=body,
                follow_redirects=False,
            )

        # Return the proxied response to the caller
        return Response(
            content=proxied_response.content,
            status_code=proxied_response.status_code,
            headers=dict(proxied_response.headers),
        )

    config = uvicorn.Config(app=app, host="localhost", port=port, log_level="info")
    server = uvicorn.Server(config)
    server.run()


@pytest.fixture
def mock_cloud_server(http_server: System) -> Generator[System, None, None]:
    port = find_free_port()
    target_url = f"http://localhost:{http_server.settings.chroma_server_http_port}"

    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(
        target=create_passthrough_proxy, args=(target_url, port), daemon=True
    )
    proc.start()
    settings = Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=port,
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=TEST_VALID_TOKEN,
        chroma_auth_token_transport_header=TOKEN_TRANSPORT_HEADER,
    )
    system = System(settings)
    api = system.instance(ServerAPI)
    system.start()
    _await_server(api)
    yield system
    system.stop()
    proc.kill()


def test_valid_key(mock_cloud_server: System) -> None:
    valid_client = CloudClient(
        tenant=DEFAULT_TENANT,
        database=DEFAULT_DATABASE,
        api_key=TEST_VALID_TOKEN,
        cloud_host="localhost",
        cloud_port=mock_cloud_server.settings.chroma_server_http_port or 8000,
        enable_ssl=False,
    )

    assert valid_client.heartbeat()


def test_invalid_key(mock_cloud_server: System) -> None:
    # Try to connect to the default tenant and database with an invalid token
    invalid_token = TEST_VALID_TOKEN + "_invalid"
    with pytest.raises(ChromaAuthError):
        client = CloudClient(
            tenant=DEFAULT_TENANT,
            database=DEFAULT_DATABASE,
            api_key=invalid_token,
            cloud_host="localhost",
            cloud_port=mock_cloud_server.settings.chroma_server_http_port or 8000,
            enable_ssl=False,
        )
        client.heartbeat()

import base64
from typing import Any, AsyncGenerator, Dict, Mapping, Optional
from unittest.mock import patch

import httpx
import pytest

from chromadb.api.async_fastapi import AsyncFastAPI
from chromadb.config import Settings, System

BASIC_CREDENTIALS = "admin:admin"
EXPECTED_BASIC_AUTHORIZATION = "Basic " + base64.b64encode(
    BASIC_CREDENTIALS.encode("utf-8")
).decode("utf-8")
TOKEN_CREDENTIALS = "test-token"
EXPECTED_BEARER_AUTHORIZATION = f"Bearer {TOKEN_CREDENTIALS}"


@pytest.fixture(autouse=True)
async def clear_async_clients() -> AsyncGenerator[None, None]:
    AsyncFastAPI._clients.clear()
    yield
    clients = list(AsyncFastAPI._clients.values())
    AsyncFastAPI._clients.clear()
    for client in clients:
        await client.aclose()


def _make_async_api(**settings_kwargs: Any) -> AsyncFastAPI:
    settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=8000,
        **settings_kwargs,
    )
    return AsyncFastAPI(System(settings))


def _header(headers: Mapping[str, str], name: str) -> Optional[str]:
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


async def _capture_heartbeat_headers(api: AsyncFastAPI) -> Dict[str, str]:
    captured: Dict[str, Any] = {}

    async def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        captured["headers"] = kwargs.get("headers") or {}
        request = httpx.Request(method, str(url))
        return httpx.Response(
            200,
            json={"nanosecond heartbeat": 1},
            request=request,
        )

    with patch.object(api._get_client(), "request", side_effect=fake_request):
        await api.heartbeat()

    headers = captured.get("headers") or {}
    return dict(headers)


def test_async_fastapi_applies_basic_auth_headers() -> None:
    api = _make_async_api(
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials=BASIC_CREDENTIALS,
    )

    assert _header(api.get_request_headers(), "Authorization") == (
        EXPECTED_BASIC_AUTHORIZATION
    )


def test_async_fastapi_applies_token_auth_headers() -> None:
    api = _make_async_api(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=TOKEN_CREDENTIALS,
    )

    assert _header(api.get_request_headers(), "Authorization") == (
        EXPECTED_BEARER_AUTHORIZATION
    )


def test_async_fastapi_applies_x_chroma_token_auth_headers() -> None:
    api = _make_async_api(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=TOKEN_CREDENTIALS,
        chroma_auth_token_transport_header="X-Chroma-Token",
    )

    headers = api.get_request_headers()
    assert _header(headers, "X-Chroma-Token") == TOKEN_CREDENTIALS
    assert _header(headers, "Authorization") is None


def test_async_fastapi_preserves_custom_headers_with_auth() -> None:
    api = _make_async_api(
        chroma_server_headers={"X-Custom": "present"},
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials=BASIC_CREDENTIALS,
    )

    headers = api.get_request_headers()
    assert _header(headers, "X-Custom") == "present"
    assert _header(headers, "Authorization") == EXPECTED_BASIC_AUTHORIZATION


def test_async_fastapi_omits_auth_headers_when_unconfigured() -> None:
    api = _make_async_api()

    headers = api.get_request_headers()
    assert _header(headers, "Authorization") is None
    assert _header(headers, "X-Chroma-Token") is None


@pytest.mark.asyncio
async def test_async_fastapi_sends_auth_on_request() -> None:
    api = _make_async_api(
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials=BASIC_CREDENTIALS,
    )

    request_headers = await _capture_heartbeat_headers(api)
    assert _header(request_headers, "Authorization") == EXPECTED_BASIC_AUTHORIZATION


@pytest.mark.asyncio
async def test_async_fastapi_sends_auth_when_loop_client_already_cached() -> None:
    unauthenticated = _make_async_api()
    unauthenticated._get_client()

    authenticated = _make_async_api(
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials=BASIC_CREDENTIALS,
    )

    assert authenticated._get_client() is unauthenticated._get_client()
    assert _header(authenticated.get_request_headers(), "Authorization") == (
        EXPECTED_BASIC_AUTHORIZATION
    )

    request_headers = await _capture_heartbeat_headers(authenticated)
    assert _header(request_headers, "Authorization") == EXPECTED_BASIC_AUTHORIZATION

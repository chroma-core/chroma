import asyncio
from typing import Any, List, cast
from unittest.mock import MagicMock, patch

from chromadb.api.async_fastapi import AsyncFastAPI
from chromadb.config import Settings, System


class _FakeAsyncHTTPClient:
    def __init__(self, **kwargs: Any) -> None:
        self.headers = kwargs.get("headers", {})
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def test_async_fastapi_clients_are_instance_scoped() -> None:
    settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
    )
    created_clients: List[_FakeAsyncHTTPClient] = []

    def factory(*_: Any, **kwargs: Any) -> _FakeAsyncHTTPClient:
        client = _FakeAsyncHTTPClient(**kwargs)
        created_clients.append(client)
        return client

    async def run() -> None:
        with patch.object(AsyncFastAPI, "require", return_value=MagicMock()):
            with patch(
                "chromadb.api.async_fastapi.httpx.AsyncClient", side_effect=factory
            ):
                api_one = AsyncFastAPI(System(settings))
                api_two = AsyncFastAPI(System(settings))

                client_one = cast(_FakeAsyncHTTPClient, api_one._get_client())
                client_two = cast(_FakeAsyncHTTPClient, api_two._get_client())

                assert client_one is not client_two
                assert len(created_clients) == 2

                await api_one._cleanup()
                assert client_one.closed is True
                assert client_two.closed is False
                assert api_two._get_client() is client_two

                await api_two._cleanup()
                assert client_two.closed is True

    asyncio.run(run())

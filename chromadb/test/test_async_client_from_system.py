import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from chromadb.api.async_client import AsyncClient
from chromadb.auth import UserIdentity
from chromadb.config import Settings, System


def test_async_client_from_system_async_reuses_provided_system() -> None:
    settings = Settings(
        chroma_api_impl="chromadb.api.async_fastapi.AsyncFastAPI",
        chroma_server_host="localhost",
        chroma_server_http_port=9000,
    )
    system = MagicMock(spec=System)
    system.settings = settings
    system.instance.return_value = MagicMock()

    with patch.object(
        AsyncClient,
        "get_user_identity",
        new=AsyncMock(
            return_value=UserIdentity(
                user_id="test-user",
                tenant="default_tenant",
                databases=["default_database"],
            )
        ),
    ):
        with patch.object(AsyncClient, "_validate_tenant_database", new=AsyncMock()):
            with patch.object(AsyncClient, "_submit_client_start_event"):
                with patch(
                    "chromadb.api.async_client.AsyncAdminClient.from_system",
                    return_value=MagicMock(),
                ):
                    client = asyncio.run(AsyncClient.from_system_async(system))

    assert client._system is system
    client.clear_system_cache()

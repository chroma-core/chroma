"""AsyncClient must pass its own tenant and database to the server API."""

from typing import Any, Dict
from uuid import uuid4

from chromadb.api.async_client import AsyncClient


class _RecordingServer:
    def __init__(self) -> None:
        self.calls: Dict[str, Dict[str, Any]] = {}

    async def _count(self, **kwargs: Any) -> int:
        self.calls["_count"] = kwargs
        return 0

    async def _peek(self, **kwargs: Any) -> Any:
        self.calls["_peek"] = kwargs
        return {}


def _client_for(tenant: str, database: str) -> "tuple[AsyncClient, _RecordingServer]":
    server = _RecordingServer()
    client = AsyncClient.__new__(AsyncClient)
    client.tenant = tenant
    client.database = database
    client._server = server  # type: ignore[assignment]
    return client, server


async def test_count_uses_client_tenant_and_database() -> None:
    client, server = _client_for("my_tenant", "my_database")
    collection_id = uuid4()

    await client._count(collection_id)

    assert server.calls["_count"]["collection_id"] == collection_id
    assert server.calls["_count"]["tenant"] == "my_tenant"
    assert server.calls["_count"]["database"] == "my_database"


async def test_peek_uses_client_tenant_and_database() -> None:
    client, server = _client_for("my_tenant", "my_database")
    collection_id = uuid4()

    await client._peek(collection_id, n=3)

    assert server.calls["_peek"]["collection_id"] == collection_id
    assert server.calls["_peek"]["n"] == 3
    assert server.calls["_peek"]["tenant"] == "my_tenant"
    assert server.calls["_peek"]["database"] == "my_database"

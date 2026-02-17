from pathlib import Path

import pytest
import chromadb

pytest.importorskip("chromadb_rust_bindings")


@pytest.mark.asyncio
async def test_async_persistent_client_round_trip(tmp_path: Path) -> None:
    client = await chromadb.AsyncPersistentClient(path=tmp_path)
    collection = await client.create_collection("async_persist")

    await collection.add(ids=["a"], embeddings=[[0.1, 0.2]])
    result = await collection.get(ids=["a"], include=["embeddings"])

    assert result["ids"] == ["a"]
    assert result["embeddings"][0] == pytest.approx([0.1, 0.2])

    client._system.stop()
    client.clear_system_cache()

    client2 = await chromadb.AsyncPersistentClient(path=tmp_path)
    collection2 = await client2.get_collection("async_persist")
    result2 = await collection2.get(ids=["a"], include=["embeddings"])

    assert result2["ids"] == ["a"]
    assert result2["embeddings"][0] == pytest.approx([0.1, 0.2])

    client2._system.stop()
    client2.clear_system_cache()

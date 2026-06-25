import asyncio
from typing import Any, Dict, List
from uuid import uuid4

from chromadb.api.models.AsyncCollection import AsyncCollection
from chromadb.api.models.Collection import Collection
from chromadb.api.types import ConditionalCommitResult, GetResult
from chromadb.types import Collection as CollectionModel


def _collection_model() -> CollectionModel:
    return CollectionModel(
        id=uuid4(),
        name="test",
        configuration_json={},
        serialized_schema=None,
        metadata=None,
        dimension=None,
        tenant="tenant",
        database="database",
    )


def _get_result(include: List[str]) -> GetResult:
    return GetResult(
        ids=["id-1"],
        embeddings=None,
        documents=["doc-1"],
        uris=None,
        data=None,
        metadatas=[{"k": "v"}],
        included=include,
    )


class FakeConditionalClient:
    def __init__(self) -> None:
        self.transaction = object()
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def _begin_conditional_transaction(self) -> object:
        self.calls.append(("begin", {}))
        return self.transaction

    def _conditional_get(self, **kwargs: Any) -> GetResult:
        self.calls.append(("get", kwargs))
        return _get_result(kwargs["include"])

    def _conditional_add(self, **kwargs: Any) -> bool:
        self.calls.append(("add", kwargs))
        return True

    def _conditional_update(self, **kwargs: Any) -> bool:
        self.calls.append(("update", kwargs))
        return True

    def _conditional_upsert(self, **kwargs: Any) -> bool:
        self.calls.append(("upsert", kwargs))
        return True

    def _conditional_delete(self, **kwargs: Any) -> bool:
        self.calls.append(("delete", kwargs))
        return True

    def _conditional_commit(self, **kwargs: Any) -> ConditionalCommitResult:
        self.calls.append(("commit", kwargs))
        return ConditionalCommitResult(
            first_inserted_record_offset=42,
            record_count=4,
        )


class AsyncFakeConditionalClient(FakeConditionalClient):
    async def _begin_conditional_transaction(self) -> object:
        return super()._begin_conditional_transaction()

    async def _conditional_get(self, **kwargs: Any) -> GetResult:
        return super()._conditional_get(**kwargs)

    async def _conditional_add(self, **kwargs: Any) -> bool:
        return super()._conditional_add(**kwargs)

    async def _conditional_update(self, **kwargs: Any) -> bool:
        return super()._conditional_update(**kwargs)

    async def _conditional_upsert(self, **kwargs: Any) -> bool:
        return super()._conditional_upsert(**kwargs)

    async def _conditional_delete(self, **kwargs: Any) -> bool:
        return super()._conditional_delete(**kwargs)

    async def _conditional_commit(self, **kwargs: Any) -> ConditionalCommitResult:
        return super()._conditional_commit(**kwargs)


def test_sync_conditional_transaction_routes_to_internal_hooks() -> None:
    client = FakeConditionalClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )

    txn = collection.conditional()
    got = txn.get(ids="id-1", include=["documents"])
    txn.add(ids="id-2", embeddings=[1.0])
    txn.update(ids="id-1", embeddings=[2.0])
    txn.upsert(ids="id-3", embeddings=[3.0])
    txn.delete(ids="id-1")
    committed = txn.commit()

    assert got["ids"] == ["id-1"]
    assert committed == {
        "first_inserted_record_offset": 42,
        "record_count": 4,
    }
    assert [name for name, _ in client.calls] == [
        "begin",
        "get",
        "add",
        "update",
        "upsert",
        "delete",
        "commit",
    ]
    assert client.calls[-1] == ("commit", {"transaction": client.transaction})


def test_async_conditional_transaction_routes_to_internal_hooks() -> None:
    async def run() -> None:
        client = AsyncFakeConditionalClient()
        collection = AsyncCollection(
            client=client,  # type: ignore[arg-type]
            model=_collection_model(),
            embedding_function=None,
        )

        txn = await collection.conditional()
        got = await txn.get(ids="id-1", include=["documents"])
        await txn.add(ids="id-2", embeddings=[1.0])
        committed = await txn.commit()

        assert got["ids"] == ["id-1"]
        assert committed == {
            "first_inserted_record_offset": 42,
            "record_count": 4,
        }
        assert [name for name, _ in client.calls] == [
            "begin",
            "get",
            "add",
            "commit",
        ]
        assert client.calls[-1] == ("commit", {"transaction": client.transaction})

    asyncio.run(run())

import asyncio
import os
from typing import Any, Dict, List, Union
from uuid import uuid4

import numpy as np
import pytest

from chromadb.api import ClientAPI
from chromadb.api.client import Client as ClientCreator
from chromadb.api.conditional_http import ConditionalHttpTransaction
from chromadb.api.models.AsyncCollection import AsyncCollection
from chromadb.api.models.Collection import Collection
from chromadb.api.types import ConditionalCommitResult, GetResult
from chromadb.config import System
from chromadb.errors import (
    BackoffError,
    ConditionalWriteConflictError,
    InvalidArgumentError,
    StaleReadError,
)
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
        self.transactions: List[object] = []
        self.calls: List[tuple[str, Dict[str, Any]]] = []
        self.get_outcomes: List[Union[GetResult, BaseException]] = []
        self.commit_outcomes: List[Union[ConditionalCommitResult, BaseException]] = []

    def _begin_conditional_transaction(self) -> object:
        self.transaction = object()
        self.transactions.append(self.transaction)
        self.calls.append(("begin", {}))
        return self.transaction

    def _conditional_get(self, **kwargs: Any) -> GetResult:
        self.calls.append(("get", kwargs))
        if self.get_outcomes:
            outcome = self.get_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
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
        if self.commit_outcomes:
            outcome = self.commit_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
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


@pytest.mark.skipif(
    os.getenv("CHROMA_RUST_BINDINGS_TEST_ONLY") != "1",
    reason="Rust bindings support check only runs in rust-bindings test mode",
)
def test_rust_bindings_conditional_transactions_require_grpc_log(
    client: ClientAPI,
) -> None:
    collection = client.create_collection(
        name=f"conditional_unsupported_{uuid4().hex}",
        embedding_function=None,
    )

    with pytest.raises(InvalidArgumentError, match="gRPC log implementation"):
        collection.conditional()


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


def test_embedded_conditional_transaction_reports_unsupported(
    sqlite: System,
) -> None:
    client = ClientCreator.from_system(sqlite)
    try:
        collection = client.create_collection(
            name=f"conditional-{uuid4()}",
            embedding_function=None,
        )

        with pytest.raises(NotImplementedError, match="Conditional transactions"):
            collection.conditional()
    finally:
        client.clear_system_cache()


def test_http_conditional_transaction_reuses_read_token_for_reads() -> None:
    transaction = ConditionalHttpTransaction()
    collection_id = uuid4()

    first_payload = transaction.prepare_get(
        collection_id,
        "tenant",
        "database",
        {
            "ids": ["id-1"],
            "where": None,
            "where_document": None,
            "limit": None,
            "offset": None,
            "include": ["documents"],
        },
    )
    assert first_payload["read_token"] is None

    transaction.record_get(first_payload, ["id-1"], 42)

    second_payload = transaction.prepare_get(
        collection_id,
        "tenant",
        "database",
        {
            "ids": ["id-2"],
            "where": None,
            "where_document": None,
            "limit": None,
            "offset": None,
            "include": ["documents"],
        },
    )
    assert second_payload["read_token"] == 42


def test_http_conditional_commit_sends_read_set_without_replay() -> None:
    transaction = ConditionalHttpTransaction()
    collection_id = uuid4()
    get_payload = transaction.prepare_get(
        collection_id,
        "tenant",
        "database",
        {
            "ids": ["present", "absent"],
            "where": None,
            "where_document": None,
            "limit": None,
            "offset": None,
            "include": ["documents"],
        },
    )
    transaction.record_get(get_payload, ["present"], 42)
    transaction.buffer_add(
        collection_id,
        "tenant",
        "database",
        ["absent"],
        [np.array([1.0], dtype=np.float32)],
        None,
        None,
        None,
    )

    payload = transaction.prepare_commit()
    assert payload is not None

    assert payload["read_token"] == 42
    assert payload["read_ids"] == ["absent", "present"]
    assert [operation["operation"] for operation in payload["operations"]] == ["add"]


def test_http_conditional_commit_sends_write_only_upsert_without_reads() -> None:
    transaction = ConditionalHttpTransaction()
    collection_id = uuid4()

    transaction.buffer_upsert(
        collection_id,
        "tenant",
        "database",
        ["unknown"],
        [np.array([1.0], dtype=np.float32)],
        None,
        None,
        None,
    )

    payload = transaction.prepare_commit()
    assert payload is not None
    assert payload["read_token"] is None
    assert payload["read_ids"] == []
    assert [operation["operation"] for operation in payload["operations"]] == ["upsert"]


def test_http_conditional_commit_accepts_numpy_embeddings() -> None:
    transaction = ConditionalHttpTransaction()
    collection_id = uuid4()

    transaction.buffer_upsert(
        collection_id,
        "tenant",
        "database",
        ["unknown"],
        [np.array([1.0], dtype=np.float32)],
        None,
        None,
        None,
    )

    payload = transaction.prepare_commit()
    assert payload is not None
    assert payload["operations"][0]["payload"]["embeddings"] == [[1.0]]


def test_http_conditional_commit_preserves_metadata_payload() -> None:
    transaction = ConditionalHttpTransaction()
    collection_id = uuid4()

    transaction.buffer_upsert(
        collection_id,
        "tenant",
        "database",
        ["unknown"],
        [np.array([1.0], dtype=np.float32)],
        [{"tag": "value", "deleted": None}],
        None,
        None,
    )

    payload = transaction.prepare_commit()
    assert payload is not None
    assert payload["operations"][0]["payload"]["metadatas"] == [
        {"tag": "value", "deleted": None}
    ]


def test_sync_conditional_run_does_not_retry_callback_errors() -> None:
    client = FakeConditionalClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    attempts = 0

    def callback(txn: object) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("callback failed")

    with pytest.raises(RuntimeError, match="callback failed"):
        collection.conditional().run(callback)

    assert attempts == 1
    assert [name for name, _ in client.calls] == ["begin"]


def test_sync_conditional_run_does_not_retry_user_raised_retryable_error() -> None:
    client = FakeConditionalClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    attempts = 0

    def callback(txn: object) -> None:
        nonlocal attempts
        attempts += 1
        raise ConditionalWriteConflictError("user-raised conflict")

    with pytest.raises(ConditionalWriteConflictError, match="user-raised conflict"):
        collection.conditional().run(callback)

    assert attempts == 1
    assert [name for name, _ in client.calls] == ["begin"]


def test_sync_conditional_run_retries_occ_conflict_with_fresh_transaction() -> None:
    client = FakeConditionalClient()
    client.commit_outcomes = [
        ConditionalWriteConflictError("conflict"),
        ConditionalCommitResult(
            first_inserted_record_offset=84,
            record_count=1,
        ),
    ]
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    attempt_transactions: List[object] = []

    def callback(txn: Any) -> str:
        attempt_transactions.append(txn._transaction)
        txn.add(ids=f"id-{len(attempt_transactions)}", embeddings=[1.0])
        return f"value-{len(attempt_transactions)}"

    result = collection.conditional().run(callback, max_retries=1)

    assert result == "value-2"
    assert len(attempt_transactions) == 2
    assert attempt_transactions == client.transactions
    assert attempt_transactions[0] is not attempt_transactions[1]
    assert [name for name, _ in client.calls] == [
        "begin",
        "add",
        "commit",
        "begin",
        "add",
        "commit",
    ]


def test_sync_conditional_run_retries_stale_read_with_fresh_transaction() -> None:
    client = FakeConditionalClient()
    client.get_outcomes = [
        StaleReadError("stale read"),
        _get_result(["documents"]),
    ]
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    attempt_transactions: List[object] = []

    def callback(txn: Any) -> str:
        attempt_transactions.append(txn._transaction)
        result = txn.get(ids="id-1", include=["documents"])
        return result["ids"][0]

    result = collection.conditional().run(callback, max_retries=1)

    assert result == "id-1"
    assert len(attempt_transactions) == 2
    assert attempt_transactions == client.transactions
    assert attempt_transactions[0] is not attempt_transactions[1]
    assert [name for name, _ in client.calls] == [
        "begin",
        "get",
        "begin",
        "get",
        "commit",
    ]


def test_sync_conditional_run_retries_backoff() -> None:
    client = FakeConditionalClient()
    client.commit_outcomes = [
        BackoffError("Backoff and retry"),
        ConditionalCommitResult(
            first_inserted_record_offset=84,
            record_count=1,
        ),
    ]
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    attempts = 0

    def callback(txn: Any) -> str:
        nonlocal attempts
        attempts += 1
        txn.add(ids=f"id-{attempts}", embeddings=[1.0])
        return f"attempt-{attempts}"

    result = collection.conditional().run(callback, max_retries=1)

    assert result == "attempt-2"
    assert attempts == 2
    assert [name for name, _ in client.calls] == [
        "begin",
        "add",
        "commit",
        "begin",
        "add",
        "commit",
    ]


def test_sync_conditional_run_treats_durable_contention_as_success() -> None:
    client = FakeConditionalClient()
    client.commit_outcomes = [
        ConditionalCommitResult(
            first_inserted_record_offset=None,
            record_count=1,
        )
    ]
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )
    callback_value = {"status": "committed"}
    attempts = 0

    def callback(txn: Any) -> Dict[str, str]:
        nonlocal attempts
        attempts += 1
        txn.add(ids="id-1", embeddings=[1.0])
        return callback_value

    result = collection.conditional().run(callback, max_retries=3)

    assert result is callback_value
    assert attempts == 1
    assert [name for name, _ in client.calls] == ["begin", "add", "commit"]


def test_sync_conditional_run_rejects_commit_inside_callback() -> None:
    client = FakeConditionalClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=_collection_model(),
        embedding_function=None,
    )

    def callback(txn: Any) -> None:
        txn.commit()

    with pytest.raises(ValueError, match="cannot be called inside run"):
        collection.conditional().run(callback)

    assert [name for name, _ in client.calls] == ["begin"]


def test_async_conditional_run_retries_occ_conflict_with_fresh_transaction() -> None:
    async def run() -> None:
        client = AsyncFakeConditionalClient()
        client.commit_outcomes = [
            ConditionalWriteConflictError("conflict"),
            ConditionalCommitResult(
                first_inserted_record_offset=84,
                record_count=1,
            ),
        ]
        collection = AsyncCollection(
            client=client,  # type: ignore[arg-type]
            model=_collection_model(),
            embedding_function=None,
        )
        attempt_transactions: List[object] = []

        async def callback(txn: Any) -> str:
            attempt_transactions.append(txn._transaction)
            await txn.add(ids=f"id-{len(attempt_transactions)}", embeddings=[1.0])
            return f"value-{len(attempt_transactions)}"

        result = await (await collection.conditional()).run(callback, max_retries=1)

        assert result == "value-2"
        assert len(attempt_transactions) == 2
        assert attempt_transactions == client.transactions
        assert attempt_transactions[0] is not attempt_transactions[1]
        assert [name for name, _ in client.calls] == [
            "begin",
            "add",
            "commit",
            "begin",
            "add",
            "commit",
        ]

    asyncio.run(run())


def test_async_conditional_run_rejects_commit_inside_callback() -> None:
    async def run() -> None:
        client = AsyncFakeConditionalClient()
        collection = AsyncCollection(
            client=client,  # type: ignore[arg-type]
            model=_collection_model(),
            embedding_function=None,
        )

        async def callback(txn: Any) -> None:
            await txn.commit()

        with pytest.raises(ValueError, match="cannot be called inside run"):
            await (await collection.conditional()).run(callback)

        assert [name for name, _ in client.calls] == ["begin"]

    asyncio.run(run())

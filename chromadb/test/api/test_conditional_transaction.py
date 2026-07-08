import asyncio
import os
from typing import Any, Dict, List, Optional, Union
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

    with pytest.raises(NotImplementedError, match="only supported.*HttpClient"):
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

    prepared = transaction.prepare_commit_payload()
    assert prepared is not None
    _, payload = prepared

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

    prepared = transaction.prepare_commit_payload()
    assert prepared is not None
    _, payload = prepared
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

    prepared = transaction.prepare_commit_payload()
    assert prepared is not None
    _, payload = prepared
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

    prepared = transaction.prepare_commit_payload()
    assert prepared is not None
    _, payload = prepared
    assert payload["operations"][0]["payload"]["metadatas"] == [
        {"tag": "value", "deleted": None}
    ]


@pytest.mark.skipif(
    os.getenv("CHROMA_RUST_BINDINGS_TEST_ONLY") != "1",
    reason="Rust bindings parity check only runs in rust-bindings test mode",
)
def test_http_conditional_transaction_matches_rust_fixed_corpus() -> None:
    import chromadb_rust_bindings

    collection_id = uuid4()
    present_ids: set[str] = set()

    for case in _conditional_transaction_fixed_corpus():
        py_txn = ConditionalHttpTransaction()
        rust_txn = chromadb_rust_bindings.ConditionalTransaction()

        for op in case["ops"]:
            op_ids = _op_ids(op)
            returned_ids = [id for id in op_ids if id in present_ids]
            py_read_token = _record_python_get(
                py_txn, collection_id, op_ids, returned_ids
            )
            rust_read_token = _record_rust_get(
                rust_txn, collection_id, op_ids, returned_ids
            )
            assert py_read_token == rust_read_token, case["label"]
            _buffer_python_op(py_txn, collection_id, op)
            _buffer_rust_op(rust_txn, collection_id, op)

        py_prepared = py_txn.prepare_commit_payload()
        rust_prepared = rust_txn.prepare_commit()
        if rust_prepared is None:
            assert py_prepared is None, case["label"]
        else:
            assert py_prepared is not None, case["label"]
            scope, py_payload = py_prepared
            assert scope.collection_id == str(collection_id)
            assert py_payload == rust_prepared.to_json(), case["label"]
            py_txn.close(7)
            rust_txn.finish_commit(7)

        _apply_case_to_present_ids(present_ids, case)


def _record_python_get(
    transaction: ConditionalHttpTransaction,
    collection_id: Any,
    ids: List[str],
    returned_ids: List[str],
) -> Optional[int]:
    payload = transaction.prepare_get(
        collection_id,
        "tenant",
        "database",
        _get_payload(ids),
    )
    transaction.record_get(payload, returned_ids, 42)
    return payload["read_token"]


def _record_rust_get(
    transaction: Any,
    collection_id: Any,
    ids: List[str],
    returned_ids: List[str],
) -> Optional[int]:
    read_token = transaction.prepare_get(
        str(collection_id),
        ids,
        None,
        None,
        None,
        None,
        ["documents", "metadatas"],
        "tenant",
        "database",
    )
    transaction.record_get_response(
        str(collection_id),
        ids,
        None,
        None,
        None,
        None,
        ["documents", "metadatas"],
        "tenant",
        "database",
        returned_ids,
        42,
    )
    return read_token


def _get_payload(ids: List[str]) -> Dict[str, Any]:
    return {
        "ids": ids,
        "where": None,
        "where_document": None,
        "limit": None,
        "offset": None,
        "include": ["documents", "metadatas"],
    }


def _buffer_python_op(
    transaction: ConditionalHttpTransaction,
    collection_id: Any,
    op: tuple[str, Any],
) -> None:
    kind, payload = op
    if kind == "add":
        transaction.buffer_add(
            collection_id,
            "tenant",
            "database",
            _record_ids(payload),
            _record_embeddings_py(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
        )
    elif kind == "update":
        transaction.buffer_update(
            collection_id,
            "tenant",
            "database",
            _record_ids(payload),
            _record_embeddings_py(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
        )
    elif kind == "upsert":
        transaction.buffer_upsert(
            collection_id,
            "tenant",
            "database",
            _record_ids(payload),
            _record_embeddings_py(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
        )
    elif kind == "delete":
        transaction.buffer_delete(collection_id, "tenant", "database", payload)
    else:
        raise AssertionError(f"unknown transaction op {kind}")


def _buffer_rust_op(transaction: Any, collection_id: Any, op: tuple[str, Any]) -> None:
    kind, payload = op
    if kind == "add":
        transaction.buffer_add(
            str(collection_id),
            _record_ids(payload),
            _record_embeddings_rust(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
            "tenant",
            "database",
        )
    elif kind == "update":
        transaction.buffer_update(
            str(collection_id),
            _record_ids(payload),
            _record_embeddings_rust(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
            "tenant",
            "database",
        )
    elif kind == "upsert":
        transaction.buffer_upsert(
            str(collection_id),
            _record_ids(payload),
            _record_embeddings_rust(payload),
            _record_metadatas(payload),
            _record_documents(payload),
            _record_uris(payload),
            "tenant",
            "database",
        )
    elif kind == "delete":
        transaction.buffer_delete(str(collection_id), payload, "tenant", "database")
    else:
        raise AssertionError(f"unknown transaction op {kind}")


def _conditional_transaction_fixed_corpus() -> List[Dict[str, Any]]:
    return [
        {"label": "empty", "ops": []},
        {"label": "add-single", "ops": [("add", _records(["a1"], 10, "add-single"))]},
        {
            "label": "add-multiple",
            "ops": [("add", _records(["a2", "a3", "a4"], 20, "add-multiple"))],
        },
        {
            "label": "update-single",
            "ops": [("update", _records(["a1"], 30, "update-single"))],
        },
        {
            "label": "update-multiple",
            "ops": [("update", _records(["a2", "a3"], 40, "update-multiple"))],
        },
        {
            "label": "upsert-absent-single",
            "ops": [("upsert", _records(["u1"], 50, "upsert-absent-single"))],
        },
        {
            "label": "upsert-absent-multiple",
            "ops": [("upsert", _records(["u2", "u3"], 60, "upsert-absent-multiple"))],
        },
        {
            "label": "upsert-present-single",
            "ops": [("upsert", _records(["u1"], 70, "upsert-present-single"))],
        },
        {
            "label": "upsert-present-multiple",
            "ops": [("upsert", _records(["a2", "u2"], 80, "upsert-present-multiple"))],
        },
        {"label": "delete-single", "ops": [("delete", ["a4"])]},
        {"label": "delete-multiple", "ops": [("delete", ["a1", "u3"])]},
        {
            "label": "multi-write",
            "ops": [
                ("add", _records(["m1", "m2"], 90, "multi-add")),
                ("update", _records(["a2"], 100, "multi-update")),
                ("upsert", _records(["u2", "m3"], 110, "multi-upsert")),
                ("delete", ["a3"]),
            ],
        },
    ]


def _record(id: str, seed: int, flavor: str) -> Dict[str, Any]:
    return {
        "id": id,
        "embedding": [float(seed), float(seed % 17), float(seed % 31)],
        "document": f"{flavor}-document-{id}-{seed}",
        "uri": f"urn:chroma-transaction-test:{flavor}:{id}:{seed}",
        "metadata": {
            "flavor": flavor,
            "seed": seed,
            "id": id,
        },
    }


def _records(ids: List[str], seed: int, flavor: str) -> List[Dict[str, Any]]:
    return [_record(id, seed + index, flavor) for index, id in enumerate(ids)]


def _op_ids(op: tuple[str, Any]) -> List[str]:
    kind, payload = op
    if kind == "delete":
        return payload
    return _record_ids(payload)


def _record_ids(records: List[Dict[str, Any]]) -> List[str]:
    return [record["id"] for record in records]


def _record_embeddings_py(records: List[Dict[str, Any]]) -> List[np.ndarray]:
    return [np.array(record["embedding"], dtype=np.float32) for record in records]


def _record_embeddings_rust(records: List[Dict[str, Any]]) -> List[List[float]]:
    return [record["embedding"] for record in records]


def _record_documents(records: List[Dict[str, Any]]) -> List[str]:
    return [record["document"] for record in records]


def _record_uris(records: List[Dict[str, Any]]) -> List[str]:
    return [record["uri"] for record in records]


def _record_metadatas(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [record["metadata"] for record in records]


def _apply_case_to_present_ids(present_ids: set[str], case: Dict[str, Any]) -> None:
    for kind, payload in case["ops"]:
        if kind in ("add", "upsert"):
            present_ids.update(_record_ids(payload))
        elif kind == "delete":
            for id in payload:
                present_ids.discard(id)


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

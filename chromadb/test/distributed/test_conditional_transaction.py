from uuid import uuid4

import pytest

from chromadb.api import ClientAPI
from chromadb.errors import ConditionalWriteConflictError
from chromadb.test.conftest import reset, skip_if_not_cluster


EMBEDDING = [1.0, 2.0, 3.0]


def _collection(client: ClientAPI, name: str = "conditional_txn"):
    reset(client)
    return client.create_collection(
        name=f"{name}_{uuid4().hex}",
        embedding_function=None,
    )


@skip_if_not_cluster()
def test_conditional_read_absent_add_success(client: ClientAPI) -> None:
    collection = _collection(client)

    txn = collection.conditional()
    result = txn.get(ids="new-id")
    assert result["ids"] == []

    txn.add(ids="new-id", embeddings=EMBEDDING, metadatas={"version": "created"})
    committed = txn.commit()

    assert committed["record_count"] == 1
    assert collection.get(ids="new-id", include=["metadatas"]) == {
        "ids": ["new-id"],
        "embeddings": None,
        "documents": None,
        "uris": None,
        "data": None,
        "metadatas": [{"version": "created"}],
        "included": ["metadatas"],
    }


@skip_if_not_cluster()
def test_conditional_concurrent_insert_after_absent_read_aborts(
    client: ClientAPI,
) -> None:
    collection = _collection(client)

    txn = collection.conditional()
    assert txn.get(ids="race-id")["ids"] == []

    collection.add(ids="race-id", embeddings=EMBEDDING)
    txn.add(ids="race-id", embeddings=EMBEDDING, metadatas={"owner": "txn"})

    with pytest.raises(ConditionalWriteConflictError):
        txn.commit()

    assert collection.get(ids="race-id", include=["metadatas"])["metadatas"] == [None]


@skip_if_not_cluster()
def test_conditional_read_present_update_success(client: ClientAPI) -> None:
    collection = _collection(client)
    collection.add(ids="present-id", embeddings=EMBEDDING, metadatas={"version": "old"})

    txn = collection.conditional()
    assert txn.get(ids="present-id", include=["metadatas"])["ids"] == ["present-id"]
    txn.update(ids="present-id", metadatas={"version": "new"})
    committed = txn.commit()

    assert committed["record_count"] == 1
    assert collection.get(ids="present-id", include=["metadatas"])["metadatas"] == [
        {"version": "new"}
    ]


@skip_if_not_cluster()
def test_conditional_read_present_delete_success(client: ClientAPI) -> None:
    collection = _collection(client)
    collection.add(ids="present-id", embeddings=EMBEDDING)

    txn = collection.conditional()
    assert txn.get(ids="present-id")["ids"] == ["present-id"]
    txn.delete(ids="present-id")
    committed = txn.commit()

    assert committed["record_count"] == 1
    assert collection.get(ids="present-id")["ids"] == []


@skip_if_not_cluster()
def test_conditional_concurrent_change_after_present_read_aborts(
    client: ClientAPI,
) -> None:
    collection = _collection(client)
    collection.add(ids="race-id", embeddings=EMBEDDING, metadatas={"version": "old"})

    txn = collection.conditional()
    assert txn.get(ids="race-id", include=["metadatas"])["metadatas"] == [
        {"version": "old"}
    ]

    collection.update(ids="race-id", metadatas={"version": "concurrent"})
    txn.update(ids="race-id", metadatas={"version": "txn"})

    with pytest.raises(ConditionalWriteConflictError):
        txn.commit()

    assert collection.get(ids="race-id", include=["metadatas"])["metadatas"] == [
        {"version": "concurrent"}
    ]


@skip_if_not_cluster()
def test_conditional_filter_get_with_limit_updates_only_returned_ids(
    client: ClientAPI,
) -> None:
    collection = _collection(client)
    ids = ["a", "b", "c", "d"]
    collection.add(
        ids=ids,
        embeddings=[EMBEDDING] * len(ids),
        metadatas=[{"group": "target"} for _ in ids],
    )

    txn = collection.conditional()
    read = txn.get(where={"group": "target"}, limit=2, include=["metadatas"])
    returned_ids = read["ids"]
    assert len(returned_ids) == 2

    txn.update(
        ids=returned_ids,
        metadatas=[{"group": "target", "status": "updated"} for _ in returned_ids],
    )
    committed = txn.commit()

    assert committed["record_count"] == len(returned_ids)
    all_records = collection.get(ids=ids, include=["metadatas"])
    metadatas_by_id = dict(zip(all_records["ids"], all_records["metadatas"]))
    for id in ids:
        if id in returned_ids:
            assert metadatas_by_id[id] == {"group": "target", "status": "updated"}
        else:
            assert metadatas_by_id[id] == {"group": "target"}


@skip_if_not_cluster()
def test_conditional_multi_update_commits_all_buffered_records(
    client: ClientAPI,
) -> None:
    collection = _collection(client)
    collection.add(
        ids=["left", "right"],
        embeddings=[EMBEDDING, EMBEDDING],
        metadatas=[
            {"side": "left", "version": "old"},
            {"side": "right", "version": "old"},
        ],
    )

    txn = collection.conditional()
    assert txn.get(ids=["left", "right"])["ids"] == ["left", "right"]
    txn.update(ids="left", metadatas={"side": "left", "version": "new"})
    txn.update(ids="right", metadatas={"side": "right", "version": "new"})
    committed = txn.commit()

    assert committed["record_count"] == 2
    assert committed["first_inserted_record_offset"] is not None
    assert collection.get(ids=["left", "right"], include=["metadatas"])[
        "metadatas"
    ] == [
        {"side": "left", "version": "new"},
        {"side": "right", "version": "new"},
    ]

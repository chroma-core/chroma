"""Tests for concurrent create_collection + delete_collection.

Regression tests for https://github.com/chroma-core/chroma/issues/7375
"""
import threading
from collections import Counter
from chromadb.api import ClientAPI


def test_concurrent_create_delete_same_name(client: ClientAPI) -> None:
    """Concurrent create + delete on same name should not both succeed."""
    client.reset()
    COLL = "race_test"

    results = Counter()

    def try_create():
        try:
            client.create_collection(COLL)
            results["create_ok"] += 1
        except Exception as e:
            results[f"create_{type(e).__name__}"] += 1

    def try_delete():
        try:
            client.delete_collection(COLL)
            results["delete_ok"] += 1
        except Exception as e:
            results[f"delete_{type(e).__name__}"] += 1

    race_issues = 0
    for _ in range(10):
        # Ensure collection doesn't exist before race
        try:
            client.delete_collection(COLL)
        except Exception:
            pass
        results.clear()

        t1 = threading.Thread(target=try_create)
        t2 = threading.Thread(target=try_delete)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Both should not succeed - that's a lost update
        if results.get("create_ok", 0) > 0 and results.get("delete_ok", 0) > 0:
            race_issues += 1

    assert race_issues == 0, (
        f"Found {race_issues}/10 race conditions where both create and delete succeeded. "
        f"This is a lost update bug."
    )


def test_create_delete_deterministic(client: ClientAPI) -> None:
    """After create + delete, collection should not exist."""
    client.reset()
    COLL = "test_deterministic"

    client.create_collection(COLL)
    client.delete_collection(COLL)

    # Collection should not exist
    collections = client.list_collections()
    assert COLL not in [c.name for c in collections], (
        f"Collection {COLL} should not exist after delete"
    )

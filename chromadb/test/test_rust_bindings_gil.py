import multiprocessing
from multiprocessing.synchronize import Event
from pathlib import Path
import sqlite3
import sys
import threading
import time
from typing import Callable

import chromadb_rust_bindings
import pytest

from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT


def _hold_sqlite_lock(path: str, locked: Event, release: Event) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        locked.set()
        # A separate process must eventually unlock even if Python's GIL is held.
        released_by_thread = release.wait(5)
        connection.rollback()
    sys.exit(0 if released_by_thread else 1)


@pytest.mark.parametrize(
    "operation",
    [
        "initialize",
        "create_database",
        "get_database",
        "delete_database",
        "list_databases",
        "create_tenant",
        "get_tenant",
        "count_collections",
        "list_collections",
        "create_collection",
        "get_collection",
        "get_collection_by_id",
        "update_collection",
        "delete_collection",
    ],
)
def test_sqlite_wait_releases_gil(tmp_path: Path, operation: str) -> None:
    db_path = str(tmp_path / "chroma.sqlite3")

    def initialize() -> chromadb_rust_bindings.Bindings:
        return chromadb_rust_bindings.Bindings(
            allow_reset=True,
            sqlite_db_config=chromadb_rust_bindings.SqliteDBConfig(
                url=db_path,
                hash_type=chromadb_rust_bindings.MigrationHash.MD5,
                migration_mode=chromadb_rust_bindings.MigrationMode.Apply,
            ),
            persist_path=str(tmp_path),
            hnsw_cache_size=16,
        )

    bindings = initialize()
    collection = bindings.create_collection("gil_test")
    bindings.create_database("test_database", DEFAULT_TENANT)
    operations: dict[str, Callable[[], object]] = {
        "initialize": initialize,
        "create_database": lambda: bindings.create_database(
            "new_database", DEFAULT_TENANT
        ),
        "get_database": lambda: bindings.get_database("test_database", DEFAULT_TENANT),
        "delete_database": lambda: bindings.delete_database(
            "test_database", DEFAULT_TENANT
        ),
        "list_databases": lambda: bindings.list_databases(tenant=DEFAULT_TENANT),
        "create_tenant": lambda: bindings.create_tenant("new_tenant"),
        "get_tenant": lambda: bindings.get_tenant(DEFAULT_TENANT),
        "count_collections": lambda: bindings.count_collections(
            DEFAULT_TENANT, DEFAULT_DATABASE
        ),
        "list_collections": bindings.list_collections,
        "create_collection": lambda: bindings.create_collection("new_collection"),
        "get_collection": lambda: bindings.get_collection(
            "gil_test", DEFAULT_TENANT, DEFAULT_DATABASE
        ),
        "get_collection_by_id": lambda: bindings.get_collection_by_id(
            str(collection.id)
        ),
        "update_collection": lambda: bindings.update_collection(
            str(collection.id), new_name="renamed_collection"
        ),
        "delete_collection": lambda: bindings.delete_collection(
            "gil_test", DEFAULT_TENANT, DEFAULT_DATABASE
        ),
    }
    if operation == "initialize":
        bindings.close()

    context = multiprocessing.get_context("spawn")
    locked, release = context.Event(), context.Event()
    holder = context.Process(target=_hold_sqlite_lock, args=(db_path, locked, release))
    holder.start()

    def release_from_python_thread() -> None:
        # Let the native call reach its SQLite wait before trying to run Python.
        time.sleep(0.2)
        release.set()

    thread = threading.Thread(target=release_from_python_thread)
    try:
        assert locked.wait(30), "SQLite lock holder did not start"
        thread.start()
        result = operations[operation]()
        assert (
            release.is_set()
        ), "Operation completed without waiting for the SQLite lock"
        if operation == "initialize":
            assert isinstance(result, chromadb_rust_bindings.Bindings)
            result.close()
        holder.join(5)
        assert (
            holder.exitcode == 0
        ), "SQLite wait prevented another Python thread from running"
    finally:
        release.set()
        if thread.ident is not None:
            thread.join(5)
        holder.join(5)
        if holder.is_alive():
            holder.terminate()
            holder.join(5)
        bindings.close()

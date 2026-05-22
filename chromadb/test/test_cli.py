import multiprocessing
import multiprocessing.context
import os
import sys
import time
from multiprocessing.synchronize import Event

import chromadb
from chromadb.api.client import Client
from chromadb.api.models.Collection import Collection
from chromadb.cli import cli
from chromadb.cli.cli import build_cli_args
from chromadb.config import System
import sqlite3
import numpy as np

from chromadb.test.property import invariants


def wait_for_server(
        host: str, port: int,
    max_retries: int = 5, initial_delay: float = 1.0
) -> bool:
    """Wait for server to be ready using exponential backoff.
    Args:
        client: ChromaDB client instance
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
    Returns:
        bool: True if server is ready, False if max retries exceeded
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            client = chromadb.HttpClient(host=host, port=port)
            heartbeat = client.heartbeat()
            if heartbeat > 0:
                return True
        except Exception:
            print("Heartbeat failed, trying again...")
            pass

        if attempt < max_retries - 1:
            time.sleep(delay)
            delay *= 2

    return False

def start_app(args: list[str]) -> None:
    sys.argv = args
    cli.app()

def test_app() -> None:
    kwargs = {"path": "chroma_test_data", "port": 8001}
    args = ["chroma", "run"]
    args.extend(build_cli_args(**kwargs))
    print(args)
    server_process = multiprocessing.Process(target=start_app, args=(args,))
    server_process.start()
    time.sleep(5)

    assert wait_for_server(host="localhost", port=8001), "Server failed to start within maximum retry attempts"

    server_process.terminate()
    server_process.join()


def test_vacuum(sqlite_persistent: System) -> None:
    system = sqlite_persistent

    # Add some data
    client = Client.from_system(system)
    collection1 = client.create_collection("collection1")
    collection2 = client.create_collection("collection2")

    def add_records(collection: Collection, num: int) -> None:
        ids = [str(i) for i in range(num)]
        embeddings = np.random.rand(num, 2)
        collection.add(ids=ids, embeddings=embeddings)

    add_records(collection1, 100)
    add_records(collection2, 2_000)
    collection1_count = collection1.count()
    collection2_count = collection2.count()

    sys.argv = ["chroma", "vacuum", "--path", system.settings.persist_directory, "--force"]
    cli.app()

    # Data must survive the vacuum run
    assert collection1.count() == collection1_count
    assert collection2.count() == collection2_count

    # Log should be clean
    invariants.log_size_below_max(system, [collection1, collection2], True)


def simulate_transactional_write(
    persist_directory: str, ready_event: Event, shutdown_event: Event
) -> None:
    db_path = os.path.join(persist_directory, "chroma.sqlite3")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE")
    conn.execute("SELECT 1")
    ready_event.set()
    shutdown_event.wait()
    conn.rollback()
    conn.close()


def test_vacuum_errors_if_locked(sqlite_persistent: System, capfd) -> None:
    """Vacuum command should fail with details if there is a long-lived lock on the database."""
    ctx = multiprocessing.get_context("spawn")
    ready_event = ctx.Event()
    shutdown_event = ctx.Event()
    process = ctx.Process(
        target=simulate_transactional_write,
        args=(sqlite_persistent.settings.persist_directory, ready_event, shutdown_event),
    )
    process.start()
    ready_event.wait()

    try:
        sys.argv = ["chroma", "vacuum", "--path", sqlite_persistent.settings.persist_directory, "--force", "--timeout", "10"]
        cli.app()
        captured = capfd.readouterr()
        assert "Failed to vacuum Chroma" in captured.err.strip()
    finally:
        shutdown_event.set()
        process.join()

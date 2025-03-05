import multiprocessing
import multiprocessing.context
import sys
import time
from multiprocessing.synchronize import Event

import chromadb
from chromadb.api.client import Client
from chromadb.api.models.Collection import Collection
from chromadb.cli.utils import set_log_file_path
from chromadb.config import Settings, System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.cli import cli
from pypika import Table
import numpy as np

from chromadb.test.property import invariants

def start_app() -> None:
    sys.argv = ["chroma", "run"]
    cli.main()

def test_app() -> None:
    server_process = multiprocessing.Process(target=start_app)
    server_process.start()
    time.sleep(5)
    client = chromadb.HttpClient()
    heartbeat = client.heartbeat()
    server_process.terminate()
    server_process.join()
    assert heartbeat > 0


def test_utils_set_log_file_path() -> None:
    log_config = set_log_file_path("chromadb/log_config.yml", "test.log")
    assert log_config["handlers"]["file"]["filename"] == "test.log"


def test_vacuum(sqlite_persistent: System) -> None:
    system = sqlite_persistent
    sqlite = system.instance(SqliteDB)

    # This is True because it's a fresh system, so let's set it to False to test that the vacuum command enables it
    config = sqlite.config
    config.set_parameter("automatically_purge", False)
    sqlite.set_config(config)

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

    # Maintenance log should be empty
    with sqlite.tx() as cur:
        t = Table("maintenance_log")
        q = sqlite.querybuilder().from_(t).select("*")
        sql, params = get_sql(q)
        cur.execute(sql, params)
        assert cur.fetchall() == []

    sys.argv = ["chroma", "vacuum", "--path", system.settings.persist_directory, "--force"]
    cli.main()

    # Maintenance log should have a vacuum entry
    with sqlite.tx() as cur:
        t = Table("maintenance_log")
        q = sqlite.querybuilder().from_(t).select("*")
        sql, params = get_sql(q)
        cur.execute(sql, params)
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][2] == "vacuum"

    # Automatic pruning should have been enabled
    if hasattr(sqlite, "config"):
        del (
            sqlite.config
        )  # the CLI will end up starting a new instance of sqlite, so we need to force-refresh the cached config here
    assert sqlite.config.get_parameter("automatically_purge").value

    # Log should be clean
    invariants.log_size_below_max(system, [collection1, collection2], True)


def simulate_transactional_write(
    settings: Settings, ready_event: Event, shutdown_event: Event
) -> None:
    system = System(settings=settings)
    system.start()
    sqlite = system.instance(SqliteDB)

    with sqlite.tx() as cur:
        cur.execute("INSERT INTO tenants DEFAULT VALUES")
        ready_event.set()
        shutdown_event.wait()

    system.stop()

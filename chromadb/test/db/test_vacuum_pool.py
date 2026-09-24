import threading

from chromadb.config import System
from chromadb.db.impl.sqlite import SqliteDB


def test_vacuum_returns_connection_to_pool(sqlite: System) -> None:
    sysdb = sqlite.instance(SqliteDB)
    sysdb.vacuum()

    # If vacuum() keeps the pool connection acquired, the next acquire from
    # another thread blocks forever. Use a short-lived reader thread and assert
    # it completes.
    done = threading.Event()

    def reader() -> None:
        sysdb.get_tenant("default_tenant")
        done.set()

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    assert done.wait(timeout=5), (
        "acquire from another thread hung after vacuum(): connection was not returned to the pool"
    )

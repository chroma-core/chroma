# create a sqlite database file
# create a thread pool of 40 threads
# create a pool with lru 1-2 seconds
import os
import tempfile
from threading import Thread
from time import sleep

from chromadb.db.impl.sqlite_pool import PerThreadPool


def test_per_thread_pool_lru() -> None:
    """Test that we can create a large number of collections and that the system
    # remains responsive."""

    def connect(pool: PerThreadPool) -> None:
        conn = pool.connect()
        print(conn)
        pool.return_to_pool(conn)
        print("returned")

    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = os.path.join(temp_dir, "test.db")
        pool = PerThreadPool(db_file, is_uri=True, connection_lru_time_seconds=1)
        t1 = Thread(target=connect, args=(pool,))
        t1.start()
        t1.join()
        sleep(2)  # Wait for LRU to expire
        t2 = Thread(target=connect, args=(pool,))
        t2.start()
        t2.join()
        # Check that the pool only has the t2 connection, t1 should have been closed and removed due to expired LRU
        assert len(pool._connections) == 1
